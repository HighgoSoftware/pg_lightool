/*-------------------------------------------------------------------------
 *
 * xlogparser.c: Parse WAL files.
 *
 *-------------------------------------------------------------------------
 */
#include "pg_lightool.h"
#include "common/fe_memutils.h"
#include "catalog/pg_control.h"
#include "access/xlogreader.h"
#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "access/xlogrecord.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"



#define MAX_ERRORMSG_LEN 1000
#define InvalidRepOriginId 0
#define pageSetLSN(page, lsn) \
	PageXLogRecPtrSet(((PageHeader) (page))->pd_lsn, lsn);

typedef struct itemIdSortData
{
	uint16		offsetindex;	/* linp array index */
	int16		itemoff;		/* page offset of item data */
	uint16		alignedlen;		/* MAXALIGN(item data len) */
} itemIdSortData;
typedef itemIdSortData *itemIdSort;



static bool allocate_recordbuf(XLogReaderState *state, uint32 reclength);
static void XLogParserXLogRead(const char *directory, TimeLineID timeline_id,
				 XLogRecPtr startptr, char *buf, Size count);
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen);
static int XLogParserReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 XLogRecPtr targetPtr, char *readBuff, TimeLineID *curFileTLI);
static bool XLogReaderValidatePageHeader_fe(XLogReaderState *state, XLogRecPtr recptr, char *phdr);
static void ResetDecoder(XLogReaderState *state);
static bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr);
static bool DecodeXLogRecord_fe(XLogReaderState *state, XLogRecord *record, char **errormsg);
static bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
					  XLogRecPtr PrevRecPtr, XLogRecord *record,
					  bool randAccess);
static bool xLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum);
static void recoverDeleteRecord(XLogReaderState *record, BlockNumber blknum);
static void recoverImageFromRecord(XLogReaderState *record, BlockNumber blknum,int pageIndex,int block_id);
static void fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2);
static void recoverUpdateRecord(XLogReaderState *record, BlockNumber blknum, bool hot_update);
static Page pageInit(Size specialSize);
static void recoverConfirmRecord(XLogReaderState *record, BlockNumber blknum);
static void recoverLockRecord(XLogReaderState *record, BlockNumber blknum);
static void recoverInplaceRecord(XLogReaderState *record, BlockNumber blknum);
static void heep2_recoverMutiinsertRecord(XLogReaderState *record, BlockNumber blknum);
static void heep2_recoverCleanRecord(XLogReaderState *record, BlockNumber blknum);		
static void heep2_recoverFreezePageRecord(XLogReaderState *record, BlockNumber blknum);				
static void heep2_recoverVisibleRecord(XLogReaderState *record, BlockNumber blknum);	
static void heep2_recoverLockRecord(XLogReaderState *record, BlockNumber blknum);
static void recoverInsertRecord(XLogReaderState *record, BlockNumber blknum);
static Page getTargetPage(int *tarIndex, bool *findInArray,BlockNumber blknum);
static OffsetNumber pageAddItemExtended(Page page,Item item,Size size,OffsetNumber offsetNumber);
static char *xLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len);
static void heapPagePruneExecute(Page page,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused);
static void heapExecuteFreezeTuple(HeapTupleHeader tuple, xl_heap_freeze_tuple *frz);
static void pageRepairFragmentation(Page page);
static bool transactionIdPrecedes(TransactionId id1, TransactionId id2);
static int32 pglz_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize);
static void compactifyTuples(itemIdSort itemidbase, int nitems, Page page);
static int itemoffcompare(const void *itemidp1, const void *itemidp2);

void
xLogReaderFree(XLogReaderState *state)
{
	int			block_id;

	for (block_id = 0; block_id <= XLR_MAX_BLOCK_ID; block_id++)
	{
		if (state->blocks[block_id].data)
			pfree(state->blocks[block_id].data);
	}
	if (state->main_data)
		pfree(state->main_data);

	pfree(state->errormsg_buf);
	if (state->readRecordBuf)
		pfree(state->readRecordBuf);
	pfree(state->readBuf);
	pfree(state);
}


static int
itemoffcompare(const void *itemidp1, const void *itemidp2)
{
	/* Sort in decreasing itemoff order */
	return ((itemIdSort) itemidp2)->itemoff -
		((itemIdSort) itemidp1)->itemoff;
}

static void
compactifyTuples(itemIdSort itemidbase, int nitems, Page page)
{
	PageHeader	phdr = (PageHeader) page;
	Offset		upper;
	int			i;

	/* sort itemIdSortData array into decreasing itemoff order */
	qsort((char *) itemidbase, nitems, sizeof(itemIdSortData),
		  itemoffcompare);

	upper = phdr->pd_special;
	for (i = 0; i < nitems; i++)
	{
		itemIdSort	itemidptr = &itemidbase[i];
		ItemId		lp;

		lp = PageGetItemId(page, itemidptr->offsetindex + 1);
		upper -= itemidptr->alignedlen;
		memmove((char *) page + upper,
				(char *) page + itemidptr->itemoff,
				itemidptr->alignedlen);
		lp->lp_off = upper;
	}

	phdr->pd_upper = upper;
}


static int32
pglz_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize)
{
	const unsigned char *sp;
	const unsigned char *srcend;
	unsigned char *dp;
	unsigned char *destend;

	sp = (const unsigned char *) source;
	srcend = ((const unsigned char *) source) + slen;
	dp = (unsigned char *) dest;
	destend = dp + rawsize;

	while (sp < srcend && dp < destend)
	{
		/*
		 * Read one control byte and process the next 8 items (or as many as
		 * remain in the compressed input).
		 */
		unsigned char ctrl = *sp++;
		int			ctrlc;

		for (ctrlc = 0; ctrlc < 8 && sp < srcend; ctrlc++)
		{
			if (ctrl & 1)
			{
				/*
				 * Otherwise it contains the match length minus 3 and the
				 * upper 4 bits of the offset. The next following byte
				 * contains the lower 8 bits of the offset. If the length is
				 * coded as 18, another extension tag byte tells how much
				 * longer the match really was (0-255).
				 */
				int32		len;
				int32		off;

				len = (sp[0] & 0x0f) + 3;
				off = ((sp[0] & 0xf0) << 4) | sp[1];
				sp += 2;
				if (len == 18)
					len += *sp++;

				/*
				 * Check for output buffer overrun, to ensure we don't clobber
				 * memory in case of corrupt input.  Note: we must advance dp
				 * here to ensure the error is detected below the loop.  We
				 * don't simply put the elog inside the loop since that will
				 * probably interfere with optimization.
				 */
				if (dp + len > destend)
				{
					dp += len;
					break;
				}

				/*
				 * Now we copy the bytes specified by the tag from OUTPUT to
				 * OUTPUT. It is dangerous and platform dependent to use
				 * memcpy() here, because the copied areas could overlap
				 * extremely!
				 */
				while (len--)
				{
					*dp = dp[-off];
					dp++;
				}
			}
			else
			{
				/*
				 * An unset control bit means LITERAL BYTE. So we just copy
				 * one from INPUT to OUTPUT.
				 */
				if (dp >= destend)	/* check for buffer overrun */
					break;		/* do not clobber memory */

				*dp++ = *sp++;
			}

			/*
			 * Advance the control bit
			 */
			ctrl >>= 1;
		}
	}

	/*
	 * Check we decompressed the right amount.
	 */
	if (dp != destend || sp != srcend)
		return -1;

	/*
	 * That's it.
	 */
	return rawsize;
}


static bool
transactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.  If both are normal, do a modulo-2^32 comparison.
	 */
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32) (id1 - id2);
	return (diff < 0);
}


static void
fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2)
{
	*infomask &= ~(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY |
				   HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
	*infomask2 &= ~HEAP_KEYS_UPDATED;

	if (infobits & XLHL_XMAX_IS_MULTI)
		*infomask |= HEAP_XMAX_IS_MULTI;
	if (infobits & XLHL_XMAX_LOCK_ONLY)
		*infomask |= HEAP_XMAX_LOCK_ONLY;
	if (infobits & XLHL_XMAX_EXCL_LOCK)
		*infomask |= HEAP_XMAX_EXCL_LOCK;
	/* note HEAP_XMAX_SHR_LOCK isn't considered here */
	if (infobits & XLHL_XMAX_KEYSHR_LOCK)
		*infomask |= HEAP_XMAX_KEYSHR_LOCK;

	if (infobits & XLHL_KEYS_UPDATED)
		*infomask2 |= HEAP_KEYS_UPDATED;
}


XLogReaderState *
XLogReaderAllocate_fe(XLogPageReadCB pagereadfunc, void *private_data)
{
	XLogReaderState *state;

	state = (XLogReaderState *)
		palloc_extended(sizeof(XLogReaderState),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!state)
		return NULL;

	state->max_block_id = -1;

	/*
	 * Permanently allocate readBuf.  We do it this way, rather than just
	 * making a static array, for two reasons: (1) no need to waste the
	 * storage in most instantiations of the backend; (2) a static char array
	 * isn't guaranteed to have any particular alignment, whereas
	 * palloc_extended() will provide MAXALIGN'd storage.
	 */
	state->readBuf = (char *) palloc_extended(XLOG_BLCKSZ,
											  MCXT_ALLOC_NO_OOM);
	if (!state->readBuf)
	{
		pfree(state);
		return NULL;
	}

	state->read_page = pagereadfunc;
	/* system_identifier initialized to zeroes above */
	state->private_data = private_data;
	/* ReadRecPtr and EndRecPtr initialized to zeroes above */
	/* readSegNo, readOff, readLen, readPageTLI initialized to zeroes above */
	state->errormsg_buf = palloc_extended(MAX_ERRORMSG_LEN + 1,
										  MCXT_ALLOC_NO_OOM);
	if (!state->errormsg_buf)
	{
		pfree(state->readBuf);
		pfree(state);
		return NULL;
	}
	state->errormsg_buf[0] = '\0';

	/*
	 * Allocate an initial readRecordBuf of minimal size, which can later be
	 * enlarged if necessary.
	 */
	if (!allocate_recordbuf(state, 0))
	{
		pfree(state->errormsg_buf);
		pfree(state->readBuf);
		pfree(state);
		return NULL;
	}

	return state;
}

static bool
allocate_recordbuf(XLogReaderState *state, uint32 reclength)
{
	uint32		newSize = reclength;

	newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
	newSize = Max(newSize, 5 * Max(BLCKSZ, XLOG_BLCKSZ));

	if (state->readRecordBuf)
		pfree(state->readRecordBuf);
	state->readRecordBuf =
		(char *) palloc_extended(newSize, MCXT_ALLOC_NO_OOM);
	if (state->readRecordBuf == NULL)
	{
		state->readRecordBufSize = 0;
		return false;
	}
	state->readRecordBufSize = newSize;
	return true;
}

XLogRecPtr
XLogFindNextRecord_fe(XLogReaderState *state, XLogRecPtr RecPtr)
{
	XLogReaderState saved_state = *state;
	XLogRecPtr	tmpRecPtr;
	XLogRecPtr	found = InvalidXLogRecPtr;
	XLogPageHeader header;
	char	   *errormsg;

	Assert(!XLogRecPtrIsInvalid(RecPtr));

	/*
	 * skip over potential continuation data, keeping in mind that it may span
	 * multiple pages
	 */
	tmpRecPtr = RecPtr;
	while (true)
	{
		XLogRecPtr	targetPagePtr;
		int			targetRecOff;
		uint32		pageHeaderSize;
		int			readLen;

		/*
		 * Compute targetRecOff. It should typically be equal or greater than
		 * short page-header since a valid record can't start anywhere before
		 * that, except when caller has explicitly specified the offset that
		 * falls somewhere there or when we are skipping multi-page
		 * continuation record. It doesn't matter though because
		 * ReadPageInternal() is prepared to handle that and will read at
		 * least short page-header worth of data
		 */
		targetRecOff = tmpRecPtr % XLOG_BLCKSZ;

		/* scroll back to page boundary */
		targetPagePtr = tmpRecPtr - targetRecOff;

		/* Read the page containing the record */
		readLen = ReadPageInternal(state, targetPagePtr, targetRecOff);
		if (readLen < 0)
			goto err;

		header = (XLogPageHeader) state->readBuf;

		pageHeaderSize = XLogPageHeaderSize(header);

		/* make sure we have enough data for the page header */
		readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize);
		if (readLen < 0)
			goto err;

		/* skip over potential continuation data */
		if (header->xlp_info & XLP_FIRST_IS_CONTRECORD)
		{
			/*
			 * If the length of the remaining continuation data is more than
			 * what can fit in this page, the continuation record crosses over
			 * this page. Read the next page and try again. xlp_rem_len in the
			 * next page header will contain the remaining length of the
			 * continuation data
			 *
			 * Note that record headers are MAXALIGN'ed
			 */
			if (MAXALIGN(header->xlp_rem_len) > (XLOG_BLCKSZ - pageHeaderSize))
				tmpRecPtr = targetPagePtr + XLOG_BLCKSZ;
			else
			{
				/*
				 * The previous continuation record ends in this page. Set
				 * tmpRecPtr to point to the first valid record
				 */
				tmpRecPtr = targetPagePtr + pageHeaderSize
					+ MAXALIGN(header->xlp_rem_len);
				break;
			}
		}
		else
		{
			tmpRecPtr = targetPagePtr + pageHeaderSize;
			break;
		}
	}

	/*
	 * we know now that tmpRecPtr is an address pointing to a valid XLogRecord
	 * because either we're at the first record after the beginning of a page
	 * or we just jumped over the remaining data of a continuation.
	 */
	while (XLogParserReadRecord(state, tmpRecPtr, &errormsg) != NULL)
	{
		/* continue after the record */
		tmpRecPtr = InvalidXLogRecPtr;

		/* past the record we've found, break out */
		if (RecPtr <= state->ReadRecPtr)
		{
			found = state->ReadRecPtr;
			goto out;
		}
	}

err:
out:
	/* Reset state to what we had before finding the record */
	state->ReadRecPtr = saved_state.ReadRecPtr;
	state->EndRecPtr = saved_state.EndRecPtr;
	XLogReaderInvalReadState(state);

	return found;
}

static int
ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen)
{
	int			readLen;
	uint32		targetPageOff;
	XLogSegNo	targetSegNo;
	XLogPageHeader hdr;

	Assert((pageptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(pageptr, targetSegNo);
	targetPageOff = (pageptr % XLogSegSize);

	/* check whether we have all the requested data already */
	if (targetSegNo == state->readSegNo && targetPageOff == state->readOff &&
		reqLen < state->readLen)
		return state->readLen;

	/*
	 * Data is not in our buffer.
	 *
	 * Every time we actually read the page, even if we looked at parts of it
	 * before, we need to do verification as the read_page callback might now
	 * be rereading data from a different source.
	 *
	 * Whenever switching to a new WAL segment, we read the first page of the
	 * file and validate its header, even if that's not where the target
	 * record is.  This is so that we can check the additional identification
	 * info that is present in the first page's "long" header.
	 */
	if (targetSegNo != state->readSegNo && targetPageOff != 0)
	{
		XLogRecPtr	targetSegmentPtr = pageptr - targetPageOff;

		readLen = state->read_page(state, targetSegmentPtr, XLOG_BLCKSZ,
								   state->currRecPtr,
								   state->readBuf, &state->readPageTLI);
		if (readLen < 0)
			goto err;

		/* we can be sure to have enough WAL available, we scrolled back */
		Assert(readLen == XLOG_BLCKSZ);

		if (!XLogReaderValidatePageHeader_fe(state, targetSegmentPtr,
										  state->readBuf))
			goto err;
	}

	/*
	 * First, read the requested data length, but at least a short page header
	 * so that we can validate it.
	 */
	readLen = state->read_page(state, pageptr, Max(reqLen, SizeOfXLogShortPHD),
							   state->currRecPtr,
							   state->readBuf, &state->readPageTLI);
	if (readLen < 0)
		goto err;

	Assert(readLen <= XLOG_BLCKSZ);

	/* Do we have enough data to check the header length? */
	if (readLen <= SizeOfXLogShortPHD)
		goto err;

	Assert(readLen >= reqLen);

	hdr = (XLogPageHeader) state->readBuf;

	/* still not enough */
	if (readLen < XLogPageHeaderSize(hdr))
	{
		readLen = state->read_page(state, pageptr, XLogPageHeaderSize(hdr),
								   state->currRecPtr,
								   state->readBuf, &state->readPageTLI);
		if (readLen < 0)
			goto err;
	}

	/*
	 * Now that we know we have the full header, validate it.
	 */
	if (!XLogReaderValidatePageHeader_fe(state, pageptr, (char *) hdr))
		goto err;

	/* update read state information */
	state->readSegNo = targetSegNo;
	state->readOff = targetPageOff;
	state->readLen = readLen;

	return readLen;

err:
	XLogReaderInvalReadState(state);
	return -1;
}

static bool
XLogReaderValidatePageHeader_fe(XLogReaderState *state, XLogRecPtr recptr,
							 char *phdr)
{
	XLogRecPtr	recaddr;
	XLogSegNo	segno;
	int32		offset;
	XLogPageHeader hdr = (XLogPageHeader) phdr;

	Assert((recptr % XLOG_BLCKSZ) == 0);

	XLByteToSeg(recptr, segno);
	offset = recptr % XLogSegSize;

	XLogSegNoOffsetToRecPtr(segno, offset, recaddr);

	if (hdr->xlp_magic != XLOG_PAGE_MAGIC)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		br_elog("invalid magic number %04X in log segment %s, offset %u",
							  hdr->xlp_magic,
							  fname,
							  offset);
		return false;
	}

	if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		br_elog("invalid info bits %04X in log segment %s, offset %u",
							  hdr->xlp_info,
							  fname,
							  offset);
		return false;
	}

	if (hdr->xlp_info & XLP_LONG_HEADER)
	{
		XLogLongPageHeader longhdr = (XLogLongPageHeader) hdr;

		if (brc.system_identifier &&
			longhdr->xlp_sysid != brc.system_identifier)
		{
			char		fhdrident_str[32];
			char		sysident_str[32];

			/*
			 * Format sysids separately to keep platform-dependent format code
			 * out of the translatable message string.
			 */
			snprintf(fhdrident_str, sizeof(fhdrident_str), UINT64_FORMAT,
					 longhdr->xlp_sysid);
			snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
					 state->system_identifier);
			br_elog("WAL file is from different database system: WAL file database system identifier is %s, pg_control database system identifier is %s",
								  fhdrident_str, sysident_str);
			return false;
		}
		else if (longhdr->xlp_seg_size != XLogSegSize)
		{
			br_elog("WAL file is from different database system: incorrect XLOG_SEG_SIZE in page header");
			return false;
		}
		else if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
		{
			br_elog("WAL file is from different database system: incorrect XLOG_BLCKSZ in page header");
			return false;
		}
	}
	else if (offset == 0)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		/* hmm, first page of file doesn't have a long header? */
		br_elog("invalid info bits %04X in log segment %s, offset %u",
							  hdr->xlp_info,
							  fname,
							  offset);
		return false;
	}

	/*
	 * Check that the address on the page agrees with what we expected.
	 * This check typically fails when an old WAL segment is recycled,
	 * and hasn't yet been overwritten with new data yet.
	 */
	if (hdr->xlp_pageaddr != recaddr)
	{
		char		fname[MAXFNAMELEN];

		XLogFileName(fname, state->readPageTLI, segno);

		br_elog("unexpected pageaddr %X/%X in log segment %s, offset %u",
							  (uint32) (hdr->xlp_pageaddr >> 32), (uint32) hdr->xlp_pageaddr,
							  fname,
							  offset);
		return false;
	}

	/*
	 * Since child timelines are always assigned a TLI greater than their
	 * immediate parent's TLI, we should never see TLI go backwards across
	 * successive pages of a consistent WAL sequence.
	 *
	 * Sometimes we re-read a segment that's already been (partially) read. So
	 * we only verify TLIs for pages that are later than the last remembered
	 * LSN.
	 */
	if (recptr > state->latestPagePtr)
	{
		if (hdr->xlp_tli < state->latestPageTLI)
		{
			char		fname[MAXFNAMELEN];

			XLogFileName(fname, state->readPageTLI, segno);

			br_elog("out-of-sequence timeline ID %u (after %u) in log segment %s, offset %u",
								  hdr->xlp_tli,
								  state->latestPageTLI,
								  fname,
								  offset);
			return false;
		}
	}
	state->latestPagePtr = recptr;
	state->latestPageTLI = hdr->xlp_tli;

	return true;
}

void
XLogReaderInvalReadState(XLogReaderState *state)
{
	state->readSegNo = 0;
	state->readOff = 0;
	state->readLen = 0;
}

static bool
ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
					  XLogRecPtr PrevRecPtr, XLogRecord *record,
					  bool randAccess)
{
	if (record->xl_tot_len < SizeOfXLogRecord)
	{
		if(0 == record->xl_tot_len)
			br_elog("LOG:invalid record length at %X/%X: wanted %u, got %u, maybe arrive end",
							  (uint32) (RecPtr >> 32), (uint32) RecPtr,
							  (uint32) SizeOfXLogRecord, record->xl_tot_len);
		else
			br_elog("invalid record length at %X/%X: wanted %u, got %u",
							  (uint32) (RecPtr >> 32), (uint32) RecPtr,
							  (uint32) SizeOfXLogRecord, record->xl_tot_len);
		return false;
	}
	if (record->xl_rmid > RM_MAX_ID)
	{
		br_elog( "invalid resource manager ID %u at %X/%X",
							  record->xl_rmid, (uint32) (RecPtr >> 32),
							  (uint32) RecPtr);
		return false;
	}
	if (randAccess)
	{
		/*
		 * We can't exactly verify the prev-link, but surely it should be less
		 * than the record's own address.
		 */
		if (!(record->xl_prev < RecPtr))
		{
			br_elog("record with incorrect prev-link %X/%X at %X/%X",
								  (uint32) (record->xl_prev >> 32),
								  (uint32) record->xl_prev,
								  (uint32) (RecPtr >> 32), (uint32) RecPtr);
			return false;
		}
	}
	else
	{
		/*
		 * Record's prev-link should exactly match our previous location. This
		 * check guards against torn WAL pages where a stale but valid-looking
		 * WAL record starts on a sector boundary.
		 */
		if (record->xl_prev != PrevRecPtr)
		{
			br_elog("record with incorrect prev-link %X/%X at %X/%X",
								  (uint32) (record->xl_prev >> 32),
								  (uint32) record->xl_prev,
								  (uint32) (RecPtr >> 32), (uint32) RecPtr);
			return false;
		}
	}

	return true;
}


static bool
ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr)
{
	pg_crc32c	crc;

	/* Calculate the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
	/* include the record header last */
	COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(record->xl_crc, crc))
	{
		br_elog("incorrect resource manager data checksum in record at %X/%X",
							  (uint32) (recptr >> 32), (uint32) recptr);
		return false;
	}

	return true;
}


static void
ResetDecoder(XLogReaderState *state)
{
	int			block_id;

	state->decoded_record = NULL;

	state->main_data_len = 0;

	for (block_id = 0; block_id <= state->max_block_id; block_id++)
	{
		state->blocks[block_id].in_use = false;
		state->blocks[block_id].has_image = false;
		state->blocks[block_id].has_data = false;
		state->blocks[block_id].apply_image = false;
	}
	state->max_block_id = -1;
}

static bool
DecodeXLogRecord_fe(XLogReaderState *state, XLogRecord *record, char **errormsg)
{
	/*
	 * read next _size bytes from record buffer, but check for overrun first.
	 */
#define COPY_HEADER_FIELD(_dst, _size)			\
	do {										\
		if (remaining < _size)					\
			goto shortdata_err;					\
		memcpy(_dst, ptr, _size);				\
		ptr += _size;							\
		remaining -= _size;						\
	} while(0)

	char	   *ptr;
	uint32		remaining;
	uint32		datatotal;
	RelFileNode *rnode = NULL;
	uint8		block_id;

	ResetDecoder(state);

	state->decoded_record = record;
	state->record_origin = InvalidRepOriginId;

	ptr = (char *) record;
	ptr += SizeOfXLogRecord;
	remaining = record->xl_tot_len - SizeOfXLogRecord;

	/* Decode the headers */
	datatotal = 0;
	while (remaining > datatotal)
	{
		COPY_HEADER_FIELD(&block_id, sizeof(uint8));

		if (block_id == XLR_BLOCK_ID_DATA_SHORT)
		{
			/* XLogRecordDataHeaderShort */
			uint8		main_data_len;

			COPY_HEADER_FIELD(&main_data_len, sizeof(uint8));

			state->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;				/* by convention, the main data fragment is
								 * always last */
		}
		else if (block_id == XLR_BLOCK_ID_DATA_LONG)
		{
			/* XLogRecordDataHeaderLong */
			uint32		main_data_len;

			COPY_HEADER_FIELD(&main_data_len, sizeof(uint32));
			state->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;				/* by convention, the main data fragment is
								 * always last */
		}
		else if (block_id == XLR_BLOCK_ID_ORIGIN)
		{
			COPY_HEADER_FIELD(&state->record_origin, sizeof(RepOriginId));
		}
		else if (block_id <= XLR_MAX_BLOCK_ID)
		{
			/* XLogRecordBlockHeader */
			DecodedBkpBlock *blk;
			uint8		fork_flags;

			if (block_id <= state->max_block_id)
			{
				br_elog("out-of-order block_id %u at %X/%X",
									  block_id,
									  (uint32) (state->ReadRecPtr >> 32),
									  (uint32) state->ReadRecPtr);
				goto err;
			}
			state->max_block_id = block_id;

			blk = &state->blocks[block_id];
			blk->in_use = true;
			blk->apply_image = false;

			COPY_HEADER_FIELD(&fork_flags, sizeof(uint8));
			blk->forknum = fork_flags & BKPBLOCK_FORK_MASK;
			blk->flags = fork_flags;
			blk->has_image = ((fork_flags & BKPBLOCK_HAS_IMAGE) != 0);
			blk->has_data = ((fork_flags & BKPBLOCK_HAS_DATA) != 0);

			COPY_HEADER_FIELD(&blk->data_len, sizeof(uint16));
			/* cross-check that the HAS_DATA flag is set iff data_length > 0 */
			if (blk->has_data && blk->data_len == 0)
			{
				br_elog("BKPBLOCK_HAS_DATA set, but no data included at %X/%X",
									  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
				goto err;
			}
			if (!blk->has_data && blk->data_len != 0)
			{
				br_elog( "BKPBLOCK_HAS_DATA not set, but data length is %u at %X/%X",
									  (unsigned int) blk->data_len,
									  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
				goto err;
			}
			datatotal += blk->data_len;

			if (blk->has_image)
			{
				COPY_HEADER_FIELD(&blk->bimg_len, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->hole_offset, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->bimg_info, sizeof(uint8));

				blk->apply_image = ((blk->bimg_info & BKPIMAGE_APPLY) != 0);

				if (blk->bimg_info & BKPIMAGE_IS_COMPRESSED)
				{
					if (blk->bimg_info & BKPIMAGE_HAS_HOLE)
						COPY_HEADER_FIELD(&blk->hole_length, sizeof(uint16));
					else
						blk->hole_length = 0;
				}
				else
					blk->hole_length = BLCKSZ - blk->bimg_len;
				datatotal += blk->bimg_len;

				/*
				 * cross-check that hole_offset > 0, hole_length > 0 and
				 * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
				 */
				if ((blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					(blk->hole_offset == 0 ||
					 blk->hole_length == 0 ||
					 blk->bimg_len == BLCKSZ))
				{
					br_elog("BKPIMAGE_HAS_HOLE set, but hole offset %u length %u block image length %u at %X/%X",
										  (unsigned int) blk->hole_offset,
										  (unsigned int) blk->hole_length,
										  (unsigned int) blk->bimg_len,
										  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
					goto err;
				}

				/*
				 * cross-check that hole_offset == 0 and hole_length == 0 if
				 * the HAS_HOLE flag is not set.
				 */
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					(blk->hole_offset != 0 || blk->hole_length != 0))
				{
					br_elog("BKPIMAGE_HAS_HOLE not set, but hole offset %u length %u at %X/%X",
										  (unsigned int) blk->hole_offset,
										  (unsigned int) blk->hole_length,
										  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
					goto err;
				}

				/*
				 * cross-check that bimg_len < BLCKSZ if the IS_COMPRESSED
				 * flag is set.
				 */
				if ((blk->bimg_info & BKPIMAGE_IS_COMPRESSED) &&
					blk->bimg_len == BLCKSZ)
				{
					br_elog("BKPIMAGE_IS_COMPRESSED set, but block image length %u at %X/%X",
										  (unsigned int) blk->bimg_len,
										  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
					goto err;
				}

				/*
				 * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE nor
				 * IS_COMPRESSED flag is set.
				 */
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) &&
					!(blk->bimg_info & BKPIMAGE_IS_COMPRESSED) &&
					blk->bimg_len != BLCKSZ)
				{
					br_elog("neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_IS_COMPRESSED set, but block image length is %u at %X/%X",
										  (unsigned int) blk->data_len,
										  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
					goto err;
				}
			}
			if (!(fork_flags & BKPBLOCK_SAME_REL))
			{
				COPY_HEADER_FIELD(&blk->rnode, sizeof(RelFileNode));
				rnode = &blk->rnode;
			}
			else
			{
				if (rnode == NULL)
				{
					br_elog("BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
										  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
					goto err;
				}

				blk->rnode = *rnode;
			}
			COPY_HEADER_FIELD(&blk->blkno, sizeof(BlockNumber));
		}
		else
		{
			br_elog("invalid block_id %u at %X/%X",
								  block_id,
								  (uint32) (state->ReadRecPtr >> 32),
								  (uint32) state->ReadRecPtr);
			goto err;
		}
	}

	if (remaining != datatotal)
		goto shortdata_err;

	/*
	 * Ok, we've parsed the fragment headers, and verified that the total
	 * length of the payload in the fragments is equal to the amount of data
	 * left. Copy the data of each fragment to a separate buffer.
	 *
	 * We could just set up pointers into readRecordBuf, but we want to align
	 * the data for the convenience of the callers. Backup images are not
	 * copied, however; they don't need alignment.
	 */

	/* block data first */
	for (block_id = 0; block_id <= state->max_block_id; block_id++)
	{
		DecodedBkpBlock *blk = &state->blocks[block_id];

		if (!blk->in_use)
			continue;

		Assert(blk->has_image || !blk->apply_image);

		if (blk->has_image)
		{
			blk->bkp_image = ptr;
			ptr += blk->bimg_len;
		}
		if (blk->has_data)
		{
			if (!blk->data || blk->data_len > blk->data_bufsz)
			{
				if (blk->data)
					pfree(blk->data);
				blk->data_bufsz = blk->data_len;
				blk->data = palloc(blk->data_bufsz);
			}
			memcpy(blk->data, ptr, blk->data_len);
			ptr += blk->data_len;
		}
	}

	/* and finally, the main data */
	if (state->main_data_len > 0)
	{
		if (!state->main_data || state->main_data_len > state->main_data_bufsz)
		{
			if (state->main_data)
				pfree(state->main_data);

			/*
			 * main_data_bufsz must be MAXALIGN'ed.  In many xlog record
			 * types, we omit trailing struct padding on-disk to save a few
			 * bytes; but compilers may generate accesses to the xlog struct
			 * that assume that padding bytes are present.  If the palloc
			 * request is not large enough to include such padding bytes then
			 * we'll get valgrind complaints due to otherwise-harmless fetches
			 * of the padding bytes.
			 *
			 * In addition, force the initial request to be reasonably large
			 * so that we don't waste time with lots of trips through this
			 * stanza.  BLCKSZ / 2 seems like a good compromise choice.
			 */
			state->main_data_bufsz = MAXALIGN(Max(state->main_data_len,
												  BLCKSZ / 2));
			state->main_data = palloc(state->main_data_bufsz);
		}
		memcpy(state->main_data, ptr, state->main_data_len);
		ptr += state->main_data_len;
	}

	return true;

shortdata_err:
	br_elog("record with invalid length at %X/%X",
						  (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
err:
	*errormsg = state->errormsg_buf;

	return false;
}


void
configXlogRead(char	*walpath)
{
	
	XLogRecPtr			first_record = 0;

	brc.xlogreader = XLogReaderAllocate_fe(XLogParserReadPage, &brc.parserPri);

	first_record = XLogFindNextRecord_fe(brc.xlogreader, brc.parserPri.startptr);
	if (first_record == InvalidXLogRecPtr)
		br_error("could not find a valid record after %X/%X",
					(uint32) (brc.parserPri.startptr >> 32),
					(uint32) brc.parserPri.startptr);

					
	if (first_record != brc.parserPri.startptr && (brc.parserPri.startptr % XLogSegSize) != 0)
			printf(ngettext("first record is after %X/%X, at %X/%X, skipping over %u byte\n",
							"first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
							(first_record - brc.parserPri.startptr)),
				   (uint32) (brc.parserPri.startptr >> 32), (uint32) brc.parserPri.startptr,
				   (uint32) (first_record >> 32), (uint32) first_record,
				   (uint32) (first_record - brc.parserPri.startptr));
	brc.parserPri.first_record = first_record;
}


XLogRecord *
XLogParserReadRecord(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg)
{
	XLogRecord *record;
	XLogRecPtr	targetPagePtr;
	bool		randAccess;
	uint32		len,
				total_len;
	uint32		targetRecOff;
	uint32		pageHeaderSize;
	bool		gotheader;
	int			readOff;

	/*
	 * randAccess indicates whether to verify the previous-record pointer of
	 * the record we're reading.  We only do this if we're reading
	 * sequentially, which is what we initially assume.
	 */
	randAccess = false;

	/* reset error state */
	*errormsg = NULL;
	state->errormsg_buf[0] = '\0';

	ResetDecoder(state);

	if (RecPtr == InvalidXLogRecPtr)
	{
		/* No explicit start point; read the record after the one we just read */
		RecPtr = state->EndRecPtr;

		if (state->ReadRecPtr == InvalidXLogRecPtr)
			randAccess = true;

		/*
		 * RecPtr is pointing to end+1 of the previous WAL record.  If we're
		 * at a page boundary, no more records can fit on the current page. We
		 * must skip over the page header, but we can't do that until we've
		 * read in the page, since the header size is variable.
		 */
	}
	else
	{
		/*
		 * Caller supplied a position to start at.
		 *
		 * In this case, the passed-in record pointer should already be
		 * pointing to a valid record starting position.
		 */
		Assert(XRecOffIsValid(RecPtr));
		randAccess = true;
	}

	state->currRecPtr = RecPtr;

	targetPagePtr = RecPtr - (RecPtr % XLOG_BLCKSZ);
	targetRecOff = RecPtr % XLOG_BLCKSZ;

	/*
	 * Read the page containing the record into state->readBuf. Request enough
	 * byte to cover the whole record header, or at least the part of it that
	 * fits on the same page.
	 */
	readOff = ReadPageInternal(state,
							   targetPagePtr,
							   Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ));
	if (readOff < 0)
		goto err;

	/*
	 * ReadPageInternal always returns at least the page header, so we can
	 * examine it now.
	 */
	pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
	if (targetRecOff == 0)
	{
		/*
		 * At page start, so skip over page header.
		 */
		RecPtr += pageHeaderSize;
		targetRecOff = pageHeaderSize;
	}
	else if (targetRecOff < pageHeaderSize)
	{
		br_elog("invalid record offset at %X/%X",
							  (uint32) (RecPtr >> 32), (uint32) RecPtr);
		goto err;
	}

	if ((((XLogPageHeader) state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) &&
		targetRecOff == pageHeaderSize)
	{
		br_elog("contrecord is requested by %X/%X",
							  (uint32) (RecPtr >> 32), (uint32) RecPtr);
		goto err;
	}

	/* ReadPageInternal has verified the page header */
	Assert(pageHeaderSize <= readOff);

	/*
	 * Read the record length.
	 *
	 * NB: Even though we use an XLogRecord pointer here, the whole record
	 * header might not fit on this page. xl_tot_len is the first field of the
	 * struct, so it must be on this page (the records are MAXALIGNed), but we
	 * cannot access any other fields until we've verified that we got the
	 * whole header.
	 */
	record = (XLogRecord *) (state->readBuf + RecPtr % XLOG_BLCKSZ);
	total_len = record->xl_tot_len;

	/*
	 * If the whole record header is on this page, validate it immediately.
	 * Otherwise do just a basic sanity check on xl_tot_len, and validate the
	 * rest of the header after reading it from the next page.  The xl_tot_len
	 * check is necessary here to ensure that we enter the "Need to reassemble
	 * record" code path below; otherwise we might fail to apply
	 * ValidXLogRecordHeader at all.
	 */
	if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord)
	{
		if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record,
								   randAccess))
			goto err;
		gotheader = true;
	}
	else
	{
		/* XXX: more validation should be done here */
		if (total_len < SizeOfXLogRecord)
		{
			if(0 == total_len)
				br_elog("LOG:invalid record length at %X/%X: wanted %u, got %u, maybe arrive end",
								  (uint32) (RecPtr >> 32), (uint32) RecPtr,
								  (uint32) SizeOfXLogRecord, total_len);
			else
				br_elog("invalid record length at %X/%X: wanted %u, got %u 111",
								  (uint32) (RecPtr >> 32), (uint32) RecPtr,
								  (uint32) SizeOfXLogRecord, total_len);
			goto err;
		}
		gotheader = false;
	}

	/*
	 * Enlarge readRecordBuf as needed.
	 */
	if (total_len > state->readRecordBufSize &&
		!allocate_recordbuf(state, total_len))
	{
		/* We treat this as a "bogus data" condition */
		br_elog("record length %u at %X/%X too long",
							  total_len,
							  (uint32) (RecPtr >> 32), (uint32) RecPtr);
		goto err;
	}

	len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
	if (total_len > len)
	{
		/* Need to reassemble record */
		char	   *contdata;
		XLogPageHeader pageHeader;
		char	   *buffer;
		uint32		gotlen;

		/* Copy the first fragment of the record from the first page. */
		memcpy(state->readRecordBuf,
			   state->readBuf + RecPtr % XLOG_BLCKSZ, len);
		buffer = state->readRecordBuf + len;
		gotlen = len;

		do
		{
			/* Calculate pointer to beginning of next page */
			targetPagePtr += XLOG_BLCKSZ;

			/* Wait for the next page to become available */
			readOff = ReadPageInternal(state, targetPagePtr,
									   Min(total_len - gotlen + SizeOfXLogShortPHD,
										   XLOG_BLCKSZ));

			if (readOff < 0)
				goto err;

			Assert(SizeOfXLogShortPHD <= readOff);

			/* Check that the continuation on next page looks valid */
			pageHeader = (XLogPageHeader) state->readBuf;
			if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				br_elog( "there is no contrecord flag at %X/%X",
									  (uint32) (RecPtr >> 32), (uint32) RecPtr);
				goto err;
			}

			/*
			 * Cross-check that xlp_rem_len agrees with how much of the record
			 * we expect there to be left.
			 */
			if (pageHeader->xlp_rem_len == 0 ||
				total_len != (pageHeader->xlp_rem_len + gotlen))
			{
				br_elog( "invalid contrecord length %u at %X/%X",
									  pageHeader->xlp_rem_len,
									  (uint32) (RecPtr >> 32), (uint32) RecPtr);
				goto err;
			}

			/* Append the continuation from this page to the buffer */
			pageHeaderSize = XLogPageHeaderSize(pageHeader);

			if (readOff < pageHeaderSize)
				readOff = ReadPageInternal(state, targetPagePtr,
										   pageHeaderSize);

			Assert(pageHeaderSize <= readOff);

			contdata = (char *) state->readBuf + pageHeaderSize;
			len = XLOG_BLCKSZ - pageHeaderSize;
			if (pageHeader->xlp_rem_len < len)
				len = pageHeader->xlp_rem_len;

			if (readOff < pageHeaderSize + len)
				readOff = ReadPageInternal(state, targetPagePtr,
										   pageHeaderSize + len);

			memcpy(buffer, (char *) contdata, len);
			buffer += len;
			gotlen += len;

			/* If we just reassembled the record header, validate it. */
			if (!gotheader)
			{
				record = (XLogRecord *) state->readRecordBuf;
				if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr,
										   record, randAccess))
					goto err;
				gotheader = true;
			}
		} while (gotlen < total_len);

		Assert(gotheader);

		record = (XLogRecord *) state->readRecordBuf;
		if (!ValidXLogRecord(state, record, RecPtr))
			goto err;

		pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) state->readBuf);
		state->ReadRecPtr = RecPtr;
		state->EndRecPtr = targetPagePtr + pageHeaderSize
			+ MAXALIGN(pageHeader->xlp_rem_len);
	}
	else
	{
		/* Wait for the record data to become available */
		readOff = ReadPageInternal(state, targetPagePtr,
								   Min(targetRecOff + total_len, XLOG_BLCKSZ));
		if (readOff < 0)
			goto err;

		/* Record does not cross a page boundary */
		if (!ValidXLogRecord(state, record, RecPtr))
			goto err;

		state->EndRecPtr = RecPtr + MAXALIGN(total_len);

		state->ReadRecPtr = RecPtr;
		memcpy(state->readRecordBuf, record, total_len);
	}

	/*
	 * Special processing if it's an XLOG SWITCH record
	 */
	if (record->xl_rmid == RM_XLOG_ID &&
		(record->xl_info & ~XLR_INFO_MASK) == XLOG_SWITCH)
	{
		/* Pretend it extends to end of segment */
		state->EndRecPtr += XLogSegSize - 1;
		state->EndRecPtr -= state->EndRecPtr % XLogSegSize;
	}

	if (DecodeXLogRecord_fe(state, record, errormsg))
		return record;
	else
		return NULL;

err:

	/*
	 * Invalidate the read state. We might read from a different source after
	 * failure.
	 */
	XLogReaderInvalReadState(state);

	if (state->errormsg_buf[0] != '\0')
		*errormsg = state->errormsg_buf;

	return NULL;
}


static int
XLogParserReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 XLogRecPtr targetPtr, char *readBuff, TimeLineID *curFileTLI)
{
	XLogParserPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	XLogParserXLogRead(private->inpath, private->timeline, targetPagePtr,
					 readBuff, count);

	return count;
}

static void
XLogParserXLogRead(const char *directory, TimeLineID timeline_id,
				 XLogRecPtr startptr, char *buf, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	static int	sendFile = -1;
	static XLogSegNo sendSegNo = 0;
	static uint32 sendOff = 0;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = recptr % XLogSegSize;

		if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo))
		{
			char		fname[MAXFNAMELEN];
			int			tries;

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo);

			XLogFileName(fname, timeline_id, sendSegNo);

			/*
			 * In follow mode there is a short period of time after the server
			 * has written the end of the previous file before the new file is
			 * available. So we loop for 5 seconds looking for the file to
			 * appear before giving up.
			 */
			for (tries = 0; tries < 10; tries++)
			{
				sendFile = fuzzy_open_file(directory, fname);
				if (sendFile >= 0)
					break;
				if (errno == ENOENT)
				{
					int			save_errno = errno;

					/* File not there yet, try again */
					pg_usleep(500 * 1000);

					errno = save_errno;
					continue;
				}
				/* Any other error, fall through and fail */
				break;
			}

			if (sendFile < 0)
				br_error("could not find file \"%s\": %s",
							fname, strerror(errno));
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
			{
				int			err = errno;
				char		fname[MAXPGPATH];

				XLogFileName(fname, timeline_id, sendSegNo);

				br_error("could not seek in log file %s to offset %u: %s",
							fname, startoff, strerror(err));
			}
			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (XLogSegSize - startoff))
			segbytes = XLogSegSize - startoff;
		else
			segbytes = nbytes;

		readbytes = read(sendFile, p, segbytes);
		if (readbytes <= 0)
		{
			int			err = errno;
			char		fname[MAXPGPATH];

			XLogFileName(fname, timeline_id, sendSegNo);

			br_error("could not read from log file %s, offset %u, length %d: %s",
						fname, sendOff, segbytes, strerror(err));
		}

		/* Update state for read */
		recptr += readbytes;

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

static bool
xLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum)
{
	DecodedBkpBlock *bkpb;

	if (!record->blocks[block_id].in_use)
		return false;

	bkpb = &record->blocks[block_id];
	if (rnode)
		*rnode = bkpb->rnode;
	if (forknum)
		*forknum = bkpb->forknum;
	if (blknum)
		*blknum = bkpb->blkno;
	return true;
}

void
recoverRecord(XLogReaderState *record)
{
	uint8				rmid = 0;
	uint8				info = 0;
	BlockNumber 		blknum = 0;
	RelFileNode			rfnode;


	rmid = XLogRecGetRmid(record);
	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	info &= XLOG_HEAP_OPMASK;

	memset(&rfnode, 0, sizeof(RelFileNode));
	xLogRecGetBlockTag(record, 0, &rfnode, NULL, &blknum);
	if(0 == rfnode.spcNode)
		rfnode.spcNode = PG_DEFAULT_TBS_OID;
	if(brc.rfn.relNode != rfnode.relNode)
		return;
	if(RM_HEAP_ID == rmid)
	{
		if (XLOG_HEAP_INSERT == info)
		{
			recoverInsertRecord(record, blknum);
		}
		else if(XLOG_HEAP_DELETE == info)
		{
			recoverDeleteRecord(record, blknum);
		}
		else if (XLOG_HEAP_UPDATE == info)
		{
			recoverUpdateRecord(record, blknum, false);
		}
		else if (XLOG_HEAP_HOT_UPDATE == info)
		{
			recoverUpdateRecord(record, blknum, true);
		}
		else if(XLOG_HEAP_CONFIRM == info)
		{
			recoverConfirmRecord(record, blknum);
		}
		else if(XLOG_HEAP_LOCK == info)
		{
			recoverLockRecord(record, blknum);
		}
		else if(XLOG_HEAP_INPLACE == info)
		{
			recoverInplaceRecord(record, blknum);
		}
	}
	else if(RM_HEAP2_ID == rmid)
	{
		if(XLOG_HEAP2_CLEAN == info)
		{
			heep2_recoverCleanRecord(record, blknum);
		}
		else if(XLOG_HEAP2_FREEZE_PAGE == info)
		{
			heep2_recoverFreezePageRecord(record, blknum);
		}
		else if(XLOG_HEAP2_VISIBLE == info)
		{
			heep2_recoverVisibleRecord(record, blknum);
		}
		else if(XLOG_HEAP2_LOCK_UPDATED == info)
		{
			heep2_recoverLockRecord(record, blknum);
		}
		else if(XLOG_HEAP2_MULTI_INSERT == info)
		{
			heep2_recoverMutiinsertRecord(record, blknum);
		}
	}
}

void
showRecord(XLogReaderState *record)
{
	uint8				rmid = 0;
	uint8				info = 0;
	BlockNumber 		blknum = 0;
	RelFileNode			rfnode;


	rmid = XLogRecGetRmid(record);
	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	info &= XLOG_HEAP_OPMASK;

	memset(&rfnode, 0, sizeof(RelFileNode));
	xLogRecGetBlockTag(record, 0, &rfnode, NULL, &blknum);
	if(RM_HEAP_ID == rmid)
	{
		if (XLOG_HEAP_INSERT == info)
		{
			br_elog("RM_HEAP_ID:INSERT: RELFILENODE(%u)",  rfnode.relNode);
		}
		else if(XLOG_HEAP_DELETE == info)
		{
			br_elog("RM_HEAP_ID:DELETE: RELFILENODE(%u)",  rfnode.relNode);
		}
		else if (XLOG_HEAP_UPDATE == info)
		{
			br_elog("RM_HEAP_ID:UPDATE: RELFILENODE(%u)",  rfnode.relNode);
		}
		else if (XLOG_HEAP_HOT_UPDATE == info)
		{
			br_elog("RM_HEAP_ID:HOT UPDATE: RELFILENODE(%u)", rfnode.relNode);
		}
		else if(XLOG_HEAP_CONFIRM == info)
		{
			br_elog("RM_HEAP_ID:CONFIRM");
		}
		else if(XLOG_HEAP_LOCK == info)
		{
			br_elog("RM_HEAP_ID:LOCK");
		}
		else if(XLOG_HEAP_INPLACE == info)
		{
			br_elog("RM_HEAP_ID:INPLACE");
		}
	}
	else if(RM_HEAP2_ID == rmid)
	{
		if(XLOG_HEAP2_CLEAN == info)
		{
			br_elog("RM_HEAP2_ID:CLEAN");
		}
		else if(XLOG_HEAP2_FREEZE_PAGE == info)
		{
			br_elog("RM_HEAP2_ID:FREEZE_PAGE");
		}
		else if(XLOG_HEAP2_VISIBLE == info)
		{
			br_elog("RM_HEAP2_ID:VISIBLE");
		}
		else if(XLOG_HEAP2_LOCK_UPDATED == info)
		{
			br_elog("RM_HEAP2_ID:LOCK_UPDATED");
		}
		else if(XLOG_HEAP2_MULTI_INSERT == info)
		{
			br_elog("RM_HEAP2_ID:MULTI_INSERT");
		}
	}
}



static char *
xLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
{
	DecodedBkpBlock *bkpb;

	if (!record->blocks[block_id].in_use)
		return NULL;

	bkpb = &record->blocks[block_id];

	if (!bkpb->has_data)
	{
		if (len)
			*len = 0;
		return NULL;
	}
	else
	{
		if (len)
			*len = bkpb->data_len;
		return bkpb->data;
	}
}


static OffsetNumber
pageAddItemExtended(Page page,Item item,Size size,OffsetNumber offsetNumber)
{
	PageHeader	phdr = (PageHeader) page;
	Size		alignedSize;
	int			lower;
	int			upper;
	ItemId		itemId;
	OffsetNumber limit;
	bool		needshuffle = false;
	int			flags = PAI_OVERWRITE | PAI_IS_HEAP;

	/*
	 * Be wary about corrupted page pointers
	 */
	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > BLCKSZ)
		br_error("corrupted page pointers: lower = %u, upper = %u, special = %u",
						phdr->pd_lower, phdr->pd_upper, phdr->pd_special);

	/*
	 * Select offsetNumber to place the new item at
	 */
	limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

	/* was offsetNumber passed in? */
	if (OffsetNumberIsValid(offsetNumber))
	{
		/* yes, check it */
		if ((flags & PAI_OVERWRITE) != 0)
		{
			if (offsetNumber < limit)
			{
				itemId = PageGetItemId(phdr, offsetNumber);
				if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId))
				{
					br_elog("will not overwrite a used ItemId");
					return InvalidOffsetNumber;
				}
			}
		}
		else
		{
			if (offsetNumber < limit)
				needshuffle = true; /* need to move existing linp's */
		}
	}
	
	/* Reject placing items beyond the first unused line pointer */
	if (offsetNumber > limit)
	{
		br_elog("specified item offset is too large");
		return InvalidOffsetNumber;
	}

	/* Reject placing items beyond heap boundary, if heap */
	if ((flags & PAI_IS_HEAP) != 0 && offsetNumber > MaxHeapTuplesPerPage)
	{
		br_elog("can't put more than MaxHeapTuplesPerPage items in a heap page");
		return InvalidOffsetNumber;
	}

	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	if (offsetNumber == limit || needshuffle)
		lower = phdr->pd_lower + sizeof(ItemIdData);
	else
		lower = phdr->pd_lower;

	alignedSize = MAXALIGN(size);

	upper = (int) phdr->pd_upper - (int) alignedSize;

	if (lower > upper)
		return InvalidOffsetNumber;

	/*
	 * OK to insert the item.  First, shuffle the existing pointers if needed.
	 */
	itemId = PageGetItemId(phdr, offsetNumber);

	if (needshuffle)
		memmove(itemId + 1, itemId,
				(limit - offsetNumber) * sizeof(ItemIdData));

	/* set the item pointer */
	ItemIdSetNormal(itemId, upper, size);

	/* copy the item's data onto the page */
	memcpy((char *) page + upper, item, size);

	/* adjust page header */
	phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;

	return offsetNumber;
}


static Page
getTargetPage(int *tarIndex, bool *findInArray,BlockNumber blknum)
{
	int				loop = 0;
	Page			targetPage = NULL;
	
	for(loop = 0; loop < brc.rbNum; loop++)
	{	
		if(brc.recoverBlock[loop] - 1 == blknum)
		{
			targetPage = brc.pageArray[loop];
			*findInArray = true;
			if(!targetPage)
			{
				brc.pageArray[loop] = pageInit(0);
				targetPage = brc.pageArray[loop];
			}
			break;
		}
	}
	*tarIndex = loop;
	return targetPage;
}

static void
heapExecuteFreezeTuple(HeapTupleHeader tuple, xl_heap_freeze_tuple *frz)
{
	HeapTupleHeaderSetXmax(tuple, frz->xmax);

	if (frz->frzflags & XLH_FREEZE_XVAC)
		HeapTupleHeaderSetXvac(tuple, FrozenTransactionId);

	if (frz->frzflags & XLH_INVALID_XVAC)
		HeapTupleHeaderSetXvac(tuple, InvalidTransactionId);

	tuple->t_infomask = frz->t_infomask;
	tuple->t_infomask2 = frz->t_infomask2;
}


static void
heep2_recoverMutiinsertRecord(XLogReaderState *record, BlockNumber blknum)
{
	xl_heap_multi_insert  *xlrec = NULL;
	Page			targetPage = NULL;
	int				tarIndex = 0, loop = 0;
	ItemPointerData target_tid;	
	char			*data = NULL;
	uint32			newlen = 0;
	OffsetNumber	pageAddResult = 0;
	Size			datalen = 0;
	bool			findInArray = false;
	bool			isinit = false;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}tbuf;
	HeapTupleHeader htup = NULL;
	bool			image = false;

	memset(&target_tid, 0, sizeof(ItemPointerData));
	isinit = (0 != (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE));
	
	if(XLogRecBlockImageApply(record, 0))
		image = true;
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
	
		char	   *endptr = NULL;
		if(!targetPage)
			br_error("Get a record now,but we can not find a base page for it.");
		
		xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);
		data = xLogRecGetBlockData(record, 0, &datalen);
		endptr = data + datalen;
		for(loop = 0; loop < xlrec->ntuples;loop++)
		{
			OffsetNumber offnum;
			xl_multi_insert_tuple *xlhdr;
			XLogRecPtr	lsn = record->EndRecPtr;

			if (isinit)
				offnum = FirstOffsetNumber + loop;
			else
				offnum = xlrec->offsets[loop];
			if (PageGetMaxOffsetNumber(targetPage) + 1 < offnum)
				br_error("invalid max offset number");

			xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(data);
			data = ((char *) xlhdr) + SizeOfMultiInsertTuple;
			newlen = xlhdr->datalen;
			Assert(newlen <= MaxHeapTupleSize);
			htup = &tbuf.hdr;
			MemSet((char *) htup, 0, SizeofHeapTupleHeader);
			memcpy((char *) htup + SizeofHeapTupleHeader,
				   (char *) data,
				   newlen);
			data += newlen;

			newlen += SizeofHeapTupleHeader;
			htup->t_infomask2 = xlhdr->t_infomask2;
			htup->t_infomask = xlhdr->t_infomask;
			htup->t_hoff = xlhdr->t_hoff;
			HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
			HeapTupleHeaderSetCmin(htup, FirstCommandId);
			ItemPointerSetBlockNumber(&htup->t_ctid, blknum);
			ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

			pageAddResult = pageAddItemExtended(targetPage, (Item) htup, newlen, offnum);
			pageSetLSN(targetPage, lsn);
			if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
				PageClearAllVisible(targetPage);
			if (pageAddResult == InvalidOffsetNumber)
				br_error("failed to add tuple");
		}
		if (data != endptr)
			br_error("total tuple length mismatch");
	}
}

static void
heapPagePruneExecute(Page page,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	OffsetNumber *offnum;
	int			i;

	/* Update all redirected line pointers */
	offnum = redirected;
	for (i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	pageRepairFragmentation(page);
}


static void 
heep2_recoverCleanRecord(XLogReaderState *record, BlockNumber blknum)
{
	Page			targetPage = NULL;
	int				tarIndex = 0;	
	bool			findInArray = false;
	bool			image = false;
	XLogRecPtr		lsn = record->EndRecPtr;
	
	if(XLogRecBlockImageApply(record, 0))
		image = true;
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		xl_heap_clean *xlrec = (xl_heap_clean *) XLogRecGetData(record);
		OffsetNumber *end = NULL;
		OffsetNumber *redirected = NULL;
		OffsetNumber *nowdead = NULL;
		OffsetNumber *nowunused = NULL;
		int			nredirected = 0;
		int			ndead = 0;
		int			nunused = 0;
		Size		datalen = 0;

		redirected = (OffsetNumber *) xLogRecGetBlockData(record, 0, &datalen);
		nredirected = xlrec->nredirected;
		ndead = xlrec->ndead;
		end = (OffsetNumber *) ((char *) redirected + datalen);
		nowdead = redirected + (nredirected * 2);
		nowunused = nowdead + ndead;
		nunused = (end - nowunused);
		Assert(nunused >= 0);
		heapPagePruneExecute(targetPage,
								redirected, nredirected,
								nowdead, ndead,
								nowunused, nunused);
		pageSetLSN(targetPage, lsn);
	}

}

static void 
heep2_recoverFreezePageRecord(XLogReaderState *record, BlockNumber blknum)
{
	Page			targetPage = NULL;
	int				tarIndex = 0;	
	bool			findInArray = false;
	bool			image = false;
	XLogRecPtr		lsn = record->EndRecPtr;
	
	if(XLogRecBlockImageApply(record, 0))
		image = true;
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		xl_heap_freeze_tuple *tuples = NULL;
		xl_heap_freeze_page *xlrec = NULL;
		int					 loop = 0;

		xlrec = (xl_heap_freeze_page *) XLogRecGetData(record);
		tuples = (xl_heap_freeze_tuple *) xLogRecGetBlockData(record, 0, NULL);
		
		for (loop = 0; loop < xlrec->ntuples; loop++)
		{
			xl_heap_freeze_tuple *xlrec_tp;
			ItemId		lp;
			HeapTupleHeader tuple;

			xlrec_tp = &tuples[loop];
			lp = PageGetItemId(targetPage, xlrec_tp->offset); /* offsets are one-based */
			tuple = (HeapTupleHeader) PageGetItem(targetPage, lp);

			heapExecuteFreezeTuple(tuple, xlrec_tp);
		}
		pageSetLSN(targetPage, lsn);
	}
}

static void 
heep2_recoverVisibleRecord(XLogReaderState *record, BlockNumber blknum)
{
	Page			targetPage = NULL;
	int 			tarIndex = 0;	
	bool			findInArray = false;
	bool			image = false;
	
	if(XLogRecBlockImageApply(record, 0))
		image = true;
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		PageSetAllVisible(targetPage);
	}

}
static void 
heep2_recoverLockRecord(XLogReaderState *record, BlockNumber blknum)
{
	Page			targetPage = NULL;
	int 			tarIndex = 0;	
	bool			findInArray = false;
	bool			image = false;
	
	if(XLogRecBlockImageApply(record, 0))
		image = true;
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		OffsetNumber			offnum = 0;
		XLogRecPtr				lsn = record->EndRecPtr;
		xl_heap_lock_updated	*xlrec = NULL;
		ItemId					lp = NULL;
		HeapTupleHeader			htup = NULL;

		xlrec = (xl_heap_lock_updated *) XLogRecGetData(record);
		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(targetPage) >= offnum)
			lp = PageGetItemId(targetPage, offnum);

		if (PageGetMaxOffsetNumber(targetPage) < offnum || !ItemIdIsNormal(lp))
			br_error("invalid lp");
		htup = (HeapTupleHeader) PageGetItem(targetPage, lp);
		
		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
								   &htup->t_infomask2);
		HeapTupleHeaderSetXmax(htup, xlrec->xmax);

		pageSetLSN(targetPage, lsn);
	}

}


static void
recoverInsertRecord(XLogReaderState *record, BlockNumber blknum)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPage = NULL;
	int				tarIndex = 0;
	xl_heap_insert  *xlrec = NULL;
	xl_heap_header  xlhdr;
	ItemPointerData target_tid;	
	char			*data = NULL;
	uint32			newlen = 0;
	OffsetNumber	pageAddResult = 0;
	Size			datalen = 0;
	bool			findInArray = false;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}tbuf;
	HeapTupleHeader htup = NULL;
	bool			image = false;
	
	memset(&xlhdr, 0, sizeof(xl_heap_header));
	memset(&target_tid, 0, sizeof(ItemPointerData));

	if(XLogRecBlockImageApply(record, 0))
		image = true;
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		if(!targetPage)
			br_elog("Get a record now,but we can not find a base page for it.");
		xlrec = (xl_heap_insert *) XLogRecGetData(record);
		ItemPointerSetBlockNumber(&target_tid, blknum);
		ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);
		data = xLogRecGetBlockData(record, 0, &datalen);

		newlen = datalen - SizeOfHeapHeader;
		Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
		memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
		data += SizeOfHeapHeader;
		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		memcpy((char *) htup + SizeofHeapTupleHeader,data,newlen);
		newlen += SizeofHeapTupleHeader;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;
		pageAddResult = pageAddItemExtended(targetPage, (Item) htup, newlen, xlrec->offnum);
		pageSetLSN(targetPage, lsn);
		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(targetPage);
		if(InvalidOffsetNumber == pageAddResult)
			br_error("failed to add tuple");
	}
}

static void
recoverDeleteRecord(XLogReaderState *record, BlockNumber blknum)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPage = NULL;
	int				tarIndex = 0;
	xl_heap_delete  *xlrec = NULL;
	xl_heap_header  xlhdr;
	ItemPointerData target_tid;	
	ItemId			lp = NULL;
	TransactionId	xid = 0, pd_prune_xid = 0;
	HeapTupleHeader htup = NULL;
	bool			image = false;
	bool			findInArray = false;
	
	memset(&xlhdr, 0, sizeof(xl_heap_header));
	memset(&target_tid, 0, sizeof(ItemPointerData));
	if(XLogRecBlockImageApply(record, 0))
		image = true;
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		if(!targetPage)
			br_error("Get a record now,but we can not find a base page for it.");
		xlrec = (xl_heap_delete *) XLogRecGetData(record);
		Assert(targetPage);
		ItemPointerSetBlockNumber(&target_tid, blknum);
		ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);
		if (PageGetMaxOffsetNumber(targetPage) >= xlrec->offnum)
			lp = PageGetItemId(targetPage, xlrec->offnum);
		if (PageGetMaxOffsetNumber(targetPage) < xlrec->offnum || !ItemIdIsNormal(lp))
			br_error("invalid lp");
		htup = (HeapTupleHeader) PageGetItem(targetPage, lp);
		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		HeapTupleHeaderClearHotUpdated(htup);
		fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask, &htup->t_infomask2);
		if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
			HeapTupleHeaderSetXmax(htup, xlrec->xmax);
		else
			HeapTupleHeaderSetXmin(htup, InvalidTransactionId);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		/* Mark the page as a candidate for pruning */
		xid = XLogRecGetXid(record);
		pd_prune_xid = ((PageHeader) (targetPage))->pd_prune_xid;
		if (!TransactionIdIsValid(pd_prune_xid) || transactionIdPrecedes(xid, pd_prune_xid))
			((PageHeader) (targetPage))->pd_prune_xid = xid; 

		if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(targetPage);
		/* Make sure there is no forward chain link in t_ctid */
		htup->t_ctid = target_tid;
		pageSetLSN(targetPage, lsn);
		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(targetPage);
	}
}

static void
recoverUpdateRecord(XLogReaderState *record, BlockNumber blknum, bool hot_update)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPageNew = NULL, targetPageOld = NULL;
	int 			tarIndex = 0;
	xl_heap_update	*xlrec = NULL;
	xl_heap_header	xlhdr;
	ItemPointerData newtid;
	uint32			newlen = 0;
	OffsetNumber	pageAddResult = 0;
	Size			datalen = 0;
	ItemId			lp = NULL;
	TransactionId	xid = 0, pd_prune_xid = 0;
	BlockNumber 	oldblk = 0,newblk = 0;
	HeapTupleData	oldtup;
	bool			findInArray = false;
		
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}tbuf;
	HeapTupleHeader htup = NULL;
	bool			newimage = false, oldimage = false;
	
	memset(&xlhdr, 0, sizeof(xl_heap_header));
	memset(&newtid, 0, sizeof(ItemPointerData));
	memset(&oldtup, 0, sizeof(HeapTupleData));
	xlrec =  (xl_heap_update *) XLogRecGetData(record);
	newblk = blknum;
	if(!xLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
		oldblk = newblk;
	ItemPointerSet(&newtid, newblk, xlrec->new_offnum);
	if(XLogRecBlockImageApply(record, 1))
		oldimage = true;
	targetPageOld = getTargetPage(&tarIndex, &findInArray, oldblk);
	
	if(!findInArray)
		/* 
		* Get a record that we does not care,but we can not return here,
		* we should mental the new partial of the record.
		*/
		;
	
	if(findInArray && oldimage )
	{
		recoverImageFromRecord(record,oldblk,tarIndex,(newblk == oldblk)?0:1);
	}
	else if(findInArray)
	{
		if(!targetPageOld)
			br_error("Get a record now,but we can not find a base page for it.");
		
		if (PageGetMaxOffsetNumber(targetPageOld) >= xlrec->old_offnum)
			lp = PageGetItemId(targetPageOld, xlrec->old_offnum);
		if (PageGetMaxOffsetNumber(targetPageOld) < xlrec->old_offnum || !ItemIdIsNormal(lp))
			br_error("invalid lp");
		htup = (HeapTupleHeader) PageGetItem(targetPageOld, lp);
		oldtup.t_data = htup;
		oldtup.t_len = ItemIdGetLength(lp);
		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		if (hot_update)
			HeapTupleHeaderSetHotUpdated(htup);
		else
			HeapTupleHeaderClearHotUpdated(htup);
		fix_infomask_from_infobits(xlrec->old_infobits_set, &htup->t_infomask, &htup->t_infomask2);
		HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		htup->t_ctid = newtid;
		/* Mark the page as a candidate for pruning */
		xid = XLogRecGetXid(record);
		pd_prune_xid = ((PageHeader) (targetPageOld))->pd_prune_xid;
		if (!TransactionIdIsValid(pd_prune_xid) || transactionIdPrecedes(xid, pd_prune_xid))
			((PageHeader) (targetPageOld))->pd_prune_xid = xid; 

		if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(targetPageOld);
		pageSetLSN(targetPageOld, lsn);
		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(targetPageOld);
	}

	findInArray = false;
	if(XLogRecBlockImageApply(record, 0))
		newimage = true;
	targetPageNew = getTargetPage(&tarIndex, &findInArray, newblk);
	if(!findInArray)
			/* Get a record that we does not care */
			return;

	if(newimage)
	{	
		if(newblk != oldblk)
		{
			recoverImageFromRecord(record,newblk,tarIndex,0);
		}
	}
	else
	{
		uint16	prefixlen = 0, suffixlen = 0;
		Size	tuplen = 0;
		char	*newp = NULL;
		char	*recdata_end = NULL;
		char	*recdata = NULL;
		
		recdata = xLogRecGetBlockData(record, 0, &datalen);
		recdata_end = recdata + datalen;

		if(!targetPageNew)
			br_error("Get a record now,but we can not find a base page for it.");
		if (PageGetMaxOffsetNumber(targetPageNew) + 1 < xlrec->new_offnum)
			br_error("invalid max offset number");

		
		if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
		{
			Assert(newblk == oldblk);
			memcpy(&prefixlen, recdata, sizeof(uint16));
			recdata += sizeof(uint16);
		}
		if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
		{
			Assert(newblk == oldblk);
			memcpy(&suffixlen, recdata, sizeof(uint16));
			recdata += sizeof(uint16);
		}
		memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
		recdata += SizeOfHeapHeader;
		tuplen = recdata_end - recdata;
		Assert(tuplen <= MaxHeapTupleSize);
		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		/*
		 * Reconstruct the new tuple using the prefix and/or suffix from the
		 * old tuple, and the data stored in the WAL record.
		 */
		newp = (char *) htup + SizeofHeapTupleHeader;
		if (prefixlen > 0)
		{
			int 		len;

			/* copy bitmap [+ padding] [+ oid] from WAL record */
			len = xlhdr.t_hoff - SizeofHeapTupleHeader;
			memcpy(newp, recdata, len);
			recdata += len;
			newp += len;

			/* copy prefix from old tuple */
			memcpy(newp, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
			newp += prefixlen;

			/* copy new tuple data from WAL record */
			len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
			memcpy(newp, recdata, len);
			recdata += len;
			newp += len;
		}
		else
		{
			/*
			 * copy bitmap [+ padding] [+ oid] + data from record, all in one
			 * go
			 */
			memcpy(newp, recdata, tuplen);
			recdata += tuplen;
			newp += tuplen;
		}
		Assert(recdata == recdata_end);
		if (suffixlen > 0)
			memcpy(newp, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);
		newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;

		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
		/* Make sure there is no forward chain link in t_ctid */
		htup->t_ctid = newtid;
		
		pageAddResult = 	pageAddItemExtended(targetPageNew, (Item) htup, newlen, xlrec->new_offnum);
		pageSetLSN(targetPageNew, lsn);
		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(targetPageNew);
		if(InvalidOffsetNumber == pageAddResult)
			br_error("failed to add tuple");
	}
}

static void
recoverInplaceRecord(XLogReaderState *record, BlockNumber blknum)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPage = NULL;
	int 			tarIndex = 0;
	bool			findInArray = false;
	HeapTupleHeader htup = NULL;
	bool			image = false;

	if(XLogRecBlockImageApply(record, 0))
		image = true;
	
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
		OffsetNumber	offnum = 0;
		ItemId			lp = NULL;
		Size			newlen = 0;
		uint32			oldlen;
		char	   		*newtup = xLogRecGetBlockData(record, 0, &newlen);
		
		if(!targetPage)
			br_error("Get a record now,but we can not find a base page for it.");
		
		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(targetPage) >= offnum)
			lp = PageGetItemId(targetPage, offnum);
		if (PageGetMaxOffsetNumber(targetPage) < offnum || !ItemIdIsNormal(lp))
			br_error("invalid lp");
		htup = (HeapTupleHeader) PageGetItem(targetPage, lp);
		oldlen = ItemIdGetLength(lp) - htup->t_hoff;
		if (oldlen != newlen)
			br_error("wrong tuple length");
		memcpy((char *) htup + htup->t_hoff, newtup, newlen);
		pageSetLSN(targetPage, lsn);
	}
}


static void
recoverConfirmRecord(XLogReaderState *record, BlockNumber blknum)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPage = NULL;
	int				tarIndex = 0;
	bool			findInArray = false;
	HeapTupleHeader htup = NULL;
	bool			image = false;

	if(XLogRecBlockImageApply(record, 0))
		image = true;
	
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		xl_heap_confirm *xlrec = (xl_heap_confirm *) XLogRecGetData(record);
		OffsetNumber	offnum = 0;
		ItemId			lp = NULL;
		if(!targetPage)
			br_error("Get a record now,but we can not find a base page for it.");
		
		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(targetPage) >= offnum)
			lp = PageGetItemId(targetPage, offnum);
		if (PageGetMaxOffsetNumber(targetPage) < offnum || !ItemIdIsNormal(lp))
			br_error("invalid lp");
		htup = (HeapTupleHeader) PageGetItem(targetPage, lp);
		ItemPointerSet(&htup->t_ctid, blknum, offnum);
		pageSetLSN(targetPage, lsn);
	}
}

static void
recoverLockRecord(XLogReaderState *record, BlockNumber blknum)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPage = NULL;
	int				tarIndex = 0;
	bool			findInArray = false;
	HeapTupleHeader htup = NULL;
	bool			image = false;

	if(XLogRecBlockImageApply(record, 0))
		image = true;
	
	targetPage = getTargetPage(&tarIndex, &findInArray, blknum);
	if(!findInArray)
		/* Get a record that we does not care */
		return;
	
	if(image)
	{
		recoverImageFromRecord(record,blknum,tarIndex,0);
	}
	else
	{
		xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);
		OffsetNumber	offnum = 0;
		ItemId			lp = NULL;
		if(!targetPage)
			br_error("Get a record now,but we can not find a base page for it.");
		
		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(targetPage) >= offnum)
			lp = PageGetItemId(targetPage, offnum);
		if (PageGetMaxOffsetNumber(targetPage) < offnum || !ItemIdIsNormal(lp))
			br_error("invalid lp");
		htup = (HeapTupleHeader) PageGetItem(targetPage, lp);
		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
								   &htup->t_infomask2);
		if (HEAP_XMAX_IS_LOCKED_ONLY(htup->t_infomask))
		{
			HeapTupleHeaderClearHotUpdated(htup);
			/* Make sure there is no forward chain link in t_ctid */
  			ItemPointerSet(&htup->t_ctid, blknum, offnum);
		}
		HeapTupleHeaderSetXmax(htup, xlrec->locking_xid);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		pageSetLSN(targetPage, lsn);
	}
}


static void
recoverImageFromRecord(XLogReaderState *record, BlockNumber blknum,int pageIndex,int block_id)
{
	DecodedBkpBlock *bkpb = NULL;
	char	   *ptr = NULL;
	char		tmp[BLCKSZ];
	Page		page;
	
	Assert(XLogRecHasBlockImage(record, block_id));

	bkpb = &record->blocks[block_id];
	ptr = bkpb->bkp_image;
	if (bkpb->bimg_info & BKPIMAGE_IS_COMPRESSED)
	{
		/* If a backup block image is compressed, decompress it */
		if (pglz_decompress(ptr, bkpb->bimg_len, tmp,
							BLCKSZ - bkpb->hole_length) < 0)
		{
			br_error("invalid compressed image at %X/%X, block %d",
								  (uint32) (record->ReadRecPtr >> 32),
								  (uint32) record->ReadRecPtr,block_id);
		}
		ptr = tmp;
	}

	page = brc.pageArray[pageIndex];
	Assert(page);
	if (bkpb->hole_length == 0)
	{
		memcpy(page, ptr, BLCKSZ);
	}
	else
	{
		memcpy(page, ptr, bkpb->hole_offset);
		/* must zero-fill the hole */
		MemSet(page + bkpb->hole_offset, 0, bkpb->hole_length);
		memcpy(page + (bkpb->hole_offset + bkpb->hole_length),
			   ptr + bkpb->hole_offset,
			   BLCKSZ - (bkpb->hole_offset + bkpb->hole_length));
	}
}

static Page
pageInit(Size specialSize)
{
	Page			page = NULL;
	PageHeader		p = NULL;

	page = (Page)malloc(BLCKSZ);
	if(!page)
		br_error("Out of memory");
	p = (PageHeader) page;

	specialSize = MAXALIGN(specialSize);

	Assert(BLCKSZ > specialSize + SizeOfPageHeaderData);

	/* Make sure all fields of page are zero, as well as unused space */
	MemSet(p, 0, BLCKSZ);

	p->pd_flags = 0;
	p->pd_lower = SizeOfPageHeaderData;
	p->pd_upper = BLCKSZ - specialSize;
	p->pd_special = BLCKSZ - specialSize;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	return page;
	/* p->pd_prune_xid = InvalidTransactionId;		done by above MemSet */
}

static void
pageRepairFragmentation(Page page)
{
	Offset		pd_lower = ((PageHeader) page)->pd_lower;
	Offset		pd_upper = ((PageHeader) page)->pd_upper;
	Offset		pd_special = ((PageHeader) page)->pd_special;
	ItemId		lp;
	int			nline,
				nstorage,
				nunused;
	int			i;
	Size		totallen;

	/*
	 * It's worth the trouble to be more paranoid here than in most places,
	 * because we are about to reshuffle data in (what is usually) a shared
	 * disk buffer.  If we aren't careful then corrupted pointers, lengths,
	 * etc could cause us to clobber adjacent disk buffers, spreading the data
	 * loss further.  So, check everything.
	 */
	if (pd_lower < SizeOfPageHeaderData ||
		pd_lower > pd_upper ||
		pd_upper > pd_special ||
		pd_special > BLCKSZ ||
		pd_special != MAXALIGN(pd_special))
		br_error("corrupted page pointers: lower = %u, upper = %u, special = %u",
						pd_lower, pd_upper, pd_special);

	nline = PageGetMaxOffsetNumber(page);
	nunused = nstorage = 0;
	for (i = FirstOffsetNumber; i <= nline; i++)
	{
		lp = PageGetItemId(page, i);
		if (ItemIdIsUsed(lp))
		{
			if (ItemIdHasStorage(lp))
				nstorage++;
		}
		else
		{
			/* Unused entries should have lp_len = 0, but make sure */
			ItemIdSetUnused(lp);
			nunused++;
		}
	}

	if (nstorage == 0)
	{
		/* Page is completely empty, so just reset it quickly */
		((PageHeader) page)->pd_upper = pd_special;
	}
	else
	{
		/* Need to compact the page the hard way */
		itemIdSortData itemidbase[MaxHeapTuplesPerPage];
		itemIdSort	itemidptr = itemidbase;

		totallen = 0;
		for (i = 0; i < nline; i++)
		{
			lp = PageGetItemId(page, i + 1);
			if (ItemIdHasStorage(lp))
			{
				itemidptr->offsetindex = i;
				itemidptr->itemoff = ItemIdGetOffset(lp);
				if (itemidptr->itemoff < (int) pd_upper ||
					itemidptr->itemoff >= (int) pd_special)
					br_error("corrupted item pointer: %u",itemidptr->itemoff);
				itemidptr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
				totallen += itemidptr->alignedlen;
				itemidptr++;
			}
		}

		if (totallen > (Size) (pd_special - pd_lower))
			br_error("corrupted item lengths: total %u, available space %u",
							(unsigned int) totallen, pd_special - pd_lower);

		compactifyTuples(itemidbase, nstorage, page);
	}

	/* Set hint bit for PageAddItem */
	if (nunused > 0)
		PageSetHasFreeLinePointers(page);
	else
		PageClearHasFreeLinePointers(page);
}


