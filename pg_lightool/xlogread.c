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



#define MAX_ERRORMSG_LEN 1000
#define InvalidRepOriginId 0


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
		if (0 == record->xl_tot_len)
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
			if (0 == total_len)
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
