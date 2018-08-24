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


#define pageSetLSN(page, lsn) \
	PageXLogRecPtrSet(((PageHeader) (page))->pd_lsn, lsn);

typedef struct itemIdSortData
{
	uint16		offsetindex;	/* linp array index */
	int16		itemoff;		/* page offset of item data */
	uint16		alignedlen;		/* MAXALIGN(item data len) */
} itemIdSortData;
typedef itemIdSortData *itemIdSort;


static bool xLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileNode *rnode, ForkNumber *forknum, BlockNumber *blknum);
static void heap_recoverDeleteRecord(XLogReaderState *record, BlockNumber blknum);
static void recoverImageFromRecord(XLogReaderState *record, BlockNumber blknum,int pageIndex,int block_id);
static void fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2);
static void heap_recoverUpdateRecord(XLogReaderState *record, BlockNumber blknum, bool hot_update);
static Page pageInit(Size specialSize);
static void heap_recoverConfirmRecord(XLogReaderState *record, BlockNumber blknum);
static void heap_recoverLockRecord(XLogReaderState *record, BlockNumber blknum);
static void heap_recoverInplaceRecord(XLogReaderState *record, BlockNumber blknum);
static void heep2_recoverMutiinsertRecord(XLogReaderState *record, BlockNumber blknum);
static void heep2_recoverCleanRecord(XLogReaderState *record, BlockNumber blknum);		
static void heep2_recoverFreezePageRecord(XLogReaderState *record, BlockNumber blknum);				
static void heep2_recoverVisibleRecord(XLogReaderState *record, BlockNumber blknum);	
static void heep2_recoverLockRecord(XLogReaderState *record, BlockNumber blknum);
static void heap_recoverInsertRecord(XLogReaderState *record, BlockNumber blknum);
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
static void getHeapInsertRecordTuple(XLogReaderState *record, HeapTupleHeader htup, BlockNumber blknum, 
						uint32 *newlen, xl_heap_insert	*xlrec);
/*static bool getHeapDeleteRecordTuple(XLogReaderState *record, HeapTupleHeader *htup_old, BlockNumber blknum, 
						uint32 *newlen, xl_heap_delete	*xlrec);*/



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
			heap_recoverInsertRecord(record, blknum);
		}
		else if(XLOG_HEAP_DELETE == info)
		{
			heap_recoverDeleteRecord(record, blknum);
		}
		else if (XLOG_HEAP_UPDATE == info)
		{
			heap_recoverUpdateRecord(record, blknum, false);
		}
		else if (XLOG_HEAP_HOT_UPDATE == info)
		{
			heap_recoverUpdateRecord(record, blknum, true);
		}
		else if(XLOG_HEAP_CONFIRM == info)
		{
			heap_recoverConfirmRecord(record, blknum);
		}
		else if(XLOG_HEAP_LOCK == info)
		{
			heap_recoverLockRecord(record, blknum);
		}
		else if(XLOG_HEAP_INPLACE == info)
		{
			heap_recoverInplaceRecord(record, blknum);
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
			/*getHeapInsertRecordTuple(XLogReaderState *record, HeapTupleHeader htup, BlockNumber blknum, 
						uint32 *newlen, xl_heap_insert	*xlrec)*/
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
		if(brc.recoverBlock[loop] == blknum)
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
getHeapInsertRecordTuple(XLogReaderState *record, HeapTupleHeader htup, BlockNumber blknum, 
						uint32 *newlen, xl_heap_insert	*xlrec)
{
	xl_heap_header	xlhdr;
	Size			datalen = 0;
	ItemPointerData target_tid;	
	char			*data = NULL;


	memset(&xlhdr, 0, sizeof(xl_heap_header));
	memset(&target_tid, 0, sizeof(ItemPointerData));
	
	ItemPointerSetBlockNumber(&target_tid, blknum);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);
	data = xLogRecGetBlockData(record, 0, &datalen);

	*newlen = datalen - SizeOfHeapHeader;
	Assert(datalen > SizeOfHeapHeader && *newlen <= MaxHeapTupleSize);
	memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
	data += SizeOfHeapHeader;
	
	MemSet((char *) htup, 0, SizeofHeapTupleHeader);
	memcpy((char *) htup + SizeofHeapTupleHeader,data,*newlen);
	*newlen += SizeofHeapTupleHeader;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = target_tid;
}

/*static bool
getHeapDeleteRecordTuple(XLogReaderState *record, HeapTupleHeader *htup_old, BlockNumber blknum, 
						uint32 *newlen, xl_heap_delete	*xlrec)
{
	xl_heap_header	xlhdr;
	ItemPointerData target_tid;	
	char			*data = NULL;
	HeapTupleHeader htup = NULL;

	htup = *htup_old;
	if(!(XLH_DELETE_CONTAINS_OLD & xlrec->flags))
		return false;

	ItemPointerSetBlockNumber(&target_tid, blknum);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	data = (char *) xlrec + SizeOfHeapDelete;
	*newlen = XLogRecGetDataLen(record) - SizeOfHeapDelete;
	memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
	data += SizeOfHeapHeader;
	memcpy((char *) htup + SizeofHeapTupleHeader,data,*newlen);
	*newlen += SizeofHeapTupleHeader;
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = target_tid;
	return true;
}

static void
getHeapUpdateRecordTuple(XLogReaderState *record, HeapTupleHeader *htup_new, HeapTupleHeader *htup_old,
								BlockNumber blknum_new, BlockNumber blknum_old, xl_heap_update *xlrec,
								uint32 *newlen,uint32 *oldlen)
{
	xl_heap_header			*xlhdr_new = NULL;
	xl_heap_header			*xlhdr_old = NULL;
	ItemPointerData 		target_tid_new;
	ItemPointerData 		target_tid_old;
	char 					*recdata = NULL;
	Size					datalen = 0;
	char	   				*newp = NULL;
	HeapTupleHeader			htuptemp = NULL;



	ItemPointerSet(&target_tid_new, blknum_new, xlrec->new_offnum);
	ItemPointerSet(&target_tid_old, blknum_old, xlrec->old_offnum);

	if(xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE)
	{
		
		htuptemp = *htup_new;
		recdata = xLogRecGetBlockData(record, 0, &datalen);
		*newlen = datalen - SizeOfHeapHeader;
		xlhdr_new = (xl_heap_header *)recdata;
		recdata += SizeOfHeapHeader;

		newp = (char *) (htuptemp) + offsetof(HeapTupleHeaderData, t_bits);
		memcpy(newp, recdata, *newlen);
		recdata += *newlen;
		newp += *newlen;
		htuptemp->t_infomask2 = xlhdr_new->t_infomask2;
		htuptemp->t_infomask = xlhdr_new->t_infomask;


		htuptemp->t_hoff = xlhdr_new->t_hoff;
		HeapTupleHeaderSetXmin(htuptemp, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htuptemp, FirstCommandId);
		htuptemp->t_ctid = target_tid_new;
		
	}

	if(xlrec->flags & XLH_UPDATE_CONTAINS_OLD)
	{
		htuptemp = *htup_old;
		recdata = XLogRecGetData(record) + SizeOfHeapUpdate;
		datalen = XLogRecGetDataLen(record) - SizeOfHeapUpdate;

		xlhdr_old  = (xl_heap_header *)recdata;
		recdata += SizeOfHeapHeader;
		newp = (char *) htuptemp + offsetof(HeapTupleHeaderData, t_bits);
		memcpy(newp, recdata, datalen);
		newp += datalen ;
		recdata += datalen ;

		htuptemp->t_infomask2 = xlhdr_old->t_infomask2;
		htuptemp->t_infomask = xlhdr_old->t_infomask;

		htuptemp->t_hoff = xlhdr_old->t_hoff;
		HeapTupleHeaderSetXmin(htuptemp, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htuptemp, FirstCommandId);
		htuptemp->t_ctid = target_tid_old;
	}
	
}
*/

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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
		
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
heap_recoverInsertRecord(XLogReaderState *record, BlockNumber blknum)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPage = NULL;
	xl_heap_insert	*xlrec = NULL;
	int				tarIndex = 0;
	OffsetNumber	pageAddResult = 0;
	bool			findInArray = false;
	HeapTupleHeader htup = NULL;
	bool			image = false;
	uint32			newlen = 0;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}tbuf;
	
	
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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
<<<<<<< HEAD
		xlrec = (xl_heap_insert *) XLogRecGetData(record);
		ItemPointerSetBlockNumber(&target_tid, blknum);
		ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);
		data = xLogRecGetBlockData(record, 0, &datalen);

		newlen = datalen - SizeOfHeapHeader;
		Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
		memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
		data += SizeOfHeapHeader;
=======
>>>>>>> origin/develop
		htup = &tbuf.hdr;
		xlrec = (xl_heap_insert *) XLogRecGetData(record);
		getHeapInsertRecordTuple(record, htup, blknum, &newlen, xlrec);
		pageAddResult = pageAddItemExtended(targetPage, (Item) htup, newlen, xlrec->offnum);
		pageSetLSN(targetPage, lsn);
		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(targetPage);
		if(InvalidOffsetNumber == pageAddResult)
			br_error("failed to add tuple");
	}
}


static void
heap_recoverDeleteRecord(XLogReaderState *record, BlockNumber blknum)
{
	XLogRecPtr		lsn = record->EndRecPtr;
	Page			targetPage = NULL;
	int				tarIndex = 0;
	xl_heap_delete  *xlrec = NULL;
	xl_heap_header  xlhdr;
	ItemPointerData target_tid;	
	TransactionId	xid = 0, pd_prune_xid = 0;
	HeapTupleHeader htup = NULL;
	bool			image = false;
	bool			findInArray = false;
	ItemId			lp = NULL;
	
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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
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
heap_recoverUpdateRecord(XLogReaderState *record, BlockNumber blknum, bool hot_update)
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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
		
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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
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
heap_recoverInplaceRecord(XLogReaderState *record, BlockNumber blknum)
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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
		
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
heap_recoverConfirmRecord(XLogReaderState *record, BlockNumber blknum)
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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
		
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
heap_recoverLockRecord(XLogReaderState *record, BlockNumber blknum)
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
		{
			br_elog("Get a record now,but we can not find a base page for page %u.",blknum);
			return;
		}
		
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


