/*-------------------------------------------------------------------------
 *
 * Identification:
 * pageread.c
 *
 * Copyright:
 * Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
 * 
 * Authored by lichuancheng@highgo.com ,20180821
 * 
 * Abstract:
 * Code about read data page from disk.
 * 
 *-------------------------------------------------------------------------
 */
#include "pg_lightool.h"
#include "storage/bufpage.h"
#include "access/htup.h"
#include "access/htup_details.h"


#define		MAX_STEP_INPUT_SIZE			1024
#define		TUPLE_STATE_NOMAL_STR		"NOMAL"
#define		TUPLE_STATE_INVALID_STR		"INVALID"
#define		TUPLE_STATE_UNCERTAIN_STR	"UNCERTAIN"
#define		TUPLE_STATE_INVALID			1
#define		TUPLE_STATE_NOMAL			2
#define		TUPLE_STATE_UNCERTAIN		3


typedef struct TupleRecord
{
	
	int 			len;
	TransactionId	xmax;
	TransactionId	xmin;
	uint16			rblock;
	uint16			roffset;
	uint16			hoff;
	uint16			lp_off;
	uint16			lp_flag;
	bool			tuplestat;
}TupleRecord;

typedef struct ReadStatistics
{
	uint64			totalLen;
	int				itemNum;
	int				inuseitem;
	int				freeitem;

	uint64			inuseitemLen;
	uint64			freeitemLen;	
	uint64			sureuseLen;
	uint64			mabeuseLen;
	uint64			specialLen;
	uint64			pageheadLen;
	uint64			freeLen;
	bool			meanful;
}ReadStatistics;


typedef struct PageReadCtl
{
	
	TupleRecord			*tupleDescbuff;
	Page				page;
	int					maxItem;
	ReadStatistics		pageSta;
	
}PageReadCtl;

typedef struct Writebuff
{
	char	writebuff[BLCKSZ];
	char	*buffPtr;
	char	*buffStart;
}Writebuff;


PageReadCtl			prc;
ReadStatistics		fileSta;
ReadStatistics		totalSta;
Writebuff			wbuff;



static void pageDesc(Page page, uint32 curPage);
static void initPageReadCtl(Page page);
static void writePageRecord(uint32 curPage);
static void	statisticsSum(ReadStatistics *simple, ReadStatistics *total, bool totalisfile);
static void writeStatic(ReadStatistics *rsItem);
static void writeTotalStatic(void);
static void writeStr(char *str, bool flushnow, bool	cleanit, int	cleansize);


static void
writeStr(char *str, bool flushnow, bool	cleanit, int	cleansize)
{
	int		buffUseSize = 0, buffRemainSize = 0;
	int		strsize;
	
	if(!wbuff.buffPtr)
	{
		wbuff.buffPtr = wbuff.buffStart = wbuff.writebuff;
	}
	buffUseSize = wbuff.buffPtr -  wbuff.buffStart;
	buffRemainSize = BLCKSZ - buffUseSize;
	strsize = strlen(str);
	if(buffRemainSize < strsize || flushnow)
	{
		printf("%s", wbuff.buffStart);
		memset(&wbuff, 0 ,sizeof(Writebuff));
		wbuff.buffPtr = wbuff.buffStart = wbuff.writebuff;
	}
	sprintf(wbuff.buffPtr, "%s", str);
	wbuff.buffPtr += strsize;
	if(cleanit)
		memset(str, 0, cleansize);
}


static void
statisticsSum(ReadStatistics *simple, ReadStatistics *total, bool totalisfile)
{
	Assert(simple && total);

	if(!simple || !total || !simple->meanful)
		return;
	
	total->freeitem += simple->freeitem;
	total->freeitemLen += simple->freeitemLen;
	total->freeLen += simple->freeLen;

	total->inuseitem += simple->inuseitem;
	total->inuseitemLen += simple->inuseitemLen;
	total->itemNum += simple->itemNum;
	total->mabeuseLen += simple->mabeuseLen;
	total->pageheadLen += simple->pageheadLen;
	total->specialLen += simple->specialLen;
	total->sureuseLen += simple->sureuseLen;
/*	if(totalisfile)
		total->totalLen = BLCKSZ * RELSEG_SIZE;
	else*/
	total->totalLen += simple->totalLen;
	total->meanful = true;
	
}

static void
writeStatic(ReadStatistics *rsItem)
{
	char	str[MAX_STEP_INPUT_SIZE] = {0};

	if(!rsItem || !rsItem->meanful)
	{
		sprintf(str, "No statistics data.\n");
		writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
		return;
	}
	
	sprintf(str, "Free Items Num:       %d\n", rsItem->freeitem);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "Use Items Num:        %d\n", rsItem->inuseitem);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "Items Size:           %ld\n", rsItem->inuseitemLen + rsItem->freeitemLen); 
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "Page Head Size:       %ld\n", rsItem->pageheadLen); 
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "Page Special Size:    %ld\n", rsItem->specialLen);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "Maybe Tuple Use Size: %ld\n", rsItem->mabeuseLen);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "Sure Tuple Use Size:  %ld\n", rsItem->sureuseLen);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "Free Size:            %ld\n", rsItem->freeLen);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
}

static int
getRatio(ReadStatistics *rs)
{
	int		result = 0;
	uint64	usesize = 0;
	double	ratio = 0;
	Assert(rs);
	usesize = rs->inuseitemLen + rs->pageheadLen + rs->specialLen + rs->sureuseLen;
//printf("rs->meanful=%d\n",rs->meanful);
//printf("usesize=%ld, rs->totalLen=%ld\n",usesize,rs->totalLen);

	
	if(!rs->meanful)
		return 0;
	ratio = ((double)usesize / (double)rs->totalLen) * 100;
	
	result = (int)ratio;
	if(0 == result)
		result = 1;
	return result;
}


static void
writeTupleRecord(TupleRecord *tr, uint32 curPage)
{
	char	str[MAX_STEP_INPUT_SIZE] = {0};
	int		loop = 0;

	if(0 != brc.ratio)
		return;
	sprintf(str, "----------------------------------------------------------------------------\n");
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "LP   STATE       XMIN      XMAX      TUPLELEN  HOFF      OFFSET RLP\n");
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "----------------------------------------------------------------------------\n");
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	
	for(;loop < prc.pageSta.itemNum; loop++)
	{
		char	*tupleState = NULL;
		if(TUPLE_STATE_NOMAL == prc.tupleDescbuff[loop].tuplestat)
			tupleState = TUPLE_STATE_NOMAL_STR;
		else if(TUPLE_STATE_UNCERTAIN == prc.tupleDescbuff[loop].tuplestat)
			tupleState = TUPLE_STATE_UNCERTAIN_STR;
		else if(TUPLE_STATE_INVALID == prc.tupleDescbuff[loop].tuplestat)
			tupleState = TUPLE_STATE_INVALID_STR;
		sprintf(str, "%-5d%-12s%-10d%-10d%-10d%-10d%-7d(%d,%d)\n",
					loop,tupleState,prc.tupleDescbuff[loop].xmin, prc.tupleDescbuff[loop].xmax, 
					prc.tupleDescbuff[loop].len, prc.tupleDescbuff[loop].hoff, prc.tupleDescbuff[loop].lp_off,
					prc.tupleDescbuff[loop].rblock, prc.tupleDescbuff[loop].roffset);
		writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	}
}

static void
writeTotalStatic(void)
{
	char	str[MAX_STEP_INPUT_SIZE] = {0};
	uint32	ratio = 0;
	
	ratio = getRatio(&totalSta);
	sprintf(str, "###################################################\n");
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	sprintf(str, "RELFILENODE:%u Use Ratio:%d%%\n", brc.rfn.relNode, ratio);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	if(brc.detail)
	{
		writeStatic(&totalSta);
	}
	sprintf(str, "###################################################\n");
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);

}


static void
writePageRecord(uint32 curPage)
{
	char	str[MAX_STEP_INPUT_SIZE] = {0};
	uint32	ratio = 0;
	uint32	totalBlkno = 0;

	ratio = getRatio(&prc.pageSta);

	if(0 != brc.ratio && ratio > brc.ratio)
		return;

	if(PAGEREAD_SHOWLEVEL_PAGE == brc.showlevel || PAGEREAD_SHOWLEVEL_ALL ==  brc.showlevel)
	{
		if(brc.detail)
		{
			sprintf(str, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
			writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
		}

		totalBlkno = (brc.curFileNo - 1) * RELSEG_SIZE + curPage;
		if(0 == brc.curFileNo - 1)
		{
			sprintf(str, "PAGE:%u(%u) OF FILE:%u use ratio:%u%%\n", curPage, totalBlkno, brc.rfn.relNode, ratio);
			writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
		}
		else
		{
			sprintf(str, "PAGE:%u(%u) OF FILE:%u.%u use ratio:%u%%\n", curPage, totalBlkno, brc.rfn.relNode, brc.curFileNo - 1, ratio);
			writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
		}
		if(brc.detail)
		{
			sprintf(str, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
			writeStatic(&prc.pageSta);
		}
	}

	if(CUR_KIND_PAGEINSPECT == brc.curkind)
	{
		writeTupleRecord(prc.tupleDescbuff,curPage);
	}
}

static void
writeFileRecord(void)
{
	char	str[MAX_STEP_INPUT_SIZE] = {0};
	uint32	ratio = 0;

	ratio = getRatio(&fileSta);
	if(0 != brc.ratio && ratio > brc.ratio)
		return;
	if(brc.detail || PAGEREAD_SHOWLEVEL_ALL == brc.showlevel)
	{
		sprintf(str, "---------------------------------------------------\n");
		writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	}
	sprintf(str, "FILE:%u.%u use ratio:%u%%\n", brc.rfn.relNode ,brc.curFileNo - 1, ratio);
	writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	if(brc.detail)
	{
		writeStatic(&fileSta);
	}
	if(brc.detail || PAGEREAD_SHOWLEVEL_ALL == brc.showlevel)
	{
		sprintf(str, "---------------------------------------------------\n");
		writeStr(str, false, true, MAX_STEP_INPUT_SIZE);
	}
}


static void
initPageReadCtl(Page page)
{
	TupleRecord	*buffTemp = NULL;
	int			itemMax = 0;

	buffTemp = prc.tupleDescbuff;
	itemMax = prc.maxItem;
	memset(&prc, 0, sizeof(PageReadCtl));
	prc.pageSta.itemNum = PageGetMaxOffsetNumber(page);
	prc.maxItem = itemMax;
	if(prc.maxItem < prc.pageSta.itemNum)
	{
		prc.maxItem = prc.pageSta.itemNum;
		if(buffTemp)
			pfree(buffTemp);
		prc.tupleDescbuff = (TupleRecord*)palloc(prc.maxItem * sizeof(TupleRecord));
	}
	else
	{
		memset((char*)buffTemp, 0, prc.maxItem * sizeof(TupleRecord));
		prc.tupleDescbuff = buffTemp;
	}
	
}


static void
pageDesc(Page page, uint32 curPage)
{
	PageHeader		phd = (PageHeader)page;
	int				loop = 0;
	ItemId			id;
	
	initPageReadCtl(page);
	if(0 == PageXLogRecPtrGet(phd->pd_lsn))
	{
		return;
	}
	prc.pageSta.meanful = true;
	prc.pageSta.totalLen = BLCKSZ;
	for(;loop < prc.pageSta.itemNum; loop++)
	{

		HeapTupleHeader		tuphdr = NULL;
		uint16				lp_offset = 0;
		uint16				lp_flags = 0;
		uint16				lp_len = 0;
		uint16				headLen = 0;
		int					tupleStat = 0;
		TransactionId		xmax = 0;
		TransactionId		xmin = 0;
		uint16				rblock = 0;
		uint16				roffset = 0;

		
		id = PageGetItemId(page, loop + 1);
		lp_offset = ItemIdGetOffset(id);
		lp_flags = ItemIdGetFlags(id);
		lp_len = ItemIdGetLength(id);
		if(0 != lp_len)
		{
			tuphdr = (HeapTupleHeader) PageGetItem(page, id);
			xmax = HeapTupleHeaderGetRawXmax(tuphdr);
			xmin = HeapTupleHeaderGetXmin(tuphdr);
			roffset = ItemPointerGetOffsetNumber(&tuphdr->t_ctid) - 1;
			rblock = ItemPointerGetBlockNumber(&tuphdr->t_ctid);
			headLen = tuphdr->t_hoff;
		}
		
		if(0 == lp_len)
		{
			prc.pageSta.freeitem++;
			tupleStat = TUPLE_STATE_INVALID;
		}
		else if(0 != xmax)
		{
			prc.pageSta.inuseitem++;
			prc.pageSta.mabeuseLen +=  lp_len;
			tupleStat = TUPLE_STATE_UNCERTAIN;
		}
		else
		{
			prc.pageSta.inuseitem++;
			prc.pageSta.sureuseLen +=  lp_len;
			tupleStat = TUPLE_STATE_NOMAL;
		}
		prc.tupleDescbuff[loop].len = lp_len;
		prc.tupleDescbuff[loop].lp_flag = lp_flags;
		prc.tupleDescbuff[loop].lp_off = lp_offset;
		prc.tupleDescbuff[loop].tuplestat = tupleStat;
		prc.tupleDescbuff[loop].xmax = xmax;
		prc.tupleDescbuff[loop].xmin = xmin;
		prc.tupleDescbuff[loop].rblock = rblock;
		prc.tupleDescbuff[loop].roffset = roffset;
		prc.tupleDescbuff[loop].hoff = headLen;

	}
	
	prc.pageSta.pageheadLen = SizeOfPageHeaderData;
	prc.pageSta.specialLen = BLCKSZ - phd->pd_special;
	prc.pageSta.inuseitemLen = prc.pageSta.inuseitem * sizeof(ItemIdData);
	prc.pageSta.freeitemLen = prc.pageSta.freeitem * sizeof(ItemIdData);
	prc.pageSta.freeLen = BLCKSZ - prc.pageSta.pageheadLen - prc.pageSta.specialLen - 
		prc.pageSta.inuseitemLen - prc.pageSta.freeitemLen - prc.pageSta.mabeuseLen - prc.pageSta.sureuseLen;

	if((PAGEREAD_SHOWLEVEL_PAGE <= brc.showlevel))
		writePageRecord(curPage);
}


void
startDataDis(void)
{
	FILE	*fp = NULL;
	char	filePath[MAXPGPATH] = {0};
	char	outPath[MAXPGPATH] = {0};
	char	page[BLCKSZ] = {0};
	int32	readsize = 0;
	bool	reachLastFile = false;
	int32	curPage = 0;
	

	sprintf(outPath, "%s/%s", brc.place, DATA_DIS_RESULT_FILE);
	memset(&prc, 0 , sizeof(PageReadCtl));
	memset(&totalSta, 0 , sizeof(ReadStatistics));
	memset(&wbuff, 0, sizeof(Writebuff));
	printf("Start Datadis Analyse...\n");
	freopen(outPath ,"w",stdout);
	while(!reachLastFile)
	{
		bool	reachEveryLastPage = false;
		
		memset(filePath, 0, MAXPGPATH);
		if(0 == brc.curFileNo)
		{
			sprintf(filePath, "%s/%u",brc.relpath,brc.rfn.relNode);


			brc.curFileNo++;
		}
		else
			sprintf(filePath, "%s/%u.%u",brc.relpath,brc.rfn.relNode, brc.curFileNo++);

		
		fp = fopen(filePath,"rb");
		if(!fp)
		{
			if (errno == ENOENT)
			{
				reachLastFile = true;
			}
			else
				br_error("can not open file %s",filePath);
		}
		else
		{
			curPage = 0;
			memset(&fileSta, 0 , sizeof(ReadStatistics));
			while(!reachEveryLastPage)
			{
				memset(page, 0, BLCKSZ);
				readsize = fread(page, 1, BLCKSZ, fp);
				if(0 == readsize)
				{
					reachEveryLastPage = true;
				}
				else if(BLCKSZ != readsize)
				{
					br_error("fail to read file %s",filePath);
				}
				else
				{
					pageDesc(page, curPage);

					statisticsSum(&prc.pageSta ,&fileSta, true);
					curPage++;
				}
			}
			if(PAGEREAD_SHOWLEVEL_FILE == brc.showlevel || PAGEREAD_SHOWLEVEL_ALL ==  brc.showlevel)
				writeFileRecord();
			statisticsSum(&fileSta, &totalSta, false);
			fclose(fp);
			fp = NULL;
		}
		
	}
	writeTotalStatic();
	writeStr("\n", true, false, 0);
	freopen("/dev/tty","w",stdout);
	printf("Datadis Analyse Success.\n");
	if(prc.tupleDescbuff)
		free(prc.tupleDescbuff);
}

void
startInspect(void)
{
	FILE	*fp = NULL;
	char	filePath[MAXPGPATH] = {0};
	char	outPath[MAXPGPATH] = {0};
	char	page[BLCKSZ] = {0};
	int32	readsize = 0;
	uint32	innerBlkno = 0;
	uint64	filesize = 0;

	sprintf(outPath, "%s/%s", brc.place, PAGE_INS_RESULT_FILE);
	memset(&prc, 0 , sizeof(PageReadCtl));
	memset(&wbuff, 0, sizeof(Writebuff));
	printf("Start Page Inspect...\n");
	freopen(outPath ,"w",stdout);

	
	memset(filePath, 0, MAXPGPATH);
	if(0 == brc.curFileNo)
	{
		sprintf(filePath, "%s/%u",brc.relpath,brc.rfn.relNode);
		brc.curFileNo++;
	}
	else
		sprintf(filePath, "%s/%u.%u",brc.relpath,brc.rfn.relNode, brc.curFileNo++);
	if(!fileExist(filePath))
		br_error("Page \'%u\' is in \"%s\" which doen not exist",brc.recoverBlock[0],filePath);
	filesize = getfileSize(filePath);
	innerBlkno = MAG_BLOCK_BLKNO(brc.recoverBlock[0]);
	if((innerBlkno) * BLCKSZ >= filesize)
		br_error("BlkNo \'%u\' is in \"%s\" but it's size is not enougnt",brc.recoverBlock[0],filePath);
	fp = fopen(filePath,"rb");
	if(!fp)
	{
		br_error("can not open file %s",filePath);
	}
	else
	{
		fseek(fp, innerBlkno * BLCKSZ, SEEK_SET);
		readsize = fread(page, 1, BLCKSZ, fp);
		if(BLCKSZ != readsize)
		{
			br_error("Fail to read file \"%s\"",filePath);
		}
		else
		{
			pageDesc(page, innerBlkno);
		}
	}
	writeStr("\n", true, false, 0);
	freopen("/dev/tty","w",stdout);
	printf("Page Inspect Success.\n");
}
/*
{
	int		loop = 0;
	char	filePath[MAXPGPATH] = {0};
	bool	getreplace = false;;

	for (; loop < brc.rbNum; loop++)
	{
		uint32		blknoIneveryFile = 0;
		if (brc.pageArray[loop])
		{
			memset(filePath, 0, MAXPGPATH);
			getTarBlockPath(filepath, loop);
			blknoIneveryFile = MAG_BLOCK_BLKNO(brc.recoverBlock[loop]);
			if (brc.debugout)
				br_elog("recover file %s pagenoInfile %u", filePath, blknoIneveryFile);
			backupOriFile(filePath);
			replaceFileBlock(filePath, blknoIneveryFile, brc.pageArray[loop]);
			getreplace = true;
		}
	}
	if(!getreplace)
		br_elog("Do nothing page replace.");
}
*/

