/*-------------------------------------------------------------------------
 *
 * Identification:
 * util.c
 * 
 * Copyright:
 * Copyright (c) 2017-2020, HighGo Software Co.,Ltd. All right reserved
 * 
 * Authored by lichuancheng@highgo.com ,20180821
 *
 * Abstract:
 * Some util tool function of the pg_lightool.
 * 
 *-------------------------------------------------------------------------
 */
 #include "pg_lightool.h"
 #include "catalog/catalog.h"
 #include <limits.h>
 #include <time.h>

 #define INFINITE_STR		"INFINITE"

void
getCurTime(char	*curtime)
{
	time_t t;
    struct tm * lt;
    time (&t);
	lt = localtime (&t);
	Assert(curtime);
	sprintf(curtime,"%04d%02d%02d%02d%02d%02d",lt->tm_year+1900,lt->tm_mon+1,lt->tm_mday,
										lt->tm_hour,lt->tm_min,lt->tm_sec);
}

bool
parse_uint32(const char *value, uint32 *result)
{
	uint64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = UINT_MAX;
		return true;
	}

	errno = 0;
	val = strtoul(value, &endptr, 0);
	if (endptr == value || *endptr)
		return false;

	if (errno == ERANGE || val != (uint64) ((uint32) val))
		return false;

	*result = val;

	return true;
}

void
getDataFile(char *relnode)
{
	char	*datafileEnableStr = "0123456789/";
	Oid		tbsoid = 0, dboid = 0, relNode = 0, tempoid = 0;
	int		ssresult = 0, loop = 0;
	
	for (; loop < strlen(relnode); loop++)
	{
		if (!strchr(datafileEnableStr,relnode[loop]))
		{
			printf("Invalid datafile argument \"%s\"\n",relnode);
			error_exit();
		}
	}
	ssresult = sscanf(relnode,"%u/%u/%u/%u",&tbsoid, &dboid, &relNode, &tempoid);
	if (3 != ssresult)
	{
		printf("Invalid datafile argument \"%s\"\n",relnode);
		error_exit();
	}
	if (0 == tbsoid)
		tbsoid = PG_DEFAULT_TBS_OID;
	brc.rfn.spcNode = tbsoid;
	brc.rfn.dbNode = dboid;
	brc.rfn.relNode = relNode;
	/*CHANGE*/
	/*need check for rel kind*/

}

void
getRelpath(void)
{
	char	basePath[MAXPGPATH] = {0};
	
	if (PG_DEFAULT_TBS_OID == brc.rfn.spcNode || 0 ==  brc.rfn.spcNode)
	{
		sprintf(basePath, "%s/base/%u", brc.pgdata, brc.rfn.dbNode);
	}
	else if (PG_GLOBLE_TBS_OID == brc.rfn.spcNode)
	{
		sprintf(basePath, "%s/database/global", brc.pgdata);
	}
	else if (FirstNormalObjectId <= brc.rfn.spcNode)
	{

		sprintf(basePath, "%s/pg_tblspc/%u/%s/%u", brc.pgdata, brc.rfn.spcNode,TABLESPACE_VERSION_DIRECTORY,
						brc.rfn.dbNode);
	}
	else
	{
		br_error("Wrong tablespace oid");
	}
	sprintf(brc.relpath, "%s", basePath);
}

void
getRecoverBlock(char *block)
{
	char	*blockEnableStr = "0123456789,";
	char	tempBkockBuff[100] = {0};
	char	*lb = NULL, *cb = NULL, *currStart = NULL;
	int		bnum = 0;
	int		blockLength = 0;
	int		loop = 0, loop1 = 0;

	Assert(block);
	blockLength = strlen(block);
	if (0 >= blockLength)
		goto error_condition;

	if(0 == strcmp("-1", block))
	{
		/*ȫ���ָ�*/
		brc.ifwholerel = true;
		return;
	}
	if (',' == block[0] || ',' == block[blockLength - 1])
		goto error_condition;
	currStart = block;
	for (; loop < blockLength; loop++)
	{
		if (!strchr(blockEnableStr,block[loop]))
			goto error_condition;
		if (',' == block[loop])
		{
			lb = cb;
			cb = block + loop;
			
			if (lb && (lb + 1 == cb))
				goto error_condition;
			memset(tempBkockBuff, 0, 11);
			memcpy(tempBkockBuff, currStart, cb - currStart);
			currStart = cb + 1;
			if (!parse_uint32(tempBkockBuff, &brc.recoverBlock[bnum]))
			{
				printf("Wrong block input \"%s\"\n", tempBkockBuff);
				error_exit();
			}
			bnum++;
		}
	}
	/*mental the last number*/
	memset(tempBkockBuff, 0, 11);
	memcpy(tempBkockBuff, currStart, (block + blockLength - 1) - currStart + 1);
	if (!parse_uint32(tempBkockBuff, &brc.recoverBlock[bnum]))
	{
		printf("Wrong block input \"%s\"\n", tempBkockBuff);
		error_exit();
	}
	bnum++;
	/* There should not be same num in block argment */
	for (loop = 0; loop < bnum; loop++)
	{
		for (loop1 = loop + 1; loop1 < bnum; loop1++)
		{
			if (brc.recoverBlock[loop1] == brc.recoverBlock[loop])
				goto error_condition;
		}
	}
	
	brc.rbNum = bnum;
	if (RECOVER_BLOCK_MAX < brc.rbNum)
	{
		printf("The number of block can not be greater than \"%d\"\n", RECOVER_BLOCK_MAX);
		error_exit();
	}
	return;
error_condition:
	{
		printf("Invalid block argument \'%s\'\n", block);
		error_exit();
	}
}


void
getTarBlockPath(char *filepath, char *relpath,int index)
{
	uint32		relFileNum = 0;

	Assert(filepath);
	relFileNum = MAG_BLOCK_FILENO(brc.recoverBlock[index]);
	if (0 != relFileNum)
		sprintf(filepath, "%s/%u.%u", relpath, brc.rfn.relNode, relFileNum);
	else
		sprintf(filepath, "%s/%u", relpath, brc.rfn.relNode);
}

void
getTarBlockPath_1(char *filepath, uint32 blockno)
{
	uint32	relFileNum = 0;

	relFileNum = MAG_BLOCK_FILENO(blockno);
	
	Assert(filepath);
	//fileno = replaceFileBlock(filepath, blknoIneveryFile, page);
	if(0 == relFileNum)
		sprintf(filepath, "%s/%u", brc.reltemppath, brc.rfn.relNode);
	else
		sprintf(filepath, "%s/%u.%d", brc.reltemppath, brc.rfn.relNode, relFileNum);
}

void
error_exit(void)
{
	exit(1);
}

void
nomal_exit(void)
{
	exit(0);
}

time_t
timestamptz_to_timet(TimestampTz t)
{
	time_t	result;

	result = (time_t) (t / USECS_PER_SEC +
						  ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
	return result;
}
