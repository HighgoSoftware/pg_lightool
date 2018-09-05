/*-------------------------------------------------------------------------
 *
 * pg_blockrecover.c - recover bad blocks
 *		  pg_blockrecover.c
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>


#include "pg_lightool.h"

#define	PG_LIGHTOOL_VER		"0.1"


LightoolCtl		brc;

static struct option long_options[] = {
	{"help", no_argument, NULL, '?'},
	{"version", no_argument, NULL, 'V'},
	{"log", no_argument, NULL, 'l'},
	{"relnode", required_argument, NULL, 'f'},
	{"block", required_argument, NULL, 'b'},
	{"waldir", required_argument, NULL, 'w'},
	{"immediate", no_argument, NULL, 'i'},
	{"place", required_argument, NULL, 'p'},
	{"detail", no_argument, NULL, 'd'},
	{"grade", required_argument, NULL, 'g'},
	{"pgdata", required_argument, NULL, 'D'},
	{"small", required_argument, NULL, 's'},
	{NULL, 0, NULL, 0}
};
		
static void do_help(void);
static void do_advice_lightool(void);
static void arguMentParser(int argc, char *argv[]);
static void blockRecoverArguCheck(void);
static void pro_on_exit(void);
static void getProname(char *argu);
static void initializeLightool(void);
static void startXlogRead(void);
static void mentalRecord(void);
static void cleanSpace(void);
static void replacePages(void);
static void do_blockrecover(void);
static void do_walshow(void);
static void do_datadis(void);
static void do_pageinspect(void);
static void walshowArguCheck(void);
static void blockRecoverArguCheck(void);
static void datadisArguCheck(void);
static void pageinspectArguCheck(void);


void
br_error(const char *fmt,...)
{
	va_list		args;

	fflush(stdout);

	fprintf(stderr, _("%s: ERROR:  "), brc.lightool);
	va_start(args, fmt);
	vfprintf(stderr, _(fmt), args);
	va_end(args);
	fputc('\n', stderr);

	exit(EXIT_FAILURE);
}

void
br_elog(const char *fmt,...)
{
	va_list		args;

	fflush(stdout);
	va_start(args, fmt);
	vfprintf(stderr, _(fmt), args);
	va_end(args);
	fputc('\n', stderr);
}


static void
do_advice_lightool(void)
{
	printf("Try \"%s --help\" for more information.\n", brc.lightool);
}

static void
do_help(void)
{
	printf("%s is a light tool of postgres\n\n", brc.lightool);
	printf("Usage:\n");
	printf("  %s OPTION blockrecover\n", brc.lightool);
	printf("  %s OPTION walshow\n", brc.lightool);
	printf("  %s OPTION datadis\n", brc.lightool);
	printf("  %s OPTION pageinspect\n", brc.lightool);

	
	printf("\nCommon Options:\n");
	printf("  -V, --version                         output version information, then exit\n");
	printf("\nFor blockrecover:\n");
	printf("  -l, --log                             whether to write a debug info\n");
	printf("  -f, --relnode=spcid/dbid/relfilenode specify files to repair\n");
	printf("  -b, --block=n1[,n2,n3]                specify blocks to repair(10 limit)\n");
	printf("  -w, --walpath=walpath                 wallog read from\n");
	printf("  -D, --pgdata=datapath                 data dir of database\n");
	printf("  -i, --immediate			            does not do a backup for old file\n");

	printf("\nFor datadis:\n");
	printf("  -f, --relnode=spcid/dbid/relfilenode specify files to dis\n");
	printf("  -D, --pgdata=datapath                 data dir of database\n");
	printf("  -p, --place=outPtah                   place where store the result\n");
	printf("  -g, --grade=level                     1 to show file level(default);\n");
	printf("                                        2 to show page level;\n");
	printf("                                        3 to show all;\n");
	printf("  -d, --detail		                    wether to show detail info\n");
	printf("  -s, --small		                    show which ratio small than it\n");

	printf("\nFor pageinspect:\n");
	printf("  -f, --relnode=spcid/dbid/relfilenode specify files to inspect\n");
	printf("  -D, --pgdata=datapath                 data dir of database\n");
	printf("  -p, --place=outPtah                   place where store the result\n");
	printf("  -b, --block=blkno                     specify blocks to inspect\n");
}

static void
blockRecoverArguCheck(void)
{
	if (!brc.relnode || !brc.walpath || !brc.blockstr || !brc.pgdata)
	{
		br_error("argument relnode,walpath,block,pgdata is necessary.\n");
	}
	
	/*datafile check*/
	getDataFile(brc.relnode);
		
	/*walpath check*/
	if (!walPathCheck(brc.walpath))
	{
		br_error("Invalid walpath argument \"%s\"\n", brc.walpath);
	}
	/*blockstr check*/
	getRecoverBlock(brc.blockstr);

	/*pgdata check*/
	checkPgdata();
}

static void
walshowArguCheck(void)
{
	if (!brc.walpath)
	{
		br_error("argument walpath is necessary.\n");
	}
	
	/*walpath check*/
	if (!walPathCheck(brc.walpath))
	{
		br_error("Invalid walpath argument \"%s\"\n", brc.walpath);
	}
}

static void
datadisArguCheck(void)
{
	if (!brc.relnode || !brc.pgdata)
	{
		br_error("argument relnode,pgdata is necessary.\n");
	}
	/*datafile check*/
	getDataFile(brc.relnode);
	/*pgdata check*/
	checkPgdata();
	/*place check*/
	checkPlace();

	if (0 == brc.showlevel)
		brc.showlevel = 1;

	if (PAGEREAD_SHOWLEVEL_ERROR == brc.showlevel)
		br_error("unsupposed show level.\n");
	if (brc.ratiostr)
	{
		if (!parse_uint32(brc.ratiostr, &brc.ratio))
			br_error("can not parser ratio argument \"%s\".\n",brc.ratiostr);
	}
	else
		brc.ratio = 0;
	brc.curFileNo = 0;
}


/*
printf("\nFor pageinspect:\n");
	printf("  -f, --relnode=spcid/dbid/relfilenode specify files to inspect\n");
	printf("  -D, --pgdata=datapath                 data dir of database\n");
	printf("  -p, --place=outPtah                   place where store the result\n");
	printf("  -b, --block=blkno                     specify blocks to inspect\n");

*/
static void
pageinspectArguCheck(void)
{
	if (!brc.relnode || !brc.blockstr || !brc.pgdata || !brc.place)
	{
		br_error("argument relnode,walpath,block,pgdata,place is necessary.\n");
	}
	/*datafile check*/
	getDataFile(brc.relnode);
	/*pgdata check*/
	checkPgdata();
	/*place check*/
	checkPlace();
	/*blockstr check*/
	getRecoverBlock(brc.blockstr);
	if (1 != brc.rbNum)
	{
		br_error("inspect single page once a time.\n");
	}
	brc.curFileNo = MAG_BLOCK_FILENO(brc.recoverBlock[0]);
	brc.showlevel = PAGEREAD_SHOWLEVEL_PAGE;
	brc.detail = true;
}


static void
arguMentParser(int argc, char *argv[])
{
	int			option_index = 0;
	char		c = 0;
	bool		commandGet = false;

	if (1 == argc)
	{
		do_advice_lightool();
		error_exit();
	}

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			do_help();
			nomal_exit();
		}
		else if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			printf("pg_lightool version %s\n",PG_LIGHTOOL_VER);
			nomal_exit();
		}
	}
	optind = 1;
	while (optind < argc)
	{
		while (-1 != (c = getopt_long(argc, argv, "?Vlf:b:w:D:ip:dg:s:",long_options, &option_index)))
		{
			switch (c)
			{
				case 'l':
					brc.debugout = true;
					break;
				case 'f':
					brc.relnode = (char*)strdup(optarg);
					break;
				case 'b':
					brc.blockstr = (char*)strdup(optarg);
					break;
				case 'w':
					brc.walpath = (char*)strdup(optarg);
					break;
				case 'D':
					brc.pgdata = (char*)strdup(optarg);
					break;
				case 'i':
					brc.immediate = true;
					break;
				case 'p':
					brc.place = (char*)strdup(optarg);
					break;
				case 'd':
					brc.detail = true;
					break;
				case 's':
					brc.ratiostr = (char*)strdup(optarg);
					break;
				case 'g':
					if (0 == strcmp("1",optarg))
						brc.showlevel = PAGEREAD_SHOWLEVEL_FILE;
					else if (0 == strcmp("2",optarg))
						brc.showlevel = PAGEREAD_SHOWLEVEL_PAGE;
					else if (0 == strcmp("3",optarg))
						brc.showlevel = PAGEREAD_SHOWLEVEL_ALL;
					else
						brc.showlevel = PAGEREAD_SHOWLEVEL_ERROR;
					break;
				default:
					do_advice_lightool();
					error_exit();
			}
		}
		if (optind < argc)
		{
			if (commandGet)
			{
				do_advice_lightool();
				error_exit();
			}
			if (0 == strcmp("blockrecover",argv[optind]))
			{
				brc.curkind = CUR_KIND_BLOCKRECOVER;
			}
			else if (0 == strcmp("walshow",argv[optind]))
			{
				brc.curkind = CUR_KIND_WALSHOW;
			}
			else if (0 == strcmp("datadis",argv[optind]))
			{
				brc.curkind = CUR_KIND_RELDATADIS;
			}
			else if (0 == strcmp("pageinspect", argv[optind]))
			{
				brc.curkind = CUR_KIND_PAGEINSPECT;
			}
			else
			{
				do_advice_lightool();
				error_exit();
			}
			commandGet = true;
			optind++;
		}
	}
	if (CUR_KIND_INVALID == brc.curkind)
	{
		do_advice_lightool();
		error_exit();
	}
}

void
pro_on_exit()
{
	if (brc.lightool)
		pfree(brc.lightool);
	if (brc.relnode)
		pfree(brc.relnode);
	if (brc.walpath)
		pfree(brc.walpath);
	if (brc.blockstr)
		pfree(brc.blockstr);
	if (brc.pgdata)
		pfree(brc.pgdata);
	if (brc.place)
		pfree(brc.place);
	if (brc.ratiostr)
		pfree(brc.ratiostr);
	cleanSpace();
}

static void
getProname(char *argu)
{
	char	*slash = NULL;
	if (NULL != (slash = strrchr(argu,'/')))
	{
		if (argu + strlen(argu) == slash - 1)
		{
			printf("Fail to get proname.\n");
			error_exit();
		}
		brc.lightool = pg_strdup(slash + 1);
	}
	else
		brc.lightool = pg_strdup(argu);
}

static void
initializeLightool(void)
{
	memset(&brc,0,sizeof(LightoolCtl));
	brc.parserPri.timeline = 1;
	brc.parserPri.startptr = InvalidXLogRecPtr;
	brc.parserPri.endptr = InvalidXLogRecPtr;
	brc.parserPri.endptr_reached = false;
	getCurTime(brc.execTime);
}

static void
startXlogRead(void)
{	
	configXlogRead(brc.walpath);
}

static void
mentalRecord(void)
{
	XLogRecord *record = NULL;
	char	   *errormsg = NULL;
	
	while (true)
	{
		record = XLogParserReadRecord(brc.xlogreader, brc.parserPri.first_record , &errormsg);
		if (!record)
		{
			break;
		}
		brc.parserPri.first_record = InvalidXLogRecPtr;
		if (CUR_KIND_BLOCKRECOVER == brc.curkind)
			recoverRecord(brc.xlogreader);
		else if (CUR_KIND_WALSHOW == brc.curkind)
			showRecord(brc.xlogreader);
	}
}

void
replacePages(void)
{
	int		loop = 0;
	char	filePath[MAXPGPATH] = {0};

	for (; loop < brc.rbNum; loop++)
	{
		uint32		blknoIneveryFile = 0;
		uint32		relFileNum = 0;
		if (brc.pageArray[loop])
		{
			memset(filePath, 0, MAXPGPATH);
			relFileNum = MAG_BLOCK_FILENO(brc.recoverBlock[loop]);
			blknoIneveryFile = MAG_BLOCK_BLKNO(brc.recoverBlock[loop]);
			if (0 != relFileNum)
				sprintf(filePath, "%s/%u.%u",brc.relpath,brc.rfn.relNode, relFileNum);
			else
				sprintf(filePath, "%s/%u",brc.relpath, brc.rfn.relNode);
			if (brc.debugout)
				br_elog("recover file %s pagenoInfile %u", filePath, blknoIneveryFile);
			backupOriFile(filePath);
			replaceFileBlock(filePath, blknoIneveryFile, brc.pageArray[loop]);	
		}
		
	}
}

static void
cleanSpace(void)
{
	int	loop = 0;
	for (; loop < RECOVER_BLOCK_MAX; loop++)
	{
		if (brc.pageArray[loop])
		{
			pfree(brc.pageArray[loop]);
			brc.pageArray[loop] = NULL;
		}
	}
	if (brc.xlogreader)
		xLogReaderFree(brc.xlogreader);
}

static void
do_blockrecover(void)
{
	int		loop = 0;

	blockRecoverArguCheck();
	if (brc.debugout)
	{
		br_elog("LOG:datafile is %u/%u/%u", brc.rfn.dbNode, brc.rfn.relNode, brc.rfn.spcNode);
		br_elog("LOG:walpath is %s", brc.walpath);
		br_elog("Recover blocks is");
		for (; loop < brc.rbNum; loop++)
			printf("%u ", brc.recoverBlock[loop]);
		printf("\n");
	}

	getFirstXlogFile(brc.walpath);
	startXlogRead();
	if (brc.debugout)
		printf("LOG:first_record 0x%x\n", (uint32)brc.parserPri.first_record);
	mentalRecord();
	if (brc.debugout)
	{
		for (loop = 0; loop < brc.rbNum; loop++)
		{
			if (brc.pageArray[loop])
				printf("%d ", brc.recoverBlock[loop]);
		}
		printf("\n");
	}
	getRelpath();
	replacePages();

}

static void
do_walshow(void)
{
	walshowArguCheck();
	if (brc.debugout)
	{
		br_elog("walpath is %s", brc.walpath);
	}
	getFirstXlogFile(brc.walpath);
	startXlogRead();
	if (brc.debugout)
		printf("first_record: 0x%x\n", (uint32)brc.parserPri.first_record);
	mentalRecord();
}

static void
do_datadis(void)
{
	datadisArguCheck();
	getRelpath();
	startDataDis();
}

static void
do_pageinspect(void)
{
	pageinspectArguCheck();
	getRelpath();
	startInspect();
}

int
main(int argc, char *argv[])
{
	bool	canshowwal = false;
	atexit(pro_on_exit);
	initializeLightool();
	getProname(argv[0]);
	arguMentParser(argc, argv);
	if (CUR_KIND_BLOCKRECOVER == brc.curkind)
	{
		do_blockrecover();
	}
	else if (CUR_KIND_WALSHOW == brc.curkind)
	{
		if (canshowwal)
			do_walshow();
		else
		{
			br_elog("wal show:Being developed");
			nomal_exit();
		}
	}
	else if (CUR_KIND_RELDATADIS == brc.curkind)
	{
		do_datadis();
	}
	else if (CUR_KIND_PAGEINSPECT == brc.curkind)
	{
		do_pageinspect();
	}
	
	return 0;
}

