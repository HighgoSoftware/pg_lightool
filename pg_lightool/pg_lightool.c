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
#define MAG_BLOCK_FILENO(blockno) (blockno/RELSEG_SIZE)
#define MAG_BLOCK_BLKNO(blockno) (blockno%RELSEG_SIZE)


LightoolCtl		brc;

static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"version", no_argument, NULL, 'V'},
		{"log", no_argument, NULL, 'l'},
		{"datafile", required_argument, NULL, 'f'},
		{"block", required_argument, NULL, 'b'},
		{"waldir", required_argument, NULL, 'w'},
		{"immediate", no_argument, NULL, 'i'},
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
static void walshowArguCheck(void);
static void blockRecoverArguCheck(void);


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

	printf("\nCommon Options:\n");
	printf("  -V, --version                         output version information, then exit\n");
	printf("  -l, --log                             whether to write a debug info\n");
	printf("  -f, --recovrel=spcid/dbid/relfilenode specify files to repair\n");
	printf("  -b, --block=n1[,n2,n3]                specify blocks to repair(10 limit)\n");
	printf("  -w, --walpath=walpath                 wallog read from\n");
	printf("  -D, --pgdata=datapath                 data dir of database\n");
	printf("  -i, --immediate			            does not do a backup for old file\n");
}

static void
blockRecoverArguCheck(void)
{
		
	if(!brc.recovrel || !brc.walpath || !brc.blockstr || !brc.pgdata)
	{
		br_error("argument recovrel,walpath,block,pgdata is necessary.\n");
	}
	
	/*datafile check*/
	getDataFile(brc.recovrel);
		
	/*walpath check*/
	if(!walPathCheck(brc.walpath))
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
		
	if(!brc.walpath)
	{
		br_error("argument walpath is necessary.\n");
	}
	
	/*walpath check*/
	if(!walPathCheck(brc.walpath))
	{
		br_error("Invalid walpath argument \"%s\"\n", brc.walpath);
	}
}


static void
arguMentParser(int argc, char *argv[])
{
	int			option_index = 0;
	char		c = 0;
	bool		commandGet = false;

	if(1 == argc)
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
		while (-1 != (c = getopt_long(argc, argv, "?Vlf:b:w:D:i",long_options, &option_index)))
		{
			switch (c)
			{
				case 'l':
					brc.debugout = true;
					break;
				case 'f':
					brc.recovrel = (char*)strdup(optarg);
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
				default:
					do_advice_lightool();
					error_exit();
			}
		}
		if (optind < argc)
		{
			if(commandGet)
			{
				do_advice_lightool();
				error_exit();
			}
			if(0 == strcmp("blockrecover",argv[optind]))
			{
				brc.curkind = CUR_KIND_BLOCKRECOVER;
			}
			else if(0 == strcmp("walshow",argv[optind]))
			{
				brc.curkind = CUR_KIND_WALSHOW;
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
	if(CUR_KIND_INVALID == brc.curkind)
	{
		do_advice_lightool();
		error_exit();
	}
}

void
pro_on_exit()
{
	if(brc.lightool)
		pfree(brc.lightool);
	if(brc.recovrel)
		pfree(brc.recovrel);
	if(brc.walpath)
		pfree(brc.walpath);
	if(brc.blockstr)
		pfree(brc.blockstr);
	if(brc.pgdata)
		pfree(brc.pgdata);
	cleanSpace();
}

static void
getProname(char *argu)
{
	char	*slash = NULL;
	if(NULL != (slash = strrchr(argu,'/')))
	{
		if(argu + strlen(argu) == slash - 1)
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
	
	while(true)
	{
		record = XLogParserReadRecord(brc.xlogreader, brc.parserPri.first_record , &errormsg);
		if (!record)
		{
			break;
		}
		brc.parserPri.first_record = InvalidXLogRecPtr;
		if(CUR_KIND_BLOCKRECOVER == brc.curkind)
			recoverRecord(brc.xlogreader);
		else if(CUR_KIND_WALSHOW == brc.curkind)
			showRecord(brc.xlogreader);
	}
}

void
replacePages(void)
{
	int		loop = 0;
	char	filePath[MAXPGPATH] = {0};

	for(; loop < brc.rbNum; loop++)
	{
		uint32		blknoIneveryFile = 0;
		uint32		relFileNum = 0;
		if(brc.pageArray[loop])
		{
			memset(filePath, 0, MAXPGPATH);
			relFileNum = MAG_BLOCK_FILENO(brc.recoverBlock[loop]);
			blknoIneveryFile = MAG_BLOCK_BLKNO(brc.recoverBlock[loop]);
			if(0 != relFileNum)
				sprintf(filePath, "%s/%u.%u",brc.relpath,brc.rfn.relNode, relFileNum);
			else
				sprintf(filePath, "%s/%u",brc.relpath, brc.rfn.relNode);
			if(brc.debugout)
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
	for(; loop < RECOVER_BLOCK_MAX; loop++)
	{
		if(brc.pageArray[loop])
		{
			pfree(brc.pageArray[loop]);
			brc.pageArray[loop] = NULL;
		}
	}
	if(brc.xlogreader)
		xLogReaderFree(brc.xlogreader);
}

static void
do_blockrecover(void)
{
	int		loop = 0;

	blockRecoverArguCheck();
	if(brc.debugout)
	{
		br_elog("LOG:datafile is %u/%u/%u", brc.rfn.dbNode, brc.rfn.relNode, brc.rfn.spcNode);
		br_elog("LOG:walpath is %s", brc.walpath);
		br_elog("Recover blocks is");
		for(; loop < brc.rbNum; loop++)
			printf("%u ", brc.recoverBlock[loop]);
		printf("\n");
	}

	getFirstXlogFile(brc.walpath);
	startXlogRead();
	if(brc.debugout)
		printf("LOG:first_record 0x%x\n", (uint32)brc.parserPri.first_record);
	mentalRecord();
	if(brc.debugout)
	{
		for(loop = 0; loop < brc.rbNum; loop++)
		{
			if(brc.pageArray[loop])
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
	if(brc.debugout)
	{
		br_elog("walpath is %s", brc.walpath);
	}
	getFirstXlogFile(brc.walpath);
	startXlogRead();
	if(brc.debugout)
		printf("first_record: 0x%x\n", (uint32)brc.parserPri.first_record);
	mentalRecord();
}


int
main(int argc, char *argv[])
{
	bool	canshowwal = false;
	atexit(pro_on_exit);
	initializeLightool();
	getProname(argv[0]);
	arguMentParser(argc, argv);
	if(CUR_KIND_BLOCKRECOVER == brc.curkind)
	{
		do_blockrecover();
	}
	else if(CUR_KIND_WALSHOW == brc.curkind)
	{
		if(canshowwal)
			do_walshow();
		else
		{
			br_elog("wal show:Being developed");
			nomal_exit();
		}
	}
	return 1;
}

