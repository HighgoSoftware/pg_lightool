/*-------------------------------------------------------------------------
 *
 * pg_blockrecover.c - recover bad blocks
 *		  pg_blockrecover.h
 *-------------------------------------------------------------------------
 */
#ifndef	PH_BLOCKRECOVER_H
#define	PH_BLOCKRECOVER_H

#define FRONTEND	1
#include "postgres_fe.h"
#include "access/xlog_internal.h"
#include "storage/bufpage.h"
#include "access/xlogdefs.h"
#include "utils/elog.h"
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>



#define	PG_BLOCKRECOVER			"0.1"
#define no_argument				0
#define required_argument		1
#define optional_argument		2
#define	PG_DEFAULT_TBS_OID		1663
#define	PG_GLOBLE_TBS_OID		1664
#define	RECOVER_BLOCK_MAX		10
#define MAXPGPATH				1024
#define FirstNormalObjectId		16384

#define	CUR_KIND_INVALID		0
#define	CUR_KIND_BLOCKRECOVER	1
#define	CUR_KIND_WALSHOW		2


typedef uintptr_t Datum;
typedef struct XLogParserPrivate
{
	TimeLineID	timeline;
	char	   *inpath;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
	XLogRecPtr	first_record;
} XLogParserPrivate;


typedef struct LightoolCtl
{
	int					curkind;
	uint32				recoverBlock[RECOVER_BLOCK_MAX];
	Page				pageArray[RECOVER_BLOCK_MAX];
	char				relpath[MAXPGPATH];
	char				execTime[20];
	XLogReaderState		*xlogreader;
	XLogParserPrivate 	parserPri;
	uint32				rbNum;
	RelFileNode			rfn;
	uint64				system_identifier;
	char*				lightool;
	char*				recovrel;
	char*				walpath;
	char*				blockstr;
	char*				pgdata;
	bool				debugout;
	bool				immediate;
}LightoolCtl;

extern LightoolCtl		brc;

extern void br_error(const char *fmt,...) pg_attribute_printf(1, 2);
extern void br_elog(const char *fmt,...) pg_attribute_printf(1, 2);

extern bool walPathCheck(char *path);
extern bool parse_uint32(const char *value, uint32 *result);
extern void getRecoverBlock(char *block);
extern void getDataFile(char *datafile);
extern void error_exit(void);
extern void nomal_exit(void);
extern XLogReaderState *XLogReaderAllocate_fe(XLogPageReadCB pagereadfunc, void *private_data);
extern XLogRecPtr XLogFindNextRecord_fe(XLogReaderState *state, XLogRecPtr RecPtr);
extern XLogRecord *XLogParserReadRecord(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg);
extern void configXlogRead(char	*walpath);
extern void getFirstXlogFile(char *waldir);
extern int fuzzy_open_file(const char *directory, const char *fname);
extern void recoverRecord(XLogReaderState *record);
extern void showRecord(XLogReaderState *record);
extern void xLogReaderFree(XLogReaderState *state);
extern void checkPgdata(void);
extern void getRelpath(void);
extern void replaceFileBlock(char* filePath, uint32 blknoIneveryFile, Page page);
extern void backupOriFile(char* filePath);
extern void getCurTime(char	*curtime);

#endif
