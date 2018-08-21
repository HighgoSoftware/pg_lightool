/*-------------------------------------------------------------------------
 *
 * fileoperat.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_lightool.h"
#include "catalog/pg_control.h"
#include "port.h"

static ControlFileData *get_controlfile(const char *DataDir, const char *progname, bool *crc_ok_p);
static bool fileExist(char *path);


int
fuzzy_open_file(const char *directory, const char *fname)
{
	int			fd = -1;
	char		fpath[MAXPGPATH];

	Assert(directory);
	/* directory / fname */
	snprintf(fpath, MAXPGPATH, "%s/%s",
			 directory, fname);
	fd = open(fpath, O_RDONLY | PG_BINARY, 0);
	if (fd < 0 && errno != ENOENT)
		return -1;
	else if (fd >= 0)
		return fd;
	return -1;
}


bool
walPathCheck(char *path)
{
	if(!path)
		return false;
	if(4 != pg_check_dir(path))
		return false;
	else
		return true;
}

static bool
fileExist(char *path)
{
	struct stat fst;
	if (lstat(path, &fst) < 0)
	{
		return false;
	}
	return true;
}


void
getFirstXlogFile(char *waldir)
{
	DIR				*chkdir = NULL;
	struct dirent	*xlde = NULL;
	TimeLineID		everytimeline = 0;
	XLogSegNo		segno = 0, segmin = 0, segmax = 0;
	

	Assert(waldir);
	chkdir = opendir(waldir);
	if (chkdir == NULL)
		br_error("can not opden dir \"%s\"", waldir);
	brc.parserPri.inpath = waldir;
	brc.parserPri.timeline = everytimeline;
	
	while (errno = 0, (xlde = readdir(chkdir)) != NULL)
	{
		struct stat fst;
		char		fullpath[MAXPGPATH * 2] = {0};

		if (0 == strcmp(xlde->d_name, ".") || 0 == strcmp(xlde->d_name, ".."))
			continue;

		snprintf(fullpath, sizeof(fullpath), "%s/%s", waldir, xlde->d_name);

		if (lstat(fullpath, &fst) < 0)
		{
			if (errno != ENOENT)
				br_error("could not stat file \"%s\": %s\n",
						 fullpath, strerror(errno));
		}

		if (S_ISREG(fst.st_mode))
		{
			XLogFromFileName(xlde->d_name, &everytimeline, &segno);
			if(0 == brc.parserPri.timeline)
			{
				brc.parserPri.timeline = everytimeline;
			}
			else if(brc.parserPri.timeline != everytimeline)
			{
				br_error("could not stat file \"%s\"\n", fullpath);
			}
			if(0 == segmin)
			{
				segmin = segno;
				segmax = segno;
				XLogSegNoOffsetToRecPtr(segmin, 0, brc.parserPri.startptr);
				XLogSegNoOffsetToRecPtr(segmax + 1, 0, brc.parserPri.endptr);
			}
			else if(segmin > segno)
			{
				segmin = segno;
				XLogSegNoOffsetToRecPtr(segmin, 0, brc.parserPri.startptr);
			}
			else if(segmax < segno)
			{
				segmax = segno;
				XLogSegNoOffsetToRecPtr(segmax + 1, 0, brc.parserPri.endptr);
			}
		}
	}
	if(brc.debugout)
		printf("LOG:parser range:0x%x~0x%x\n", (uint32)brc.parserPri.startptr, (uint32)brc.parserPri.endptr);
}

static ControlFileData *
get_controlfile(const char *DataDir, const char *progname, bool *crc_ok_p)
{
	ControlFileData *ControlFile;
	int			fd;
	char		ControlFilePath[MAXPGPATH];
	pg_crc32c	crc;

	AssertArg(crc_ok_p);

	ControlFile = palloc(sizeof(ControlFileData));
	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
#ifndef FRONTEND
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for reading: %m",
						ControlFilePath)));
#else
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, ControlFilePath, strerror(errno));
		exit(EXIT_FAILURE);
	}
#endif

	if (read(fd, ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
#ifndef FRONTEND
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", ControlFilePath)));
#else
	{
		fprintf(stderr, _("%s: could not read file \"%s\": %s\n"),
				progname, ControlFilePath, strerror(errno));
		exit(EXIT_FAILURE);
	}
#endif

	close(fd);

	/* Check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
				(char *) ControlFile,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	*crc_ok_p = EQ_CRC32C(crc, ControlFile->crc);

	/* Make sure the control file is valid byte order. */
	if (ControlFile->pg_control_version % 65536 == 0 &&
		ControlFile->pg_control_version / 65536 != 0)
		br_error("byte ordering mismatch");


	return ControlFile;
}


void
checkPgdata(void)
{
	ControlFileData *cfd = NULL;
	bool			crcret = false;	
	if('/' == brc.pgdata[strlen(brc.pgdata)-1])
		brc.pgdata[strlen(brc.pgdata)-1] = 0;
	if(!walPathCheck(brc.pgdata))
	{
		br_error("Invalid pgdata argument \"%s\"\n", brc.pgdata);
	}
	cfd = get_controlfile(brc.pgdata, brc.lightool, &crcret);
	brc.system_identifier = cfd->system_identifier;
	pfree(cfd);
}

void
backupOriFile(char* filePath)
{
	char	*backupStr = "lightool_backup";
	char	backupFile[MAXPGPATH] = {0};
	if(!brc.immediate)
	{
		sprintf(backupFile,"%s.%s_%s", filePath, backupStr, brc.execTime);
		if(!fileExist(backupFile))
		{
			char	cmd[MAXPGPATH] = {0};
			if(brc.debugout)
				br_elog("LOG:backup file %s",filePath);
			sprintf(cmd,"cp %s %s",filePath, backupFile);
			system(cmd);
		}
	}
}

void
replaceFileBlock(char* filePath, uint32 blknoIneveryFile, Page page)
{
	FILE	*fp = NULL;

	
	Assert(filePath);
	Assert(1 <= blknoIneveryFile);
	Assert(page);

	fp = fopen(filePath,"rb+");
	if(!fp)
		br_error("Fail to open file \"%s\"", filePath);

	br_elog("    BlockRecover:file->%s,page:%u",filePath,blknoIneveryFile);
	fseek(fp, 0, SEEK_SET);
	fseek(fp, blknoIneveryFile * BLCKSZ, SEEK_CUR);
	fwrite(page, BLCKSZ, 1, fp);
	 

	fclose(fp);
	
}
