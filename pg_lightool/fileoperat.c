/*-------------------------------------------------------------------------
 *
 * fileoperat.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_lightool.h"
#include "catalog/pg_control.h"
#include "port.h"

static void delete_dir(char *path, bool oridelete);
static int getTarFileNum(char *path, Oid relnode);

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
	if (!path)
		return false;
	if (4 != pg_check_dir(path))
		return false;
	else
		return true;
}

uint64
getfileSize(char *path)
{
	struct 	stat fst;
	uint64	result = 0;
	
	Assert(path && 0 != strlen(path));
	if (lstat(path, &fst) < 0)
	{
		br_error("file \"%s\" do not exist or fail to lstat", path);
	}
	if (!S_ISREG(fst.st_mode))
		br_error("file \"%s\" is not a regular dile", path);
	result = (uint64)fst.st_size;
	return result;
}

bool
fileExist(char *path)
{
	struct stat fst;
	if (lstat(path, &fst) < 0)
	{
		return false;
	}
	if (S_ISREG(fst.st_mode))
		return true;
	else
		return false;
}

static bool
dirExist(char *path)
{
	struct stat fst;
	if (lstat(path, &fst) < 0)
	{
		return false;
	}
	if (S_ISDIR(fst.st_mode))
		return true;
	else
		return false;
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
			if (0 == brc.parserPri.timeline)
			{
				brc.parserPri.timeline = everytimeline;
			}
			else if (brc.parserPri.timeline != everytimeline)
			{
				br_error("could not mental muti timeline.\n");
			}
			if (0 == segmin)
			{
				segmin = segno;
				segmax = segno;
				XLogSegNoOffsetToRecPtr(segmin, 0, brc.parserPri.startptr);
				XLogSegNoOffsetToRecPtr(segmax + 1, 0, brc.parserPri.endptr);
			}
			else if (segmin > segno)
			{
				segmin = segno;
				XLogSegNoOffsetToRecPtr(segmin, 0, brc.parserPri.startptr);
			}
			else if (segmax < segno)
			{
				segmax = segno;
				XLogSegNoOffsetToRecPtr(segmax + 1, 0, brc.parserPri.endptr);
			}
		}
	}
	if(brc.backuppath)
	{
		if(brc.parserPri.startptr > brc.startlsn || brc.parserPri.endptr < brc.startlsn)
			br_error("can not find lsn %x/%x", (uint32)(brc.startlsn >> 32), (uint32)(brc.startlsn));
		else
			brc.parserPri.startptr = brc.startlsn;
	}
	if (brc.debugout)
		br_elog("LOG:parser range:0x%x~0x%x\n", (uint32)brc.parserPri.startptr, (uint32)brc.parserPri.endptr);
}

ControlFileData *
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
	if ('/' == brc.pgdata[strlen(brc.pgdata)-1])
		brc.pgdata[strlen(brc.pgdata)-1] = 0;
	if (!walPathCheck(brc.pgdata))
	{
		br_error("Invalid pgdata argument \"%s\"\n", brc.pgdata);
	}
	cfd = get_controlfile(brc.pgdata, brc.lightool, &crcret);
	brc.system_identifier = cfd->system_identifier;
	brc.tlid = cfd->checkPointCopy.ThisTimeLineID;
	pfree(cfd);
}

void
checkPlace(void)
{
	char	path[MAXPGPATH] = {0};
	FILE	*fp = NULL;
	Assert(brc.place);

	sprintf(path, "%s/%s", brc.place, DATA_DIS_RESULT_FILE);
	if (!dirExist(brc.place))
		br_error("Dir \"%s\" is not exist\n", brc.place);

	fp = fopen(path,"w");
	if (!fp)
		br_error("Fail to open file \"%s\"\n", path);
	fclose(fp);
	
}

void
backupOriFile(char* filePath)
{
	char	*backupStr = "lightool_backup";
	char	backupFile[MAXPGPATH] = {0};
	if (!brc.immediate)
	{
		sprintf(backupFile,"%s.%s_%s", filePath, backupStr, brc.execTime);
		if (!fileExist(backupFile))
		{
			char	cmd[MAXPGPATH] = {0};
			if (brc.debugout)
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
	if (!fp)
	{
		if(2 == errno)
		{
			fp = fopen(filePath,"wb");
		}
		if(!fp)
			br_error("Fail to open file \"%s\" to write", filePath);
	}
	if(!brc.ifwholerel)
		br_elog("    BlockRecover:file->%s,page:%u",filePath,blknoIneveryFile);
	fseek(fp, 0, SEEK_SET);
	if(-1 == fseek(fp, blknoIneveryFile * BLCKSZ, SEEK_CUR))
		br_error("fail to fseek to %ld of file %s", (int64)blknoIneveryFile * BLCKSZ, filePath);
	fwrite(page, BLCKSZ, 1, fp);
	fclose(fp);
}

/*
* 递归删除目录
*/
static void
delete_dir(char *path, bool oridelete)
{
	DIR 			*dir = NULL;
	struct dirent 	*ent = NULL;
	char			subPath[MAXPGPATH] = {0};

	Assert(path);
	Assert(dirExist(path));

	dir = opendir(path);
	while(NULL != (ent = readdir(dir)))
	{
		sprintf(subPath, "%s/%s", path, ent->d_name);
		if(DT_DIR == ent->d_type)
		{
			if(0 == strcmp(ent->d_name, ".") || 0 == strcmp(ent->d_name, ".."))
			{
				continue;
			}
			else
			{
				delete_dir(subPath, true);
			}
		}
		else if(DT_REG == ent->d_type)
		{
			if(-1 == remove(subPath))
			{
				br_error("can not remove file %s", subPath);
			}
			if(brc.debugout)
			{
				br_elog("remove file %s", subPath);
			}
		}
	}
	if(oridelete)
	{
		if(-1 == remove(path))
		{
			br_error("can not remove dir %s", path);
		}
		if(brc.debugout)
		{
			br_elog("remove dir %s", path);
		}
	}
	closedir(dir);
}

static int
getTarFileNum(char *path, Oid relnode)
{
	DIR		*dir = NULL;
	struct dirent *ent = NULL;
	char	relNodeStr_o[30] = {0};
	char	relNodeStr[30] = {0};
	int		countNum = 0;

	Assert(path);
	Assert(dirExist(path));

	sprintf(relNodeStr, "%u.", relnode);
	sprintf(relNodeStr_o, "%u", relnode);
	dir = opendir(path);
	while(NULL != (ent = readdir(dir)))
	{
		if(ent->d_type == DT_REG)
		{
			if(strstr(ent->d_name, relNodeStr) || 0 == strcmp(ent->d_name,relNodeStr_o))
			{
				countNum++;
			}
		}
	}
	if(brc.debugout)
		br_elog("[getTarFileNum]countNum=%d",countNum);
	return countNum;
}

static void
copyfile(char *orifile, char *tarfile)
{
	FILE	*in = NULL;
	FILE	*out = NULL;
	size_t	filesize = 0;
	char	page[BLCKSZ] = {0};
	size_t	readCount = 0, writeCount = 0;

	Assert(orifile && tarfile && fileExist(orifile) && !fileExist(tarfile));
	in = fopen(orifile, "rb");
	if(!in)
		br_error("can not open file %s to read:%m", orifile);
	out = fopen(tarfile, "wb");
	if(!out)
		br_error("can not open file %s to write:%m", tarfile);

	filesize = getfileSize(orifile);
	Assert(0 == filesize % BLCKSZ);
	while(true)
	{
		memset(page, 0, BLCKSZ);
		readCount = fread(page, 1, BLCKSZ,in);
		if(0 == readCount)
			break;
		else if(BLCKSZ != readCount)
			br_error("read wrong size %ld", readCount);
		
		writeCount = fwrite(page, 1, BLCKSZ, out);
		if(BLCKSZ != writeCount)
			br_error("write wrong size %ld", readCount);
	}
	fclose(in);
	fclose(out);
	if(brc.debugout)
		br_elog("[copyfile]copy file %s size:%ld", tarfile, filesize);
}

static void
copyfiles(char *oridir, char *tardir, Oid relnode)
{
	int 	filenum = 0;
	int		loop = 0;
//	char	relNodeStr[30] = {0};
	char	loopfile_or[MAXPGPATH] = {0};
	char	loopfile_ta[MAXPGPATH] = {0};
	

	Assert(oridir && tardir && dirExist(oridir) && dirExist(tardir));

	filenum = getTarFileNum(oridir, relnode);
	for(loop = 0; loop < filenum; loop++)
	{
		if(0 == loop)
		{
			sprintf(loopfile_or, "%s/%u", oridir, relnode);
			sprintf(loopfile_ta, "%s/%u", tardir, relnode);
		}
		else
		{
			sprintf(loopfile_or, "%s/%u.%d", oridir, relnode, loop);
			sprintf(loopfile_ta, "%s/%u.%d", tardir, relnode, loop);
		}
		copyfile(loopfile_or, loopfile_ta);
	}
}

/*
* 创建临时目录
* 从备份集拷贝目标表的备份到临时目录
*/
void
copyBackupRel(void)
{
	char	*secPathptr = NULL;
	char	relPathInBackup[MAXPGPATH] = {0};
	char	tempdir[MAXPGPATH] = {0};


	sprintf(tempdir, "%s%u", WHOLE_RELATION_RECOVER_TEMPDIR, brc.rfn.relNode);
	sprintf(brc.reltemppath, "%s/%s", brc.relpath, tempdir);

	/*保证备份文件的拷贝目标路径存在且为空。*/
	if(dirExist(brc.reltemppath))
	{
		if(brc.debugout)
			br_elog("[copyBackupRel]delete path %s", brc.reltemppath);
		delete_dir(brc.reltemppath, false); 
	}
	else
	{
		if(-1 == mkdir(brc.reltemppath, 0755))
			br_error("can not create dir %s:%m", brc.reltemppath);
	}

	if(!brc.backuppath)
		return;
	secPathptr = brc.relpath + strlen(brc.pgdata);
	sprintf(relPathInBackup, "%s/%s", brc.backuppath, secPathptr);
	copyfiles(relPathInBackup, brc.reltemppath, brc.rfn.relNode);
}

void
moveRestoreFile(void)
{
	char	postmasterPath[MAXPGPATH] = {0};
	char	restoreDirName[MAXPGPATH] = {0};
	char	cmd1[MAXPGPATH] = {0};
	char	cmd2[MAXPGPATH] = {0};

	sprintf(postmasterPath, "%s/postmaster.pid", brc.pgdata);

	if(fileExist(postmasterPath))
	{
		br_elog("database is running, alternative datafile is %s by hand after stop database.", brc.reltemppath);
		return;
	}

	sprintf(restoreDirName, "%s/../wtrbk_%u_%s", brc.reltemppath, brc.rfn.relNode, brc.execTime);//reltemppath
	if(-1 == mkdir(restoreDirName, 0755))
		br_error("can not create dir %s:%m", restoreDirName);

	sprintf(postmasterPath, "%s/postmaster.pid", brc.pgdata);

	sprintf(cmd1, "mv %s/../%u* %s",brc.reltemppath, brc.rfn.relNode, restoreDirName);
	if(brc.debugout)
		br_elog("cmd1=%s", cmd1);

	sprintf(cmd2, "mv %s/* %s/../", brc.reltemppath, brc.reltemppath);
	if(brc.debugout)
		br_elog("cmd2=%s", cmd2);

	system(cmd1);
	system(cmd2);
	br_elog("whole table recover sucess.");
}