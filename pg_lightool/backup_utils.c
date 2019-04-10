/*-------------------------------------------------------------------------
 *
 * backup_utils.c
 * 此代码用于读取备份集数据
 *-------------------------------------------------------------------------
 */
 #include "backup_utils.h"
 #include "storage/fd.h"

 #define BACKUP_LABEL_FILE		"backup_label"

 static bool
read_backup_label(XLogRecPtr *RedoStartLSN, bool *backupEndRequired,
				  bool *backupFromStandby)
{
	char		startxlogfilename[MAXFNAMELEN];
	char		backuplablefile[MAXPGPATH] = {0};
	TimeLineID	tli;
	FILE	   *lfp;
	char		ch;
	char		backuptype[20];
	char		backupfrom[20];
	uint32		hi,
				lo;

	*backupEndRequired = false;
	*backupFromStandby = false;

	/*
	 * See if label file is present
	 */
	sprintf(backuplablefile, "%s/%s", brc.backuppath, BACKUP_LABEL_FILE);
	lfp = fopen(backuplablefile, "r");
	if (!lfp)
	{
		br_error("could not read file \"%s\": %m", backuplablefile);
	}

	/*
	 * Read and parse the START WAL LOCATION and CHECKPOINT lines (this code
	 * is pretty crude, but we are not expecting any variability in the file
	 * format).
	 */
	if (fscanf(lfp, "START WAL LOCATION: %X/%X (file %08X%16s)%c",
			   &hi, &lo, &tli, startxlogfilename, &ch) != 5 || ch != '\n')
			br_error("invalid data in file \"%s\"", backuplablefile);
	*RedoStartLSN = ((uint64) hi) << 32 | lo;
	/*if (fscanf(lfp, "CHECKPOINT LOCATION: %X/%X%c",
			   &hi, &lo, &ch) != 3 || ch != '\n')
			br_error("invalid data in file \"%s\"", backuplablefile);
	*checkPointLoc = ((uint64) hi) << 32 | lo;*/

	/*
	 * BACKUP METHOD and BACKUP FROM lines are new in 9.2. We can't restore
	 * from an older backup anyway, but since the information on it is not
	 * strictly required, don't error out if it's missing for some reason.
	 */
	if (fscanf(lfp, "BACKUP METHOD: %19s\n", backuptype) == 1)
	{
		if (strcmp(backuptype, "streamed") == 0)
			*backupEndRequired = true;
	}

	if (fscanf(lfp, "BACKUP FROM: %19s\n", backupfrom) == 1)
	{
		if (strcmp(backupfrom, "standby") == 0)
			*backupFromStandby = true;
	}

	fclose(lfp);
	
	return true;
}

void
checkBackup(void)
{
	ControlFileData *cfd = NULL;
	bool			crcret = false;
	TimeLineID		backuptlid = 0;
//	XLogRecPtr		checkPointLoc;
	bool			backupEndRequired;
	bool			backupFromStandby;
	
	if(!brc.backuppath)
		return;
	cfd = get_controlfile(brc.backuppath, brc.lightool, &crcret);
	if(brc.system_identifier != cfd->system_identifier)
		br_error("The system_identifier of database is %lu while the backup is %lu", brc.system_identifier, cfd->system_identifier);

	backuptlid = cfd->checkPointCopy.ThisTimeLineID;
	if(backuptlid != brc.tlid)
		br_error("The timelingid of database is %u while the backup is %u", brc.tlid, backuptlid);
	
	read_backup_label(&brc.startlsn, &backupEndRequired, &backupFromStandby);
	pfree(cfd);
}