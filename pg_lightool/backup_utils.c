/*-------------------------------------------------------------------------
 *
 * backup_utils.c
 * 此代码用于读取备份集数据
 *-------------------------------------------------------------------------
 */
 #include "backup_utils.h"
 #include "storage/fd.h"
 #include <time.h>

#define IsAlnum(c)		(isalnum((unsigned char)(c)))
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

bool
parse_time(const char *value, time_t *time)
{
	size_t		len;
	char	   *tmp;
	int			i;
	struct tm	tm;
	char		junk[2];

	/* tmp = replace( value, !isalnum, ' ' ) */
	tmp = malloc(strlen(value) + + 1);
	len = 0;
	for (i = 0; value[i]; i++)
		tmp[len++] = (IsAlnum(value[i]) ? value[i] : ' ');
	tmp[len] = '\0';

	/* parse for "YYYY-MM-DD HH:MI:SS" */
	memset(&tm, 0, sizeof(tm));
	tm.tm_year = 0;		/* tm_year is year - 1900 */
	tm.tm_mon = 0;		/* tm_mon is 0 - 11 */
	tm.tm_mday = 1;		/* tm_mday is 1 - 31 */
	tm.tm_hour = 0;
	tm.tm_min = 0;
	tm.tm_sec = 0;
	i = sscanf(tmp, "%04d %02d %02d %02d %02d %02d%1s",
		&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
		&tm.tm_hour, &tm.tm_min, &tm.tm_sec, junk);
	free(tmp);

	if (i < 1 || 6 < i)
		return false;

	/* adjust year */
	if (tm.tm_year < 100)
		tm.tm_year += 2000 - 1900;
	else if (tm.tm_year >= 1900)
		tm.tm_year -= 1900;

	/* adjust month */
	if (i > 1)
		tm.tm_mon -= 1;

	/* determine whether Daylight Saving Time is in effect */
	tm.tm_isdst = -1;

	*time = mktime(&tm);

	return true;
}


void
checkEndLoc(void)
{
	if(brc.endxidstr && brc.endtimestr)
		br_error("endxid and endtime can not pointed at same time.");
	if(brc.endxidstr || brc.endtimestr)
		if(!brc.backuppath)
		{
			br_error("only whole table recover support argument encxid and endtime");
		}
	if(brc.endxidstr)
	{
		char	tempbuf[20] = {0};

		sscanf(brc.endxidstr, "%u", &brc.endxid);
		sprintf(tempbuf, "%u", brc.endxid);
		if(0 != strcmp(brc.endxidstr, tempbuf))
			br_error("wrong argument endxid %s", brc.endxidstr);
	}

	if(brc.endtimestr)
	{
		if(!parse_time(brc.endtimestr, &brc.endtime))
			br_error("wrong argument endtime %s", brc.endtimestr);
	}
}