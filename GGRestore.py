import sys,os,getopt,time
import traceback
import commands
import re,csv,glob,shutil,tarfile
sys.path.append("/DG/activeRelease/lib/python_lib/")

from pythonodbcdriver import pyodbcdriver ### Using pyodbc to get GG connection handle
import FileLogger

LOG_FILE_PATH = '/DGlogs/GGRestore.log'
logger = FileLogger.logger(LOG_FILE_PATH);
log = logger.getlogger();
connectionObject=None
GGTableList=[]
RestoreCSVTempPath='/DGlogs/GGRestoreCSV/'

def Usage():
    print '  \033[1;35m This script would Restore all Grid Gain Data present in input BackUP/*.tgz File.\033[1;m'
    print '  \n'
    print '  \033[1;35m USAGE : source /DG/activeRelease/Tools/kodiakScripts.conf; python2.7 /DG/activeRelease/Tools/CronJobs/GGRestore.py -f /DGdata/Backup/DBBackup/<DBBackUPTarFile.tgz File/Path>\033[1;m'
    print '  The input BackUP tgz File name should be of format as shown below'
    print '  \033[1;35m DG_$PTTSERVERID_$CLUSTERID_$DATETIME.tgz   \033[1;m'
    print '  \033[1;35m DG_444441_2_20180530124557.tgz \033[1;m'
    sys.exit(2)


def main(argv):
   global GGTbl_Input_TarFile
   if len(sys.argv) != 3:
        Usage()
   try:
      opts, args = getopt.getopt(argv,"hf:",["fGGTbl_Input_TarFile="])
   except getopt.GetoptError:
      Usage()
   for opt, arg in opts:
      if opt == '-h':
         Usage()
      elif opt in ("-f", "--fGGTbl_Input_TarFile"):
         GGTbl_Input_TarFile = arg

def GGRestorePreValidations():
    if not os.path.exists(GGTbl_Input_TarFile):
        log.error("INPUT BACKUP/TAR FILE: %s does not exist"%(GGTbl_Input_TarFile))
        print '\033[1;31m INPUT BACKUP/TAR FILE:  '+str(GGTbl_Input_TarFile)+'  DOES NOT EXIST. INPUT CORRECT BACKUP/TAR FILE PATH \'OR\' GENERATE BACKUP FILE AND RETRY: \033[1;m'
        CleanUPGGainhandles(1)
    TgzFileName=os.path.basename(GGTbl_Input_TarFile)
    ClusterID=TgzFileName.split('_')[2]
    if ClusterID != os.environ.get("CLUSTERID"):
        log.error("Cluster ID of input BACKUP/TAR FILE %s does not match with local GG cluster ID"%(GGTbl_Input_TarFile))
        print '\033[1;31m CLUSTERID OF INPUT BACKUP/TAR FILE:  '+str(GGTbl_Input_TarFile)+'  DOES NOT MATCH WOTH LOCAL GRID GAIN CLUSTERID \033[1;m' 
        CleanUPGGainhandles(1)

def GGRestorePreCleanUP():
    log.info("Cleaning stale files from "+RestoreCSVTempPath+"")
    os.system("/bin/rm -rvf "+RestoreCSVTempPath+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")
    os.system("/bin/mkdir -p "+RestoreCSVTempPath+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"") 


def GGRestoreValidation(Tbl,OpsStrng):
    #time.sleep(15)
    SelectChkQry='select count(*) from '+Tbl+';'
    SelectChkQryCount=connectionObject.execute(SelectChkQry,1)
    print SelectChkQryCount
    if  SelectChkQryCount == -1:
        log.error("Failed to Execute Query %s"%(SelectChkQryCount))
        sys.exit(1)
    print (""+Tbl+" COUNT "+OpsStrng+" is :%s"%((SelectChkQryCount[1][0][0])))
    
def DeleteDataGGTbl(GGTblName):
    log.info("Deleting data from GG Tbl: %s"%(GGTblName))
    GGTblDeleteQry='delete from '+GGTblName+';'
    Status=connectionObject.execute(GGTblDeleteQry,0)
    if Status[0] == -1:
        log.error("Failed to Execute Query %s"%(GGTblDeleteQry))
        print '\033[1;31m FAILED TO DELETE TABLE:  '+str(GGTblName)+'  \033[1;m'
        CleanUPGGainhandles(1)
    log.info("Successfully deleted existing data from GG Tbl: %s"%(GGTblName))

def InsertDataGGTbl(CSVFile,GGTblName):
    log.info("Inserting data in GG Tbl: %s"%(GGTblName))
    with open(CSVFile) as csvopen:
        GGTblCSVDataList=csvopen.readlines()
    GGColNamesStrg=GGTblCSVDataList[0].rstrip('\n').replace('$#$#',',')
    GGTblDataList=GGTblCSVDataList[1:]
    for GGColData in GGTblDataList:
        ### Formatting/Preparing the GGColData for insertion using single quotes ie: All data to GG is stringzed ####
        GGColData=', '.join("'{0}'".format(w) for w in GGColData.strip().split('$#$#'))
        GGTblInsertQry='insert into '+GGTblName.strip()+' '+'('+GGColNamesStrg.strip()+')'+' values ('+GGColData+');'
        Status=connectionObject.execute(GGTblInsertQry,0)
        if Status[0] == -1:
            log.error("Failed to Execute Query %s"%(GGTblInsertQry))
            print '\033[1;31m FAILED TO INSERT DATA IN TABLE:  '+str(GGTblName)+'  \033[1;m'
            CleanUPGGainhandles(1)
    log.info("SUCCESSFULLY RESTORED GRID GAIN DATA FOR INPUT TABLE: %s"%(GGTblName)) 

def UnTarCSVFilesRestoreGG():
    log.info("UnTarring the CSV Files list from tgz File %s"%(GGTbl_Input_TarFile))
    print '\033[1;32mEXTRACTING INPUT ARCHIVE:    '+str(GGTbl_Input_TarFile)+'  ++++++++ ......................................... ++++++++ \n\033[1;m'
    TgzFileName=os.path.basename(GGTbl_Input_TarFile)
    os.system("cp "+GGTbl_Input_TarFile+" "+RestoreCSVTempPath+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")
    log.info("Extracting Archive ++++++++++++++++..............................+++++++++++++++++%s"%(GGTbl_Input_TarFile))
    os.chdir(RestoreCSVTempPath)
    try:
        log.info("Extracting all CSV Files into:%s"%(RestoreCSVTempPath))
        os.system("tar -xzf "+TgzFileName+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")
    except:
        log.error("Failed to Untar/Extract Archive %s"%(GGTbl_Input_TarFile))
        print '\033[1;31m FAILED TO EXTRACT ARCHIVE:  '+str(GGTbl_Input_TarFile)+'\033[1;m'
        CleanUPGGainhandles(1)
    if glob.glob('*.csv'):
        for CSVFile in glob.glob('*.csv'):
            CSVFile=os.path.basename(CSVFile).rstrip('\n')
            log.info("Reading current untarred CSV File %s"%(CSVFile))
            GGTblName=re.split('(\d+)',CSVFile)[0].replace("DG_", "DG.").rstrip('_')
            #GGTblName='DG.'+CSVFile.split('_'+os.environ.get("PTTSERVERID"))[0].split('DG_')[1]
            log.info("For current CSV File: %s Tbl name is: %s"%(CSVFile,GGTblName))
            GGRestoreValidation(GGTblName,'BEFORE DELETION')
	    DeleteDataGGTbl(GGTblName)
            InsertDataGGTbl(CSVFile,GGTblName) 
            GGRestoreValidation(GGTblName,'AFTER INSERTION')
    print '\033[1;32mSUCCESSFULLY RESTORED GRID GAIN DATA\n\033[1;m'

def CleanUPGGainhandles(flag):
    if(connectionObject!=None and connectionObject!=-1):
        connectionObject.close_connection()
        log.info("Connection Object Closed.")
    log.info("Logger closed.")
    logger.closeLog();
    sys.exit(flag)

################################ Main() ############################################################################################
###################### Connecting to local GG IP address ###########################################################################

GG_SQL_PORT=str(commands.getoutput("egrep '^GG_SQL_PORT=' /DG/activeRelease/dat/CommonConfig.properties | awk -F'=' '{print $2}'")).strip()
log.info("GG_SQL_PORT : "+GG_SQL_PORT)

GGLocalIP=os.environ.get("LOCAL_IP_ADDRESS")
connectionString = GGLocalIP+":"+GG_SQL_PORT
connectionObject=pyodbcdriver(connectionString,log)
status,message=connectionObject.get_connection()
log.info(message)

if status != 0:
    log.error("GG Connection Failed")
    logger.closeLog();
    sys.exit(1)

log.info("GG Connection Success")

###--------------------------------------------------------- function calls ----------------------------------------------------###

GGTbl_Input_TarFile=''
if __name__ == "__main__":
    main(sys.argv[1:])

GGRestorePreCleanUP()
GGRestorePreValidations()
UnTarCSVFilesRestoreGG()

####### GGRestoreValidation is to check if data has been restored in GG since isql has update issue #######
#GGRestoreValidation()
CleanUPGGainhandles(0)

## Modified on 7/25/2018 4:00 PM
