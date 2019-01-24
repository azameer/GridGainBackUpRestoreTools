import sys,os,getopt
import traceback
import commands
import re,csv,glob,tarfile
import time,datetime
sys.path.append("/DG/activeRelease/lib/python_lib/")

from pythonodbcdriver import pyodbcdriver ### Using pyodbc to get GG connection handle
import FileLogger

LOG_FILE_PATH = '/DGlogs/GGBackup.log'
logger = FileLogger.logger(LOG_FILE_PATH);
log = logger.getlogger();
connectionObject=None
CSVFilesList=[]
GGTableList=[]
BackUPCSVPaths='/DGlogs/GGBackUPCSV/'

def Usage():
    print '  \033[1;35m This script would take a BackUP of all given Grid Gain Tables. The GG tables to be backed up is given in a text file format as shown below. The coloumn names are comma separated & follow the table name with colon as shown below.  \033[1;m'
    print '  \n'
    print '  \033[1;35m USAGE : source /DG/activeRelease/Tools/kodiakScripts.conf; python2.7 /DG/activeRelease/Tools/CronJobs/GGBackup.py -f <DBTableNameInputFile.txt>\033[1;m'
    print '  cat DBTableNameInputFile.txt'
    print '  \033[1;35m DG.EMAIL_TAGS: PTTSERVERID,PARAMETERID,PARAMETERNAME,PARAMETERTAG,PARAMETERDESCRIPTION \033[1;m'
    print '  \033[1;35m DG.MICROSVCS_COMMONCONFIG: COUNTRYCODE,CLUSTERID,PARAMNAME,PARAMVALUE,PARAMSCOPE,PATH,LASTUPDATETIME \033[1;m'
    print '  \033[1;35m DG.OIDC_CLIENT_MAP: OIDC_CLIENT_TYPE,OIDC_SVR_CODE,OIDC_KEY_NAME \033[1;m'
    sys.exit(2)


def main(argv):
   global GGTbl_Input_File
   if len(sys.argv) != 3:
        Usage()
   try:
      opts, args = getopt.getopt(argv,"hf:",["fGGTbl_Input_File="])
   except getopt.GetoptError:
      Usage()
   for opt, arg in opts:
      if opt == '-h':
         Usage()
      elif opt in ("-f", "--fGGTbl_Input_File"):
         GGTbl_Input_File = arg

def GGBackPreCleanUP():
   log.info("Cleaning stale files from "+BackUPCSVPaths+"")
   os.system("/bin/rm -rvf "+BackUPCSVPaths+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")
   os.system("/bin/mkdir -p "+BackUPCSVPaths+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")


def RemoveQuotesCSV(CSVFileName):
    try:
        with open(CSVFileName,'r') as f:
            content = f.readlines()
            os.system("/bin/rm -rvf "+CSVFileName+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")
        with open(CSVFileName, 'a') as csvfile:
            csvfile.write(content[0])
            for line in content[1:]:
                ## Strip single && double quotes ##
                line = re.sub('"','', line)
                line=line.replace("'", "")
                ###################################
                csvfile.write(line)
    except IOError:
        print '\033[1;31m ERROR: REMOVING QUOTES FROM CSV: '+CSVFileName+' \033[1;m'

def WriteGGColDataCSV(TblName,Coloumns,CSVFileName):
    GGColDataLists=[]
    SelectGGColQuery="select "+re.sub('\s+|\n','',Coloumns)+" from "+re.sub('\s+|\n','',TblName)+""
    SelectGGData=connectionObject.execute(SelectGGColQuery,1)
    for ColData in SelectGGData[1]:
        #print ColData
        if type(ColData) is str:
            log.warn("WARNING: No data present in GG Table:%s"%(TblName))
        else:
            ### Typecast all values to string for joining ######
            ColData=[str(x) for x in ColData]
            GGColDataLists.append(list(['$#$#'.join(ColData)]))
    ## GG Coloumn data captured as List of Lists ie: [[],[]] relevent to each coloumn name & written to out csv ### 
    log.info("Writing coloumn data for GG Table:%s"%(TblName))
    try:
        with open(CSVFileName, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            Coloumns=re.sub(',','$#$#',re.sub('\s+|\n','',Coloumns))
            csvwriter.writerow(Coloumns.split())
            csvwriter.writerows(GGColDataLists)
    except IOError:
        log.info("ERROR: Writing coloumn data for GG Table:%s"%(TblName))
        print '\033[1;31m ERROR: WRITING COLOUMN DATA FOR GG TBL: '+TblName+' \033[1;m'
        CleanUPGGainhandles(1) 
    RemoveQuotesCSV(CSVFileName)
	
def GenerateGGTblCSVFiles():
    try:
        with open(GGTbl_Input_File) as fopen:
            GGTblData=fopen.readlines()
    except IOError: # parent of IOError, OSError *and* WindowsError where available
        log.info("ERROR: Input File :%s not found"%(GGTbl_Input_File))
        print '\033[1;31m ERROR: Input File '+GGTbl_Input_File+' not found \033[1;m'
        sys.exit(1)
    for TblColData in GGTblData:
        (TblName,Coloumns)=TblColData.split(':')
        DateTimeStrg=datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        CSVFileName='DG_'+re.sub('^dg.|^DG.','',TblName)+'_'+os.environ.get("PTTSERVERID")+'_'+os.environ.get("CLUSTERID")+'_'+re.sub('\s+|\n|_|:','',DateTimeStrg)+'.csv'
        log.info("Writing GG Coloumn Data:%s to CSV File %s"%(Coloumns,CSVFileName))
        WriteGGColDataCSV(TblName,Coloumns,BackUPCSVPaths+CSVFileName)

def TarGGTblCSVFiles():
    log.info("Tarring the CSV Files list")
    if not os.path.exists('/DGdata/Backup/DBBackup/'):
        os.makedirs('/DGdata/Backup/DBBackup/')
    DateTimeStrg=datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    TgzFileName='/DGdata/Backup/DBBackup/DG_'+os.environ.get("PTTSERVERID")+'_'+os.environ.get("CLUSTERID")+'_'+re.sub('\s+|\n|_|:','',DateTimeStrg)+'.tgz'
    log.info("Creating Archive %s"%(TgzFileName))
    os.chdir(BackUPCSVPaths)
    try:
        log.info("Archiving all CSV Files into /DGdata/Backup/DBBackup/ Location")
        os.system("tar -cvzf "+TgzFileName+" *.csv 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")
    except:
        log.info("ERROR: Failed to Archive all CSV Files to "+TgzFileName+"")
        print '\033[1;31m Failed to Archive all CSV Files to '+TgzFileName+' \033[1;m'
        CleanUPGGainhandles(1)
    log.info("GG Backup is success")
    print '\033[1;32m GG Backup is success \033[1;m'
    print '\033[1;32m ARCHIVED FILE IS: '+TgzFileName+' \033[1;m'
    os.system("/bin/rm -rvf "+BackUPCSVPaths+" 2>>"+LOG_FILE_PATH+" >> "+LOG_FILE_PATH+"")

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
GGTbl_Input_File=''
if __name__ == "__main__":
    main(sys.argv[1:])

GGBackPreCleanUP()
GenerateGGTblCSVFiles()
TarGGTblCSVFiles()
CleanUPGGainhandles(0)

## Modified on 7/25/2018 4:00 PM
