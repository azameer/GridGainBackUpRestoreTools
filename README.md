This script GGBackup.py would take a BackUP of all given Grid Gain Tables. The GG tables to be backed up is given in a text file 
format as shown below. The coloumn names are comma separated & follow the table name with colon as shown below.  \033[1;m'

USAGE : python2.7 GGBackup.py -f <DBTableNameInputFile.txt>\033[1;m'

cat DBTableNameInputFile.txt'

DG.EMAIL_TAGS: PTTSERVERID,PARAMETERID,PARAMETERNAME,PARAMETERTAG,PARAMETERDESCRIPTION \033[1;m'
DG.MICROSVCS_COMMONCONFIG: COUNTRYCODE,CLUSTERID,PARAMNAME,PARAMVALUE,PARAMSCOPE,PATH,LASTUPDATETIME \033[1;m'
DG.OIDC_CLIENT_MAP: OIDC_CLIENT_TYPE,OIDC_SVR_CODE,OIDC_KEY_NAME \033[1;m'

OUTPUT: The ouput would be a tar file in a specific path

This script GGRestore.py would Restore all Grid Gain Data present in input BackUP/*.tgz File.

USAGE :python2.7 GGRestore.py -f /DGdata/Backup/DBBackup/<DBBackUPTarFile.tgz File/Path

The input BackUP tgz File name should be of format as shown below
DG_$PTTSERVERID_$CLUSTERID_$DATETIME.tgz
DG_444441_2_20180530124557.tgz
