	# -*- coding: utf-8 -*-	
r'''	
Created on: Mon 14 Dec 20 18:09:21	
Author: SAS2PY Code Conversion Tool	
SAS Input File: gw_ipm_renratio regional auto	
SAS File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS_SRC_CDE	
Generated Python File: Sas2PyConvertedScript_Out	
Python File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS2PY_TRANSLATED	
'''	
''' Importing necessary standard Python 3 modules	
Please uncomment the commented modules if necessary. '''	
''' Importing necessary project specific core utility python modules.'''	
'''Please update the below path according to your project specification where core SAS to Python code conversion core modules stored'''	
import yaml	
import logging	
from sas2py_func_lib_repo_acg import *	
from sas2py_code_converter_funcs_acg import *	
import sys	
import re	
import sqlite3	
import psutil	
import os	
import gc	
import pandas as pd	
import numpy as np	
import itertools	
from sas7bdat import SAS7BDAT	
from functools import reduce, partial	
from google.cloud import bigquery	
# Seting up logging info #	
config_file = None	
yaml_file = None	
try:	
    config_file = open('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_renratio_regional_auto/config.yaml', 'r+')	
    yaml_file = yaml.load(config_file)	
except Exception as e:	
    print("Error reading config file | ERROR : ", e)	
finally:	
    config_file.close()	
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/data02/keys/lab-e42-bq.json"	
LatestMon = yaml_file['LatestMon']	
Beg2 = yaml_file['Beg2']	
End2 = yaml_file['End2']	
InfMon2 = yaml_file['InfMon2']	
Beg1 = yaml_file['Beg1']	
End1 = yaml_file['End1']	
InfMon1 = yaml_file['InfMon1']	
InfMon0 = yaml_file['InfMon0']	
project_id  = yaml_file['gcp_project_id']	
output_dataset = yaml_file['gcp_output_dataset_id']	
gen1_dataset_id = yaml_file['gcp_gen1_dataset_id']	
gen2_dataset_id = yaml_file['gcp_gen2_dataset_id']	
history_dataset_id = yaml_file['gcp_history_dataset_id']	
output_tables = yaml_file['output_tables']	
log_file_name = os.path.basename(__file__).lstrip('sas2py_converted_script_')[:-3]	
logging.basicConfig(filename= yaml_file['logs'] + os.sep + log_file_name + '.log',level=logging.INFO,format='%(asctime)s - '+ log_file_name.upper()+' %(levelname)s - %(message)s')	
SQLitePythonWorkDb = yaml_file['workDb']	
sqliteConnection=sqlite3.connect(SQLitePythonWorkDb)	
sqliteDb = SQLitePythonWorkDb	
#SQLitePythonGenWorkDb = 'gen1_gen2_output.db'	
#sqliteGenConnection = sqlite3.connect(SQLitePythonWorkDb)	
logging.info('Execution python script started.')	
logging.info('Imported all necessary python modules.')	
if (sqliteConnection):	
    logging.info('Sqlite3 temporary work DB set up completed.')	
''' Imported Python Dictionary To Capture SAS Macros:	
	1.It's functionality to mimic SAS macro variables concept	
	2.All macro variables found is SAS script would be added to this SasMacroDict dictonary	
	3.In Python script dictionary keys are nothing but SAS macrovariables'''	
logging.info('Global dictionary initiated to resolve SAS macro varaibles.')	
def procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable):	
    if '_sqlitesorted' in tgtSqliteTable:	
        tgtSqliteTable = tgtSqliteTable.replace('_sqlitesorted', '')	
    try:	
        sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
        cursor = sqliteConnection.cursor()	
        if (sqliteConnection):	
            logging.info('Connected to SQLite temporary work DB')	
        logging.info('executing {0} table'.format(tgtSqliteTable))	
        cursor.executescript(sql)	
        cursor.close()	
        cursor = sqliteConnection.cursor()	
        row_count_sql = "select max(_ROWID_) from {} limit 1;".format(	
            tgtSqliteTable)	
        cursor.execute(row_count_sql)	
        row_num = cursor.fetchall()[0][0]	
        cursor.close()	
    except sqlite3.Error as error:	
        logging.error('Table creation is unsucessful due to {}'.format(error))	
    finally:	
        if (sqliteConnection):	
            sqliteConnection.close()	
        logging.info('Table {} created successfully with {} records.'.format(	
            tgtSqliteTable.upper(), row_num))	
        logging.info('Sqlite working DB connection is closed.')	
def mcrResl(Query):	
    # Query = re.sub(r"&&","&",Query) #to handle multiple macros resolution	
    try:	
        # run until all macros resolved	
        while len(re.search(r"&[\w]+\.?", Query).group(0)) > 0:	
            McrRegex = r"&[\w]+\.?"	
            McrMatches = re.finditer(McrRegex, Query, re.MULTILINE)	
            for matchNum, match in enumerate(McrMatches, start=1):	
                Mcr = match.group().strip()	
                if Mcr.find('.') > 0:  # Check for macro ending	
                    McrNm = Mcr[1:-1]	
                else:	
                    McrNm = Mcr.strip()[1:]	
                try:	
                    McrVal = eval(f'{McrNm}')	
                    McrVal = str(McrVal)	
                    Query = re.sub(Mcr, McrVal, Query)	
                except NameError:	
                    try:	
                        McrVal = eval(f'{McrNm.lower()}')	
                        McrVal = str(McrVal)	
                        Query = re.sub(Mcr, McrVal, Query)	
                    except NameError:	
                        pass	
    except (KeyError):	
        logging.error(' {} SAS Macro variable unresolved'.format(Mcr))	
    except (AttributeError, KeyError):	
        pass	
    return Query	
def df_lower_colNames(dfName, tablename=None):	
    # handling data frame column case senstivity	
    dfName.columns = map(str.lower, dfName.columns)	
    rows = len(dfName.index)	
    # tabNm = [x for x in globals() if globals()[x] is dfName][0]	
    # logging data frame creation	
    if tablename is not None:	
        logging.info(	
            'There were total {} records read from table:{}.'.format(rows, tablename))	
    else:	
        logging.info(	
            'There were total {} records read from table DATAFRAME.'.format(rows))	
def df_creation_logging(dfName, tablename=None):	
    rows = len(dfName.index)	
    cols = len(dfName.columns)	
    try:	
        tabNm = [x for x in globals() if globals()[x] is dfName][0]	
    except:	
        tabNm = tablename	
    logging.info('Table {} created successfully with {} records and {} columns.'.format(	
        tabNm.upper(), rows, cols))	
### SAS Source Code Line Numbers START:1 & END:1.###	
def df_remove_indexCols(mrgResultTmpDf):	
    # removing unnecessary columns to stop writing to sqlite table	
    #no_index_cols = [c for c in mrgResultTmpDf.columns if (c != "index_x" and c != "index_y" and  c != "index")]	
    #mrgResultTmpDf = mrgResultTmpDf[no_index_cols]	
    return mrgResultTmpDf.loc[:, ~mrgResultTmpDf.columns.str.startswith('index')]	
    # df.loc[:,~df.columns.str.startswith('index')	
def sqliteToBQ(output_tables):	
    i = 1	
    incr_recs = 0	
    sql = "select * from outfiledb"	
    logging.info('Getting table outfiledb from sqlitedb')		
    try:	
        SQLitePythonWorkDb = yaml_file['workDb']	
        sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
        df=pd.read_sql(sql,con=sqliteConnection)	
        df.to_gbq(destination_table = output_dataset + '.' + output_tables, project_id = project_id, if_exists='replace')	
        logging.info('output_table {} created successfully with {} records and {} columns.'.format(output_tables.upper(),incr_recs,cols))	
        del df	
        gc.collect()	
    except sqlite3.Error as e:	
        logging.error('output_table {} creation is failed.'.format(output_tables.upper()))	
        logging.error('Error - {}'.format(e))	
    finally:	
        sqliteConnection.close()	
# Null values handling	
def sas2pyNvl(v):	
    if str(v).strip() == '':	
        return 0	
    else:	
        return v	
def df_memory_mgmt(dfList):	
    process = psutil.Process(os.getpid())	
    mem = ((process.memory_info()[0])/1024)/1024	
    logging.info('Memory in use before clean up in MB:{}'.format(mem))	
    dfL = [','.join([x for x in dfList])]	
    #del [[dfL]]	
    # gc.collect()	
    logging.info('Cleaning up memory consumed by the following data frames.')	
    logging.info('{}'.format(dfL))	
    for df in dfList:	
        del df	
        df = pd.DataFrame()	
        del df	
        #logging.info('Memory management is in progress for {}'.format(df.upper()))	
    gc.collect()	
    process = psutil.Process(os.getpid())	
    mem = ((process.memory_info()[0])/1024)/1024	
    logging.info('Memory in use after clean up in MB :{}'.format(mem))	
# Function to handle sas merge data step merge process	
suffix_list = ('_s2pL', '_s2pR')	
def sas2pyMergedfs(df1, df2, on=[]):	
    for keyi in on:	
        if df1[keyi].dtypes == df2[keyi].dtypes:	
            pass	
        else:	
            if df1[keyi].dtypes == 'float64' and df2[keyi].dtypes == 'int64':	
                df1 = df1[df1[keyi].notna()]	
                df1 = df1.astype({keyi: 'int'})	
            elif df2[keyi].dtypes == 'float64' and df1[keyi].dtypes == 'int64':	
                df2 = df2[df2[keyi].notna()]	
                df2 = df2.astype({keyi: 'int'})	
            elif df2[keyi].dtypes == 'object' and df1[keyi].dtypes == 'int64':	
                df2 = df2[df2[keyi].notna()]	
                df2 = df2.astype({keyi: 'int'})	
            elif df1[keyi].dtypes == 'object' and df2[keyi].dtypes == 'int64':	
                df1 = df1[df1[keyi].notna()]	
                df1 = df1.astype({keyi: 'int'})	
            elif df1[keyi].dtypes == 'object' and df2[keyi].dtypes == 'float64':	
                df1 = df1[df1[keyi].notna()]	
                df2 = df2[df2[keyi].notna()]	
                df2 = df2.astype({keyi: 'int'})	
                df1 = df1.astype({keyi: 'int'})	
            elif df2[keyi].dtypes == 'object' and df1[keyi].dtypes == 'float64':	
                df1 = df1[df1[keyi].notna()]	
                df2 = df2[df2[keyi].notna()]	
                df2 = df2.astype({keyi: 'int'})	
                df1 = df1.astype({keyi: 'int'})	
    df1 = df1.drop_duplicates()	
    df2 = df2.drop_duplicates()	
    df1.set_index(on)	
    df2.set_index(on)	
    df1.sort_index(axis=1)	
    mrgResultTmpDf = pd.merge(df1, df2, how='outer',	
                              on=on, suffixes=suffix_list)	
    mrgCols = set([col.split('s2p')[0][:-1]	
                   for col in mrgResultTmpDf.columns if 's2p' in col])	
    for col in mrgCols:	
        colR = col+suffix_list[1]	
        colL = col+suffix_list[0]	
        mrgResultTmpDf[col] = mrgResultTmpDf[colR].combine_first(	
            mrgResultTmpDf[colL])	
        mrgResultTmpDf.drop([colR, colL], axis=1, inplace=True)	
    mrgResultTmpDf = mrgResultTmpDf.drop_duplicates()	
    return mrgResultTmpDf	
### SAS Source Code Line Numbers START:1 & END:1.###	
'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.	
Please find below Please find below SAS code lines.	
libname Agent "T:\Shared\Acturial\Special Projects\Research\Agents\Business Intelligence";	
'''	
### SAS Source Code Line Numbers START:2 & END:2.###	
'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.	
Please find below Please find below SAS code lines.	
libname outfile "T:\Shared\Acturial\BISProd\RenewalRatio\InputOutput";	
'''	
### SAS Source Code Line Numbers START:4 & END:4.###	
'''SAS Comment:/*Update each month*/ '''	
### SAS Source Code Line Numbers START:5 & END:5.###	
'''SAS Comment:*** AJS: This needs to be rewritten using SAS dates and automation w/ one hardcoded date; '''	
### SAS Source Code Line Numbers START:6 & END:6.###	
### SAS Macro varaibles conversion in python for:%let LatestMon = 202010; /* Run-As-Of Month */###	
#LatestMon = int(yaml_file['LatestMon'])	
### SAS Source Code Line Numbers START:7 & END:7.###	
### SAS Macro varaibles conversion in python for:%let Beg2 = 20201001; /* Fist day of LatestMon */###	
#Beg2 = int(yaml_file['Beg2'])	
### SAS Source Code Line Numbers START:8 & END:8.###	
### SAS Macro varaibles conversion in python for:%let End2 = 20201031; /* Last business day of LatestMon */###	
#End2 = int(yaml_file['End2'])	
### SAS Source Code Line Numbers START:10 & END:10.###	
### SAS Macro varaibles conversion in python for:%let InfMon2 = 202009; /* 1 month less than LatestMon */###	
#InfMon2 = int(yaml_file['InfMon2'])	
### SAS Source Code Line Numbers START:11 & END:11.###	
### SAS Macro varaibles conversion in python for:%let Beg1 = 20200901; /* First day of InfMon2 */###	
#Beg1 = int(yaml_file['Beg1'])	
### SAS Source Code Line Numbers START:12 & END:12.###	
### SAS Macro varaibles conversion in python for:%let End1 = 20200930; /* Last business day of InfMon2 */###	
#End1 = int(yaml_file['End1'])	
### SAS Source Code Line Numbers START:14 & END:14.###	
### SAS Macro varaibles conversion in python for:%let InfMon1 = 202008; /* 2 months less than LatestMon */###	
#InfMon1 = int(yaml_file['InfMon1'])	
### SAS Source Code Line Numbers START:15 & END:15.###	
### SAS Macro varaibles conversion in python for:%let InfMon0 = 202007; /* 3 months less than LatestMon */###	
#InfMon0 = int(yaml_file['InfMon0'])	
### SAS Source Code Line Numbers START:18 & END:18.###	
'''SAS Comment:/* Get the policy list in each inforce month, gen1 and gen2 combined*/ '''	
### SAS Source Code Line Numbers START:19 & END:42.###	
''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''	
def GetPol(InfMon):	
    '''libname RegInf "T:\Shared\Acturial\Pricing\Regional Pricing\IPM SAS Data\Auto\In-force\&InfMon"'''	
    '''WARNING: Below SAS step has not converted in this release.	
    libname RegInf "T:\Shared\Acturial\Pricing\Regional Pricing\IPM SAS Data\Auto\In-force\&InfMon";	
    '''	
    '''SAS Comment:*AJS: Need to add TERMINCEP to combined INF dataset used for retention comparison and make it the same data type between Gen 1 & 2; '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    client=bigquery.Client()	
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen1_dataset_id, InfMon)	
    df_inforce_gen1 = client.query(sql).to_dataframe()	
    '''	
    sqliteGenConnection = sqlite3.connect(SQLitePythonGenWorkDb)	
    df = pd.read_sql_query("select * from inforce{}".format(InfMon),sqliteGenConnection)    	
    # lowering all columns	
    df_lower_colNames(df, 'inforce{}'.format(InfMon))	
    # logging info	
    df_creation_logging(df, "inforce{}".format(InfMon))	
    # putting into the sqliteDB	
    df.to_sql("inforce{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace', index=True)	
    sqliteGenConnection.close()	
    '''	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    '''df = pd.read_sql_query(	
        "select * from inforce{}".format(InfMon), sqliteConnection)'''	
    df = df_inforce_gen1	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'inforce')	
    df = df[['mvstate', 'policy', 'hhclient', 'termincep']]	
    df['termincep'] = df['termincep'].astype(str)	
    df['termeffmonth'] = df['termincep'].str.slice(0, 6).astype(int)	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns="termincep")	
    df = df_remove_indexCols(df)	
    logging.info(	
        "inforceG1 created successfully with {} records".format(len(df)))	
    del df_inforce_gen1	
    # Push results data frame to Sqlite DB	
    df.to_sql("inforceG1", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen2_dataset_id, InfMon)	
    df_inforce_gen2 = client.query(sql).to_dataframe()	
    	
    '''	
    sqliteGenConnection = sqlite3.connect(SQLitePythonGenWorkDb)	
    df = pd.read_sql_query("select * from inforcegenii{}".format(InfMon),sqliteGenConnection)    	
    # lowering all columns	
    df_lower_colNames(df, 'inforcegenii{}'.format(InfMon))	
    # logging info	
    df_creation_logging(df, "inforcegenii{}".format(InfMon))	
    # putting into the sqliteDB	
    df.to_sql("inforcegenii{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace', index=True)	
    sqliteGenConnection.close()	
    '''	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    '''df = pd.read_sql_query(	
        "select * from inforcegenii{}".format(InfMon), sqliteConnection)'''	
    df = df_inforce_gen2	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'inforce')	
    df = df[['mvstate', 'policy', 'hhclient', 'termincep']]	
    df['termincep'] = df['termincep'].astype(str)	
    df['termeffmonth'] = df['termincep'].str.slice(0, 6).astype(int)	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns="termincep")	
    df = df_remove_indexCols(df)	
    logging.info(	
        "inforceG2 name created successfully with {} records".format(len(df)))	
    del df_inforce_gen2	
    # Push results data frame to Sqlite DB	
    df.to_sql("inforceG2", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    # Converting source inforceG1 data into datafram.	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    inforceG1 = pd.read_sql_query("select * from inforceG1 ", sqliteConnection)	
    # Converting source inforceG2 data into datafram.	
    inforceG2 = pd.read_sql_query("select * from inforceG2 ", sqliteConnection)	
    # Concatenate the source data frames	
    df = pd.concat([inforceG1, inforceG2], ignore_index=True, sort=False)	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "InfRen{} created successfully with {} records".format(InfMon, len(df)))	
    df.to_sql("InfRen{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
'''Uncomment to execute the below sas macro'''	
# GetPol(<< Provide require args here >>)	
### SAS Source Code Line Numbers START:44 & END:51.###	
"""ERROR: Unable to convert the below SAS block/code into python	
    data junk1;	
    stuff = &InfMon1;	
    do while (stuff < &LatestMon);	
    stuff = stuff + 1;	
    if MOD(stuff,100) = 13 then stuff = stuff + 88;	
    call execute ('%GetPol (InfMon ='||stuff||')');	
    end;	
    run;	
    """	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
stuff = InfMon1	
while(stuff < LatestMon):	
    stuff = stuff+1	
    if stuff % 100 == 13:	
        stuff = stuff+88	
    GetPol(stuff)	
df = pd.DataFrame()	
df['stuff'] = stuff	
df.to_sql("junk1", con=sqliteConnection, if_exists='replace')	
### SAS Source Code Line Numbers START:54 & END:547.###	
''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''	
def Renew(InfMon):	
    '''libname RegInf "T:\Shared\Acturial\Pricing\Regional Pricing\IPM SAS Data\Auto\In-force\&InfMon"'''	
    '''WARNING: Below SAS step has not converted in this release.	
    libname RegInf "T:\Shared\Acturial\Pricing\Regional Pricing\IPM SAS Data\Auto\In-force\&InfMon";	
    '''	
    '''SAS Comment:/***** Gen1 *********************************************************************************************************************/ '''	
    '''SAS Comment:/********************************************************************************************************************************/ '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    client=bigquery.Client()	
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen1_dataset_id, InfMon)	
    df_inforce_gen1 = client.query(sql).to_dataframe()	
    '''	
    sqliteGenConnection = sqlite3.connect(SQLitePythonGenWorkDb)	
    df = pd.read_sql_query("select * from inforce{}".format(InfMon),sqliteGenConnection)    	
    # lowering all columns	
    df_lower_colNames(df, 'inforce{}'.format(InfMon))	
    # logging info	
    df_creation_logging(df, "inforce{}".format(InfMon))	
    # putting into the sqliteDB	
    df.to_sql("inforce{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace', index=True)	
    sqliteGenConnection.close()	
    '''	
		
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    '''df = pd.read_sql_query(	
        "select * from inforce{}".format(InfMon), sqliteConnection)'''	
    df = df_inforce_gen1	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'inforce')	
    df = df[['mvstate', 'policy', 'client', 'hhclient', 'cdtscore', 'seqagtno', 'inception', 'termincep',	
             'duedate', 'mltprdind', 'bi_prm', 'cp_prm', 'cl_prm', 'memberind', 'primaryclass', 'mvyear', 'billplan']]	
    df['duedatenum'] = df['duedate'].astype(int)	
    # ***Start manual effort here...	
    # if &InfMon = &InfMon1 then do;	
    # BegDue = &Beg1;	
    # EndDue = &End1;	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    if InfMon == InfMon1:	
        BegDue = Beg1	
        EndDue = End1	
    elif InfMon == InfMon2:	
        BegDue = Beg2	
        EndDue = End2	
    else:	
        BegDue = 0	
        EndDue = 0	
    # ***Start manual effort here...	
    # else if &InfMon = &InfMon2 then do;	
    # BegDue = &Beg2;	
    # EndDue = &End2;	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    logging.info(	
        "Gen1 created successfully with {} records".format(len(df)))	
    del df_inforce_gen1	
    # Push results data frame to Sqlite DB	
    df.to_sql("Gen1", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    	
    #*******************************End of Data Step Process**************************************************#	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************	
    '''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from Gen1 where DueDateNum between {} and {}".format(BegDue, EndDue), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Gen1')	
    # End manual effort.***	
    df['duedate'] = df['duedate'].astype(str)	
    df['duemon'] = df['duedate'].str.slice(0, 6).astype(int)	
    df = df.loc[(df.seqagtno != np.nan) | (df.seqagtno != '') | (df.seqagtno != ' ') | ~(df.seqagtno.isnull())]	
    df = df.loc[(df.policy != np.nan) | (df.policy != '') | (df.policy != ' ') | ~(df.policy.isnull())]	
    # if policy^=.; # Manual effort require.	
    df['bi_prm'] = df['bi_prm'].astype(np.float64)	
    df['cp_prm'] = df['cp_prm'].astype(np.float64)	
    df['cl_prm'] = df['cl_prm'].astype(np.float64)	
    indexNames = df[(df['bi_prm'].isin([0, np.nan, 0.0]) | df['bi_prm'].isnull()) & ((df['cp_prm'].isin([0, np.nan, 0.0])) | df['cp_prm'].isnull()) & ((df['cl_prm'].isin([0, np.nan, 0.0])) | df['cl_prm'].isnull())].index	
    df.drop(indexNames, inplace=True)	
    # if bi_prm in (0 .) and comp_prm in (0 .) and cmpf_prm in (0 .) and coll_prm in (0 .) then delete; # Manual effort require.	
    df['productgen'] = 'Gen1'	
    # ***Start manual effort here...	
    # if bi_prm not in (0 .) and sum(0,comp_prm,cmpf_prm,coll_prm)^=0 then Cov=1;	
    df['cdtscore'] = df['cdtscore'].apply(str)	
    df['cov'] = [1 if (~((a in [0, 0.0, np.nan]) | (np.isnan(a))) & ~(((np.isnan(b)) & (np.isnan(c))) | ((b+c) in [0, 0.0, np.nan]))) else 0 if (((b in [0, 0.0, np.nan]) | (np.isnan(b))) & ((c in [0, 0.0, np.nan]) | (np.isnan(c)))) else 99 for a, b, c in zip(df['bi_prm'], df['cp_prm'], df['cl_prm'])]	
    if not df.empty:	
        df.loc[(df['mvstate'].isin(	
			['IA', 'IN', 'IL', 'MN', 'WI', 'OH'])), 'premier'] = 1	
        df.loc[(df['mvstate'].isin(['IA', 'IN', 'IL', 'MN', 'WI', 'OH'])  & (df['cdtscore'].isin(['06', '07', '08', '09', '6', '7', '8', '9']))), 'premier'] = 3	
        df.loc[(df['mvstate'].isin(['IA', 'IN', 'IL', 'MN', 'WI', 'OH']))	
			   & (df['cdtscore'].isin(['03', '04', '05', '3', '4', '5'])), 'premier'] = 2	
        df.loc[(df['mvstate'].isin(['ND', 'NE'])), 'premier'] = 1	
        df.loc[(df['mvstate'].isin(['ND', 'NE'])) & (	
			df['cdtscore'].isin(['07', '08', '09', '7', '8', '9'])), 'premier'] = 3	
        df.loc[(df['mvstate'].isin(['ND', 'NE'])) & (	
			df['cdtscore'].isin(['05', '06', '5', '6'])), 'premier'] = 2	
        df.loc[(df['mvstate'].isin(	
			['IA', 'IN', 'IL', 'MN', 'WI', 'OH'])), 'premier2'] = 1	
        df.loc[(df['mvstate'].isin(['IA', 'IN', 'IL', 'MN', 'WI', 'OH']))			   & (df['cdtscore'].isin(['08', '09', '8', '9'])), 'premier2'] = 4	
        df.loc[(df['mvstate'].isin(['IA', 'IN', 'IL', 'MN', 'WI', 'OH']))	
			   & (df['cdtscore'].isin(['05', '06', '07', '5', '6', '7'])), 'premier2'] = 3	
        df.loc[(df['mvstate'].isin(['IA', 'IN', 'IL', 'MN', 'WI', 'OH']))	
			   & (df['cdtscore'].isin(['02', '03', '04', '2', '3', '4'])), 'premier2'] = 2	
        df.loc[(df['mvstate'].isin(['ND', 'NE'])), 'premier2'] = 1	
        df.loc[(df['mvstate'].isin(['ND', 'NE'])) & (	
			df['cdtscore'].isin(['08', '09', '8', '9'])), 'premier2'] = 4	
        df.loc[(df['mvstate'].isin(['ND', 'NE'])) & (	
			df['cdtscore'].isin(['07', '06', '6', '7'])), 'premier2'] = 3	
        df.loc[(df['mvstate'].isin(['ND', 'NE'])) & (	
			df['cdtscore'].isin(['05', '5'])), 'premier2'] = 2	
    else:	
        df['premier'] = np.nan	
        df['premier2'] = np.nan	
    df['mulprod'] = [0 if x in (np.nan, 'N', '', ' ')	
					 else 1 for x in df['mltprdind']]	
    df['member'] = [1 if x == 'Y' else 0 for x in df['memberind']]	
    df['termincep'] = df['termincep'].astype(str)	
    df['inception'] = df['inception'].astype(str)	
    df['primaryclass_temp'] = df['primaryclass'].fillna(0).astype(np.int64)	
    df['tenure'] = df['termincep'].str.slice(0, 4).astype(float)-df['inception'].str.slice(0, 4).astype(float)+(df['termincep'].str.slice(4, 6).astype(float) -	
					df['inception'].str.slice(4, 6).astype(float))/12+(df['termincep'].str.slice(6, 8).astype(float)-df['inception'].str.slice(6, 8).astype(float))/365	
    df['tenure'] = df['tenure'] * 10	
    df['tenure'] = df.tenure.apply(np.floor)	
    df.loc[(df.tenure == np.nan) | (df.tenure.isnull()) | (df.tenure == '') | (df.tenure == ' '), 'tenure'] = 0	
    df.loc[df.tenure.isin([4.0, 4]), 'tenure'] = 3	
    df.loc[df.tenure >= 5.0, 'tenure'] = 5	
    df['tenure'] = df.tenure.astype(np.int64)	
    df['assigneddrvage'] = [65 if x in [8031, 8032, 8033, 8038, 8039, 1801, 1802, 1803, 1808, 1809, 2801, 2802, 2803, 2808, 2809] else 45 if x in [1851, 1852, 1853, 1858, 1859, 2851, 2852, 2853, 2858, 2859, 3851, 3852, 3853, 3858, 3859, 1861, 1862, 1863, 1868, 1869] else 30 if x in [2861, 2862, 2863, 2868, 2869, 3861, 3862, 3863, 3868, 3869, 4861, 4862, 4863, 4868, 4869] else 25 if x in [4871, 4872, 4873, 4878, 4879, 5871, 5872, 5873, 5878, 5879, 8708, 8709, 7871, 7872, 7873, 7878, 7879, 6871, 6872, 6873, 6878, 6879] else 1 if x in [1254, 1255, 1354, 1355, 2254, 2255, 2354, 2355, 1256, 1257, 1356, 1357, 2256, 2257, 2356, 2357, 1754, 1755, 1704, 1705, 2754, 2755, 	
    2704, 2705, 1756, 1757, 1706, 1707, 2756, 2757, 2706, 2707, 8064, 8065, 8164, 8165, 8074, 8075, 8174, 8175, 8084, 8085, 8184, 8185, 8094, 8095, 8194, 8195, 8066, 	
    8067, 8166, 8167, 8076, 8077, 8176, 8177, 8086, 8087, 8186, 8187, 8096, 8097, 8196, 8197, 8460, 8463, 8660, 8663, 8470, 8473, 8670, 8673, 8480, 8483, 8680, 8683, 	
    8490, 8493, 8690, 8693, 8466, 8468, 8666, 8668, 8476, 8478, 8676, 8678, 8486, 8488, 8686, 8688, 8496, 8498, 8696, 8698, 2871, 2872, 2873, 2878, 2879, 8964, 8965, 	
    8966, 8967, 8974, 8975, 8976, 8977, 8984, 8985, 8986, 8987, 8994, 8995, 8996, 8997, 1554, 1555, 1556, 1557, 2554, 2555, 2556, 2557] else np.nan for x in df['primaryclass_temp']]	
    df['termincep'] = df['termincep'].astype(str)	
    df['termincep'] = df['termincep'].astype(str)	
    df['termyr'] = df['termincep'].str.slice(0, 4).astype(int)	
    df['mvyear'] = df['mvyear'].astype(int)	
    df['vehage'] = np.maximum(0, (df['termyr']-df['mvyear']))	
    # if substr(billplan,1,3) in ("EFT") then EFT1=1; # Manual effort require.	
    df['eft1'] = 0	
    col = 'billplan'	
    conditions = [df[col].str.slice(0, 3) == 'EFT', df[col] == ""]	
    choices = [1, -1]	
    df['eft1'] = np.select(conditions, choices)	
    logging.info(	
        "Gen1 created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("Gen1", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    	
    #*******************************End of Data Step Process**************************************************#	
    '''SAS Comment:/***** Gen2 *********************************************************************************************************************/ '''	
    '''Python Indentation required, DO loop start detected. Please intend the code	
    if mod(&InfMon,100) >11 :	
    if input(substr(DueDate,1,6),best12.) ==  &InfMon+89end	    else:	
    do if input(substr(DueDate,1,6),best12.)=&InfMon+1end '''	    '''SAS Comment:/********************************************************************************************************************************/ 	
    *********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************	
    '''	
    	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen2_dataset_id, InfMon)	
    df_inforce_gen2 = client.query(sql).to_dataframe()	
    '''	
    # Open connection to Sqlite work data base	
    sqliteGenConnection = sqlite3.connect(SQLitePythonGenWorkDb)	
    df = pd.read_sql_query("select * from inforcegenii{}".format(InfMon),sqliteGenConnection)    	
    # lowering all columns	
    df_lower_colNames(df, 'inforcegenii{}'.format(InfMon))	
    # logging info	
    df_creation_logging(df, "inforcegenii{}".format(InfMon))	
    # putting into the sqliteDB	
    df.to_sql("inforcegenii{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace', index=True)	
    sqliteGenConnection.close()	
    '''	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    '''df = pd.read_sql_query(	
        "select * from inforcegenii{}".format(InfMon), sqliteConnection)'''	
    df = df_inforce_gen2	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'inforce')	
    # Drop columns from source df data in datafram.	
    df = df.drop(columns="product")	
    # ***Start manual effort here...	
    # if &InfMon = &InfMon1 then do;	
    # BegDue = &Beg1;	
    # EndDue = &End1;	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    if InfMon == InfMon1:	
        BegDue = Beg1	
        EndDue = End1	
    elif InfMon == InfMon2:	
        BegDue = Beg2	
        EndDue = End2	
    else:	
        BegDue = 0	
        EndDue = 0	
    # ***Start manual effort here...	
    # else if &InfMon = &InfMon2 then do;	
    # BegDue = &Beg2;	
    # EndDue = &End2;	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    df = df_remove_indexCols(df)	
    logging.info(	
        "Gen2 created successfully with {} records".format(len(df)))	
    del df_inforce_gen2	
    # Push results data frame to Sqlite DB	
    df.to_sql("Gen2", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from Gen2 where DueDate between {} and {}".format(BegDue, EndDue), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Gen2')	
    # End manual effort.***	
    df['duemon'] = (df['duedate']/100).astype(np.int64)	
    # if seqagtno^=.; # Manual effort require.	
    df = df.loc[(df.seqagtno != np.nan) | (df.seqagtno != '') | (df.seqagtno != ' ') | ~(df.seqagtno.isnull())]	
    df = df.loc[(df.policy != np.nan) | (df.policy != '') | (df.policy != ' ') | ~(df.policy.isnull())]	
    # if policy^=.; # Manual effort require.	
    df['bi_prm'] = df['bi_prm'].astype(np.float64)	
    df['comp_prm'] = df['comp_prm'].astype(np.float64)	
    df['cmpf_prm'] = df['cmpf_prm'].astype(np.float64)	
    df['coll_prm'] = df['coll_prm'].astype(np.float64)	
    indexNames = df[(df['bi_prm'].isin([0, np.nan, 0.0]) | df['bi_prm'].isnull()) & ((df['comp_prm'].isin([0, np.nan, 0.0])) | df['comp_prm'].isnull()) & ((df['cmpf_prm'].isin([0, np.nan, 0.0])) | df['cmpf_prm'].isnull()) & ((df['coll_prm'].isin([0, np.nan])) | df['coll_prm'].isnull()) ].index	
    df.drop(indexNames, inplace=True)	
    # if bi_prm in (0 .) and comp_prm in (0 .) and cmpf_prm in (0 .) and coll_prm in (0 .) then delete; # Manual effort require.	
    df['productgen'] = 'Gen2'	
    # ***Start manual effort here...	
    # if bi_prm not in (0 .) and sum(0,comp_prm,cmpf_prm,coll_prm)^=0 then Cov=1;	
    df['cdtscore'] = df['cdtscore'].apply(str)	
    df['cov'] = [1 if (~((a in [0, 0.0, np.nan]) | (np.isnan(a))) & ~(((np.isnan(b) & np.isnan(c) & np.isnan(d)) | ((b+c+d) in [0, 0.0, np.nan])))) else 0 if (((b in [0, 0.0, np.nan]) | (np.isnan(b))) & ((c in [0, 0.0, np.nan]) | (np.isnan(c))) & ((d in [0, 0.0, np.nan]) | (np.isnan(d)))) else 99 for a, b, c, d in zip(df['bi_prm'], df['comp_prm'], df['cmpf_prm'], df['coll_prm'])]	
		
    df['premier'] = [3 if x in ['64', '66', '68', '70', '07', '08', '09', '10', '7', '8', '9']	
					 else 2 if x in ['58', '60', '62', '04', '05', '06', '4', '5', '6'] else 1 for x in df['cdtscore']]	
    if not df.empty:	
        df.loc[(df['mvstate'].isin(['IL', 'MN', 'WI', 'OH'])), 'premier2'] = 1	
        df.loc[(df['mvstate'].isin(['IL', 'MN', 'WI', 'OH'])) & (	
			df['cdtscore'].isin(['68', '70'])), 'premier2'] = 4	
        df.loc[(df['mvstate'].isin(['IL', 'MN', 'WI', 'OH'])) & (	
			df['cdtscore'].isin(['62', '64', '66'])), 'premier2'] = 3   	
        df.loc[(df['mvstate'].isin(['IL', 'MN', 'WI', 'OH'])) & (	
			df['cdtscore'].isin(['56', '58', '60'])), 'premier2'] = 2	
        df.loc[(df['mvstate'].isin(['KY', 'WV'])), 'premier2'] = 1	
        df.loc[(df['mvstate'].isin(['KY', 'WV'])) & (df['cdtscore'].isin(['9', '09', '10'])), 'premier2'] = 4	
        df.loc[(df['mvstate'].isin(['KY', 'WV'])) & (	
			df['cdtscore'].isin(['6', '7', '8', '06', '07', '08'])), 'premier2'] = 3	
        df.loc[(df['mvstate'].isin(['KY', 'WV'])) & (	
			df['cdtscore'].isin(['3', '4', '5', '03', '04', '05'])), 'premier2'] = 2	
        df.loc[(df['mvstate'].isin(	
			['GA', 'TN', 'IA', 'IN', 'NE', 'ND'])), 'premier2'] = 1	
        df.loc[(df['mvstate'].isin(['GA', 'TN', 'IA', 'IN', 'NE', 'ND'])) & (	
			df['cdtscore'].isin(['R4', 'R6', 'R8', 'T0', 'T2', 'T4', 'T6', 'T8'])), 'premier2'] = 4	
        df.loc[(df['mvstate'].isin(['GA', 'TN', 'IA', 'IN', 'NE', 'ND'])) & (df['cdtscore'].isin(['J6', 'J8', 'L0', 'L2',	
																								  'L4', 'L6', 'L8', 'N0', 'N2', 'N4', 'N6', 'N8', 'P0', 'P2', 'P4', 'P6', 'P8', 'R0', 'R2'])), 'premier2'] = 3	
        df.loc[(df['mvstate'].isin(['GA', 'TN', 'IA', 'IN', 'NE', 'ND'])) & (df['cdtscore'].isin(	
			['D8', 'F0', 'F2', 'F4', 'F6', 'F8', 'H0', 'H2', 'H4', 'H6', 'H8', 'J0', 'J2', 'J4'])), 'premier2'] = 2	
    else:	
        df['premier'] = np.nan	
        df['premier2'] = np.nan	
    df['mulprod'] = [0 if (x in [np.nan, "N", "B", "D", ' ', ''])	
					 else 1 for x in df['mltprdind']]	
    df['pifd'] = [0 if x in (np.nan, "N", '', ' ') else 1 for x in df['pifind']]	
    df['member'] = [1 if x == 'Y' else 0 for x in df['memberind']]	
    df['cved'] = df['clmviolno']	
    df.loc[((df.cved == np.nan) | (df.cved == ' ') | (df.cved == '') | (df['cved'].isnull())), 'cved'] = 99	
    df['termincep'] = df['termincep'].astype(str)	
    df['inception'] = df['inception'].astype(str)	
    df['termyear'], df['termmonth'], df['termday'] = df['termincep'].str.slice(0, 4).astype(	
		int), df['termincep'].str.slice(4, 6).astype(int), df['termincep'].str.slice(6, 8).astype(int)	
    df['incepyear'], df['incepmonth'], df['incepday'] = df['inception'].str.slice(0, 4).astype(	
		int), df['inception'].str.slice(4, 6).astype(int), df['inception'].str.slice(6, 8).astype(int)	
    df['maxyear'], df['maxmonth'], df['maxday'] = df['maxtendte'].str.slice(0, 4).astype(	
		int), df['maxtendte'].str.slice(5, 7).astype(int), df['maxtendte'].str.slice(8, 10).astype(int)	
    # df['maxtendte_new'] = pd.to_datetime(df["maxtendte"])	
    # df['maxyear_1'], df['maxmonth_1'], df['maxday_1'] = df['maxtendte_new'].dt.year, df['maxtendte_new'].dt.month, df['maxtendte_new'].dt.day	
    # df.loc[~(df['maxtendte'].isnull()), ['maxyear', 'maxmonth', 'maxday']] = df.loc[~(df['maxtendte_new'].isnull()), ['maxyear_1', 'maxmonth_1', 'maxday_1']].values.tolist()	
    # df = df.drop(columns=['maxtendte_new', 'maxyear_1',	
						  # 'maxmonth_1', 'maxday_1'])	
    # df['maxyear'], df['maxmonth'], df['maxday'] = df['maxyear'].fillna(	
		# 0), df['maxmonth'].fillna(0), df['maxday'].fillna(0)	
    # df['maxyear'], df['maxmonth'], df['maxday'] = df['maxyear'].astype(	
		# float), df['maxmonth'].astype(float), df['maxday'].astype(float)	
    # End manual effort.***	
    df['tenure1_temp1'] = df['termyear']-df['maxyear'] + \	
		((df['termmonth']-df['maxmonth'])/12) + \	
		((df['termday']-df['maxday'])/365)	
    df['tenure1_temp2'] = df['termyear']-df['incepyear'] + \	
		((df['termmonth']-df['incepmonth'])/12) + \	
		((df['termday']-df['incepday'])/365)	
    df.loc[~(df['maxtendte'].isnull()), 'tenure'] = df.loc[~(	
		df['maxtendte'].isnull()), 'tenure1_temp1']	
    df['tenure'] = df['tenure'].apply(np.floor)	
    df.loc[df['maxtendte'].isnull(	
	), 'tenure'] = df.loc[df['maxtendte'].isnull(), 'tenure1_temp2']	
    df['tenure'] = df.tenure.apply(np.floor)	
    df.loc[(df.tenure == np.nan) | (df.tenure.isnull()) | (df.tenure == '') | (df.tenure == ' '), 'tenure'] = 0	
    df.loc[df.tenure.isin([4.0, 4]), 'tenure'] = 3	
    df.loc[df.tenure >= 5.0, 'tenure'] = 5	
    df['prins'] = df['prinscde']	
    df['obirthdte'] = df['obirthdte'].astype(str)	
    df['ybrthdte'] = df['ybrthdte'].astype(str)	
    df['obirthdteyear'], df['obirthdtemonth'], df['obirthdteday'] = df['obirthdte'].str.slice(0, 4).astype(	
		int), df['obirthdte'].str.slice(4, 6).astype(int), df['obirthdte'].str.slice(6, 8).astype(int)	
    df['ybrthdteyear'], df['ybrthdtemonth'], df['ybrthdteday'] = df['ybrthdte'].str.slice(0, 4).astype(	
		int), df['ybrthdte'].str.slice(4, 6).astype(int), df['ybrthdte'].str.slice(6, 8).astype(int)	
    df['ageo'] = df['termyear'] - df['obirthdteyear'] +((df['termmonth']-df['obirthdtemonth'])/12)+((df['termday']-(df['obirthdteday']))/365)	
    df['ageo'] = df['ageo'].apply(np.floor)	
    df['agey'] = df['termyear'] - df['ybrthdteyear'] +((df['termmonth']-df['ybrthdtemonth'])/12)+((df['termday']-(df['ybrthdteday']))/365)	
    df['agey'] = df['agey'].apply(np.floor)	
    df['vhlevel'] = df['vhlevel'].astype(str)	
    df.loc[df['vhlevel'].isin(['0', '99', '0.0', '99.0']), 'vhlevel'] = np.nan	
    df['mvyear'] = df['mvyear'].astype(int)	
    df['termincep'] = df['termincep'].astype(str)	
    df['termyr'] = df['termincep'].str.slice(0, 4).astype(int)	
    df['vehage'] = np.maximum(0, (df['termyr']-df['mvyear']))	
    # if substr(billplan,1,3) in ("EFT") then EFT1=1; # Manual effort require.	
    df['eft1'] = 0	
    col = 'billplan'	
    conditions = [df[col].str.slice(0, 3) == 'EFT', df[col] == ""]	
    choices = [1, -1]	
    df['eft1'] = np.select(conditions, choices)	
    df['aaadrivedisc'] = [1 if x == 'Y' else 0 for x in df['aaadrhhind']]	
    df = df[['mvstate', 'policy', 'client', 'hhclient', 'clmviolno', 'cdtscore', 'seqagtno', 'maxtendte', 'inception', 'termincep', 'duemon', 'mltprdind', 'bi_prm', 'comp_prm', 'cmpf_prm', 'coll_prm', 'memberind', 'prinscde', 'ybrthdte', 'obirthdte', 'vhlevel', 'mvyear', 'productgen', 'cov', 'premier', 'premier2', 'mulprod', 'member', 'cved', 'tenure', 'prins', 'ageo', 'agey', 'vehage', 'eft1', 'pifind', 'pifd', 'aaadrivedisc']]	
    df = df_remove_indexCols(df)	
    logging.info(	
        "Gen2 created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("Gen2", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''SAS Comment:/***** Combine & Summarize ******************************************************************************************************/ '''	
    '''SAS Comment:/********************************************************************************************************************************/ '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    Gen1 = pd.read_sql_query("select * from Gen1 ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    # Drop columns from source df data in datafram.	
    Gen1 = Gen1.drop(columns=["termincep", "inception"])	
    df_lower_colNames(Gen1, 'Gen1')	
    # Converting source df data into datafram.	
    Gen2 = pd.read_sql_query("select * from Gen2 ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    # Drop columns from source df data in datafram.	
    Gen2 = Gen2.drop(columns=["termincep", "inception"])	
    df_lower_colNames(Gen2, 'Gen2')	
    df = pd.concat([Gen1, Gen2], ignore_index=True, sort=False)	
    # if cov=1 then VehFullCov=1; # Manual effort require.	
    df['cov'] = df['cov'].astype(str)	
    df['vehfullcov'] = [1 if x == '1' else 0 for x in df['cov']]	
    # ***Start manual effort here...	
    # else VehFullCov=0;	
    # End manual effort.***	
    # Keep columns in the taget df data in datafram.	
    df = df[['duemon', 'member', 'mvstate', 'policy', 'client', 'hhclient', 'seqagtno', 'productgen', 'cov', 'premier', 'premier2', 'mulprod', 'tenure', 'cved', 'vehfullcov', 'prins', 'assigneddrvage', 'ageo', 'agey', 'vhlevel', 'vehage', 'eft1', 'pifind', 'pifd', 'aaadrivedisc']]	
    df = df_remove_indexCols(df)	
    if 'level_0' in df.columns:	
        df = df.drop(columns="level_0")	
    logging.info(	
        "Renew1 created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("Renew1", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''SAS Comment:*** AJS - Updated to identify Joint Venture agents by Community Code in the agent table from WebFOCUS (ignore EC Excel file); '''	
    '''SAS Comment:* Get distribution channel - Agent type; '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # reading the file from csv	
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_renratio_regional_auto/data/agent{}.sas7bdat'.format(InfMon)) as reader:	
    df = reader.to_data_frame()	
    # putting into the sqliteDB	
    df.to_sql("agent{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace', index=True)	
    sqliteConnection.close()	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from agent{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'agent'.format)	
    df = df[['seql_no_', 'agent_type', 'community']]	
    df['agent'] = df['agent_type']	
    df = df_remove_indexCols(df)	
    logging.info(	
        "agent created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("agent", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    # Sql Code Start and End Lines - 298&301 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """DROP TABLE IF EXISTS Renew2;	
        create table Renew2 as select a.*, b.agent, b.community from Renew1 a left join	
            agent b on a.seqagtno=b.seql_no_"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Renew2"	
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)	
    except:	
        e = sys.exc_info()[0]	
        logging.error('Table creation/update is failed.')	
        logging.error('Error - {}'.format(e))	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from Renew2 ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Renew2')	
    df['community'] = df['community'].astype(str)	
    if not df.empty:	
        df.loc[df['community'].isin(	
		['O002', '0002', 'O089', '0089', 'K006', 'V004']), 'aaa_ec'] = 1	
    else:	
        df['aaa_ec'] = np.nan	
    # if community in ('O002', 'O089', 'K006', 'V004') then AAA_EC=1;	
    # End manual effort.***	
    df['seqagtno'] = df['seqagtno'].astype(str)	
    # if seqagtno=379346 then agent=3; # Manual effort require.	
    df.loc[df.seqagtno == '379346', 'agent'] = 3	
    df = df_remove_indexCols(df)	
    logging.info(	
        "Renew2 created successfully with {} records".format(len(df)))	
    if 'level_0' in df.columns:	
        df = df.drop(columns="level_0")	
    # Push results data frame to Sqlite DB	
    df.to_sql("Renew2", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    if (InfMon % 100) > 11:	
        InfRen1 = InfMon+89	
    else:	
        InfRen1 = InfMon+1	
    if InfRen1 < LatestMon:	
        Join1(InfMon)	
    elif InfRen1 != LatestMon+1:	
        Join2(InfMon)	
    #*******************************End of Data Step Process**************************************************#	
    '''	
    WARNING: Below SAS step has not converted in this release.	
    proc summary data=Renew6 nway;	
    class mvstate productgen policy;	
    var DueMon agent AAA_EC cov vehfullcov premier premier2 mulprod tenure CVED renew_1mon renew_2mon Member prins AssignedDrvAge vhlevel VehAge EFT1 PIFD AAADriveDisc;	
    output out=Hhld1 (drop=_type_ rename=_freq_=VehCnt)max(DueMon agent AAA_EC VehFullCov premier premier2 mulprod tenure CVED renew_1mon renew_2mon Member prins AssignedDrvAge AgeO vhlevel EFT1 PIFD AAADriveDisc)=sum(cov)=CovSum mean(cov)=CovAvg max(cov)=CovMax min(cov AssignedDrvAge AgeY vhlevel VehAge)=CovMin AssignedDrvY AgeY vhlevelB1 VehAgeN1;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query("select * from Renew6", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Renew6')	
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)	
    agg_cols = {'duemon', 'agent', 'aaa_ec', 'cov', 'vehfullcov', 'premier', 'premier2', 'mulprod', 'tenure', 'cved', 'renew_1mon', 'renew_2mon', 'member', 'prins', 'assigneddrvage', 'vhlevel', 'vehage', 'eft1', 'pifd', 'aaadrivedisc'}	
    final_cols = list(agg_cols.intersection(str_cols))	
    df[final_cols] = df[final_cols].fillna(value='')	
    for i in df.columns:	
        try:	
            df[i] = df[i].apply(pd.to_numeric)	
        except:	
            pass	
    #df = df.groupby(['mvstate', 'productgen', 'policy']).agg({'duemon': max, 'agent': max, 'aaa_ec': max, 'vehfullcov': max, 'premier': max, 'premier2': max, 'mulprod': max, 'tenure': max, 'cved': max, 'renew_1mon': max, 'renew_2mon': max, 'member': max, 'prins': max, 'assigneddrvage': max, 'vhlevel': max, 'eft1': max, 'pifd': max, 'aaadrivedisc': max, 'agey':min, 'vehagen1':pd.NamedAgg(column='vehage', aggfunc=min), 'vhlevelb1':pd.NamedAgg(column='vhlevel', aggfunc=min), 'assigneddrvy':pd.NamedAgg(column='assigneddrvage', aggfunc=min), 'covmin':pd.NamedAgg(column='cov', aggfunc=min), 'covmax':pd.NamedAgg(column='cov', aggfunc=max), 'covsum':pd.NamedAgg(column='cov', aggfunc=sum), 'covavg':pd.NamedAgg(column='cov', aggfunc=np.mean)}).reset_index()	
    df_summ = df.groupby(['mvstate', 'productgen', 'policy'], as_index=False).agg(	
        duemon=pd.NamedAgg(column='duemon', aggfunc=max), agent=pd.NamedAgg(column='agent', aggfunc=max), aaa_ec=pd.NamedAgg(column='aaa_ec', aggfunc=max),	
        vehfullcov=pd.NamedAgg(column='vehfullcov', aggfunc=max), premier=pd.NamedAgg(column='premier', aggfunc=max), premier2=pd.NamedAgg(column='premier2', aggfunc=max),	
        mulprod=pd.NamedAgg(column='mulprod', aggfunc=max), tenure=pd.NamedAgg(column='tenure', aggfunc=max), cved=pd.NamedAgg(column='cved', aggfunc=max),	
        renew_1mon=pd.NamedAgg(column='renew_1mon', aggfunc=max), renew_2mon=pd.NamedAgg(column='renew_2mon', aggfunc=max), member=pd.NamedAgg(column='member', aggfunc=max),	
        prins=pd.NamedAgg(column='prins', aggfunc=max), assigneddrvage=pd.NamedAgg(column='assigneddrvage', aggfunc=max), assigneddrvy=pd.NamedAgg(column='assigneddrvage', aggfunc=min),	
        ageo=pd.NamedAgg(column='ageo', aggfunc=max), vhlevel=pd.NamedAgg(column='vhlevel', aggfunc=max), vhlevelb1=pd.NamedAgg(column='vhlevel', aggfunc=min), eft1=pd.NamedAgg(column='eft1', aggfunc=max),	
        pifd=pd.NamedAgg(column='pifd', aggfunc=max), aaadrivedisc=pd.NamedAgg(column='aaadrivedisc', aggfunc=max), covsum=pd.NamedAgg(column='cov', aggfunc=sum), covavg=pd.NamedAgg(column='cov', aggfunc=np.mean),	
        covmax=pd.NamedAgg(column='cov', aggfunc=max), covmin=pd.NamedAgg(column='cov', aggfunc=min), agey=pd.NamedAgg(column='agey', aggfunc=min), vehagen1=pd.NamedAgg(column='vehage', aggfunc=min))	
    df['policy_temp'] = df['policy']	
    df_summ[['mvstate', 'productgen', 'policy', 'vehcnt']] = df.groupby(['mvstate', 'productgen', 'policy'], as_index=False)['policy_temp'].count()	
    df_creation_logging(df_summ, 'Hhld1')	
    df_summ = df_remove_indexCols(df_summ)	
    df_summ.to_sql("Hhld1", con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    sql = """select * from {}.{}.driver where mondate = {}""".format(project_id, gen1_dataset_id, InfMon)	
    df_driver_gen1 = client.query(sql).to_dataframe()	
    '''	
    # Open connection to Sqlite work data base	
    sqliteGenConnection = sqlite3.connect(SQLitePythonGenWorkDb)	
    df = pd.read_sql_query("select * from driver{}".format(InfMon),sqliteGenConnection)    	
    # lowering all columns	
    df_lower_colNames(df, 'driver{}'.format(InfMon))	
    # logging info	
    df_creation_logging(df, "driver{}".format(InfMon))	
    # putting into the sqliteDB	
    df.to_sql("driver{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace', index=True)	
    sqliteGenConnection.close()	
    '''	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    '''df = pd.read_sql_query(	
        "select * from driver{}".format(InfMon), sqliteConnection)'''	
    df = df_driver_gen1	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'driver')	
    df = df[['policy', 'drivtype', 'drvstatus', 'birthdte']]	
    df['birthdte1'] = df['birthdte']	
    df = df_remove_indexCols(df)	
    logging.info(	
        "driver created successfully with {} records".format(len(df)))	
    del df_driver_gen1	
    # Push results data frame to Sqlite DB	
    df.to_sql("driver", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen2_dataset_id, InfMon)	
    df_driver_gen2 = client.query(sql).to_dataframe()	
    '''	
    # Open connection to Sqlite work data base	
    sqliteGenConnection = sqlite3.connect(SQLitePythonGenWorkDb)	
    df = pd.read_sql_query("select * from drivergenii{}".format(InfMon),sqliteGenConnection)    	
    # lowering all columns	
    df_lower_colNames(df, 'drivergenii{}'.format(InfMon))	
    # logging info	
    df_creation_logging(df, "drivergenii{}".format(InfMon))	
    # putting into the sqliteDB	
    df.to_sql("drivergenii{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace', index=True)	
    sqliteGenConnection.close()	
    '''	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df1 = pd.read_sql_query("select * from driver", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df1, 'driver')	
    # Drop column name in source df data in dataframe	
    df1 = df1.drop(columns="birthdte")	
    # Rename columns in source df data in datafram.	
    df1 = df1.rename(columns={"birthdte1": "birthdte"})	
    # Converting source df data into datafram.	
    '''df2 = pd.read_sql_query(	
        "select * from drivergenii{}".format(InfMon), sqliteConnection)'''	
    df2 = df_driver_gen2	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df2, 'drivergenii')	
    df2 = df2[['policy', 'drivtype', 'drvstatus', 'birthdte']]	
    df = pd.concat([df1, df2], ignore_index=True, sort=False)	
    df['bday'] = df.birthdte	
    df['policy'] = df['policy'].astype(float)	
    # ((df['birthdte'].str.slice(4, 6).astype(int), df['birthdte'].str.slice(6, 8).astype(int), df['birthdte'].str.slice(0, 4).astype(int))	
    # if drivtype="A" or drvstatus="A"; # Manual effort require.	
    df = df.loc[(df.drivtype == 'A') | (df.drvstatus == 'A')]	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns="birthdte")	
    df = df_remove_indexCols(df)	
    logging.info(	
        "driver created successfully with {} records".format(len(df)))	
    del df_driver_gen2	
    # Push results data frame to Sqlite DB	
    df.to_sql("driver", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query("select * from driver", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'driver')	
    df = df_remove_indexCols(df)	
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)	
    agg_cols = {'bday'}	
    final_cols = list(agg_cols.intersection(str_cols))	
    df[final_cols] = df[final_cols].fillna(value='')	
    # df = df.groupby(['policy']).agg(bdayy=pd.NamedAgg(column='bday', aggfunc=max), bdayo=pd.NamedAgg(column='bday', aggfunc=min)).reset_index()	
    # df_summ = df.groupby(['policy'], as_index=False).agg({'bday': 'max', 'bday': 'min'})	
    # df_summ = df_summ.rename(columns={"max(bday)": "bdayy", "min(bday)": "bdayo"})	
    df_summ = df.groupby(['policy'], as_index=False).agg(bdayy=pd.NamedAgg(	
        column='bday', aggfunc=max), bdayo=pd.NamedAgg(column='bday', aggfunc=min))	
    df_summ['bdayy_year'], df_summ['bdayy_month'], df_summ['bdayy_day'] = (df_summ['bdayy'].str.slice(0, 4).astype(float), df_summ['bdayy'].str.slice(4, 6).astype(float), df_summ['bdayy'].str.slice(6, 8).astype(float))	
    df_summ['bdayo_year'], df_summ['bdayo_month'], df_summ['bdayo_day'] = (df_summ['bdayo'].str.slice(0, 4).astype(float, errors = 'ignore'), df_summ['bdayo'].str.slice(4, 6).astype(float, errors = 'ignore'), df_summ['bdayo'].str.slice(6, 8).astype(float, errors = 'ignore'))	
    df_creation_logging(df_summ, 'driver')	
    df_summ.to_sql("driver", con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    	
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen1_dataset_id, InfMon)	
    df_inforce_gen1 = client.query(sql).to_dataframe()	
		
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    '''df = pd.read_sql_query(	
        "select * from inforce{}".format(InfMon), sqliteConnection)'''	
    df = df_inforce_gen1	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'inforce')	
    df = df[['policy', 'termincep']]	
    df = df_remove_indexCols(df)	
    logging.info(	
        "termincep created successfully with {} records".format(len(df)))	
    del df_inforce_gen1	
    # Push results data frame to Sqlite DB	
    df.to_sql("termincep", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen2_dataset_id, InfMon)	
    df_inforce_gen2 = client.query(sql).to_dataframe()	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df1 = pd.read_sql_query("select * from termincep ", sqliteConnection)	
    df_lower_colNames(df1, 'termincep')	
    # Converting source df data into datafram.	
    #df2 = pd.read_sql_query("select * from inforcegenii{}".format(InfMon), sqliteConnection)	
    df2 = df_inforce_gen2	
    df_lower_colNames(df2, "inforce")	
    # handling data frame column case senstivity.#	
    df = pd.concat([df1, df2], ignore_index=True, sort=False)	
    # df_lower_colNames(df, 'inforcegenii{}'.format(InfMon)	
    df = df[['policy', 'termincep']]	
    df['incep'] = df.termincep	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns="termincep")	
    df = df_remove_indexCols(df)	
    logging.info(	
        "termincep created successfully with {} records".format(len(df)))	
    del df_inforce_gen2	
    # Push results data frame to Sqlite DB	
    df.to_sql("termincep", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=termincep nway;	
    class policy;	
    var incep;	
    output out=termincep (drop=_type_ _freq_) min=;	
    run;	
    	
    else if primaryclass in (1254 1255 1354 1355 2254 2255 2354 2355 1256 1257 1356 1357 2256 2257 2356 2357 1754 1755 1704 1705 2754 2755	
    2704 2705 1756 1757 1706 1707 2756 2757 2706 2707 8064 8065 8164 8165 8074 8075 8174 8175 8084 8085 8184 8185 8094 8095 8194 8195 8066	
    8067 8166 8167 8076 8077 8176 8177 8086 8087 8186 8187 8096 8097 8196 8197 8460 8463 8660 8663 8470 8473 8670 8673 8480 8483 8680 8683	
    8490 8493 8690 8693 8466 8468 8666 8668 8476 8478 8676 8678 8486 8488 8686 8688 8496 8498 8696 8698 2871 2872 2873 2878 2879 8964 8965	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query("select * from termincep", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'termincep')	
    df = df_remove_indexCols(df)	
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)	
    agg_cols = {'incep'}	
    final_cols = list(agg_cols.intersection(str_cols))	
    df[final_cols] = df[final_cols].fillna(value='')	
    df = df.groupby(['policy']).agg({'incep': min}).reset_index()	
    df['incep_year'], df['incep_month'], df['incep_day'] = (df['incep'].str.slice(0, 4).astype(float), df['incep'].str.slice(4, 6).astype(float), df['incep'].str.slice(6, 8).astype(float))	
    df_creation_logging(df, 'termincep')	
    df.to_sql("termincep", con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    # Sql Code Start and End Lines - 355&362 #	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """DROP TABLE IF EXISTS Hhld2;create table Hhld2 as select a.*,	
			CAST((c.incep_year)-(b.bdayy_year)+((c.incep_month)-(b.bdayy_month))/12+((c.incep_day)-(b.bdayy_day))/365 AS INT) as AgeY1,	
			CAST((c.incep_year)-(b.bdayo_year)+((c.incep_month)-(b.bdayo_month))/12+((c.incep_day)-(b.bdayo_day))/365 AS INT) as AgeO1	
			from Hhld1 a left join driver b 	
			on a.policy=b.policy	
			left join termincep c	
			on a.policy=c.policy"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Hhld2"	
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)	
    except:	
        e = sys.exc_info()[0]	
        logging.error('Table creation/update is failed.')	
        logging.error('Error - {}'.format(e))	
			
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from Hhld2 ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Hhld2')	
    df['agey1'] = df['agey1'].apply(np.floor)	
    df['ageo1'] = df['ageo1'].apply(np.floor)	
    # length AgentType $40; # Manual effort require.	
    df['agenttype'] = "Unknown"	
    df['agent'] = df['agent'].astype('Int64')	
    # ***Start manual effort here...	
    df.loc[df['agent'].isin([1, 7, 8]), 'agenttype'] = 'Captive'	
    # if Agent in (1 7 8) then AgentType="Captive";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    df.loc[df.agent == 9, 'agenttype'] = 'EA'	
    # else if Agent = 9 then AgentType="EA";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    df.loc[df['agent'].isin([3, 4]), 'agenttype'] = 'MSC/HB'	
    # else if Agent in (3 4) then AgentType="MSC/HB";	
    # End manual effort.***'''	
    df.loc[df['agent'].isin([2, 6]), 'agenttype'] = 'IA'	
    # ***Start manual effort here...	
    df.loc[(df['mvstate'].isin(['KY', 'WV', 'OH']))  & (df.aaa_ec == 1) & (~df['agent'].isin([1, 7, 8, 9,3,4])), 'agenttype'] = 'EC'	
    # End manual effort.***'''	
    df.loc[(df['agent'].isin([2, 6]))&~((df['mvstate'].isin(['KY', 'WV', 'OH'])) & (df.aaa_ec == 1)), 'agenttype'] = 'IA'	
    # if AgentType="Unknown" then AgentType="MSC/HB"; # Manual effort require.	
    df.loc[df.agenttype == 'Unknown', 'agenttype'] = 'MSC/HB'	
    # length Coverage $40; # Manual effort require.	
    df['coverage'] = "xxxxxxxxxxxxxxxx"	
    df['covmax'] = df['covmax'].astype(str)	
    # ***Start manual effort here...	
    # else if CovMax=0 then Coverage="Lia Only";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else Coverage="Mixed";	
    df['coverage'] = ['Full Cov' if x == z else 'Lia Only' if y ==	
                      '0' else 'Mixed' for x, y, z in zip(df['covsum'], df['covmax'], df['vehcnt'])]	
    # End manual effort.***'''	
    # length PremierGrp $40; # Manual effort require.	
    # if premier=1 then PremierGrp="Low"; # Manual effort require.	
    # ***Start manual effort here...	
    # else if premier=2 then PremierGrp="Med";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    df['premier'] = df['premier'].astype('Int64')	
    df['premiergrp'] = ['Low' if x == 1 else 'Med' if x ==	
                        2 else 'High' for x in df['premier']]	
    # End manual effort.***'''	
    # length PremierGrp2 $40; # Manual effort require.	
    # if premier2=1 then PremierGrp2="Low"; # Manual effort require.	
    # ***Start manual effort here...	
    # else if premier2=2 then PremierGrp2="Mid-Low";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    df['premier2'] = df['premier2'].apply(np.int64)	
    df['premiergrp2'] = ['Low' if x == 1 else 'Mid-Low' if x ==	
                         2 else 'Mid-High' if x == 3 else 'High' for x in df['premier2']]	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else PremierGrp2="High";	
    # End manual effort.***'''	
    # length MultiProd $40; # Manual effort require.	
    # if mulprod=0 then MultiProd="No"; # Manual effort require.	
    # ***Start manual effort here...	
    # else MultiProd="Yes";	
    # End manual effort.***'''	
    # length Mem $40; # Manual effort require.	
    df['multiprod'] = ['No' if x == 0 else 'Yes' for x in df['mulprod']]	
    # if Member=0 then Mem="No"; # Manual effort require.	
    # ***Start manual effort here...	
    # else Mem="Yes";	
    # End manual effort.***'''	
    # length PIF $40; # Manual effort require.	
    df['mem'] = ['No' if x == 0 else 'Yes' for x in df['member']]	
    # if PIFD=0 then PIF="No"; # Manual effort require.	
    # ***Start manual effort here...	
    # else PIF="Yes";	
    df['pifd'] = df['pifd'].astype(str)	
    df['pif'] = ['No' if ((x == '0') | (x == '0.0')) else 'Yes' for x in df['pifd']]	
    # End manual effort.***'''	
    # if productgen="Gen1" then PIF=""; # Manual effort require.	
    df.loc[df.productgen == 'Gen1', 'pif'] = ""	
    # length PriorInsStatus $40; # Manual effort require.	
    df['prins'] = df['prins'].astype('Int64')	
    # ***Start manual effort here..	
    # if mvstate in ('WV' 'IA') then do;	
    df.loc[(df['mvstate'].isin(['WV', 'IA'])), 'priorinsstatus'] = "N/A"	
    df.loc[(df['mvstate'].isin(['WV', 'IA'])) & (	
        df['prins'].isin([1212, 1222])), 'priorinsstatus'] = "<100/300"	
    df.loc[(df['mvstate'].isin(['WV', 'IA'])) & (	
        df['prins'].isin([1112, 1122])), 'priorinsstatus'] = "20/40"	
    df.loc[(df['mvstate'].isin(['WV', 'IA'])) & (	
        df['prins'].isin([1312, 1322])), 'priorinsstatus'] = ">=100/300"	
    # if prins in (1212 1222) then PriorInsStatus="<100/300";	
    # else if prins in (1112 1122) then PriorInsStatus="20/40";	
    # else if prins in (1312 1322) then PriorInsStatus=">=100/300";	
    # else PriorInsStatus="N/A";	
    # end;	
    # End manual effort.***	
    # ***Start manual effort here...	
    # if mvstate in ('MN') then do;	
    df.loc[(df['mvstate'].isin(['MN'])), 'priorinsstatus'] = "N/A"	
    df.loc[(df['mvstate'].isin(['MN'])) & (df['prins'].isin(	
        [1212, 1222])), 'priorinsstatus'] = "<100/300"	
    df.loc[(df['mvstate'].isin(['MN'])) & (	
        df['prins'].isin([1112, 1122])), 'priorinsstatus'] = "30/60"	
    df.loc[(df['mvstate'].isin(['MN'])) & (df['prins'].isin(	
        [1312, 1322])), 'priorinsstatus'] = ">=100/300"	
    # if prins in (1212 1222) then PriorInsStatus="<100/300";	
    # else if prins in (1112 1122) then PriorInsStatus="30/60";	
    # else if prins in (1312 1322) then PriorInsStatus=">=100/300";	
    # else PriorInsStatus="N/A";	
    # end;	
    # End manual effort.***	
    # ***Start manual effort here...	
    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA',	
                                'IN', 'NE', 'ND'])), 'priorinsstatus'] = "N/A"	
    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA', 'IN', 'NE', 'ND'])) & (	
        df['prins'].isin([1212, 1222])), 'priorinsstatus'] = "<100/300"	
    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA', 'IN', 'NE', 'ND'])) & (	
        df['prins'].isin([1112, 1122])), 'priorinsstatus'] = "25/50"	
    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA', 'IN', 'NE', 'ND'])) & (	
        df['prins'].isin([1312, 1322])), 'priorinsstatus'] = ">=100/300"	
    # if mvstate in ('WI' 'KY' 'OH' 'TN' 'GA' 'IN' 'NE' 'ND') then do;	
    # if prins in (1212 1222) then PriorInsStatus="<100/300";	
    # else if prins in (1112 1122) then PriorInsStatus="25/50";	
    # else if prins in (1312 1322) then PriorInsStatus=">=100/300";	
    # else PriorInsStatus="N/A";	
    # end;	
    # End manual effort.***	
    # ***Start manual effort here...	
    df.loc[(df['mvstate'].isin(['IL'])), 'priorinsstatus'] = "N/A"	
    df.loc[(df['mvstate'].isin(['IL'])) & (df['prins'].isin(	
        [1212, 1222])), 'priorinsstatus'] = "<100/300"	
    df.loc[(df['mvstate'].isin(['IL'])) & (	
        df['prins'].isin([1112, 1122])), 'priorinsstatus'] = "<=25/50"	
    df.loc[(df['mvstate'].isin(['IL'])) & (df['prins'].isin(	
        [1312, 1322])), 'priorinsstatus'] = ">=100/300"	
    # if mvstate = 'IL' then do;	
    # if prins in (1212 1222) then PriorInsStatus="<100/300";	
    # else if prins in (1112 1122) then PriorInsStatus="<=25/50";	
    # else if prins in (1312 1322) then PriorInsStatus=">=100/300";	
    # else PriorInsStatus="N/A";	
    # end;	
    # End manual effort.***	
    # if productgen="Gen1" then PriorInsStatus=""; # Manual effort require.	
    df.loc[df.productgen == 'Gen1', 'priorinsstatus'] = ''	
    df.loc[((df.agey1 == np.nan) | (df.agey1.isnull()) | (df.agey1 == '') | (df.agey1 == ' ')), 'agey1'] = df['agey']	
    df.loc[((df.agey1 == np.nan) | (df.agey1.isnull()) | (df.agey1 == '') | (df.agey1 == ' ')), 'agey1'] = df['assigneddrvy']	
    # if AgeY1=. then AgeY1=AgeY; # Manual effort require.	
    # if AgeY1=. then AgeY1=AssignedDrvY; # Manual effort require.	
    # length AgeYoungest $40; # Manual effort require.	
    # if AgeY1>=65 then AgeYoungest=">64"; # Manual effort require.	
    df['ageyoungest'] = [">64" if x >= 65 else "45-64" if x >=	
                         45 else "30-44" if x >= 30 else "25-29" if x >= 25 else "<25" for x in df['agey1']]	
    # ***Start manual effort here...	
    # else if AgeY1>=45 then AgeYoungest="45-64";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else if AgeY1>=30 then AgeYoungest="30-44";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else if AgeY1>=25 then AgeYoungest="25-29";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else AgeYoungest="<25";	
    # End manual effort.***'''	
    # if AgeO1=. then AgeO1=AgeO; # Manual effort require.	
    df.loc[((df.ageo1 == np.nan) | (df.ageo1.isnull()) | (df.ageo1 == '') | (df.ageo1 == ' ')), 'ageo1'] = df['ageo']	
    df.loc[((df.ageo1 == np.nan) | (df.ageo1.isnull()) | (df.ageo1 == '') | (df.ageo1 == ' ')), 'ageo1'] = df['assigneddrvage']	
    # if AgeO1=. then AgeO1=AssignedDrvAge; # Manual effort require.	
    # length AgeOldest $40; # Manual effort req	
    # ***Start manual effort here...	
    df['ageoldest'] = ["<25" if x < 25 else "25-29" if x <	
                       30 else "30-44" if x < 45 else "45-64" if x < 65 else ">64" for x in df['ageo1']]	
    # else if AgeO1<30 then AgeOldest="25-29";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else if AgeO1<45 then AgeOldest="30-44";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else if AgeO1<65 then AgeOldest="45-64";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else AgeOldest=">64";	
    # End manual effort.***'''	
    # if vhlevel=. then VHLevelW="N/A"; # Manual effort require.	
    df['vhlevelw'] = ['N/A' if ((x ==	
                      np.nan) | (x == '') | (x == ' ') | np.isnan(x))  else int(x) for x in df['vhlevel']]	
    df['vhlevelw'] = [('0' + str(x)) if len(str(x)) == 1  else str(x) for x in df['vhlevelw']]	
    # ***Start manual effort here...	
    # else VHLevelW=put(vhlevel,z2.);	
    # End manual effort.***'''	
    # if productgen="Gen1" then VHLevelW=""; # Manual effort require.	
    df.loc[df.productgen == 'Gen1', 'vhlevelw'] = ''	
    # if vhlevelB1=. then VHLevelB="N/A"; # Manual effort require.	
    df['vhlevelb'] = ['N/A' if ((x ==	
                      np.nan) | (x == '') | (x == ' ') | np.isnan(x))  else int(x) for x in df['vhlevelb1']]	
    df['vhlevelb'] = [('0' + str(x)) if len(str(x)) == 1  else str(x) for x in df['vhlevelb']]	
    # ***Start manual effort here...	
    # else VHLevelB=put(vhlevelB1,z2.);	
    # End manual effort.***'''	
    # if productgen="Gen1" then VHLevelB=""; # Manual effort require.	
    df.loc[df.productgen == 'Gen1', 'vhlevelb'] = ''	
    # length NoVeh $40; # Manual effort require.	
    # if VehCnt>3 then NoVeh=">3"; # Manual effort require.	
    df['vehcnt'] = df['vehcnt'].astype(np.int64)	
    df['noveh'] = ['>3' if x > 3 else '3' if x >	
                   2 else '2' if x > 1 else '1' for x in df['vehcnt']]	
    # else if VehCnt>2 then NoVeh="3";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else if VehCnt>1 then NoVeh="2";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else NoVeh="1";	
    # End manual effort.***'''	
    # length VehAgeN $40; # Manual effort require.	
    # if VehAgeN1>15 then VehAgeN=">15"; # Manual effort require.	
    df['vehagen'] = ['>15' if x > 15 else '11-15' if x > 10 else '6-10' if x >	
                     5 else '2-5' if x > 1 else '0-1' for x in df['vehagen1']]	
    # ***Start manual effort here...	
    # else if VehAgeN1>10 then VehAgeN="11-15";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else if VehAgeN1>5 then VehAgeN="6-10";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else if VehAgeN1>1 then VehAgeN="2-5";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else VehAgeN="0-1";	
    # End manual effort.***'''	
    # if EFT1=1 then EFT="Yes "; # Manual effort require.	
    df['eft'] = ['Yes  ' if x == 1 else 'No   ' if x ==	
                 0 else "" for x in df['eft1']]	
    # ***Start manual effort here...	
    # else if EFT1=0 then EFT="No ";	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else EFT="";	
    # End manual effort.***'''	
    # length AAADrive $40; # Manual effort require.	
    df['aaadrivedisc'] = df['aaadrivedisc'].astype(str)	
    df['aaadrive'] = ["Yes" if ((x == '1') | (x == '1.0')) else "No" for x in df['aaadrivedisc']]	
    df.loc[df.productgen == 'Gen1', 'aaadrive'] = ""	
    # ***Start manual effort here...	
    # else AAADrive="No";	
    # End manual effort.***'''	
    # if productgen="Gen1" then AAADrive=""; # Manual effort require.	
    # Keep columns in the taget df data in datafram.	
    df = df[['mem', 'agenttype', 'renew_2mon', 'multiprod', 'ageoldest', 'aaadrive', 'vehagen', 'cved', 'renew_1mon', 'policy', 'premiergrp', 'vhlevelw',	
             'duemon', 'ageyoungest', 'priorinsstatus', 'mvstate', 'eft', 'productgen', 'tenure', 'pif', 'vhlevelb', 'noveh', 'premiergrp2', 'coverage']]	
    df = df_remove_indexCols(df)	
    logging.info(	
        "Inf{} created successfully with {} records".format(InfMon, len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("Inf{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
		
def Join1(InfMon):	
    '''SAS Comment:/* if a policy renews within 1 month or within 2 months. */ '''	
    if (InfMon % 100) > 11:	
        InfRen1 = InfMon+89	
    else:	
        InfRen1 = InfMon+1	
    if (InfMon % 100) > 10:	
        InfRen2 = InfMon+90	
    else:	
        InfRen2 = InfMon+2	
    '''SAS Comment:*AJS subsetting of comparison dataset requested; 	
    *********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************	
    '''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from InfRen{}".format(InfRen1), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'InfRen{}'.format(InfRen1))	
    df = df.loc[df.termeffmonth == InfRen1]	
    # if termeffmonth=&InfRen1; # Manual effort require.	
    df = df_remove_indexCols(df)	
    logging.info(	
        "InfRenSubset{} created successfully with {} records".format(InfRen1, len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("InfRenSubset{}".format(InfRen1),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 13&15 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS InfRenSubset{}_sqlitesorted;	
        CREATE TABLE InfRenSubset{}_sqlitesorted AS SELECT * FROM	
            InfRenSubset{} ORDER BY mvstate,policy,hhclient;DROP TABLE	
            InfRenSubset{};ALTER TABLE InfRenSubset{}_sqlitesorted RENAME TO	
            InfRenSubset{}""".format(InfRen1, InfRen1, InfRen1, InfRen1, InfRen1, InfRen1)	
        sql = mcrResl(sql)	
        tgtSqliteTable = mcrResl("InfRenSubset&InfRen1")	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from InfRen{}".format(InfRen2), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'InfRen{}'.format(InfRen2))	
    df = df.loc[df.termeffmonth == InfRen1]	
    # if termeffmonth=&InfRen1; # Manual effort require.	
    df = df_remove_indexCols(df)	
    logging.info(	
        "InfRenSubset{} created successfully with {} records".format(InfRen2, len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("InfRenSubset{}".format(InfRen2),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 22&24 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS InfRenSubset{}_sqlitesorted;	
        CREATE TABLE InfRenSubset{}_sqlitesorted AS SELECT * FROM	
            InfRenSubset{} ORDER BY mvstate,policy,hhclient;DROP TABLE	
            InfRenSubset{};ALTER TABLE InfRenSubset{}_sqlitesorted RENAME TO	
            InfRenSubset{}""".format(InfRen2, InfRen2, InfRen2, InfRen2, InfRen2)	
        sql = mcrResl(sql)	
        tgtSqliteTable = mcrResl("InfRenSubset&InfRen2_sqlitesorted")	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    # Sql Code Start and End Lines - 26&31 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """DROP TABLE IF EXISTS Renew3;	
        create table Renew3 as select c.*, d.mvstate as renew12 from (select a.*,	
            b.mvstate as renew11 from Renew2 a left join (select distinct mvstate, policy	
            from InfRenSubset{}) b on a.mvstate=b.mvstate and a.policy=b.policy) c left	
            join (select distinct mvstate, policy from InfRenSubset{}) d on	
            c.mvstate=d.mvstate and c.policy=d.policy""".format(InfRen1, InfRen2)	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Renew3"	
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)	
    except:	
        e = sys.exc_info()[0]	
        logging.error('Table creation/update is failed.')	
        logging.error('Error - {}'.format(e))	
    # Sql Code Start and End Lines - 33&38 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """DROP TABLE IF EXISTS Renew4;	
        create table Renew4 as select c.*, d.mvstate as renew22 from (select a.*,	
            b.mvstate as renew21 from Renew3 a left join (select distinct mvstate, hhclient	
            from InfRenSubset{}) b on a.mvstate=b.mvstate and a.hhclient=b.hhclient)	
            c left join (select distinct mvstate, hhclient from InfRenSubset{}) d on	
            c.mvstate=d.mvstate and c.hhclient=d.hhclient""".format(InfRen1, InfRen2)	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Renew4"	
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)	
    except:	
        e = sys.exc_info()[0]	
        logging.error('Table creation/update is failed.')	
        logging.error('Error - {}'.format(e))	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """/*DROP TABLE IF EXISTS Renew5; 	
        create table Renew5 as select c.*, d.mvstate as renew32 from (select a.*,	
            b.mvstate as renew31 from Renew4 a left join (select distinct mvstate, client	
            from InfRen{}) b on a.mvstate=b.mvstate and a.client=b.client) cleft join	
            (select distinct mvstate, client from InfRen{}) d on c.mvstate=d.mvstate	
            and c.client=d.client*/""".format(InfRen1, InfRen2)	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Renew5"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from Renew4 ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Renew4')	
    # ***Start manual effort here...	
    df['renew_1mon'] = [0 if (x == "") & (	
        y == "") else 1 for x, y in zip(df['renew11'], df['renew21'])]	
    df['renew_2mon'] = [0 if (x == "") & (	
        y == "") else 1 for x, y in zip(df['renew12'], df['renew22'])]	
    # if renew11 = "" and renew21 = "" then renew_1mon = 0;	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else renew_1mon = 1;	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # if renew12 = "" and renew22 = "" then renew_2mon = 0;	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else renew_2mon = 1;	
    # End manual effort.***'''	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["renew12", "renew22", "renew11", "renew21"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "Renew6 created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("Renew6", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    """	
        if renew11 = "" and renew21 = "" :	
    renew_1mon  = 0	
            if renew12 = "" and renew22 = "" :	
    renew_2mon  = 0	
        '''Uncomment to execute the below sas macro'''	
    #Join1(<< Provide require args here >>)	
    ### SAS Source Code Line Numbers START:606 & END:643.###	
    """	
    ''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''	
def Join2(InfMon):	
    '''SAS Comment:/* if a policy renews within 1 month or within 2 months. */ '''	
    if (InfMon % 100) > 11:	
        InfRen1 = InfMon+89	
    else:	
        InfRen1 = InfMon+1	
    '''SAS Comment:*AJS subsetting of comparison dataset requested;	
    *********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from InfRen{}".format(InfRen1), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'InfRen{}'.format(InfRen1))	
    df = df.loc[df.termeffmonth == InfRen1]	
    df = df.drop(columns=["level_0"])	
    # if termeffmonth=&InfRen1; # Manual effort require.	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "InfRenSubset{} created successfully with {} records".format(InfRen1, len(df)))	
    df.to_sql("InfRenSubset{}".format(InfRen1),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 12&14 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS InfRenSubset{}_sqlitesorted;	
        CREATE TABLE InfRenSubset{}_sqlitesorted AS SELECT * FROM	
            InfRenSubset{} ORDER BY mvstate,policy,hhclient;DROP TABLE	
            InfRenSubset{};ALTER TABLE InfRenSubset{}_sqlitesorted RENAME TO	
            InfRenSubset{}""".format(InfRen1, InfRen1, InfRen1, InfRen1, InfRen1, InfRen1)	
        sql = mcrResl(sql)	
        tgtSqliteTable = mcrResl("InfRenSubset&InfRen1_sqlitesorted")	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    # Sql Code Start and End Lines - 16&18 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """DROP TABLE IF EXISTS Renew3;	
        create table Renew3 as select a.*, b.mvstate as renew11 from Renew2 a left join	
            (select distinct mvstate, policy from InfRenSubset{}) b on	
            a.mvstate=b.mvstate and a.policy=b.policy""".format(InfRen1)	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Renew3"	
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)	
    except:	
        e = sys.exc_info()[0]	
        logging.error('Table creation/update is failed.')	
        logging.error('Error - {}'.format(e))	
    # Sql Code Start and End Lines - 20&22 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """DROP TABLE IF EXISTS Renew4;	
        create table Renew4 as select a.*, b.mvstate as renew21 from Renew3 a left join	
            (select distinct mvstate, hhclient from InfRenSubset{}) b on	
            a.mvstate=b.mvstate and a.hhclient=b.hhclient""".format(InfRen1)	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Renew4"	
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)	
    except:	
        e = sys.exc_info()[0]	
        logging.error('Table creation/update is failed.')	
        logging.error('Error - {}'.format(e))	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    '''	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """/* DROP TABLE IF EXISTS Renew5;	
        create table Renew5 as select a.*, b.mvstate as renew31 from Renew4 a left	
            join (select distinct mvstate, client from InfRen{}) b on	
            a.mvstate=b.mvstate and a.client=b.client*/""".format(InfRen1)	
        sql = mcrResl(sql)	
        tgtSqliteTable = "Renew5"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from Renew4 ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Renew4')	
    # ***Start manual effort here...	
    # if renew11 = "" and renew21 = "" then renew_1mon = 0;	
    df['renew_1mon'] = [0 if (x == "") & (	
        y == "") else 1 for x, y in zip(df['renew11'], df['renew21'])]	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else renew_1mon = 1;	
    # End manual effort.***'''	
    df['renew_2mon'] = 0	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["renew11", "renew21"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "Renew6 created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("Renew6", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''	
    if renew11 = "" and renew21 = "" :	
    renew_1mon  = 0	
    Uncomment to execute the below sas macro	
    #Join2(<< Provide require args here >>)	
    '''	
    ### SAS Source Code Line Numbers START:646 & END:653.###	
    """ERROR: Unable to convert the below SAS block/code into python	
    data junk1;	
    stuff = &InfMon0;	
    do while (stuff < &LatestMon);	
    stuff = stuff + 1;	
    if mod(stuff,100) = 13 then stuff = stuff + 88;	
    call execute ('%Renew (InfMon ='||stuff||')');	
    end;	
    run;	
    """	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
stuff = InfMon0	
while(stuff < LatestMon):	
    stuff = stuff+1	
    if stuff % 100 == 13:	
        stuff = stuff+88	
    Renew(stuff)	
df = pd.DataFrame()	
df['stuff'] = stuff	
df.to_sql("junk1", con=sqliteConnection, if_exists='replace')	
sqliteConnection.close()	
### SAS Source Code Line Numbers START:656 & END:656.###	
'''SAS Comment:*** AJS: Dimension processing for database population; '''	
### SAS Source Code Line Numbers START:658 & END:1495.###	
''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''	
def PopDB(InfMon):	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # if productgen='Gen1' then system = '1'; # Manual effort require.	
    # ***Start manual effort here...	
    # else if productgen='Gen2' then system = '2';	
    df['system'] = ['1' if x == 'Gen1' else '2' if x ==	
                    'Gen2' else '?' for x in df['productgen']]	
    # End manual effort.***'''	
    # ***Start manual effort here...	
    # else system = '?';	
    # End manual effort.***'''	
    df['product'] = 'AUT'	
    df['migrind'] = 'N'	
    df = df.rename(columns={'mvstate': 'state'})	
    # length agttyp $10; # Manual effort require.	
    df['agttyp'] = df['agenttype']	
    df['tenure'] = df['tenure'].astype(np.int64)	
    df['cved'] = df['cved'].astype(str)	
    df['vhlevelb'] = df['vhlevelb'].astype(str)	
    df['vhlevelw'] = df['vhlevelw'].astype(str)	
    df['policy'] = df['policy'].astype(np.int64)	
    # length dim $25; # Manual effort require.	
    df['dim'] = ' '	
    # length dimval $40; # Manual effort require.	
    df['dimval'] = ' '	
    # length tenuretxt $40; # Manual effort require.	
    df['tenuretxt'] = ['0' if ((x == np.nan) | (x == 0) | np.isnan(x)) else '1' if x == 1 else '2' if x ==	
                       2 else '3-4' if ((x == 3) | (x == 4)) else '5+' if x >= 5 else x for x in df['tenure']]	
    df['cvedtxt'] = ['0' if x == '0.0' else '1' if x == '1.0' else '2' if x == '2.0' else '3' if x ==	
                     '3.0' else '4' if x == '4.0' else '5' if x == '5.0' else '99' if x == '99.0' else x for x in df['cved']]	
		
    # length VHlevelBtxt $40; # Manual effort require.	
    df['vhlevelbtxt'] = ['N/A' if x == 'N/A' else '1-3' if x in ['01', '02', '03'] else '4-7' if x in ['04','05','06','07'] else '8-11' if x in ['08', '09', '10', '11'] else x for x in df['vhlevelb']]	
    df['vhlevelwtxt'] = ['N/A' if x == 'N/A' else '1-3' if x in ['01', '02', '03'] else '4-7' if x in ['04','05','06','07'] else '8-11' if x in ['08', '09', '10', '11'] else x for x in df['vhlevelw']]	
    # Rename columns in the target df data in datafram.	
    df = df_remove_indexCols(df)	
    logging.info(	
        "Inf{} created successfully with {} records".format(InfMon, len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("Inf{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'premiergrp']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryPremier")	
    grouped_df.to_sql("summaryPremier", con=sqliteConnection,	
                           if_exists='replace')	
    sqliteConnection.close()	
    '''	
    *********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************	
    '''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryPremier ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryPremier')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    #df = df.loc[df['_type_'].isin([10, 11, 14, 15])]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'ALL AGENTS'	
    # if PremierGrp=' ' then PremierGrp='Total'; # Manual effort require.	
    df.loc[df['premiergrp'].isnull(), 'premiergrp'] = 'Total'	
    # select(PremierGrp); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 9 '''	
    # end;	
    df['valseq'] = [1 if x == 'Low' else 2 if x == 'Med' else 3 if x ==	
                    'High' else 4 if x == 'Total' else 9 for x in df['premiergrp']]	
    # End manual effort.***	
    df['dimval'] = df['premiergrp']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Premier'	
    df['dimseq'] = 1	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["premiergrp"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryPremier created successfully with {} records".format(len(df)))	
    df.to_sql("summaryPremier", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system MultiProd;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryMultiProd (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'multiprod']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'multiprod']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryMultiProd")	
    grouped_df.to_sql("summaryMultiProd",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryMultiProd ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryMultiProd')	
    #df = df.loc[df['_type_'].isin([10, 11, 14, 15)	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    grp_lst = ['state', 'agttyp', 'system', 'multiprod']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if MultiProd=' ' then MultiProd='Total'; # Manual effort require.	
    df.loc[df['multiprod'].isnull(), 'multiprod'] = 'Total'	
    # select(MultiProd); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==	
                    'No' else 3 if x == 'Total' else 9 for x in df['multiprod']]	
    # End manual effort.***	
    df['dimval'] = df['multiprod']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Multiproduct'	
    df['dimseq'] = 2	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["multiprod"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "summaryMultiProd created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("summaryMultiProd", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 120&122 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    '''SAS Comment:* Coverage processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system Coverage;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryCoverage (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'coverage']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'coverage']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryCoverage")	
    grouped_df.to_sql("summaryCoverage",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryCoverage ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryCoverage')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'coverage']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if Coverage=' ' then Coverage='Total'; # Manual effort require.	
    df.loc[df['coverage'].isnull(), 'coverage'] = 'Total'	
    # select(Coverage); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'Full Cov' else 2 if x == 'Lia Only' else 3 if x ==	
                    'Mixed' else 4 if x == 'Total' else 9 for x in df['coverage']]	
    # End manual effort.***	
    df['dimval'] = df['coverage']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Coverage'	
    df['dimseq'] = 3	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["coverage"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryCoverage created successfully with {} records".format(len(df)))	
    df.to_sql("summaryCoverage", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 159&161 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryCoverage_sqlitesorted;	
        CREATE TABLE summaryCoverage_sqlitesorted AS SELECT * FROM summaryCoverage ORDER	
            BY state,agttyp,system,valseq;DROP TABLE summaryCoverage;ALTER TABLE	
            summaryCoverage_sqlitesorted RENAME TO summaryCoverage"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryCoverage_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* Vehicle # processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system NoVeh;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryNoVeh (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'noveh']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'noveh']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryNoVeh")	
    grouped_df.to_sql("summaryNoVeh", con=sqliteConnection,	
                           if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryNoVeh ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryNoVeh')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    grp_lst = ['state', 'agttyp', 'system', 'noveh']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if NoVeh=' ' then NoVeh='Total'; # Manual effort require.	
    df.loc[df['noveh'].isnull(), 'noveh'] = 'Total'	
    # select(NoVeh); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == '1' else 2 if x == '2' else 3 if x ==	
                    '3' else 4 if x == '>3' else 5 if x == 'Total' else 9 for x in df['noveh']]	
    # End manual effort.***	
    df['dimval'] = df['noveh']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Vehicle #'	
    df['dimseq'] = 4	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["noveh"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryNoVeh created successfully with {} records".format(len(df)))	
    df.to_sql("summaryNoVeh", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 199&201 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryNoVeh_sqlitesorted;	
        CREATE TABLE summaryNoVeh_sqlitesorted AS SELECT * FROM summaryNoVeh ORDER BY	
            state,agttyp,system,valseq;DROP TABLE summaryNoVeh;ALTER TABLE	
            summaryNoVeh_sqlitesorted RENAME TO summaryNoVeh;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryNoVeh_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* Newest Veh Age processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system VehAgeN;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryVehAgeN (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'vehagen']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'vehagen']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryVehAgeN")	
    grouped_df.to_sql("summaryVehAgeN", con=sqliteConnection,	
                           if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryVehAgeN ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryVehAgeN')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'vehagen']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if VehAgeN=' ' then VehAgeN='Total'; # Manual effort require.	
    df.loc[df['vehagen'].isnull(), 'vehagen'] = 'Total'	
    # select(VehAgeN); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 6	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == '0-1' else 2 if x == '2-5' else 3 if x == '6-10' else 4 if x ==	
                    '11-15' else 5 if x == '>15' else 6 if x == 'Total' else 9 for x in df['vehagen']]	
    # End manual effort.***	
    df['dimval'] = df['vehagen']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Newest Veh Age'	
    df['dimseq'] = 5	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["vehagen"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "summaryVehAgeN created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("summaryVehAgeN", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 240&242 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryVehAgeN_sqlitesorted;	
        CREATE TABLE summaryVehAgeN_sqlitesorted AS SELECT * FROM summaryVehAgeN ORDER	
            BY state,agttyp,system,valseq;DROP TABLE summaryVehAgeN;ALTER TABLE	
            summaryVehAgeN_sqlitesorted RENAME TO summaryVehAgeN;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryVehAgeN_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
	'''	
    '''SAS Comment:* Membership processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system Mem;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryMem (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    grp_lst = ['state', 'agttyp', 'system', 'mem']  # Take the columns in Class	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'mem']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryMem")	
    grouped_df.to_sql(	
        "summaryMem", con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryMem ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryMem')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'mem']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if Mem=' ' then Mem='Total'; # Manual effort require.	
    df.loc[df['mem'].isnull(), 'mem'] = 'Total'	
    # select(Mem); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==	
                    'No' else 3 if x == 'Total' else 9 for x in df['mem']]	
    # End manual effort.***	
    df['dimval'] = df['mem']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Membership'	
    df['dimseq'] = 6	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["mem"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryMem created successfully with {} records".format(len(df)))	
    df.to_sql("summaryMem", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 278&280 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryMem_sqlitesorted;	
        CREATE TABLE summaryMem_sqlitesorted AS SELECT * FROM summaryMem ORDER BY	
            state,agttyp,system,valseq;DROP TABLE summaryMem;ALTER TABLE	
            summaryMem_sqlitesorted RENAME TO summaryMem"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryMem_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
	'''	
    '''SAS Comment:* Oldest Driver processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system AgeOldest;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryAgeOldest (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'ageoldest']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'ageoldest']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryAgeOldest")	
    grouped_df.to_sql("summaryAgeOldest",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryAgeOldest ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryAgeOldest')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'ageoldest']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if AgeOldest=' ' then AgeOldest='Total'; # Manual effort require.	
    df.loc[df['ageoldest'].isnull(), 'ageoldest'] = 'Total'	
    # select(AgeOldest); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 6	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == '<25' else 2 if x == '25-29' else 3 if x == '30-44' else 4 if x == '45-64' else 5 if x == '>64' else 6 if x == 'Total'	
                    else 9 for x in df['ageoldest']]	
    # End manual effort.***	
    df['dimval'] = df['ageoldest']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Oldest Driver'	
    df['dimseq'] = 7	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["ageoldest"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryAgeOldest created successfully with {} records".format(len(df)))	
    df.to_sql("summaryAgeOldest", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 319&321 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryAgeOldest_sqlitesorted;	
        CREATE TABLE summaryAgeOldest_sqlitesorted AS SELECT * FROM summaryAgeOldest	
            ORDER BY state,agttyp,system,valseq;DROP TABLE summaryAgeOldest;ALTER TABLE	
            summaryAgeOldest_sqlitesorted RENAME TO summaryAgeOldest"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryAgeOldest_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
	'''	
    '''SAS Comment:* Youngest Driver processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system AgeYoungest;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryAgeYoungest (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'ageyoungest']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'ageyoungest']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryAgeYoungest")	
    grouped_df.to_sql("summaryAgeYoungest",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from summaryAgeYoungest ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryAgeYoungest')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'ageyoungest']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if AgeYoungest=' ' then AgeYoungest='Total'; # Manual effort require.	
    df.loc[df['ageyoungest'].isnull(), 'ageyoungest'] = 'Total'	
    # select(AgeYoungest); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 6	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == '<25' else 2 if x == '25-29' else 3 if x == '30-44' else 4 if x ==	
                    '45-64' else 5 if x == '>64' else 6 if x == 'Total' else 9 for x in df['ageyoungest']]	
    # End manual effort.***	
    df['dimval'] = df['ageyoungest']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Youngest Driver'	
    df['dimseq'] = 8	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["ageyoungest"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "summaryAgeYoungest created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("summaryAgeYoungest", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 360&362 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryAgeYoungest_sqlitesorted;	
        CREATE TABLE summaryAgeYoungest_sqlitesorted AS SELECT * FROM summaryAgeYoungest	
            ORDER BY state,agttyp,system,valseq;DROP TABLE summaryAgeYoungest;ALTER TABLE	
            summaryAgeYoungest_sqlitesorted RENAME TO summaryAgeYoungest"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryAgeYoungest_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* Tenure processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system tenuretxt;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryTenuretxt (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'tenuretxt']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'tenuretxt']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryAgeTenuretxt")	
    grouped_df.to_sql("summaryTenuretxt",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryTenuretxt ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryTenuretxt')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'tenuretxt']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if tenuretxt=' ' then tenuretxt='Total'; # Manual effort require.	
    df.loc[df['tenuretxt'].isnull(), 'tenuretxt'] = 'Total'	
    # select(tenuretxt); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 6	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == '0' else 2 if x == '1' else 3 if x == '2' else 4 if x == '3-4' else 5 if x == '5+' else 6 if x == 'Total'	
                    else 9 for x in df['tenuretxt']]	
    # End manual effort.***	
    df['dimval'] = df['tenuretxt']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Tenure'	
    df['dimseq'] = 9	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["tenuretxt"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryTenuretxt created successfully with {} records".format(len(df)))	
    df.to_sql("summaryTenuretxt", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 399&401 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryTenuretxt_sqlitesorted;	
        CREATE TABLE summaryTenuretxt_sqlitesorted AS SELECT * FROM summaryTenuretxt	
            ORDER BY state,agttyp,system,valseq;DROP TABLE summaryTenuretxt;ALTER TABLE	
            summaryTenuretxt_sqlitesorted RENAME TO summaryTenuretxt"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryTenuretxt_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* EFT processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system EFT;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryEFT (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    grp_lst = ['state', 'agttyp', 'system', 'eft']  # Take the columns in Class	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'eft']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryEFT")	
    grouped_df.to_sql(	
        "summaryEFT", con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryEFT ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryEFT')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'eft']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if EFT=' ' then EFT='Total'; # Manual effort require.	
    df.loc[df['eft'].isnull(), 'eft'] = 'Total'	
    # select(EFT); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==	
                    'No' else 3 if x == 'Total' else 9 for x in df['eft']]	
    # End manual effort.***	
    df['dimval'] = df['eft']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'EFT'	
    df['dimseq'] = 10	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["eft"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "summaryEFT created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("summaryEFT", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 436&438 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryEFT_sqlitesorted;	
        CREATE TABLE summaryEFT_sqlitesorted AS SELECT * FROM summaryEFT ORDER BY	
            state,agttyp,system,valseq;DROP TABLE summaryEFT;ALTER TABLE	
            summaryEFT_sqlitesorted RENAME TO summaryEFT"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryEFT_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* CVED processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system CVEDtxt;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryCVED (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{} where cvedtxt != 'nan'".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'cvedtxt']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'cvedtxt']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryCVED")	
    grouped_df.to_sql(	
        "summaryCVED", con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryCVED ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryCVED')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'cvedtxt']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if CVEDtxt=' ' then CVEDtxt='Total'; # Manual effort require.	
    df.loc[df['cvedtxt'].isnull(), 'cvedtxt'] = 'Total'	
    # select(CVEDtxt); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 6	
    df['valseq'] = 7	
    df['valseq'] = 8	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == '5' else 2 if x == '4' else 3 if x == '3' else 4 if x == '2' else 5 if x == '1' else 6 if x == '0' else 7 if x == '99' else 8 if x == 'Total'	
                    else 9 for x in df['cvedtxt']]	
    # End manual effort.***	
    df['dimval'] = df['cvedtxt']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'CVED'	
    df['dimseq'] = 11	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["cvedtxt"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryCVED created successfully with {} records".format(len(df)))	
    df.to_sql("summaryCVED", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 478&480 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryCVED_sqlitesorted;	
        CREATE TABLE summaryCVED_sqlitesorted AS SELECT * FROM summaryCVED ORDER BY	
            state,agttyp,system,valseq;DROP TABLE summaryCVED;ALTER TABLE	
            summaryCVED_sqlitesorted RENAME TO summaryCVED"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryCVED_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
       '''	
    '''SAS Comment:* Prior Insurance processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system PriorInsStatus;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryPriorIns (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{} where priorinsstatus != ''".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'priorinsstatus']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'priorinsstatus']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryPriorIns")	
    grouped_df.to_sql("summaryPriorIns",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryPriorIns ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryPriorIns')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin(10, 11, 14, 15)]	
    grp_lst = ['state', 'agttyp', 'system', 'priorinsstatus']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if PriorInsStatus=' ' then PriorInsStatus='Total'; # Manual effort require.	
    df.loc[df['priorinsstatus'].isnull(), 'priorinsstatus'] = 'Total'	
    # if state='MN' then do; # Manual effort require.	
    # select(PriorInsStatus); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if ((x == 'N/A') & (y == 'MN')) else 2 if ((x == '30/60') & (y == 'MN')) else 3 if ((x == '<100/300') & (y == 'MN')) else 4 if ((x == '>=100/300') & (y == 'MN')) else 5 if ((x == 'Total') & (y == 'MN'))	
                    else 9 for x, y in zip(df['priorinsstatus'], df['state'])]	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    # ***Start manual effort here...	
    # if state in ('WV', 'IA') then do;	
    df['valseq'] = [1 if ((x == 'N/A') & (y in ['WV', 'IA'])) else 2 if ((x == '20/40') & (y in ['WV', 'IA'])) else 3 if ((x == '<100/300') & (y in ['WV', 'IA'])) else 4 if ((x == '>=100/300') & (y in ['WV', 'IA'])) else 5 if ((x == 'Total') & (y in ['WV', 'IA']))	
                    else 9 for x, y in zip(df['priorinsstatus'], df['state'])]	
    #select (PriorInsStatus);	
    # when ('N/A ') valseq=1;	
    # when ('20/40 ') valseq=2;	
    # when ('<100/300 ') valseq=3;	
    # when ('>=100/300') valseq=4;	
    # when ('Total ') valseq=5;	
    # otherwise valseq=9;	
    # end;	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    # ***Start manual effort here...	
    # if state in ('OH', 'WI', 'KY', 'GA', 'TN', 'IN', 'NE', 'ND') then do;	
    df['valseq'] = [1 if ((x == 'N/A') & (y in ['OH', 'WI', 'KY', 'GA', 'TN', 'IN', 'NE', 'ND'])) else 2 if ((x == '25/50') & (y in ['OH', 'WI', 'KY', 'GA', 'TN', 'IN', 'NE', 'ND'])) else 3 if ((x == '<100/300') & (y in ['OH', 'WI', 'KY', 'GA', 'TN', 'IN', 'NE', 'ND'])) else 4 if ((x == '>=100/300') & (y in ['OH', 'WI', 'KY', 'GA', 'TN', 'IN', 'NE', 'ND'])) else 5 if ((x == 'Total') & (y in ['OH', 'WI', 'KY', 'GA', 'TN', 'IN', 'NE', 'ND']))	
                    else 9 for x, y in zip(df['priorinsstatus'], df['state'])]	
    # select(PriorInsStatus);	
    # when ('N/A ') valseq=1;	
    # when ('25/50 ') valseq=2;	
    # when ('<100/300 ') valseq=3;	
    # when ('>=100/300') valseq=4;	
    # when ('Total ') valseq=5;	
    # otherwise valseq=9;	
    # end;	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    # ***Start manual effort here...	
    # if state = 'IL' then do;	
    df['valseq'] = [1 if ((x == 'N/A') & (y == 'IL')) else 2 if ((x == '25/50') & (y == 'IL')) else 3 if ((x == '<100/300') & (y == 'IL')) else 4 if ((x == '>=100/300') & (y == 'IL')) else 5 if ((x == 'Total') & (y == 'IL'))	
                    else 9 for x, y in zip(df['priorinsstatus'], df['state'])]	
    # select(PriorInsStatus);	
    # when ('N/A ') valseq=1;	
    # when ('<=25/50 ') valseq=2;	
    # when ('<100/300 ') valseq=3;	
    # when ('>=100/300') valseq=4;	
    # when ('Total ') valseq=5;	
    # otherwise valseq=9;	
    # end;	
    # End manual effort.***	
    # end;	
    # End manual effort.***	
    df['dimval'] = df['priorinsstatus']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'PriorIns'	
    df['dimseq'] = 12	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["priorinsstatus"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "summaryPriorIns created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("summaryPriorIns", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 549&551 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryPriorIns_sqlitesorted;	
        CREATE TABLE summaryPriorIns_sqlitesorted AS SELECT * FROM summaryPriorIns ORDER	
            BY state,agttyp,system,valseq;DROP TABLE summaryPriorIns;ALTER TABLE	
            summaryPriorIns_sqlitesorted RENAME TO summaryPriorIns"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryPriorIns_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* Best Vehicle History processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system VHlevelBtxt;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryVHlevelB (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{} where vhlevelbtxt != ''".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelbtxt']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'vhlevelbtxt']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryVHlevelB")	
    grouped_df.to_sql("summaryVHlevelB",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryVHlevelB ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryVHlevelB')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin([10, 11, 14, 15])]	
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelbtxt']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if VHlevelBtxt=' ' then VHlevelBtxt='Total'; # Manual effort require.	
    df.loc[df['vhlevelbtxt'].isnull(), 'vhlevelbtxt'] = 'Total'	
    # select(VHlevelBtxt); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 9	
    #end;'''	
    # End manual effort.***	
    df['valseq'] = [1 if x == 'N/A' else 2 if x == '1-3' else 3 if x == '4-7' else 4 if x == '8-11' else 5 if x == 'Total'	
                    else 9 for x in df['vhlevelbtxt']]	
    df['dimval'] = df['vhlevelbtxt']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Best Veh Hist'	
    df['dimseq'] = 13	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["vhlevelbtxt"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "summaryVHlevelB created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("summaryVHlevelB", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 588&590 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryVHlevelB_sqlitesorted;	
        CREATE TABLE summaryVHlevelB_sqlitesorted AS SELECT * FROM summaryVHlevelB ORDER	
            BY state,agttyp,system,valseq;DROP TABLE summaryVHlevelB;ALTER TABLE	
            summaryVHlevelB_sqlitesorted RENAME TO summaryVHlevelB"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryVHlevelB_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system VHlevelWtxt;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryVHlevelW (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{} where vhlevelwtxt != ''".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelwtxt']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'vhlevelwtxt']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryVHlevelW")	
    grouped_df.to_sql("summaryVHlevelW",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryVHlevelW ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryVHlevelW')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin([10, 11, 14, 15])]	
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelwtxt']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if VHlevelWtxt=' ' then VHlevelWtxt='Total'; # Manual effort require.	
    df.loc[df['vhlevelwtxt'].isnull(), 'vhlevelwtxt'] = 'Total'	
    # select(VHlevelWtxt); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'N/A' else 2 if x == '1-3' else 3 if x == '4-7' else 4 if x == '8-11' else 5 if x == 'Total'	
                    else 9 for x in df['vhlevelwtxt']]	
    # End manual effort.***	
    df['dimval'] = df['vhlevelwtxt']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Worst Veh Hist'	
    df['dimseq'] = 14	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["vhlevelwtxt"])	
    df = df_remove_indexCols(df)	
    logging.info(	
        "summaryVHlevelW created successfully with {} records".format(len(df)))	
    # Push results data frame to Sqlite DB	
    df.to_sql("summaryVHlevelW", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 627&629 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryVHlevelW_sqlitesorted;	
        CREATE TABLE summaryVHlevelW_sqlitesorted AS SELECT * FROM summaryVHlevelW ORDER	
            BY state,agttyp,system,valseq;DROP TABLE summaryVHlevelW;ALTER TABLE	
            summaryVHlevelW_sqlitesorted RENAME TO summaryVHlevelW"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryVHlevelW_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* Paid in Full processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system PIF;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryPIF (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{} where pif != ''".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    grp_lst = ['state', 'agttyp', 'system', 'pif']  # Take the columns in Class	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'pif']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryPIF")	
    grouped_df.to_sql(	
        "summaryPIF", con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryPIF ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryPIF')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin([10, 11, 14, 15])]	
    grp_lst = ['state', 'agttyp', 'system', 'pif']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if PIF=' ' then PIF='Total'; # Manual effort require.	
    df.loc[df['pif'].isnull(), 'pif'] = 'Total'	
    # select(PIF); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==	
                    'No' else 3 if x == 'Total' else 9 for x in df['pif']]	
    # End manual effort.***	
    df['dimval'] = df['pif']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Paid In Full'	
    df['dimseq'] = 15	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["pif"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryPIF created successfully with {} records".format(len(df)))	
    df.to_sql("summaryPIF", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 664&666 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryPIF_sqlitesorted;CREATE TABLE summaryPIF_sqlitesorted AS SELECT * FROM summaryPIF ORDER BY	
            state,agttyp,system,valseq;DROP TABLE summaryPIF;ALTER TABLE	
            summaryPIF_sqlitesorted RENAME TO summaryPIF"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryPIF_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* Premier processing - second grouping; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system premiergrp2;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryPremier2 (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp2']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'premiergrp2']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryPremier2")	
    grouped_df.to_sql("summaryPremier2",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryPremier2 ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryPremier2')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin([10, 11, 14, 15])]	
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp2']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if PremierGrp2=' ' then PremierGrp2='Total'; # Manual effort require.	
    df.loc[df['premiergrp2'].isnull(), 'premiergrp2'] = 'Total'	
    # select(PremierGrp2); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 4	
    df['valseq'] = 5	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'Low' else 2 if x == 'Mid-Low' else 3 if x == 'Mid-High' else 4 if x == 'High' else 5 if x == 'Total'	
                    else 9 for x in df['premiergrp2']]	
    # End manual effort.***	
    df['dimval'] = df['premiergrp2']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'Premier2'	
    df['dimseq'] = 16	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["premiergrp2"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryPremier2 created successfully with {} records".format(len(df)))	
    df.to_sql("summaryPremier2", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 702&704 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryPremier2_sqlitesorted;CREATE TABLE summaryPremier2_sqlitesorted AS SELECT * FROM summaryPremier2 ORDER	
            BY state,agttyp,valseq;DROP TABLE summaryPremier2;ALTER TABLE	
            summaryPremier2_sqlitesorted RENAME TO summaryPremier2"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryPremier2_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:*proc print data=summaryPremier2; '''	
    '''SAS Comment:*  title 'summaryPremier2'; '''	
    '''WARNING SAS commnet block detected.	
    Any SAS steps within the block are converted to python code but commented.	
    # Sql Code Start and End Lines - 0&0 #	
    """***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************"""	
    #Connections to Sqlite DB and fetch all data from source table to process	
    #Please check if any SAS functions are not converted in SqLite query.	
    try:	
        sql = """*run;"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = ""	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''SAS Comment:* AAA Drive processing; '''	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary data=Inf&InfMon;	
    class state agttyp system AAADrive;	
    var renew_1mon renew_2mon;	
    id DueMon product migrind;	
    output out=summaryAAADrive (rename=_freq_=nfrccnt)sum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query(	
        "select * from Inf{} where aaadrive != ''".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'Inf{}'.format(InfMon))	
    # Take the columns in Class	
    grp_lst = ['state', 'agttyp', 'system', 'aaadrive']	
    id_list = ['duemon', 'product', 'migrind']  # Take the columns in ID	
    grouped_df = pd.DataFrame()	
    cnt = 0	
    df_0 = df.drop(columns=grp_lst)	
    df_0_summ = df_0[['renew_1mon', 'renew_2mon']].sum()	
    for i in range(1, len(grp_lst)+1):	
        for j in itertools.combinations(grp_lst, i):	
            cnt = cnt + 1	
            df1 = df.groupby(list(j))[	
                ['renew_1mon', 'renew_2mon']].sum().reset_index()	
            df2 = df.sort_values(list(j)+id_list)	
            df2 = df2[list(j)+id_list]	
            df2['IsFirst'], df2['IsLast'] = [False, False]	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsFirst'].head(1).index, 'IsFirst'] = True	
            df2.loc[df2.groupby(list(j) + id_list)	
                    ['IsLast'].tail(1).index, 'IsLast'] = True	
            df2 = df2[df2['IsLast']]	
            df2 = df2.drop(columns=['IsFirst', 'IsLast'])	
            resdf = pd.merge(df1, df2, on=list(j), how='inner')	
            grouped_df = grouped_df.append(resdf, ignore_index=True)	
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)	
    grouped_df = grouped_df[grp_lst+id_list + ['renew_1mon', 'renew_2mon']]	
    grouped_df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'aaadrive']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(grouped_df, "summaryAAADrive")	
    grouped_df.to_sql("summaryAAADrive",	
                           con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query("select * from summaryAAADrive ", sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'summaryAAADrive')	
    # if _TYPE_ in (10,11,14,15); # Manual effort require.	
    #df = df.loc[df['_type_'].isin([10, 11, 14, 15])]	
    grp_lst = ['state', 'agttyp', 'system', 'aaadrive']	
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |	
				(~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]	
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.	
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'	
    # if AAADrive=' ' then AAADrive='Total'; # Manual effort require.	
    df.loc[df['aaadrive'].isnull(), 'aaadrive'] = 'Total'	
    # select(AAADrive); # Manual effort require.	
    '''df['valseq'] = 1	
    df['valseq'] = 2	
    df['valseq'] = 3	
    df['valseq'] = 9	
    #end;'''	
    df['valseq'] = [1 if x == 'Yes' else 2 if x == 'No' else 3 if x == 'Total'	
                    else 9 for x in df['aaadrive']]	
    # End manual effort.***	
    df['dimval'] = df['aaadrive']	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    df['dim'] = 'AAADrive'	
    df['dimseq'] = 17	
    # Drop columns in the target df data in datafram.	
    df = df.drop(columns=["aaadrive"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "summaryAAADrive created successfully with {} records".format(len(df)))	
    df.to_sql("summaryAAADrive", con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
    # Sql Code Start and End Lines - 739&741 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS summaryAAADrive_sqlitesorted;CREATE TABLE summaryAAADrive_sqlitesorted AS SELECT * FROM summaryAAADrive ORDER	
            BY state,agttyp,valseq;DROP TABLE summaryAAADrive;ALTER TABLE	
            summaryAAADrive_sqlitesorted RENAME TO summaryAAADrive"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = "summaryAAADrive_sqlitesorted"	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # length dim $25; # Manual effort require.	
    # Converting source df data into datafram.	
    Premier = pd.read_sql_query(	
        "select * from summaryPremier", sqliteConnection)	
    MultiProd = pd.read_sql_query(	
        "select * from summaryMultiProd", sqliteConnection)	
    Coverage = pd.read_sql_query(	
        "select * from summaryCoverage", sqliteConnection)	
    NoVeh = pd.read_sql_query("select * from summaryNoVeh", sqliteConnection)	
    VehAgeN = pd.read_sql_query(	
        "select * from summaryVehAgeN", sqliteConnection)	
    Mem = pd.read_sql_query("select * from summaryMem", sqliteConnection)	
    AgeOldest = pd.read_sql_query(	
        "select * from summaryAgeOldest", sqliteConnection)	
    AgeYoungest = pd.read_sql_query(	
        "select * from summaryAgeYoungest", sqliteConnection)	
    Tenuretxt = pd.read_sql_query(	
        "select * from summaryTenuretxt", sqliteConnection)	
    EFT = pd.read_sql_query("select * from summaryEFT", sqliteConnection)	
    CVED = pd.read_sql_query("select * from summaryCVED", sqliteConnection)	
    PriorIns = pd.read_sql_query(	
        "select * from summaryPriorIns", sqliteConnection)	
    VHlevelB = pd.read_sql_query(	
        "select * from summaryVHlevelB", sqliteConnection)	
    VHlevelW = pd.read_sql_query(	
        "select * from summaryVHlevelW", sqliteConnection)	
    PIF = pd.read_sql_query("select * from summaryPIF", sqliteConnection)	
    Premier2 = pd.read_sql_query(	
        "select * from summaryPremier2", sqliteConnection)	
    AAADrive = pd.read_sql_query(	
        "select * from summaryAAADrive", sqliteConnection)	
    # Concatenate the source data frames	
    summary = pd.concat([Premier, MultiProd, Coverage, NoVeh, VehAgeN, Mem, AgeOldest, AgeYoungest, Tenuretxt,	
                         EFT, CVED, PriorIns, VHlevelB, VHlevelW, PIF, Premier2, AAADrive], ignore_index=True, sort=False)	
    if 'level_0' in summary.columns:	
        summary = summary.drop(columns=["level_0"])	
    # Push results data frame to Sqlite DB	
    df_creation_logging(summary,"finalsummary{}".format(InfMon))	
    summary.to_sql("finalsummary{}".format(InfMon), con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # retain DueMon system product agttyp state migrind dim dimseq dim dimseq dimval valseq nfrccnt renew_1mon renew1ratio renew_2mon renew2ratio; # Manual effort require.	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from finalsummary{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'finalsummary{}'.format(InfMon))	
    if 'level_0' in df.columns:	
        df = df.drop(columns=["level_0"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "finalsummary{} created successfully with {} records".format(InfMon, len(df)))	
    df.to_sql("finalsummary{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from finalsummary{} where (agttyp in ('Captive', 'EA'))".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'finalsummary{}'.format(InfMon))	
    # Converting source df data into datafram.	
    df['agttyp'] = 'Captive/EA'	
    if 'level_0' in df.columns:	
        df = df.drop(columns=["level_0"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "finalsummaryCaptiveEA{} created successfully with {} records".format(InfMon, len(df)))	
    df.to_sql("finalsummaryCaptiveEA{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    '''WARNING: Below SAS step has not converted in this release.	
    proc summary nway data=finalsummaryCaptiveEA&InfMon;	
    class state agttyp system dimseq valseq;	
    var nfrccnt renew_1mon renew_2mon;	
    id DueMon system product agttyp state migrind dim dimseq dim dimseq dimval valseq;	
    output out=finalsummary2CaptiveEA&InfMonsum=;	
    run;	
    '''	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    df = pd.read_sql_query("select * from finalsummaryCaptiveEA{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'finalsummaryCaptiveEA{}'.format(InfMon))	
    str_cols = set(df.select_dtypes(include = ['object', 'string']).columns)	
    agg_cols = {'duemon', 'product', 'migrind', 'dim', 'nfrccnt', 'renew_1mon', 'renew_2mon'}	
    final_cols = list(agg_cols.intersection(str_cols))	
    df[final_cols] = df[final_cols].fillna(value = '')	
    df = df.groupby(['state', 'agttyp', 'system', 'dimseq', 'valseq']).agg(	
        {'duemon' : max, 'product' : max, 'migrind' : max, 'dim' : max, 'nfrccnt' : max, 'renew_1mon' : max, 'renew_2mon' : max}).reset_index()	
    #df['nfrccnt'] = df.groupby(['state', 'agttyp', 'system', 'dimseq', 'valseq']).size().reset_index(name = 'counts')['counts']	
    df_creation_logging(df, "finalsummary2CaptiveEA{}".format(InfMon))	
    df.to_sql("finalsummary2CaptiveEA{}".format(InfMon), con=sqliteConnection, if_exists='replace')	
    sqliteConnection.close()	
    '''*********************************************************************************	
    Below python code is to execute standard SAS data step	
    *********************************************************************************'''	
    # Please Note - If any library references remove them accordingly post your code analysis.#	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source df data into datafram.	
    df = pd.read_sql_query(	
        "select * from finalsummary2CaptiveEA{}".format(InfMon), sqliteConnection)	
    # handling data frame column case senstivity.#	
    df_lower_colNames(df, 'finalsummary2CaptiveEA{}'.format(InfMon))	
    df['renew1ratio'] = df['renew_1mon']/df['nfrccnt']	
    df['renew2ratio'] = df['renew_2mon']/df['nfrccnt']	
    # retain DueMon system product agttyp state migrind dim dimseq dim dimseq dimval valseq nfrccnt renew_1mon renew1ratio renew_2mon renew2ratio; # Manual effort require.	
    if 'level_0' in df.columns:	
        df = df.drop(columns=["level_0"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "finalsummary2CaptiveEA{} created successfully with {} records".format(InfMon, len(df)))	
    df.to_sql("finalsummary2CaptiveEA{}".format(InfMon),	
              con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    #*******************************End of Data Step Process**************************************************#	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source finalsummary&InfMon data into datafram.	
    finalsummary = pd.read_sql_query(	
        "select * from finalsummary{}".format(InfMon), sqliteConnection)	
    # Converting source finalsummary2CaptiveEA&InfMon data into datafram.	
    finalsummary2 = pd.read_sql_query(	
        "select * from finalsummary2CaptiveEA{}".format(InfMon), sqliteConnection)	
    # Concatenate the source data frames	
    df = pd.concat([finalsummary, finalsummary2], ignore_index=True, sort=False)	
    # Push results data frame to Sqlite DB	
    logging.info("finalsummaryout{} created successfully with {} records".format(InfMon, len(df)))	
    if 'level_0' in df.columns:	
        df = df.drop(columns=["level_0"])	
    df.to_sql("finalsummaryout{}".format(InfMon), con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
    ''' 	
    Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db	
    Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.	
    '''	
    # Sql Code Start and End Lines - 795&797 #	
    '''***************************************************	
    Below Python Code Executes The Standard SAS PROC SQL.	
    ******************************************************'''	
    # Connections to Sqlite DB and fetch all data from source table to process	
    # Please check if any SAS functions are not converted in SqLite query.	
    '''	
    try:	
        sql = """DROP TABLE IF EXISTS finalsummaryout&InfMon_sqlitesorted;CREATE TABLE finalsummaryout&InfMon_sqlitesorted AS SELECT * FROM	
            finalsummaryout&InfMon ORDER BY system,state,agttyp,dimseq,valseq;DROP	
            TABLE finalsummaryout&InfMon;ALTER TABLE	
            finalsummaryout&InfMon_sqlitesorted RENAME TO finalsummaryout&InfMon"""	
        sql = mcrResl(sql)	
        tgtSqliteTable = mcrResl("finalsummaryout&InfMon_sqlitesorted")	
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
    except:	
       e = sys.exc_info()[0]	
       logging.error('Table creation/update is failed.')	
       logging.error('Error - {}'.format(e))	
    '''	
    '''*********************************************************************************	
    Below python code is to execute SAS data step with BY varaible in python	
    *********************************************************************************'''	
    '''**WARNING:Below steps are not included in logic calculation. Please amend them manually.	
    retain;frstdim = first.dimseq;elsecnt = cnt + 1;seqhold = valseq;output;if seqhold = cnt then return;elsedo until (seqhold = cnt);nfrccnt = .;renew_1mon = .;renew1ratio = .;renew_2mon = .;renew2ratio = .;dimval = ' ';valseq = cnt;output;cnt = cnt + 1;end;'''	
    # Open connection to Sqlite work data base	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
    # Converting source finalsummaryout&InfMon data into datafram.	
    df = pd.read_sql_query(	
        "select * from finalsummaryout{}".format(InfMon), sqliteConnection)	
    # lowering all column names#Generate first and last temporary indicators in the given data frames.	
    df_lower_colNames(df, 'finalsummaryout{}'.format(InfMon))	
    var_list = ['system', 'state', 'agttyp', 'dimseq'] 	
    df = df.sort_values(by = var_list)	
    df['IsFirst'] ,df['IsLast'] = [False, False]	
    df.loc[df.groupby(var_list)['IsFirst'].head(1).index, 'IsFirst'] = True	
    df.loc[df.groupby(var_list)['IsLast'].tail(1).index, 'IsLast'] = True	
    df['cnt'] = df.groupby(var_list).cumcount() + 1	
    df_grouped = df.groupby(var_list)['valseq'].max()	
    #Output first occurance values in data to the target data frame.	
    df['frstdim'] = df['IsFirst']	
    df['seqhold'] = df['valseq']	
    df = df.drop(columns=['IsFirst','IsLast'])	
    df_new = df.set_index(var_list)	
    d = df_grouped.to_dict()	
    temp_df = pd.DataFrame(columns = df.columns)	
    out = []	
    for index, new_df in df_new.groupby(level=list(range(len(var_list)))):	
        for val in range(1, d[index]-1):	
            if val not in new_df['valseq'].to_list():	
                ungroup_df = new_df.reset_index()	
                temp = ungroup_df.iloc[0]	
                temp['nfrccnt'] = np.nan	
                temp['renew_1mon'] = np.nan	
                temp['renew1ratio'] = np.nan	
                temp['renew_2mon'] = np.nan	
                temp['renew2ratio'] = np.nan	    	
                temp['dimval'] = np.nan	
                temp['valseq'] = val	
                temp_df = temp_df.append(temp)	
    fixempty = pd.concat([df, temp_df],ignore_index=True,sort=False)	
    fixempty = df_remove_indexCols(fixempty)	
    if 'level_0' in fixempty.columns:	
        fixempty = fixempty.drop(columns=["level_0"])	
    # Push results data frame to Sqlite DB	
    logging.info(	
        "fixempty{} created successfully with {} records".format(InfMon, len(fixempty)))	
    fixempty.to_sql("fixempty{}".format(InfMon),	
                    con=sqliteConnection, if_exists='replace')	
    # Close connection to Sqlite work data base	
    sqliteConnection.close()	
### SAS Source Code Line Numbers START:1517 & END:1517.###	
'''SAS Comment:*** end first run block *************************************; '''	
### SAS Source Code Line Numbers START:1520 & END:1520.###	
'''SAS Comment:*** subsequent runs *****************************************; '''	
### SAS Source Code Line Numbers START:1522 & END:1524.###	
'''*********************************************************************************	
Below python code is to execute standard SAS data step	
*********************************************************************************'''	
client=bigquery.Client()	
sql = """select * from {}.{}.renewrtn_regautodb""".format(project_id, output_dataset)	
df_renewrtn_regautodb = client.query(sql).to_dataframe()	
df_renewrtn_regautodb.to_gbq(destination_table = output_dataset + '.' + 'renewrtn_regautodb_backup', project_id = project_id, if_exists='replace')	
'''	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
# reading the file from csv	
with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_renratio_regional_auto/data/renewrtn_regautodb.sas7bdat') as reader:	
df = reader.to_data_frame()	
df_lower_colNames(df, 'renewrtn_regautodb')	
df['duemon'] = df.duemon.astype('Int64')	
df = df.loc[df.duemon < int(LatestMon)]	
# putting into the sqliteDB	
df.to_sql("renewrtn_regautodb",  con=sqliteConnection, if_exists='replace', index=True)	
sqliteConnection.close()	
'''	
# Please Note - If any library references remove them accordingly post your code analysis.#	
# Open connection to Sqlite work data base	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
# Converting source df data into datafram.	
'''df = pd.read_sql_query(	
	"select * from renewrtn_regautodb", sqliteConnection)'''	
df = df_renewrtn_regautodb	
df['duemon'] = df.duemon.astype('Int64')	
df = df.loc[df.duemon < int(LatestMon)]	
# handling data frame column case senstivity.#	
df_lower_colNames(df, 'renewrtn_regautodb')	
if 'level_0' in df.columns:	
df = df.drop(columns=["level_0"])	
# Push results data frame to Sqlite DB	
logging.info(	
	"renewrtn_regautodb_backup created successfully with {} records".format(len(df)))	
df.to_sql("outfile_renewrtn_regautodb_backup",	
		  con=sqliteConnection, if_exists='replace')	
# Close connection to Sqlite work data base	
sqliteConnection.close()	
# Open connection to Sqlite work data base	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
#df = pd.read_sql_query("select * from renewrtn_regautodb where duemon < {}".format(InfMon2), sqliteConnection)	
df = df_renewrtn_regautodb	
df['duemon'] = df.duemon.astype('Int64')	
df = df.loc[df.duemon < int(LatestMon)]	
df = df.loc[df.duemon < int(InfMon2)]	
# lowering all columns	
df_lower_colNames(df, 'renewrtn_regautodb')	
df = df.sort_values(by = ['duemon', 'system', 'state', 'agttyp', 'dimseq', 'valseq'])	
# logging info	
df_creation_logging(df, "renewrtn_regautodb")	
del df_renewrtn_regautodb	
# putting into the sqliteDB	
df.to_sql("renewrtn_regautodb",	
	  con=sqliteConnection, if_exists='replace', index=True)	
sqliteConnection.close()	
# #*******************************End of Data Step Process**************************************************#	
# ### SAS Source Code Line Numbers START:1527 & END:1530.###	
# ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
# Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
# # Sql Code Start and End Lines - 1527&1530 #	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
stuff = InfMon0	
while(stuff < LatestMon):	
    stuff = stuff+1	
    if stuff % 100 == 13:	
        stuff = stuff+88	
    PopDB(stuff)	
df = pd.DataFrame()	
df['stuff'] = stuff	
df.to_sql("junk1", con=sqliteConnection, if_exists='replace')	
# Open connection to Sqlite work data base	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
# Converting source regauto data into datafram.	
regauto = pd.read_sql_query(	
    "select * from renewrtn_regautodb", sqliteConnection)	
# Converting source fixempty data into datafram.	
fixempty1 = pd.read_sql_query(	
    "select * from fixempty{}".format(InfMon1), sqliteConnection)	
df_lower_colNames(fixempty1, 'fixempty{}'.format(InfMon1))	
# Converting source fixempty data into datafram.	
fixempty2 = pd.read_sql_query(	
    "select * from fixempty{}".format(InfMon2), sqliteConnection)	
df_lower_colNames(fixempty2, 'fixempty{}'.format(InfMon2))	
# Concatenate the source data frames	
outfiledb = pd.concat([regauto, fixempty1, fixempty2],	
                      ignore_index=True, sort=False)	
if 'level_0' in outfiledb.columns:	
    outfiledb = outfiledb.drop(columns=["level_0"])	
# Push results data frame to Sqlite DB	
df_creation_logging(outfiledb, 'outfiledb')	
outfiledb.to_sql("outfiledb", con=sqliteConnection, if_exists='replace')	
# Close connection to Sqlite work data base	
sqliteConnection.close()	
sqliteToBQ(output_tables)	
### SAS Source Code Line Numbers START:1546 & END:1548.###	
''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.	
Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''	
# Sql Code Start and End Lines - 1546&1548 #	
'''***************************************************	
Below Python Code Executes The Standard SAS PROC SQL.	
******************************************************'''	
# Connections to Sqlite DB and fetch all data from source table to process	
# Please check if any SAS functions are not converted in SqLite query.	
'''	
try:	
    sql = """DROP TABLE IF EXISTS outfiledb_sqlitesorted;CREATE TABLE outfiledb_sqlitesorted AS SELECT * FROM outfiledb ORDER	
        BY DueMon,system,state,agttyp,dimseq,valseq;DROP TABLE outfiledb;ALTER	
        TABLE outfiledb_sqlitesorted RENAME TO outfiledb"""	
    sql = mcrResl(sql)	
    tgtSqliteTable = "outfiledb_sqlitesorted"	
    procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)	
except:	
    e = sys.exc_info()[0]	
    logging.error('Table creation/update is failed.')	
    logging.error('Error - {}'.format(e))	
'''	
### SAS Source Code Line Numbers START:1550 & END:1552.###	
'''*********************************************************************************	
Below python code is to execute standard SAS data step	
*********************************************************************************'''	
# Please Note - If any library references remove them accordingly post your code analysis.#	
# Open connection to Sqlite work data base	
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)	
# Converting source df data into datafram.	
df = pd.read_sql_query("select * from outfiledb ", sqliteConnection)	
# handling data frame column case senstivity.#	
df_lower_colNames(df, 'outfiledb')	
if 'level_0' in df.columns:	
    df = df.drop(columns=["level_0"])	
# Push results data frame to Sqlite DB	
logging.info(	
    "renewrtn_regautodb created successfully with {} records".format(len(df)))	
df.to_sql("renewrtn_regautodb",	
          con=sqliteConnection, if_exists='replace')	
# Close connection to Sqlite work data base	
sqliteConnection.close()	
#*******************************End of Data Step Process**************************************************#	
### SAS Source Code Line Numbers START:1553 & END:1555.###	
'''SAS Comment:*/	
*** end subsequent runs *************************************; '''