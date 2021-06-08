# -*- coding: utf-8 -*-
r'''
Created on: Mon 14 Dec 20 18:09:20
Author: SAS2PY Code Conversion Tool
SAS Input File: gw_ipm_hhldrtn home
SAS File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS_SRC_CDE
Generated Python File: Sas2PyConvertedScript_Out
Python File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS2PY_TRANSLATED
'''

''' Importing necessary standard Python 3 modules
Please uncomment the commented modules if necessary. '''

''' Importing necessary project specific core utility python modules.'''
'''Please update the below path according to your project specification where core SAS to Python code conversion core modules stored'''

import yaml
import pandas as pd
import logging
from sas2py_func_lib_repo_acg import *
from sas2py_code_converter_funcs_acg import *
import sys
import re
import sqlite3
import psutil
import os
import gc
from functools import reduce, partial
import numpy as np
from sas7bdat import SAS7BDAT
import warnings
import itertools
import dateutil.parser
import warnings
warnings.filterwarnings("ignore")
from google.cloud import bigquery

config_file = None
yaml_file = None

try:
    config_file = open('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/config.yaml', 'r+')
    yaml_file = yaml.load(config_file)
except Exception as e:
    print("Error reading config file | ERROR : ", e)
finally:
    config_file.close()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/data02/keys/lab-e42-bq.json"

project_id  = yaml_file['gcp_project_id']
output_dataset = yaml_file['gcp_output_dataset_id']
history_dataset_id = yaml_file['gcp_history_dataset_id']
output_tables = yaml_file['output_tables']

# Seting up logging info #

log_file_name = os.path.basename(__file__).lstrip('sas2py_converted_script_')[:-3]
logging.basicConfig(filename= yaml_file['logs'] + os.sep + log_file_name + '.log',level=logging.INFO,format='%(asctime)s - '+ log_file_name.upper()+' %(levelname)s - %(message)s')

''' Creating temporary sqlite working DB to store all temporay stating results '''

SQLitePythonWorkDb = yaml_file['workDb']
sqliteConnection=sqlite3.connect(SQLitePythonWorkDb)
sqliteDb = SQLitePythonWorkDb

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
        try:
            cursor.execute(row_count_sql)
            row_num = cursor.fetchall()[0][0]
        except:
            row_num = 0
            logging.info('Deleting records from {} table'.format(tgtSqliteTable))
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


'''def df_lower_colNames(dfName, tablename=None):
    # handling data frame column case senstivity
    dfName.columns = map(str.lower, dfName.columns)
    rows = len(dfName.index)
    tabNm = [x for x in globals() if globals()[x] is dfName][0]
    # logging data frame creation
    if tablename is not None:
        logging.info(
            'There were total {} records read from table{}.'.format(rows, tablename))
    else:
        logging.info('There were total {} records read from table{}.'.format(
            rows, tabNm.upper()))
        df.term_val_amt
        df['term_val_amt']
        df = df[df.covg_cd1 == 'undefined']'''


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
    # no_index_cols = [c for c in mrgResultTmpDf.columns if (c != "index_x" and c != "index_y" and  c != "index")]
    # mrgResultTmpDf = mrgResultTmpDf[no_index_cols]
    return mrgResultTmpDf.loc[:, ~mrgResultTmpDf.columns.str.startswith('index')]
    # df.loc[:,~df.columns.str.startswith('index')


def sqliteToBQ(output_tables):
    i = 1
    incr_recs = 0
    sql = "select * from NFRCFLDB"
    logging.info('Getting table NFRCFLDB from sqlitedb')	
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
    # del [[dfL]]
    # gc.collect()
    logging.info('Cleaning up memory consumed by the following data frames.')
    logging.info('{}'.format(dfL))
    for df in dfList:
        del df
        df = pd.DataFrame()
        del df
        # logging.info('Memory management is in progress for {}'.format(df.upper()))
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
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:2 & END:2.###
'''SAS Comment:************************PROGRAM INTRODUCTION **************************; '''
### SAS Source Code Line Numbers START:3 & END:3.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:4 & END:4.###
'''SAS Comment:*     NAME:  QUARTERLY, HOME INFORCE, HOUSEHOLD RETENTION RATIO        ; '''
### SAS Source Code Line Numbers START:5 & END:5.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:6 & END:6.###
'''SAS Comment:*   SYSTEM:  LEGACY & IPM                                              ; '''
### SAS Source Code Line Numbers START:7 & END:7.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:8 & END:8.###
'''SAS Comment:* FUNCTION:  THIS PROGRAM PROCESSES THE PRIOR AND CURRENT REPORTING    ; '''
### SAS Source Code Line Numbers START:9 & END:9.###
'''SAS Comment:*            PERIOD INFORCE DATA AND CALCULATES THE CURRENT RETENTION  ; '''
### SAS Source Code Line Numbers START:10 & END:10.###
'''SAS Comment:*            AND RETENTION RATIO. THE INFORMATION IS LOADED TO A SAS   ; '''
### SAS Source Code Line Numbers START:11 & END:11.###
'''SAS Comment:*            DATABASE. THE DATABASE IS INPUT TO A SEPARATE PROGRAM TO  ; '''
### SAS Source Code Line Numbers START:12 & END:12.###
'''SAS Comment:*            PRODUCE THE EXCEL SPREADSHEET REPORTS FOR EACH STATE.     ; '''
### SAS Source Code Line Numbers START:13 & END:13.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:14 & END:14.###
'''SAS Comment:*            HOUSEHOLD IS DEFINED DIFFERENTLY BETWEEN MICHIGAN AND     ; '''
### SAS Source Code Line Numbers START:15 & END:15.###
'''SAS Comment:*            REGIONAL STATES. FOR MICHIGAN, THE LEGACY AND IPM FILES   ; '''
### SAS Source Code Line Numbers START:16 & END:16.###
'''SAS Comment:*            ARE SORTED SEPARATELY ON MEMBERNO AND PREMIER AND THEN    ; '''
### SAS Source Code Line Numbers START:17 & END:17.###
'''SAS Comment:*            DEDUPED BY MEMBERNO (KEEPING THE RECORD WITH THE HIGHEST  ; '''
### SAS Source Code Line Numbers START:18 & END:18.###
'''SAS Comment:*            PREMIER). THE FILES ARE THEN STACKED, IPM ON TOP, AND     ; '''
### SAS Source Code Line Numbers START:19 & END:19.###
'''SAS Comment:*            DEDUPED BY MEMBERNO ONLY. THE RESULT IS THE IPM RECORD IS ; '''
### SAS Source Code Line Numbers START:20 & END:20.###
'''SAS Comment:*            THE MASTER REGARDLESS OF PREMIER. FOR REGIONAL STATES,    ; '''
### SAS Source Code Line Numbers START:21 & END:21.###
'''SAS Comment:*            HOUSEHOLD IS BASED ON THE SEQUENTIAL CLIENT NUMBER WITH   ; '''
### SAS Source Code Line Numbers START:22 & END:22.###
'''SAS Comment:*            THE HIGHEST PREMIER INDICATOR.                            ; '''
### SAS Source Code Line Numbers START:23 & END:23.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:24 & END:24.###
'''SAS Comment:*            ANOTHER PROGRAM WILL PRODUCE SEPARATE SPREADSHEETS FOR    ; '''
### SAS Source Code Line Numbers START:25 & END:25.###
'''SAS Comment:*            VARIOUS DIMENSIONS SUCH AS PREMIER AND MUTLIPRODUCT       ; '''
### SAS Source Code Line Numbers START:26 & END:26.###
'''SAS Comment:*            INDICATORS, HOME AGE, ROOF AGE, FORM CODE, AND MEMBERSHIP ; '''
### SAS Source Code Line Numbers START:27 & END:27.###
'''SAS Comment:*            INDICATOR. EACH OF THESE DIMENSIONS ARE ASSIGNED A        ; '''
### SAS Source Code Line Numbers START:28 & END:28.###
'''SAS Comment:*            SEQUENCE NUMBER ON THE DATABASE TO REPRESENT THE ORDER IN ; '''
### SAS Source Code Line Numbers START:29 & END:29.###
'''SAS Comment:*            WHICH THEY ARE PRESENTED IN THE EXCEL SPREADSHEET FOR EACH; '''
### SAS Source Code Line Numbers START:30 & END:30.###
'''SAS Comment:*            PARTICULAR STATE. EACH SHEET HAS 3 SEPARATE AREAS UNDER   ; '''
### SAS Source Code Line Numbers START:31 & END:31.###
'''SAS Comment:*            WHICH INFORCE/RETENTION COUNTS AND RATIO PERCENTAGE IS    ; '''
### SAS Source Code Line Numbers START:32 & END:32.###
'''SAS Comment:*            REPORTED - PRIOR YEAR REPORTING PERIOD INFORCE COUNT, THE ; '''
### SAS Source Code Line Numbers START:33 & END:33.###
'''SAS Comment:*            CURRENT REPORTING PERIOD RETENTION COUNT, AND THE         ; '''
### SAS Source Code Line Numbers START:34 & END:34.###
'''SAS Comment:*            CALCULATED RETENTION RATIO. THE PRIOR YEAR REPORTING      ; '''
### SAS Source Code Line Numbers START:35 & END:35.###
'''SAS Comment:*            PERIOD IS THE STANDARD FOR COMPARISON.                    ; '''
### SAS Source Code Line Numbers START:36 & END:36.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:37 & END:37.###
'''SAS Comment:*            EACH DIMENSION HAS VALUES/RANGES REPORTED ACROSS ALL THREE; '''
### SAS Source Code Line Numbers START:38 & END:38.###
'''SAS Comment:*            AREAS OF THE REPORT. SEE THE EXAMPLE BELOW FOR PREMIER    ; '''
### SAS Source Code Line Numbers START:39 & END:39.###
'''SAS Comment:*            INDICATOR - DIMENSION SEQUENCE = 1:                       ; '''
### SAS Source Code Line Numbers START:40 & END:40.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:41 & END:41.###
'''SAS Comment:* PRIOR INFORCE          CURRENT RETENTION         RETENTION RATIO     ; '''
### SAS Source Code Line Numbers START:42 & END:42.###
'''SAS Comment:*LOW   MED  HIGH         LOW   MED  HIGH           LOW   MED    HIGH   ; '''
### SAS Source Code Line Numbers START:43 & END:43.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:44 & END:44.###
'''SAS Comment:*            THE DATABASE RETAINS THE INFORCE AND RETENTION COUNTS AND ; '''
### SAS Source Code Line Numbers START:45 & END:45.###
'''SAS Comment:*            CALCULATED RATIO FOR EACH QUARTER, SYSTEM, PRODUCT, AGENT ; '''
### SAS Source Code Line Numbers START:46 & END:46.###
'''SAS Comment:*            TYPE, STATE, MIGR_IND, DIMENSION, AND DIMENSION VALUE. THE; '''
### SAS Source Code Line Numbers START:47 & END:47.###
'''SAS Comment:*            DIMENSION VALUES ARE THE REPORT COLUMNS. THERE IS A       ; '''
### SAS Source Code Line Numbers START:48 & END:48.###
'''SAS Comment:*            SEQUENCE NUMBER ASSOCIATED WITH EACH DIMENSION AND        ; '''
### SAS Source Code Line Numbers START:49 & END:49.###
'''SAS Comment:*            DIMENSION VALUE FOR REPORTING PURPOSES. THE DATABASE      ; '''
### SAS Source Code Line Numbers START:50 & END:50.###
'''SAS Comment:*            STRUCTURE IS COLUMNAR ALSO KNOWN AS LONG. THE REPORT      ; '''
### SAS Source Code Line Numbers START:51 & END:51.###
'''SAS Comment:*            PROGRAM WILL CONVERT THE COLUMNAR (LONG) STRUCTURE TO A   ; '''
### SAS Source Code Line Numbers START:52 & END:52.###
'''SAS Comment:*            ROW (WIDE)STRUCTURE.                                      ; '''
### SAS Source Code Line Numbers START:53 & END:53.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:54 & END:54.###
'''SAS Comment:*    INPUT:  QUARTER END DATE CARD (IUT015QM)                          ; '''
### SAS Source Code Line Numbers START:55 & END:55.###
'''SAS Comment:*            LEGACY HOME INFORCE EXTRACT                               ; '''
### SAS Source Code Line Numbers START:56 & END:56.###
'''SAS Comment:*            IPM HOME INFORCE EXTRACT                                  ; '''
### SAS Source Code Line Numbers START:57 & END:57.###
'''SAS Comment:*            LEGACY AGENT LIST                                         ; '''
### SAS Source Code Line Numbers START:58 & END:58.###
'''SAS Comment:*            IPM AGENT LIST                                            ; '''
### SAS Source Code Line Numbers START:59 & END:60.###
'''SAS Comment:*            HHLDRTN_HOMEDB
*                                                                      ; '''
### SAS Source Code Line Numbers START:61 & END:61.###
'''SAS Comment:*   OUTPUT:  HHLDRTN_HOMEDB - QUARTERLY HOUSEHOLD RETENTION INFORCE DB ; '''
### SAS Source Code Line Numbers START:62 & END:62.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:63 & END:63.###
'''SAS Comment:*DEPENDENCIES: MAINFRAME LEGACY AND IPM INFORCE FTP                    ; '''
### SAS Source Code Line Numbers START:64 & END:64.###
'''SAS Comment:*              MAINFRAME DATECARD (IUT015QM) FTP                       ; '''
### SAS Source Code Line Numbers START:65 & END:65.###
'''SAS Comment:*              WEBFOCUS AGENT LIST TRANSFER AND CONVERSION TO SAS DB   ; '''
### SAS Source Code Line Numbers START:66 & END:66.###
'''SAS Comment:*              LEGACY AGENT LIST TRANSFER AND CONVERSION TO SAS DB     ; '''
### SAS Source Code Line Numbers START:67 & END:67.###
'''SAS Comment:*              T:\Shared\Acturial\BISProd\RenewalRatio\SourceCode\     ; '''
### SAS Source Code Line Numbers START:68 & END:68.###
'''SAS Comment:*              RTNHOME_NFRCCNV PROGRAM MUST RUN TO CREATE THE          ; '''
### SAS Source Code Line Numbers START:69 & END:69.###
'''SAS Comment:*              MICHIGAN HOME AND REGIONAL HOME CONVERSION DATABASES.   ; '''
### SAS Source Code Line Numbers START:70 & END:70.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:71 & END:71.###
'''SAS Comment:*   BALANCING: - BALANCE CURRENT AND PRIOR LEGACY/IPM INFORCE          ; '''
### SAS Source Code Line Numbers START:72 & END:72.###
'''SAS Comment:*              - TRACK HHLD BREAKDOWNS FOR LEGACY/IPM MI COMBINED,     ; '''
### SAS Source Code Line Numbers START:73 & END:73.###
'''SAS Comment:*                LEGACY ONLY, IPM ONLY AND REGIONAL                    ; '''
### SAS Source Code Line Numbers START:74 & END:74.###
'''SAS Comment:*              - BALANCE HHLD BREAKDOWNS TO CREATION OF SEPARATE       ; '''
### SAS Source Code Line Numbers START:75 & END:75.###
'''SAS Comment:*                DATABASES                                             ; '''
### SAS Source Code Line Numbers START:76 & END:76.###
'''SAS Comment:*              NOTE:  INVALID AGENT TYPES WILL BE PRINTED IN THE LOG.  ; '''
### SAS Source Code Line Numbers START:77 & END:77.###
'''SAS Comment:*              TRACK TO SEE WHY THEY'RE INVALID TO DETERMINE WHAT      ; '''
### SAS Source Code Line Numbers START:78 & END:78.###
'''SAS Comment:*              SHOULD BE DONE IF ANYTHING. OTHERWISE, THE AGENT WILL BE; '''
### SAS Source Code Line Numbers START:79 & END:79.###
'''SAS Comment:*              SLOTTED UNDER UNKNOWN.                                  ; '''
### SAS Source Code Line Numbers START:80 & END:80.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:81 & END:81.###
'''SAS Comment:*              FOR MICHIGAN, LEGACY PLUS IPM INFORCE WILL BALANCE TO   ; '''
### SAS Source Code Line Numbers START:82 & END:82.###
'''SAS Comment:*              LEGACY AND IPM COMBINED. HOWEVER, THE RETENTION WILL NOT; '''
### SAS Source Code Line Numbers START:83 & END:83.###
'''SAS Comment:*              THE REASON IS A POLICY COULD HAVE MIGRATED FROM LEGACY  ; '''
### SAS Source Code Line Numbers START:84 & END:84.###
'''SAS Comment:*              TO IPM. SO, IT WON'T BE CAPTURED UNDER RETAINED ON THE  ; '''
### SAS Source Code Line Numbers START:85 & END:85.###
'''SAS Comment:*              THE REPORTS FOR THE SEPARATE SYSTEMS, BUT IT WILL BE    ; '''
### SAS Source Code Line Numbers START:86 & END:86.###
'''SAS Comment:*              CAPTURED ON THE COMBINED REPORT.                        ; '''
### SAS Source Code Line Numbers START:87 & END:87.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:88 & END:88.###
'''SAS Comment:*    NOTES:  THIS JOB CREATED FOR P&PD.                                ; '''
### SAS Source Code Line Numbers START:89 & END:89.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:90 & END:90.###
'''SAS Comment:*            CANNOT RUN THIS PROGRAM MULTIPLE TIMES WITHOUT DELETING   ; '''
### SAS Source Code Line Numbers START:91 & END:91.###
'''SAS Comment:*            ALL WORK FILES. THE EASIEST WAY TO DO THIS IS TO QUIT SAS ; '''
### SAS Source Code Line Numbers START:92 & END:92.###
'''SAS Comment:*            AND GET BACK IN AND RERUN. THIS PROGRAM CREATES A WORK    ; '''
### SAS Source Code Line Numbers START:93 & END:93.###
'''SAS Comment:*            FILE FOR WHICH DATA IS APPENDED TO THROUGH MULTIPLE       ; '''
### SAS Source Code Line Numbers START:94 & END:94.###
'''SAS Comment:*            ITERATIONS OF A MACRO.                                    ; '''
### SAS Source Code Line Numbers START:95 & END:95.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:96 & END:96.###
'''SAS Comment:*            REMEMBER THIS PROCESS CREATES AN ACTUAL DATABASE. THE     ; '''
### SAS Source Code Line Numbers START:97 & END:97.###
'''SAS Comment:*            DATABASE IS AUTOMATICALLY BACKED UP IN THIS PROCESS.      ; '''
### SAS Source Code Line Numbers START:98 & END:98.###
'''SAS Comment:*            THIS MUST BE TAKEN INTO CONSIDERATION FOR RERUNS.         ; '''
### SAS Source Code Line Numbers START:99 & END:99.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:100 & END:100.###
'''SAS Comment:*            THIS PROGRAM DOES NOT HAVE ANY MANUAL PARMS TO UPDATE.    ; '''
### SAS Source Code Line Numbers START:101 & END:101.###
'''SAS Comment:*            THE DATECARD FOR THE CURRENT QUARTER IS FTP'D FROM THE    ; '''
### SAS Source Code Line Numbers START:102 & END:102.###
'''SAS Comment:*            MAINFRAME TO DIRECTORY, T\Shared\Acturial\FTPINBOUND.     ; '''
### SAS Source Code Line Numbers START:103 & END:103.###
'''SAS Comment:*            THE PROGRAM READS THE CURRENT QUARTER DATE AND CALCULATES ; '''
### SAS Source Code Line Numbers START:104 & END:104.###
'''SAS Comment:*            DATE FOR THE SAME QUARTER FROM THE PRIOR YEAR. THESE DATES; '''
### SAS Source Code Line Numbers START:105 & END:105.###
'''SAS Comment:*            ARE THEN USED TO ACCESS DATE QUALIFIED FILES. THE CURRENT ; '''
### SAS Source Code Line Numbers START:106 & END:106.###
'''SAS Comment:*            QUARTER FILES ARE USED TO CALCULATE RETENTION BASED ON THE; '''
### SAS Source Code Line Numbers START:107 & END:107.###
'''SAS Comment:*            INFORCE COUNTS FOR THE SAME PERIOD IN THE PRIOR YEAR.     ; '''
### SAS Source Code Line Numbers START:108 & END:108.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:109 & END:109.###
'''SAS Comment:*            THE LEGACY AND IPM HOME INFORCE FILES FOR THE CURRENT     ; '''
### SAS Source Code Line Numbers START:110 & END:110.###
'''SAS Comment:*            QUARTER ARE FTP'D FROM THE MAINFRAME TO THE FTPINBOUND    ; '''
### SAS Source Code Line Numbers START:111 & END:111.###
'''SAS Comment:*            DIRECTORY IN BINARY FORMAT NAMED, LGCYBNRY AND IPMBNRY    ; '''
### SAS Source Code Line Numbers START:112 & END:112.###
'''SAS Comment:*            RESPECTIVELY. THIS PROGRAM EXECUTES THE CIMPORT PROCEDURE ; '''
### SAS Source Code Line Numbers START:113 & END:113.###
'''SAS Comment:*            TO CREATE SAS FILES FROM THE BINARY TEXT FILES. THESE SAS ; '''
### SAS Source Code Line Numbers START:114 & END:114.###
'''SAS Comment:*            FILES ARE CALLED LGHMNFRC (LEGACY) AND FINALOUT2 (IPM)    ; '''
### SAS Source Code Line Numbers START:115 & END:115.###
'''SAS Comment:*            BASED ON THE NAMES GIVEN IN THE MAINFRAME CODE. LGHMNFRC  ; '''
### SAS Source Code Line Numbers START:116 & END:116.###
'''SAS Comment:*            AND FINALOUT2 ARE THEN INPUT & MANIPULATED TO CREATE DATE ; '''
### SAS Source Code Line Numbers START:117 & END:117.###
'''SAS Comment:*            QUALIFIED SAS FILES FOR THE CURRENT QUARTER TO BE USED TO ; '''
### SAS Source Code Line Numbers START:118 & END:118.###
'''SAS Comment:*            CALCULATE RETENTION. THE FILE CREATED THIS RUN WILL BE    ; '''
### SAS Source Code Line Numbers START:119 & END:119.###
'''SAS Comment:*            USED IN THE SAME PERIOD NEXT YEAR AS THE INFORCE FILE.    ; '''
### SAS Source Code Line Numbers START:120 & END:120.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:121 & END:121.###
'''SAS Comment:*            THE PROGRAM IS STRUCTURED TO DO THE FOLLOWING:            ; '''
### SAS Source Code Line Numbers START:122 & END:122.###
'''SAS Comment:* PART 1 - CREATE CURRENT LEGACY AND IPM DATE QUALIFIED FILES. THE     ; '''
### SAS Source Code Line Numbers START:123 & END:123.###
'''SAS Comment:*          INPUT FILES ARE CONVERTED TO STANDARDIZE FIELDS AND TO PULL ; '''
### SAS Source Code Line Numbers START:124 & END:124.###
'''SAS Comment:*          THE CORRECT AGENT TYPE.                                     ; '''
### SAS Source Code Line Numbers START:125 & END:125.###
'''SAS Comment:* PART 2 - BREAKOUT THE CURRENT AND PRIOR INFORCE FILES BY HOUSEHOLD.  ; '''
### SAS Source Code Line Numbers START:126 & END:126.###
'''SAS Comment:*          THIS IS DONE IN THE PRCSFILE MACRO WHICH IS PERFORMED TWO   ; '''
### SAS Source Code Line Numbers START:127 & END:127.###
'''SAS Comment:*          TIMES. ONCE FOR CURRENT AND ONCE FOR PRIOR.                 ; '''
### SAS Source Code Line Numbers START:128 & END:128.###
'''SAS Comment:* PART 3 - CREATE RETAINED DATABASES BASED ON THE REPORTING REQUIREMENT; '''
### SAS Source Code Line Numbers START:129 & END:129.###
'''SAS Comment:*          MI LEGACY AND IPM COMBINED, LEGACY ONLY, IPM ONLY, REGIONAL ; '''
### SAS Source Code Line Numbers START:130 & END:130.###
'''SAS Comment:* PART 4 - CALCULATE PRIOR INFORCE, CURRENT RETENTION, AND THE         ; '''
### SAS Source Code Line Numbers START:131 & END:131.###
'''SAS Comment:*          RETENTION RATIO FOR EACH DIMENSION:  PREMIER INDICATOR, ETC ; '''
### SAS Source Code Line Numbers START:132 & END:132.###
'''SAS Comment:*          AND POPULATE ROWS IN THE TEMPORARY DATABASE. THIS IS DONE   ; '''
### SAS Source Code Line Numbers START:133 & END:133.###
'''SAS Comment:*          BY THE CRTDB MACRO WHICH IS EXECUTED FOUR TIMES ONCE FOR    ; '''
### SAS Source Code Line Numbers START:134 & END:134.###
'''SAS Comment:*          EACH DATABASE.                                              ; '''
### SAS Source Code Line Numbers START:135 & END:135.###
'''SAS Comment:* PART 5 - CREATE ROWS ON THE TEMPORARY DATABASE FOR ANY MISSING       ; '''
### SAS Source Code Line Numbers START:136 & END:136.###
'''SAS Comment:*          DIMENSIONS. THIS IS NECESSARY FOR FINAL REPORTING.          ; '''
### SAS Source Code Line Numbers START:137 & END:137.###
'''SAS Comment:* PART 6 - BACKUP THE ACTUAL DATABASE AND CREATE THE NEW. THE CURRENT  ; '''
### SAS Source Code Line Numbers START:138 & END:138.###
'''SAS Comment:*          QUARTER DATA IS ADDED TO THE EXISTING DATABASE.             ; '''
### SAS Source Code Line Numbers START:139 & END:139.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:140 & END:140.###
'''SAS Comment:*            THE FINAL OUTPUT IS A SAS DATABASE, HHLDRTN_HOMEDB. THIS  ; '''
### SAS Source Code Line Numbers START:141 & END:141.###
'''SAS Comment:*            DATABASE IS INPUT TO THE REPORT WRITER PROGRAM TO PRODUCE ; '''
### SAS Source Code Line Numbers START:142 & END:142.###
'''SAS Comment:*            THE FINAL EXCEL SPREADSHEETS. THIS PROGRAM CREATES A      ; '''
### SAS Source Code Line Numbers START:143 & END:143.###
'''SAS Comment:*            BACKUP OF THE DATABASE EACH RUN.                          ; '''
### SAS Source Code Line Numbers START:144 & END:144.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:145 & END:145.###
'''SAS Comment:************************ REVISION LOG *********************************; '''
### SAS Source Code Line Numbers START:146 & END:146.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:147 & END:147.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:148 & END:148.###
'''SAS Comment:*   DATE        INIT     DESCRIPTION OF CHANGE                         ; '''
### SAS Source Code Line Numbers START:149 & END:149.###
'''SAS Comment:*   09/13       PLT      NEW PROGRAM                                   ; '''
### SAS Source Code Line Numbers START:150 & END:150.###
'''SAS Comment:*   03/15       PLT      Split out EA agent type for IPM only. Rewrite ; '''
### SAS Source Code Line Numbers START:151 & END:151.###
'''SAS Comment:*                        to process using the conversion files created ; '''
### SAS Source Code Line Numbers START:152 & END:152.###
'''SAS Comment:*                        in the renewal ratio process.                 ; '''
### SAS Source Code Line Numbers START:153 & END:153.###
'''SAS Comment:*                        If necessary to rerun prior to 201412, you    ; '''
### SAS Source Code Line Numbers START:154 & END:154.###
'''SAS Comment:*                        must use program HHLDRTN HOME prior to 201412 ; '''
### SAS Source Code Line Numbers START:155 & END:155.###
'''SAS Comment:*   09/15       PLT    - New premier breakout.                         ; '''
### SAS Source Code Line Numbers START:156 & END:156.###
'''SAS Comment:*   01/16       PLT    - New prior claim categories.                   ; '''
### SAS Source Code Line Numbers START:157 & END:157.###
'''SAS Comment:*                      - Correct issue where entries for agent types   ; '''
### SAS Source Code Line Numbers START:158 & END:158.###
'''SAS Comment:*                        SSA & EA were not created.                    ; '''
### SAS Source Code Line Numbers START:159 & END:159.###
'''SAS Comment:*   02/18       PLT    - Breakout MSC & HB, but keep MSC/HB as well    ; '''
### SAS Source Code Line Numbers START:160 & END:160.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:161 & END:161.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
OPTIONS MPRINT SYMBOLGEN MSTORED SASMSTORE=StoreMac;
'''

### SAS Source Code Line Numbers START:162 & END:162.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME StoreMac 'T:\Shared\Acturial\BISMacros\SAS';
'''

### SAS Source Code Line Numbers START:164 & END:164.###
'''SAS Comment:*A common conversion file is created in the New Business Policies Inforce process that is used across the household; '''
### SAS Source Code Line Numbers START:165 & END:165.###
'''SAS Comment:*retention ratio, renewal retention ratio and cancellation ratio processes.; '''
### SAS Source Code Line Numbers START:166 & END:166.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME NFRCFILE 'T:\Shared\Acturial\BISProd\CommonDataSources\Home\Inforce\';
'''

### SAS Source Code Line Numbers START:167 & END:167.###
'''SAS Comment:*LIBNAME NFRCFILE 'T:\Shared\Acturial\BISProd\RenewalRatio\InputOutput\'; '''
### SAS Source Code Line Numbers START:170 & END:170.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME NFRCOUT 'T:\Shared\Acturial\BISProd\RetentionRatio\InputOutput\';
'''

### SAS Source Code Line Numbers START:172 & END:172.###
'''SAS Comment:*FILENAME IUT015M 'T:\Shared\Acturial\BISProd\CommonDataSources\Datecards\IUT015M_1508.TXT'; '''
### SAS Source Code Line Numbers START:175 & END:175.###
'''SAS Comment:*Use the following date qualified files for testing; '''
### SAS Source Code Line Numbers START:176 & END:176.###
'''SAS Comment:*FILENAME IUT015M 'T:\Shared\Acturial\BISProd\CommonDataSources\Datecards\IUT015M_1412.TXT'; '''
### SAS Source Code Line Numbers START:177 & END:177.###
'''SAS Comment:*THIS IS WHAT THE INPUT DATECARD LOOKS LIKE; '''
### SAS Source Code Line Numbers START:178 & END:178.###
'''SAS Comment:*IUT015  2014112720141231; '''
### SAS Source Code Line Numbers START:179 & END:179.###
'''SAS Comment:*IUT015QM2013100120131231; '''
### SAS Source Code Line Numbers START:182 & END:353.###

# SAS Comment:*NOTE:  VALUE CLAUSES WITH FORMAT NAMES THAT BEGIN WITH A DOLLAR SIGN ARE CHARACTER.
# SAS Comment:*THE DISTINCTION BETWEEN NUMERIC AND CHARACTER APPLIES TO THE COLUMN TO WHICH THE
# SAS Comment:*FORMAT WILL BE APPLIED -- NOT THE DISPLAYED VALUE.
# SAS Comment:*INVALUE IS AN INFORMAT
# SAS Comment:*FOR ALL THE RETENTION RATIO REPORT DIMENSIONS: PREMIER INDICATOR, MULTIPRODUCT INDICATOR, HOME
# SAS Comment:*AGE, ROOF AGE, AND FORM CODE, WE CREATE A FORMAT TO REPRESENT THE DIMENSION SEQUENCE AND
# SAS Comment:*THE DIMENSION VALUE. THE SEQUENCE IS THE ORDER IN WHICH THE VALUE WILL APPEAR ON THE FINAL
# SAS Comment:*REPORT. THE VALUE IS THE TITLE THAT WILL APPEAR ON THE REPORT.
# SAS Comment:*MI LEGACY AND IPM AGENT TYPES. IPM AGENT TYPES ARE ONE CHARACTER SPACE FILLED.


def AGTTYP_SEQ(inPut):
    inPut = str(inPut)
    if inPut in ('E'):
        return 1
    elif inPut in ('2000', '0103', '0201', '1', '7', '8'):
        return 2
    elif inPut in ('0603', '0605', '0607', '0703', '3'):
        return 3
    elif inPut in ('2', '6'):
        return 4
    elif inPut in ('9'):
        return 5
    elif inPut in ('9901', '6000', '6001', '1401', '1901', '4'):
        return 6
    else:
        return 7


def AGTTYP_VAL(inPut):
    inPut = str(inPut)
    if inPut == '1':
        return 'EC'
    elif inPut == '2':
        return 'SSA'
    elif inPut == '3':
        return 'MSC'
    elif inPut == '4':
        return 'IA'
    elif inPut == '5':
        return 'EA'
    elif inPut == '6':
        return 'HB'
    elif inPut == '7':
        return 'Unknown'

# SAS Comment:***THE FOLLOWING IS OBSOLETE. The code was left in case we revert back. New breakout as of 09-2015.
# SAS Comment:*PREMIER INDICATOR. For MI, a premier indicator 7-10 is HIGH. For non-MI, a premier indicator
# SAS Comment:*of 7 is MEDIUM. A value of 10 is valid for IPM.


def OLD_MI_PREMIER_SEQ(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in range(7, 11):
        return 3
    elif inPut in range(4, 7):
        return 2
    elif inPut in range(0, 4):
        return 1


def OLD_MI_PREMIER_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Low'
    elif inPut in [2]:
        return 'Medium'
    elif inPut in [3]:
        return 'High'


def OLD_NONMI_PREMIER_SEQ(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in range(8, 11):
        return 3
    elif inPut in range(4, 8):
        return 2
    elif inPut in range(0, 4):
        return 1


def OLD_NONMI_PREMIER_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Low'
    elif inPut in [2]:
        return 'Medium'
    elif inPut in [3]:
        return 'High'

# SAS Comment:*****************************************************************************
# SAS Comment:*MICHIGAN MULTIPRODUCT INDICATOR
# SAS Comment:*Legacy values 0, 1, 2, 3
# SAS Comment:* 0 = HOME ONLY
# SAS Comment:* 1 = HOME + AUTO - NO LIFE
# SAS Comment:* 2 = HOME + AUTO + LIFE
# SAS Comment:* 3 = HOME + LIFE - NO AUTO


def MI_MULTI_SEQ(inPut):
    inPut = str(inPut)
    if inPut in ['A', 'L', '1', '3']:
        return 1
    elif inPut in ['B', '2']:
        return 2
    elif inPut in ['N', '0', ' ', np.nan]:
        return 3


def MI_MULTI_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Auto or Life'
    elif inPut in [2]:
        return 'Both'
    elif inPut in [3]:
        return 'None'

# SAS Comment:*NON-MICHIGAN MULTIPRODUCT INDICATOR


def NONMI_MULTI_SEQ(inPut):
    inPut = str(inPut)
    if inPut in ['A', 'L', 'B']:
        return 1
    elif inPut in ['N', ' ', np.nan]:
        return 2


def NONMI_MULTI_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Yes'
    elif inPut in [2]:
        return 'No'

# SAS Comment:*****************************************************************************
# SAS Comment:*HOME AGE. If the home age is > 2000, it indicates the year built is input
# SAS Comment:*as zeroes (UNKNOWN). Another classification for "UNKNOWN" is a negative value calculated
# SAS Comment:*for age because the year built is not numeric.


def HMAGE_SEQ(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in range(0, 6):
        return 1
    elif inPut in range(6, 11):
        return 2
    elif inPut in range(11, 16):
        return 3
    elif inPut in range(16, 21):
        return 4
    elif inPut in range(21, 2001):
        return 5
    else:
        return 6


def HMAGE_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return '0-5'
    elif inPut in [2]:
        return '6-10'
    elif inPut in [3]:
        return '11-15'
    elif inPut in [4]:
        return '16-20'
    elif inPut in [5]:
        return '21+'
    elif inPut in [6]:
        return 'Unknown'

# SAS Comment:*****************************************************************************
# SAS Comment:*ROOF AGE. If the roof age is > 2000, it indicates the ROOF YEAR is input
# SAS Comment:*as zeroes (UNKNOWN). The classification "UNKNOWN" also indicates non numeric roof years.
# SAS Comment:*Zero and non numeric roof years default to year built. Any roof > 100 years old defaults
# SAS Comment:*to UNKNOWN.


def RFAGE_SEQ(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in range(0, 6):
        return 1
    elif inPut in range(6, 11):
        return 2
    elif inPut in range(11, 16):
        return 3
    elif inPut in range(16, 21):
        return 4
    elif inPut in range(21, 2001):
        return 5
    else:
        return 6


def RFAGE_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return '0-5'
    elif inPut in [2]:
        return '6-10'
    elif inPut in [3]:
        return '11-15'
    elif inPut in [4]:
        return '16-20'
    elif inPut in [5]:
        return '21+'
    elif inPut in [6]:
        return 'Unknown'

# SAS Comment:*****************************************************************************
# SAS Comment:*MICHIGAN FORM CODE


def FORM_SEQ(inPut):
    inPut = str(inPut)
    if inPut in ['H3', 'H5']:
        return 1
    elif inPut in ['H4']:
        return 2
    elif inPut in ['H6']:
        return 3
    else:
        return 4


def FORM_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'H3 & H5'
    if inPut in [2]:
        return 'H4'
    elif inPut in [3]:
        return 'H6'
    elif inPut in [4]:
        return 'Other'

# SAS Comment:*MEMBERSHIP INDICATOR. Applies only to Non-MI IPM. Everyone in MI has a membership.


def MBRIND_SEQ(inPut):
    inPut = str(inPut)
    if inPut in ['Y']:
        return 1
    elif inPut in ['N']:
        return 2


def MBRIND_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Yes'
    elif inPut in [2]:
        return 'No'

# SAS Comment:*****************************************************************************
# SAS Comment:*NEW PREMIER INDICATOR


def MI_KY_WV_PREMIER_SEQ(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in range(9, 11):
        return 4
    elif inPut in range(6, 9):
        return 3
    elif inPut in range(3, 6):
        return 2
    elif inPut in range(0, 3):
        return 1


def MI_KY_WV_PREMIER_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Low'
    elif inPut in [2]:
        return 'Mid-Low'
    elif inPut in [3]:
        return 'Mid-High'
    elif inPut in [4]:
        return 'High'


def RGNL_PREMIER_SEQ(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in range(8, 10):
        return 4
    elif inPut in range(5, 8):
        return 3
    elif inPut in range(2, 5):
        return 2
    elif inPut in range(0, 2):
        return 1


def RGNL_PREMIER_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Low'
    elif inPut in [2]:
        return 'Mid-Low'
    elif inPut in [3]:
        return 'Mid-High'
    elif inPut in [4]:
        return 'High'


def LGCY_PREMIER_SEQ(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut == 9:
        return 4
    elif inPut in range(6, 9):
        return 3
    elif inPut in range(2, 6):
        return 2
    elif inPut in range(0, 2):
        return 1


def LGCY_PREMIER_VAL(inPut):
    try:
        inPut = int(float(inPut))
    except:
        pass
    if inPut in [1]:
        return 'Low'
    elif inPut in [2]:
        return 'Mid-Low'
    elif inPut in [3]:
        return 'Mid-High'
    elif inPut in [4]:
        return 'High'


### SAS Source Code Line Numbers START:354 & END:354.###
'''SAS Comment:************************************END OF PROC FORMAT******************************************; '''
### SAS Source Code Line Numbers START:356 & END:356.###
'''SAS Comment:************************************END OF PROC FORMAT******************************************; '''
### SAS Source Code Line Numbers START:358 & END:358.###
'''SAS Comment:***********************************MACRO BEGINS HERE*******************************************; '''
### SAS Source Code Line Numbers START:359 & END:359.###
'''SAS Comment:*THIS SECTION IS DONE TWO TIMES. ONCE FOR THE PRIOR YEAR AND ONCE FOR THE CURRENT YEAR. THIS   ; '''
### SAS Source Code Line Numbers START:360 & END:360.###
'''SAS Comment:*MACRO READS AND PROCESSES THE CURRENT QUARTER AND THE PRIOR YEAR DATA FOR THE SAME QUARTER.   ; '''
### SAS Source Code Line Numbers START:361 & END:361.###
'''SAS Comment:*THIS SECTION SORTS AND DEDUPS THE DATA TO DEFINE HOUSEHOLD PER THE PROJECT REQUIREMENTS AND   ; '''
### SAS Source Code Line Numbers START:362 & END:362.###
'''SAS Comment:*BREAKS THE DATA INTO SEPARATE DATABASES FOR PROCESSING: MI & IPM COMBINED, MI LEGACY ONLY,    ; '''
### SAS Source Code Line Numbers START:363 & END:363.###
'''SAS Comment:*MI IPM ONLY, AND IPM REGIONAL ONLY.                                                           ; '''
### SAS Source Code Line Numbers START:364 & END:364.###
'''SAS Comment:***********************************MACRO BEGINS HERE*******************************************; '''
### SAS Source Code Line Numbers START:365 & END:490.###

''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''


def PRCSFILE(DTEQUAL):
    '''SAS Comment:**TEMP TEMP TEMP TEMP TEMP** TEMPORARILY COMBINE MICHIGAN AND REGIONAL*Create a legacy work database; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/data/mihmcnv_{}.sas7bdat'.format(DTEQUAL)) as reader:
    df = reader.to_data_frame()
    df.to_sql("nfrcfile_mihmcnv_{}".format(DTEQUAL),
                  con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
	
	'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/data/nfrcfile_mihmcnv_{}.csv".format(DTEQUAL))
    # lowering all columns
    df_lower_colNames(df, 'mihmcnv')
    # logging info
    df_creation_logging(df, "nfrcfile_mihmcnv_{}".format(DTEQUAL))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_mihmcnv_{}".format(DTEQUAL),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_mihmcnv_{}".format(DTEQUAL), sqliteConnection)
	# handling data frame column case senstivity.#
    df_lower_colNames(df, 'mihmcnv')
    # TITLE; # Manual effort require.
    # if state not = 'MI' then delete; # Manual effort require.
    df = df.loc[(df.state == 'MI')]
    df['qtrnd'] = df.mnthnd
    # Drop columns in the target df data in datafram.
    df = df.drop(columns="mnthnd")
    # if syscd = 'L' then output LGDB; # Manual effort require.
    df_sys = df.loc[(df.syscd == 'L')]
    df_sys.to_sql("LGDB", con=sqliteConnection,
                  if_exists='replace', index=True)
    logging.info(
        "LGDB created successfully with {} records".format(len(df_sys)))
    # elseif syscd = 'I' then output MIIPMDB; # Manual effort require.
    df_sys = df.loc[(df.syscd == 'I')]
    df_sys.to_sql("MIIPMDB", con=sqliteConnection,
                  if_exists='replace', index=True)
    logging.info(
        "MIIPMDB created successfully with {} records".format(len(df_sys)))
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*Create a Michigan IPM work database and an IPM regional state work database; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/data/rgnhmcnv_{}.sas7bdat'.format(DTEQUAL)) as reader:
    df = reader.to_data_frame()
    df.to_sql("nfrcfile_rgnhmcnv_{}".format(DTEQUAL),
                  con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
	
    '''sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/data/nfrcfile_rgnhmcnv_{}.csv".format(DTEQUAL))
    # lowering all columns
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}'.format(DTEQUAL))
    # logging info
    df_creation_logging(df, "nfrcfile_rgnhmcnv_{}".format(DTEQUAL))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_rgnhmcnv_{}".format(DTEQUAL),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_rgnhmcnv_{}".format(DTEQUAL), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'rgnhmcnv')
    # if state = 'MI' then delete; # Manual effort require.
    df = df.loc[(df.state != 'MI')]
    df['qtrnd'] = df.mnthnd
    df['system'] = df.syscd
    # Drop columns in the target df data in datafram.
    df = df.drop(columns="mnthnd")
    df = df_remove_indexCols(df)
    logging.info(
        "NONMI_{} created successfully with {} records".format(DTEQUAL, len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("NONMI_{}".format(DTEQUAL),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:***********************************************************************************************; '''
    '''SAS Comment:*Ensure the file is sorted in policy number order first. For legacy, this is the BASIC7 and    ; '''
    '''SAS Comment:*suffix. SEQPOLNO was created on the legacy file for consistent naming with IPM. The field is  ; '''
    '''SAS Comment:*the policy basic7 and suffix. The suffix is a sequential value. By first sorting with the     ; '''
    '''SAS Comment:*suffix, the order is from oldest policy to the newest.                                        ; '''

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 44&46 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS LGDB_sqlitesorted;CREATE TABLE LGDB_sqlitesorted AS SELECT * FROM LGDB ORDER BY
    #         SEQPOLNO;DROP TABLE LGDB;ALTER TABLE LGDB_sqlitesorted RENAME TO
    #         LGDB;"""
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = "LGDB_sqlitesorted"
    #     procSql_standard_Exec(SQLitePythonWorkDbb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    '''SAS Comment:*For MI, must sort descending premier and then sort dup by member number. This gets you to HHLD; '''
    '''SAS Comment:*level retaining the highest premier. For legacy, MBRNO is also the BASIC7.                    ; '''

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 51&53 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS LGDB_sqlitesorted;CREATE TABLE LGDB_sqlitesorted AS SELECT * FROM LGDB ORDER BY
    #         MBRNO,DESCENDING,PRIND;DROP TABLE LGDB;ALTER TABLE LGDB_sqlitesorted
    #         RENAME TO LGDB"""
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = "LGDB_sqlitesorted"
    #     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    ''' Conversion of PROC SORT with NODUPKEY option enabled into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source LGDB data into datafram.
    LGDB = pd.read_sql_query("select * from LGDB", sqliteConnection)
    # lowering all column names #Remove duplicated columns
    df_lower_colNames(LGDB)
    LGDB = LGDB.sort_values(
        ["mbrno"], ascending=True).drop_duplicates(["mbrno"])
    LGDB = df_remove_indexCols(LGDB)
    df_creation_logging(LGDB, "LGDB")
    LGDB.to_sql("LGDB", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''SAS Comment:*Ensure the file is sorted in policy number order first to ensure the order is from the oldest ; '''
    '''SAS Comment:*to the newest; '''

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 63&65 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS MIIPMDB_sqlitesorted; CREATE TABLE MIIPMDB_sqlitesorted AS SELECT * FROM MIIPMDB ORDER BY
    #         SEQPOLNO;DROP TABLE MIIPMDB;ALTER TABLE MIIPMDB_sqlitesorted RENAME TO
    #         MIIPMDB"""
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = "MIIPMDB_sqlitesorted"
    #     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    '''SAS Comment:*Sort IPM MI database by the member number. Retain the record with the highest premier indicator; '''

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 68&70 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS MIIPMDB_sqlitesorted; CREATE TABLE MIIPMDB_sqlitesorted AS SELECT * FROM MIIPMDB ORDER BY
    #         MBRNO,DESCENDING,PRIND;DROP TABLE MIIPMDB;ALTER TABLE
    #         MIIPMDB_sqlitesorted RENAME TO MIIPMDB"""
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = "MIIPMDB_sqlitesorted"
    #     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    ''' Conversion of PROC SORT with NODUPKEY option enabled into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source MIIPMDB data into datafram.
    MIIPMDB = pd.read_sql_query("select * from MIIPMDB", sqliteConnection)
    # lowering all column names #Remove duplicated columns
    df_lower_colNames(MIIPMDB)
    MIIPMDB = MIIPMDB.sort_values(
        ["mbrno"], ascending=True).drop_duplicates(["mbrno"])
    MIIPMDB = df_remove_indexCols(MIIPMDB)
    df_creation_logging(MIIPMDB, "MIIPMDB")
    MIIPMDB.to_sql("MIIPMDB", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    # sqliteConnection.close()

    '''SAS Comment:*Concatenate the sorted MI IPM and legacy databases and sort again removing duplicate records  ; '''
    '''SAS Comment:*based on the member number. This is insureds that have multiple homes across the systems. In  ; '''
    '''SAS Comment:*the case of duplicates, the IPM record is retained and is considered the master.              ; '''
    '''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
    SYSTEM = 'C';MIGRIND = 'N';'''
    # Converting source MIIPMDB data into datafram.
    MIIPMDB = pd.read_sql_query("select * from MIIPMDB", sqliteConnection)
    # Converting source LGDB data into datafram.
    LGDB = pd.read_sql_query("select * from LGDB ", sqliteConnection)
    # Concatenate the source data frames
    MIDB = pd.concat([MIIPMDB, LGDB], ignore_index=True, sort=False)
    MIDB['system'] = 'C'
    MIDB['MIGRIND'] = 'N'
    MIDB = df_remove_indexCols(MIDB)
    df_creation_logging(MIDB, "MIDB_{}".format(DTEQUAL))
    # Push results data frame to Sqlite DB
    MIDB.to_sql("MIDB_{}".format(DTEQUAL),
                con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''SAS Comment:*At this point you have the MI IPM and legacy files stacked. Now, sort out any duplicate legacy; '''
    '''SAS Comment:*member numbers regardless of the premier indicator. IPM is the master in determining          ; '''
    '''SAS Comment:*household.                                                                                    ; '''

    ''' Conversion of PROC SORT with NODUPKEY option enabled into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source MIDB_&DTEQUAL data into datafram.
    MIDB = pd.read_sql_query(
        "select * from MIDB_{}".format(DTEQUAL), sqliteConnection)
    # lowering all column names #Remove duplicated columns
    df_lower_colNames(MIDB)
    MIDB = MIDB.sort_values(
        ["mbrno"], ascending=True).drop_duplicates(["mbrno"])
    MIDB = df_remove_indexCols(MIDB)
    df_creation_logging(MIDB, "midb_{}".format(DTEQUAL))
    MIDB.to_sql("MIDB_{}".format(DTEQUAL),
                con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''SAS Comment:*Now after combining IPM and legacy and sorting dups, create separate databases for each system.; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from MIDB_{}".format(DTEQUAL), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'MIDB_{}'.format(DTEQUAL))
    df['system'] = df.syscd

    # IF SYSCD = 'L' THEN OUTPUT LGDB_&DTEQUAL; # Manual effort require.
    df_l = df.loc[(df.syscd == 'L')]
    df_l = df_l.drop(['level_0'], axis=1)
    df_l.to_sql("LGDB_{}".format(DTEQUAL), con=sqliteConnection,
                if_exists='replace', index=True)
    logging.info("LGDB_{} created successfully with {} records".format(
        DTEQUAL, len(df_l)))
    # ***Start manual effort here...
    # ELSE OUTPUT MIIPMDB_&DTEQUAL;
    df_l = df.loc[(df.syscd != 'L')]
    df_l = df_l.drop(['level_0'], axis=1)
    df_l.to_sql("MIIPMDB_{}".format(DTEQUAL), con=sqliteConnection,
                if_exists='replace', index=True)
    # End manual effort.***'''
    logging.info("MIIPMDB_{} created successfully with {} records".format(
        DTEQUAL, len(df_l)))
    # Push results data frame to Sqlite DB

    # Push results data frame to Sqlite DB

    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*First, sort non-MI IPM by the actual IPM policy number; '''

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 110&112 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS NONMI_{}_sqlitesorted; CREATE TABLE NONMI_{}_sqlitesorted AS SELECT * FROM
    #         NONMI_{} ORDER BY SEQPOLNO;DROP TABLE NONMI_{};ALTER TABLE
    #         NONMI_{}_sqlitesorted RENAME TO NONMI_{}""".format(DTEQUAL)
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = mcrResl("NONMI_{}_sqlitesorted").format(DTEQUAL)
    #     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    '''SAS Comment:*For non-MI, HHLD is determined by CLIENT; '''
    '''SAS Comment:*There are cases where there are multiple homes for the same client, but there are different; '''
    '''SAS Comment:*premier indicator values. The record for the highest premier value is retained.            ; '''

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 117&119 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS NONMI_{}_sqlitesorted; CREATE TABLE NONMI_{}_sqlitesorted AS SELECT * FROM
    #         NONMI_{} ORDER BY STATE,SEQCLTNO,DESCENDING,PRIND,SEQPOLNO;DROP TABLE
    #         NONMI_{};ALTER TABLE NONMI_{}_sqlitesorted RENAME TO
    #         WNONMI_{}""".format(DTEQUAL)
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = mcrResl("NONMI_{}_sqlitesorted").format(DTEQUAL)
    #     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    ''' Conversion of PROC SORT with NODUPKEY option enabled into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source NONMI_&DTEQUAL data into datafram.
    NONMI = pd.read_sql_query(
        "select * from NONMI_{} ".format(DTEQUAL), sqliteConnection)
    # lowering all column names #Remove duplicated columns
    df_lower_colNames(NONMI)
    NONMI = NONMI.sort_values(
        ["state", "seqcltno"], ascending=True).drop_duplicates(["state", "seqcltno"])
    NONMI = df_remove_indexCols(NONMI)
    df_creation_logging(NONMI, "NONMI_{}".format(DTEQUAL))
    NONMI.to_sql("NONMI_{}".format(DTEQUAL),
                 con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()


'''Uncomment to execute the below sas macro'''
# PRCSFILE(<< Provide require args here >>)

### SAS Source Code Line Numbers START:491 & END:491.###
'''SAS Comment:***********************************MACRO ENDS HERE**********************************************; '''
### SAS Source Code Line Numbers START:496 & END:496.###
'''SAS Comment:***********************************MACRO BEGINS HERE*******************************************; '''
### SAS Source Code Line Numbers START:497 & END:497.###
'''SAS Comment:*THIS SECTION IS PERFORMED FOUR TIMES. ONCE FOR EACH DATABASE TYPE: MI IPM/LEGACY COMBINED,    ; '''
### SAS Source Code Line Numbers START:498 & END:498.###
'''SAS Comment:*MI IPM ONLY, MI LEGACY ONLY, AND REGIONAL STATES. THIS IS WHERE THE DIMENSIONS ARE CREATED AND; '''
### SAS Source Code Line Numbers START:499 & END:499.###
'''SAS Comment:*THE CALCULATIONS FOR INFORCE AND RETENTION ARE PERFORMED.                                     ; '''
### SAS Source Code Line Numbers START:500 & END:500.###
'''SAS Comment:***********************************MACRO BEGINS HERE*******************************************; '''
### SAS Source Code Line Numbers START:501 & END:1000.###

''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''
try:
	sql = """DROP TABLE IF EXISTS NFRCOUT;"""
	sql = mcrResl(sql)
	tgtSqliteTable = "NFRCOUT"
	procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
	e = sys.exc_info()[0]
	logging.error('Table creation/update is failed.')
	logging.error('Error - {}'.format(e))

def CRTDB(DBQUAL):
    '''SAS Comment:*THIS IS DONE FOUR TIMES ONCE FOR EACH DATABASE (MICMB, MIIPM, MILGCY, AND NON MI); '''
    '''SAS Comment:*THIS IS ALSO PERFORMED ONLY FOR THE PRIOR YEAR; '''
    '''*********************************************************************************
	Below python code is to execute standard SAS data step
	*********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from {} ".format(DBQUAL), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, '{}'.format(DBQUAL))
    df = df_remove_indexCols(df)
    logging.info(
        " HHLDNFRC created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("HHLDNFRC", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:***SET AGENT TYPE ON ALL RECORDS**************************************************************************; '''
    '''*********************************************************************************
	Below python code is to execute standard SAS data step
	*********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from HHLDNFRC ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'HHLDNFRC')

    df['valseq'] = df['agttypcd'].apply(AGTTYP_SEQ)
    df['agttyp'] = df['valseq'].apply(AGTTYP_VAL)
    df_unknown = df.loc[df.agttyp == 'Unknown']
    df_unknown = df_remove_indexCols(df_unknown)
    logging.info("unkagtdb created with {} records".format(len(df_unknown)))
    df_unknown.to_sql("unkagtdb", con=sqliteConnection, if_exists='replace')

    df.loc[df.agttyp == 'Unknown', 'agttyp'] = 'HB'
    df.loc[df.agttyp == 'Unknown', 'valseq'] = 6

    df['dim'] = 'Old Premier'
    df['dimseq'] = 1

    df['n_1'] = df['prind'].apply(OLD_MI_PREMIER_SEQ)
    df['m_1'] = df['prind'].apply(OLD_NONMI_PREMIER_SEQ)
    df['valseq'] = df.apply(lambda x: x['n_1'] if x['state']
                            == 'MI' else x['m_1'], axis=1)
    df = df.drop(columns=['n_1', 'm_1'])

    df['n_1'] = df['valseq'].apply(OLD_MI_PREMIER_VAL)
    df['m_1'] = df['valseq'].apply(OLD_NONMI_PREMIER_VAL)
    df['dimval'] = df.apply(lambda x: x['n_1'] if x['state']
                            == 'MI' else x['m_1'], axis=1)
    df = df.drop(columns=['n_1', 'm_1'])

    df = df_remove_indexCols(df)
    df['agtsave'] = df['agttyp']
    count = 1
    logging.info('{} homenfrc - {}'.format(count, len(df)))
    count = count + 1
    df.to_sql("homenfrc", con=sqliteConnection, if_exists='replace')
    df_homenfrc = df.loc[df['agttyp'].isin(['SSA', 'EA'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['MSC', 'HB'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df['dim'] = 'Multiproduct'
    df['dimseq'] = 2

    df['multind'] = df['multind'].astype(str)
    df['n_1'] = df['multind'].apply(NONMI_MULTI_SEQ)
    df['m_1'] = df['multind'].apply(MI_MULTI_SEQ)
    df['valseq'] = df.apply(lambda x: x['m_1'] if x['state']
                            == 'MI' else x['n_1'], axis=1)
    df = df.drop(columns=['n_1', 'm_1'])

    df['valseq'] = df['valseq'].astype(str)
    df['n_1'] = df['valseq'].apply(MI_MULTI_VAL)
    df['m_1'] = df['valseq'].apply(NONMI_MULTI_VAL)

    df['dimval'] = df.apply(lambda x: x['n_1'] if x['state']
                            == 'MI' else x['m_1'], axis=1)
    df = df.drop(columns=['n_1', 'm_1'])
    df = df_remove_indexCols(df)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['SSA', 'EA'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['MSC', 'HB'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df['dim'] = 'Home Age'
    df['dimseq'] = 3

    df['valseq'] = '7'
    df['dimval'] = 'Other'
    col = 'form'
    conditions = [df[col].isin(['H3', 'H5', 'H6'])]
    choices1 = [df['hmage'].apply(HMAGE_SEQ)]
    choices2 = [df['valseq'].apply(HMAGE_VAL)]
    df['valseq'] = np.select(conditions, choices1)
    df['dimval'] = np.select(conditions, choices2)
    df = df_remove_indexCols(df)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['SSA', 'EA'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['MSC', 'HB'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df['dim'] = 'Roof Age'
    df['dimseq'] = 4

    df_homenfrc = df.loc[(df['system'] == 'I') & (df.form == 'H4')]
    df_homenfrc['dimval'] = 'Other'
    df_homenfrc['valseq'] = 7
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    
    df_homenfrc = df.loc[(df.system == 'I') & (df.form != 'H4')]
    df_homenfrc['valseq'] = df_homenfrc['rfage'].apply(RFAGE_SEQ)
    df_homenfrc['dimval'] = df_homenfrc['valseq'].apply(RFAGE_VAL)
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df['dim'] = 'Form'
    df['dimseq'] = 5

    df['valseq'] = df['form'].apply(FORM_SEQ)
    df['dimval'] = df['valseq'].apply(FORM_VAL)

    df.loc[(df.syscd != 'L') & (df.form == 'H3'), 'dimval'] = 'H3'
    df = df_remove_indexCols(df)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['SSA', 'EA'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['MSC', 'HB'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df.state != 'MI']
    df_homenfrc['dim'] = 'Membership'
    df_homenfrc['dimseq'] = 6
    df_homenfrc['valseq'] = df_homenfrc['mbrind'].apply(MBRIND_SEQ)
    df_homenfrc['dimval'] = df_homenfrc['valseq'].apply(MBRIND_VAL)
    temp = len(df_homenfrc)
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA']))
                         & (df.state != 'MI')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    temp = temp + len(df_homenfrc)
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB']))
                         & (df.state != 'MI')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, temp + len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df['dim'] = 'Premier'
    df['dimseq'] = 7
    df['valseq'] = df['prind'].apply(RGNL_PREMIER_SEQ)
    df['dimval'] = df['valseq'].apply(RGNL_PREMIER_VAL)

    df.loc[df.syscd == 'L', 'valseq'] = df.loc[df.syscd ==
                                               'L', 'prind'].apply(LGCY_PREMIER_SEQ)
    df.loc[df.syscd == 'L', 'dimval'] = df.loc[df.syscd ==
                                               'L', 'valseq'].apply(LGCY_PREMIER_VAL)

    df.loc[(df.syscd != 'L') & (df.state.isin(['MI', 'KY', 'WV'])), 'valseq'] = df.loc[(
        df.syscd != 'L') & (df.state.isin(['MI', 'KY', 'WV'])), 'prind'].apply(MI_KY_WV_PREMIER_SEQ)
    df.loc[(df.syscd != 'L') & (df.state.isin(['MI', 'KY', 'WV'])), 'dimval'] = df.loc[(
        df.syscd != 'L') & (df.state.isin(['MI', 'KY', 'WV'])), 'valseq'].apply(MI_KY_WV_PREMIER_VAL)

    df = df_remove_indexCols(df)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['SSA', 'EA'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[df['agttyp'].isin(['MSC', 'HB'])]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df['dim'] = 'Claim Counts'
    df['dimseq'] = 9

    df['valseq'] = 5
    df['dimval'] = '4+'
    col = 'hototloss'
    conditions = [df[col] == '0', df[col] ==
                  '1', df[col] == '2', df[col] == '3']
    choices1 = [1, 2, 3, 4]
    choices2 = ['0', '1', '2', '3']
    df['valseq'] = np.select(conditions, choices1)
    df['dimval'] = np.select(conditions, choices2)
    df_homenfrc = df.loc[df.system == 'I']
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df['dim'] = 'Claim Types'
    df['dimseq'] = 17
    df['valseq'] = 4
    df['dimval'] = 'None'
    df['totlossa'] = df['totlossa'].astype('Int64')
    df['totlossb'] = df['totlossb'].astype('Int64')

    df.loc[(df.totlossa > 0) & (df.totlossb > 0), 'valseq'] = 3
    df.loc[(df.totlossa > 0) & (df.totlossb > 0), 'dimval'] = 'Both'

    df.loc[(df.totlossa > 0) & (df.totlossb == 0), 'valseq'] = 1
    df.loc[(df.totlossa > 0) & (df.totlossb == 0), 'dimval'] = 'A'

    df.loc[(df.totlossa == 0) & (df.totlossb > 0), 'valseq'] = 2
    df.loc[(df.totlossa == 0) & (df.totlossb > 0), 'dimval'] = 'B'

    df_homenfrc = df.loc[df.system == 'I']
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    # End manual effort.***
    # df=df_cmb_nfrcrtnd.copy()
    df['dim'] = 'Prior Claims'
    df['dimseq'] = 18
    df['valseq'] = 2
    df['dimval'] = 'No'
    df['totlossa'] = df['totlossa'].astype('Int64')
    df['totlossb'] = df['totlossb'].astype('Int64')
    df.loc[(df.totlossa > 0) | (df.totlossb > 0), 'valseq'] = 1
    df.loc[(df.totlossa > 0) | (df.totlossb > 0), 'dimval'] = 'Yes'
    df_homenfrc = df.loc[df.system == 'I']
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'Captive/EA'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

    df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB']))
                         & (df.system == 'I')]
    df_homenfrc['agtsave'] = df_homenfrc['agttyp']
    df_homenfrc['agttyp'] = 'MSC/HB'
    df_homenfrc = df_remove_indexCols(df_homenfrc)
    logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
    count = count + 1
    df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment: ***************************************************
	'''
    '''SAS Comment: ***************************************************
	'''

    '''WARNING: Below SAS step has not converted in this release.
	PROC PRINT DATA = WORK.UNKAGTDB
	RUN
	'''
    '''SAS Comment: *SUMMARIZE INFORCE AND RETENTION COUNT BY AGENT TYPE
	'''
    # TODO categorical variable proc summary ID statement
    '''WARNING: Below SAS step has not converted in this release.
	PROC SUMMARY DATA = WORK.HOMENFRC NWAY
	CLASS QTRND SYSTEM PRODUCT STATE AGTTYP DIMSEQ VALSEQ
	ID DIM DIMVAL
	VAR PREV_NFRCCNT CURR_RTNCNT
	OUTPUT OUT = WORK.DIMSUM(DROP=_TYPE_ _FREQ_)SUM =
	RUN
	'''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from HOMENFRC", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'HOMENFRC')
    str_cols = set(df.select_dtypes(include = ['object', 'string']).columns)
    agg_cols = {'dim', 'dimval', 'prev_nfrccnt', 'curr_rtncnt'}
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value = '')
    df = df.groupby(['qtrnd', 'system', 'product', 'state', 'agttyp', 'dimseq', 'valseq']).agg(
        {'dim' : max, 'dimval' : max, 'prev_nfrccnt': sum, 'curr_rtncnt': sum}).reset_index()
    df_creation_logging(df, "DIMSUM")
    df.to_sql("DIMSUM", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()

    '''SAS Comment: *CREATE ROW TOTALS BY AGTTYP; '''

    '''Python Indentation required, DO loop start detected. Please intend the code'''
    #     IF SYSTEM  ==  'I' :
    #
    # '''Python Indentation required, DO loop start detected. Please intend the code'''
    #     if hototloss  ==  '1' :
    # '''Python Indentation required, DO loop start detected. Please intend the code'''
    #     if hototloss  ==  '2' :
    # '''Python Indentation required, DO loop start detected. Please intend the code'''
    #     if hototloss  ==  '3' :
    # '''Python Indentation required, DO loop start detected. Please intend the code'''
    #     IF TOTLOSSA > 0 AND TOTLOSSB  ==  0 :
    # '''Python Indentation required, DO loop start detected. Please intend the code'''
    #     IF TOTLOSSA  ==  0 AND TOTLOSSB > 0 :
    # 	    TITLE 'UNKNOWN AGENT TYPES'
    '''WARNING: Below SAS step has not converted in this release.
	PROC SUMMARY DATA = WORK.DIMSUM NWAY
	CLASS QTRND SYSTEM PRODUCT STATE AGTTYP DIMSEQ
	ID VALSEQ DIM DIMVAL
	VAR PREV_NFRCCNT CURR_RTNCNT
	OUTPUT OUT = WORK.ROWSUM(DROP=_TYPE_ _FREQ_)SUM =
	RUN
	'''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from DIMSUM", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'DIMSUM')
    str_cols = set(df.select_dtypes(include = ['object', 'string']).columns)
    agg_cols = {'valseq', 'dim', 'dimval', 'prev_nfrccnt', 'curr_rtncnt'}
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value = '')
    df = df.groupby(['qtrnd', 'system', 'product', 'state', 'agttyp', 'dimseq']).agg(
        {'valseq': max, 'dim' : max, 'dimval':max, 'prev_nfrccnt': sum, 'curr_rtncnt': sum}).reset_index()
    df_creation_logging(df, "ROWSUM")
    df.to_sql("ROWSUM", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()

    '''SAS Comment: *CREATE THE "TOTAL" DIMVAL; '''
    '''*********************************************************************************
	Below python code is to execute standard SAS data step
	*********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from ROWSUM ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'ROWSUM')
    df['dimval'] = 'Total'
    # ***Start manual effort here...
    # IF DIMSEQ IN ('1', '2') THEN VALSEQ = 4;
    # df['valseq']=[4 if x isin('1','2') for x in df['dimseq']]
    df.loc[df['dimseq'].isin(['1', '2']), 'valseq'] = 4
    # End manual effort.***'''

    # ***Start manual effort here...
    # IF DIMSEQ IN ('3', '4') THEN VALSEQ = 8;
    # df['valseq']=[8 if x isin('3','4') for x in df['dimseq']]
    df.loc[df['dimseq'].isin(['3', '4']), 'valseq'] = 8
    # End manual effort.***'''

    # ***Start manual effort here...
    # IF STATE NOT = 'MI' AND DIMSEQ = '2' THEN VALSEQ = 3;
    df.loc[(df.state != 'MI') & (df.dimseq == '2'), 'valseq'] = 3
    # End manual effort.***'''

    # ***Start manual effort here...
    # IF STATE = 'MI' AND DIMSEQ = '5' THEN VALSEQ = 5;
    # df['valseq']=[5 if (x=='MI') & (y=='5') else 4 if y=='5' for x,y in df['state'],df['dimseq'] ]
    df.loc[(df['state'] == 'MI') & (df['dimseq'] == '5'), 'valseq'] = 5
    # End manual effort.***'''

    # ***Start manual effort here...
    # ELSEIF DIMSEQ = '5' THEN VALSEQ = 4;
    df.loc[df['dimseq'] == '5', 'valseq'] = 4
    # End manual effort.***'''

    # ***Start manual effort here...
    # IF DIMSEQ IN ('6', '18') THEN VALSEQ = 3;
    # df['valseq']=[3 if x isin('6','18') for x in df['dimseq']]
    df.loc[df['dimseq'].isin(['6', '18']), 'valseq'] = 3
    # End manual effort.***'''

    # ***Start manual effort here...
    # IF DIMSEQ IN ('7', '17') THEN VALSEQ = 5;
    # df['valseq']=[5 if x isin('7','17') for x in df['dimseq']]
    df.loc[df['dimseq'].isin(['7', '17']), 'valseq'] = 5
    # End manual effort.***'''

    # ***Start manual effort here...
    # IF DIMSEQ = '9' THEN VALSEQ = 6;
    df.loc[df.dimseq == '9', 'valseq'] = 6
    # End manual effort.***'''
    df = df_remove_indexCols(df)
    logging.info(
        "ROWSUM2 created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("ROWSUM2", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    # OUTPUT OUT=DIMSUM (DROP=_TYPE_ _FREQ_)
    # SUM=
    '''SAS Comment:*******************************************************************************; '''
    '''SAS Comment:*CONCATENATE THE "TOTAL" DIMVAL WITH THE OTHER DIMENSION VALUES FOR EACH AGTTYP; '''
    # OUTPUT OUT=ROWSUM (DROP=_TYPE_ _FREQ_)
    # SUM=
    # Converting source DIMSUM data into datafram.
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    DIMSUM = pd.read_sql_query("select * from DIMSUM ", sqliteConnection)
    # Converting source ROWSUM2 data into datafram.
    ROWSUM2 = pd.read_sql_query("select * from ROWSUM2 ", sqliteConnection)
    # Concatenate the source data frames
    SUMDB = pd.concat([DIMSUM, ROWSUM2], ignore_index=True, sort=False)
    SUMDB = df_remove_indexCols(SUMDB)
    df_creation_logging(SUMDB, "SUMDB")
    # Push results data frame to Sqlite DB
    SUMDB.to_sql("SUMDB", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
	 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 428&430 #
    '''***************************************************
	Below Python Code Executes The Standard SAS PROC SQL.
	******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS SUMDB_sqlitesorted; CREATE TABLE SUMDB_sqlitesorted AS SELECT * FROM SUMDB ORDER BY
    #         STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE SUMDB;ALTER TABLE
    #         SUMDB_sqlitesorted RENAME TO SUMDB"""
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = "SUMDB_sqlitesorted"
    #     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    #    IF DIMSEQ = '5' :
    # VALSEQ = 4
    '''SAS Comment:*ROLL UP SUMMARIES FOR EACH AGENT TYPE TO CREATE THE "ALL AGENT" CATEGORY; '''
    '''SAS Comment:*The SSA & EA agent types are accounted for in the Captive/EA category; '''
    # TODO categorical variable in ID statement

    '''WARNING: Below SAS step has not converted in this release.
	PROC SUMMARY DATA=WORK.SUMDB NWAY;
	where agttyp not in ('SSA', 'EA', 'MSC', 'HB');
	CLASS QTRND SYSTEM PRODUCT STATE DIMSEQ VALSEQ;
	ID DIM DIMVAL;
	VAR PREV_NFRCCNT CURR_RTNCNT;
	OUTPUT OUT=WORK.AGTSUM (DROP=_TYPE_ _FREQ_)SUM=;
	RUN;
	'''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from SUMDB where agttyp not in ('SSA', 'EA', 'MSC', 'HB')", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'SUMDB')
    str_cols = set(df.select_dtypes(include = ['object', 'string']).columns)
    agg_cols = {'dim', 'dimval', 'prev_nfrccnt', 'curr_rtncnt'}
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value = '')
    df = df.groupby(['qtrnd', 'system', 'product', 'state', 'dimseq', 'valseq']).agg(
        {'dim' : max, 'dimval' :max, 'prev_nfrccnt': sum, 'curr_rtncnt': sum}).reset_index()
    df_creation_logging(df, "AGTSUM")
    df.to_sql("AGTSUM", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()

    '''SAS Comment:*CREATE THE "All Agents" AGTTYP; '''
    '''*********************************************************************************
	Below python code is to execute standard SAS data step
	*********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from AGTSUM ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'AGTSUM')
    df['agttyp'] = 'All Agents'
    df = df_remove_indexCols(df)
    logging.info(
        "AGTSUM2 created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("AGTSUM2", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*CONCATENATE "All Agents" AGTTYP WITH THE OTHER AGTTYPs; '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source AGTSUM2 data into datafram.
    AGTSUM2 = pd.read_sql_query("select * from AGTSUM2 ", sqliteConnection)
    # Converting source SUMDB data into datafram.
    SUMDB = pd.read_sql_query("select * from SUMDB ", sqliteConnection)
    # Concatenate the source data frames
    NEWSUM = pd.concat([AGTSUM2, SUMDB], ignore_index=True, sort=False)
    df = df_remove_indexCols(NEWSUM)
    df_creation_logging(NEWSUM, "NEWSUM")
    # Push results data frame to Sqlite DB
    NEWSUM.to_sql("NEWSUM", con=sqliteConnection, if_exists='replace')
    # OUTPUT OUT = AGTSUM(DROP=_TYPE_ _FREQ_)
    # SUM =
    sqliteConnection.close()
    '''SAS Comment:*Calculate the retention ratio for all rows; '''
    '''*********************************************************************************
	Below python code is to execute standard SAS data step
	*********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from NEWSUM ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'NEWSUM')
    df['rtnratio'] = df.curr_rtncnt / df.prev_nfrccnt
    df['migrind'] = 'N'
    df = df_remove_indexCols(df)
    logging.info("NFRCDB created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("NFRCDB", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
	 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 469&471 #
    '''***************************************************
	Below Python Code Executes The Standard SAS PROC SQL.
	******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    # try:
    #     sql = """DROP TABLE IF EXISTS NFRCDB_sqlitesorted; CREATE TABLE NFRCDB_sqlitesorted AS SELECT * FROM NFRCDB ORDER BY
    #         STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE NFRCDB;ALTER TABLE
    #         NFRCDB_sqlitesorted RENAME TO NFRCDB"""
    #     sql = mcrResl(sql)
    #     tgtSqliteTable = "NFRCDB_sqlitesorted"
    #     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    # except:
    #    e = sys.exc_info()[0]
    #    logging.error('Table creation/update is failed.')
    #    logging.error('Error - {}'.format(e))

    '''SAS Comment:*Create an interim, inforce database. This data must be further manipulated to create rows for  ; '''
    '''SAS Comment:*missing valseq values across the various dimensions.                                           ; '''

    # Sql Code Start and End Lines - 476&494 #
    '''***************************************************
	Below Python Code Executes The Standard SAS PROC SQL.
	******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS NTRMOUT; CREATE TABLE NTRMOUT AS SELECT
			QTRND,SYSTEM,PRODUCT,AGTTYP,STATE,MIGRIND,DIM,DIMSEQ,DIMVAL,VALSEQ,PREV_NFRCCNT
			AS NFRCCNT,CURR_RTNCNT AS RTNCNT,RTNRATIO FROM NFRCDB"""
        sql = mcrResl(sql)
        tgtSqliteTable = "NTRMOUT"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))

    '''WARNING: Below SAS step has not converted in this release.
	PROC APPEND BASE=NFRCOUT DATA=NTRMOUT;
	run;
	'''
    '''PROC APPEND BASE=NFRCOUT DATA=NTRMOUT'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source NFRCOUT data into datafram.
    NTRMOUT = pd.read_sql_query("select * from NTRMOUT ", sqliteConnection)
    df_creation_logging(NTRMOUT, 'NTRMOUT')
    df_lower_colNames(NTRMOUT)
    try:
        NFRCOUT = pd.read_sql_query("select * from NFRCOUT ", sqliteConnection)
    except:
        NFRCOUT = pd.DataFrame(columns = list(NTRMOUT.columns))
    df_lower_colNames(NFRCOUT)
    NFRCOUT = pd.concat([NFRCOUT, NTRMOUT], ignore_index=True, sort=False)
    if 'level_0' in NFRCOUT.columns:
        NFRCOUT = NFRCOUT.drop(columns = 'level_0')
    df_creation_logging(NFRCOUT, 'NFRCOUT')
    NFRCOUT.to_sql("NFRCOUT", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()


'''Uncomment to execute the below sas macro'''
# CRTDB(<< Provide require args here >>)

### SAS Source Code Line Numbers START:1001 & END:1001.###
'''SAS Comment:***********************************MACROS ENDS HERE*********************************************; '''
### SAS Source Code Line Numbers START:1005 & END:1005.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:1006 & END:1006.###
'''SAS Comment:****PROCESSING BEGINS HERE****PROCESSING BEGINS HERE****PROCESSING BEGINS HERE****              ; '''
### SAS Source Code Line Numbers START:1007 & END:1007.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:1008 & END:1008.###
'''SAS Comment:*IF THE CONVERSION FILES ARE GOOD AND IT IS NECESSARY TO RERUN TO RECREATE THE DATABASE,        ; '''
### SAS Source Code Line Numbers START:1009 & END:1009.###
'''SAS Comment:*COMMENT OUT EVERYTHING IN PART 1 AFTER READING IN THE DATECARD. THE DATECARD IS USED IN THE    ; '''
### SAS Source Code Line Numbers START:1010 & END:1010.###
'''SAS Comment:*OTHER SECTIONS OF THE CODE.                                                                    ; '''
### SAS Source Code Line Numbers START:1013 & END:1013.###
'''SAS Comment:***THIS SECTION PROCESSES THE CURRENT LEGACY AND IPM INFORCE FILES ONLY AND CREATES DATE        ; '''
### SAS Source Code Line Numbers START:1014 & END:1014.###
'''SAS Comment:***QUALIFIED FILES OF EACH. THE DATE QUALIFIED FILES ARE PROCESSED IN PART II AND ARE COMPARED  ; '''
### SAS Source Code Line Numbers START:1015 & END:1015.###
'''SAS Comment:***TO THE PRIOR YEAR DATE QUALIFIED FILES TO DETERMINE THE RETENTION.                           ; '''
### SAS Source Code Line Numbers START:1017 & END:1017.###
'''SAS Comment:*THIS IS WHAT THE INPUT DATECARD LOOKS LIKE; '''
### SAS Source Code Line Numbers START:1018 & END:1018.###
'''SAS Comment:*IUT015QM2013033020130628; '''
### SAS Source Code Line Numbers START:1020 & END:1035.###
"""ERROR: Unable to convert the below SAS block/code into python
DATA _NULL_;
 INFILE IUT015M;
 INPUT @17 QTRND_CCYYMM 6. @17 QTRND_CCYY 4. @21 QTRND_MM 2. ;
 ***DEFINE CURRDTE AND PREVDTE AS INTEGER FOR COMPARISON LATER ;
 CALL SYMPUT('CURR_CCYYMM',PUT(QTRND_CCYYMM,Z6.));
 CALL SYMPUT('PREV_CCYYMM',PUT(QTRND_CCYYMM - 100,Z6.));
 CALL SYMPUT('CURR_CCYY',PUT(QTRND_CCYY,Z4.));
 CALL SYMPUT('CURR_MM',PUT(QTRND_MM,Z2.));
 RUN;
"""

file_str = open(
    r"/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/data/IUT015M.TXT").read()

'''df='IUT015  2021013020210226' '''
c_qtrnd__ccyymm = file_str[16:22]
p_qtrnd__ccyymm = str((int(file_str[16:22]))-100)
qtrnd_ccyy = file_str[16:20]
qtrnd_mm = file_str[20:22]
mnthnd_ccyymmdd = file_str[16:24]

global curr_ccyymm, prev_ccyymm, curr_ccyy, curr_mm

curr_ccyymm = datetime.datetime.strptime(
    c_qtrnd__ccyymm, "%Y%m").strftime("%Y%m").strip()
prev_ccyymm = datetime.datetime.strptime(
    p_qtrnd__ccyymm, "%Y%m").strftime("%Y%m").strip()
curr_ccyy = datetime.datetime.strptime(qtrnd_ccyy, "%Y").strftime("%Y").strip()
curr_mm = datetime.datetime.strptime(
    qtrnd_mm, "%m").strftime("%m").strip()

### SAS Source Code Line Numbers START:1038 & END:1038.###
'''SAS Comment:**************************************PART 2****************************************************; '''
### SAS Source Code Line Numbers START:1039 & END:1039.###
'''SAS Comment:*EXECUTE MACRO TO PROCESS THE PREVIOUS AND CURRENT YEAR INFORCE FILES; '''
### SAS Source Code Line Numbers START:1040 & END:1040.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
# Please Validate Before Execution.'''
PRCSFILE(prev_ccyymm)

### SAS Source Code Line Numbers START:1041 & END:1041.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
PRCSFILE(curr_ccyymm)

### SAS Source Code Line Numbers START:1043 & END:1043.###
'''SAS Comment:***END OF FIRST MACRO TO CREATE PREVIOUS AND CURRENT YEAR DATABASES USED TO CALCULATE RETENTION; '''
### SAS Source Code Line Numbers START:1046 & END:1046.###
'''SAS Comment:**************************************PART 3****************************************************; '''
### SAS Source Code Line Numbers START:1047 & END:1047.###
'''SAS Comment:*NOW, MATCH THE CURRENT INFORCE DATABASE TO THE PRIOR INFORCE DATABASE; '''
### SAS Source Code Line Numbers START:1048 & END:1048.###
'''SAS Comment:*THE CURR_RTNCNT IS THE ACTUAL RETAINED;                                                                                ; '''
### SAS Source Code Line Numbers START:1050 & END:1050.###
'''SAS Comment:*THIS IS THE MI IPM AND LEGACY DATABASES COMBINED. THE SYSTEM CODE IS SET TO "C" (COMBINED) ; '''
### SAS Source Code Line Numbers START:1051 & END:1061.###

# Sql Code Start and End Lines - 1051&1061 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS MICMB_NFRCRTN; CREATE TABLE MICMB_NFRCRTN AS SELECT A.* ,A.NFRCCNT AS PREV_NFRCCNT ,B.NFRCCNT
        AS CURR_RTNCNT FROM MIDB_{} A LEFT JOIN MIDB_{} B
        ON B.MBRNO = A.MBRNO""".format(prev_ccyymm, curr_ccyymm)
    sql = mcrResl(sql)
    tgtSqliteTable = "MICMB_NFRCRTN"
    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:1063 & END:1063.###
'''SAS Comment:*THIS IS THE MI LEGACY DATABASE; '''
### SAS Source Code Line Numbers START:1064 & END:1074.###

# Sql Code Start and End Lines - 1064&1074 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS LGCY_NFRCRTN; CREATE TABLE LGCY_NFRCRTN AS SELECT A.* ,A.NFRCCNT AS PREV_NFRCCNT ,B.NFRCCNT AS
        CURR_RTNCNT FROM LGDB_{} A LEFT JOIN LGDB_{} B ON
        B.MBRNO = A.MBRNO""".format(prev_ccyymm, curr_ccyymm)
    sql = mcrResl(sql)
    tgtSqliteTable = "LGCY_NFRCRTN"
    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:1076 & END:1076.###
'''SAS Comment:*THIS IS THE MI IPM DATABASE; '''
### SAS Source Code Line Numbers START:1077 & END:1087.###

# Sql Code Start and End Lines - 1077&1087 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS MIIPM_NFRCRTN; CREATE TABLE MIIPM_NFRCRTN AS SELECT A.* ,A.NFRCCNT AS prev_nfrccnt ,B.NFRCCNT
        AS curr_rtncnt FROM MIIPMDB_{} A LEFT JOIN
        MIIPMDB_{} B ON B.MBRNO = A.MBRNO""".format(prev_ccyymm, curr_ccyymm)
    sql = mcrResl(sql)
    tgtSqliteTable = "MIIPM_NFRCRTN"
    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:1090 & END:1090.###
'''SAS Comment:*THIS IS THE NON-MI IPM DATABASES; '''
### SAS Source Code Line Numbers START:1091 & END:1102.###

# Sql Code Start and End Lines - 1091&1102 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS NONMI_NFRCRTN; CREATE TABLE NONMI_NFRCRTN AS SELECT A.* ,A.NFRCCNT AS prev_nfrccnt ,B.NFRCCNT
        AS curr_rtncnt FROM NONMI_{} A LEFT JOIN NONMI_{}
        B ON B.STATE = A.STATE AND B.SEQCLTNO = A.SEQCLTNO""".format(prev_ccyymm, curr_ccyymm)
    sql = mcrResl(sql)
    tgtSqliteTable = "NONMI_NFRCRTN"
    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:1105 & END:1105.###
'''SAS Comment:**************************************PART 4****************************************************; '''
### SAS Source Code Line Numbers START:1106 & END:1106.###
'''SAS Comment:*EXECUTE MACRO TO CREATE THE APPROPRIATE ROWS ON THE DATABASE                                   ; '''
### SAS Source Code Line Numbers START:1107 & END:1107.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
CRTDB('MICMB_NFRCRTN')

### SAS Source Code Line Numbers START:1108 & END:1108.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
CRTDB('MIIPM_NFRCRTN')

### SAS Source Code Line Numbers START:1109 & END:1109.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
CRTDB('LGCY_NFRCRTN')

### SAS Source Code Line Numbers START:1110 & END:1110.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
CRTDB('NONMI_NFRCRTN')

### SAS Source Code Line Numbers START:1112 & END:1112.###
'''SAS Comment:*NOTES:                                                                                        ; '''
### SAS Source Code Line Numbers START:1114 & END:1114.###
'''SAS Comment:*MICMB HAS MI LEGACY AND IPM COMBINED.                                                         ; '''
### SAS Source Code Line Numbers START:1116 & END:1116.###
'''SAS Comment:*ROOF AGE DOES NOT APPLY TO MI LEGACY. THEREFORE, ROOF AGE IS NOT A DIMENSION ON THE MICMB AND ; '''
### SAS Source Code Line Numbers START:1117 & END:1117.###
'''SAS Comment:*THE LGCY NFRCRTN DATABASES.                                                                   ; '''
### SAS Source Code Line Numbers START:1119 & END:1119.###
'''SAS Comment:*NONMI IS THE REGIONAL STATES.                                                                 ; '''
### SAS Source Code Line Numbers START:1121 & END:1121.###
'''SAS Comment:*IN THE ORIGINAL DESIGN, IT WAS THOUGHT THE MIGRATION INDICATOR WOULD BE A FACTOR. AT THIS TIME; '''
### SAS Source Code Line Numbers START:1122 & END:1122.###
'''SAS Comment:*IT IS NOT AND IS ALWAYS SET TO "N".                                                           ; '''
### SAS Source Code Line Numbers START:1124 & END:1124.###
'''SAS Comment:***END OF SECOND MACRO TO CREATE THE HOUSEHOLD RETENTION INFORCE DATABASE.                     ; '''
### SAS Source Code Line Numbers START:1127 & END:1127.###
'''SAS Comment:*There may be some dimensions that do not have counts for every valseq. This code will create   ; '''
### SAS Source Code Line Numbers START:1128 & END:1128.###
'''SAS Comment:*missing valseq numbers and zero out the counts. This is required for the report writer to put  ; '''
### SAS Source Code Line Numbers START:1129 & END:1129.###
'''SAS Comment:*the counts in the right column of the spreadsheet. There must be rows for each valseq for each ; '''
### SAS Source Code Line Numbers START:1130 & END:1130.###
'''SAS Comment:*dimension. The DIMVAL is not used in the report writer. Therefore, this field is left blank    ; '''
### SAS Source Code Line Numbers START:1131 & END:1131.###
'''SAS Comment:*when the row is created.                                                                       ; '''
### SAS Source Code Line Numbers START:1133 & END:1133.###
'''SAS Comment:**************************************PART 5****************************************************; '''
### SAS Source Code Line Numbers START:1134 & END:1136.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 1134&1136 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
#     sql = """DROP TABLE IF EXISTS NFRCOUT_sqlitesorted; CREATE TABLE NFRCOUT_sqlitesorted AS SELECT * FROM NFRCOUT ORDER BY
#         SYSTEM,STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE NFRCOUT;ALTER TABLE
#         NFRCOUT_sqlitesorted RENAME TO NFRCOUT"""
#     sql = mcrResl(sql)
#     tgtSqliteTable = "NFRCOUT_sqlitesorted"
#     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
# except:
#    e = sys.exc_info()[0]
#    logging.error('Table creation/update is failed.')
#    logging.error('Error - {}'.format(e))



### SAS Source Code Line Numbers START:1143 & END:1170.###
'''*********************************************************************************
Below python code is to execute SAS data step with BY varaible in python
*********************************************************************************'''

'''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
QTRND = &CURR_CCYYMM;RETAIN;FRSTDIM = FIRST.DIMSEQ;ELSE CNT = CNT + 1;SEQHOLD = VALSEQ;OUTPUT;IF SEQHOLD = CNT THEN RETURN;ELSE DO UNTIL (SEQHOLD = CNT);NFRCCNT = .;RTNCNT = .;RTNRATIO = .;DIMVAL = ' ';VALSEQ = CNT;OUTPUT;CNT = CNT +1;END;'''

# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source NFRCOUT data into datafram.
df = pd.read_sql_query("select * from NFRCOUT ", sqliteConnection)
# lowering all column names#Generate first and last temporary indicators in the given data frames.
df_lower_colNames(df)
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
    for val in range(1, d[index] + 1):
        if val not in new_df['valseq'].to_list():
            ungroup_df = new_df.reset_index()
            temp = ungroup_df.iloc[0]
            temp['nfrccnt1'] = np.nan
            temp['rtncnt'] = np.nan
            temp['rtnratio'] = np.nan
            temp['dimval'] = np.nan
            temp['valseq'] = val
            temp_df = temp_df.append(temp)
FXDNFRC = pd.concat([df, temp_df],ignore_index=True,sort=False)
FXDNFRC = df_remove_indexCols(FXDNFRC)
df_creation_logging(FXDNFRC, "FXDNFRC")
# Push results data frame to Sqlite DB
FXDNFRC.to_sql("FXDNFRC", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()

'''*******************************End of Merge Process**************************************************'''

### SAS Source Code Line Numbers START:1173 & END:1173.###
'''SAS Comment:**************************************PART 6****************************************************; '''
### SAS Source Code Line Numbers START:1174 & END:1174.###
'''SAS Comment:*Backup current database; '''
### SAS Source Code Line Numbers START:1175 & END:1177.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
sql = """select * from {}.{}.hhldrtn_homedb""".format(project_id, output_dataset)
df_hhldrtn_homedb = client.query(sql).to_dataframe()
df_hhldrtn_homedb.to_gbq(destination_table = output_dataset + '.' + 'hhldrtn_homedb_backup', project_id = project_id, if_exists='replace')
'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/data/hhldrtn_homedb.sas7bdat') as reader:
    df = reader.to_data_frame()
    df_lower_colNames(df, 'cnclrat_homedb')
    df['qtrnd'] = df.qtrnd.astype('Int64')
    df = df.loc[df.qtrnd < int(curr_ccyymm)]
    df.to_sql("NFRCOUT_HHLDRTN_HOMEDB",
                  con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
'''
# df = pd.read_csv(
    # "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn/data/NFRCOUT_HHLDRTN_HOMEDB.csv")
# # lowering all columns
# df_lower_colNames(df, 'NFRCOUT_HHLDRTN_HOMEDB')
# # logging info
# df_creation_logging(df, "NFRCOUT_HHLDRTN_HOMEDB")
# # putting into the sqliteDB
# df.to_sql("NFRCOUT_HHLDRTN_HOMEDB", con=sqliteConnection,
          # if_exists='replace', index=True)
# sqliteConnection.close()

# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
'''df = pd.read_sql_query(
    "select * from NFRCOUT_HHLDRTN_HOMEDB ", sqliteConnection)'''
df = df_hhldrtn_homedb
# handling data frame column case senstivity.#
df_lower_colNames(df, 'hhldrtn_homedb')
df['qtrnd'] = df.qtrnd.astype('Int64')
df = df.loc[df.qtrnd < int(curr_ccyymm)]
# Push results data frame to Sqlite DB
df.to_sql("NFRCOUT_HHLDRTN_HOMEDB_BACKUP",
          con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1179 & END:1179.###

'''WARNING SAS commnet block detected.
Any SAS steps within the block are converted to python code but commented.
# Sql Code Start and End Lines - 0&0 #
"""***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************"""
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
#    sql = """*Concatenate the database with the fixed inforce file for the current run;"""
#    sql = mcrResl(sql)
#    tgtSqliteTable = ""
#    procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
# except:
#   e = sys.exc_info()[0]
#   logging.error('Table creation/update is failed.')
#   logging.error('Error - {}'.format(e))
'''
### SAS Source Code Line Numbers START:1180 & END:1185.###
'''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
DROP FRSTDIM CNT SEQHOLD;'''
# Converting source NFRCOUT.HHLDRTN_HOMEDB data into datafram.
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
'''HHLDRTN_HOMEDB = pd.read_sql_query(
    "select * from NFRCOUT_HHLDRTN_HOMEDB ", sqliteConnection)'''
HHLDRTN_HOMEDB = df_hhldrtn_homedb
df_lower_colNames(HHLDRTN_HOMEDB, 'hhldrtn_homedb')
HHLDRTN_HOMEDB['qtrnd'] = HHLDRTN_HOMEDB.qtrnd.astype('Int64')
HHLDRTN_HOMEDB = HHLDRTN_HOMEDB.loc[df.qtrnd < int(curr_ccyymm)]
# Converting source FXDNFRC data into datafram.
FXDNFRC = pd.read_sql_query("select * from FXDNFRC ", sqliteConnection)
df_lower_colNames(FXDNFRC, 'FXDNFRC')
# Concatenate the source data frames
NFRCFLDB = pd.concat([HHLDRTN_HOMEDB, FXDNFRC], ignore_index=True, sort=False)
NFRCFLDB = NFRCFLDB.drop(columns = ['frstdim', 'cnt', 'seqhold'])
NFRCFLDB = df_remove_indexCols(NFRCFLDB)
df_creation_logging(NFRCFLDB, "NFRCFLDB")
# Push results data frame to Sqlite DB
NFRCFLDB.to_sql("NFRCFLDB", con=sqliteConnection, if_exists='replace')
del df_hhldrtn_homedb
sqliteConnection.close()

sqliteToBQ(output_tables)

### SAS Source Code Line Numbers START:1187 & END:1189.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 1187&1189 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
#     sql = """DROP TABLE IF EXISTS NFRCFLDB_sqlitesorted; CREATE TABLE NFRCFLDB_sqlitesorted AS SELECT * FROM NFRCFLDB ORDER BY
#         SYSTEM,PRODUCT,STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE NFRCFLDB;ALTER TABLE
#         NFRCFLDB_sqlitesorted RENAME TO NFRCFLDB"""
#     sql = mcrResl(sql)
#     tgtSqliteTable = "NFRCFLDB_sqlitesorted"
#     procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
# except:
#    e = sys.exc_info()[0]
#    logging.error('Table creation/update is failed.')
#    logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:1191 & END:1191.###
'''SAS Comment:*Output the final database; '''
### SAS Source Code Line Numbers START:1192 & END:1194.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from NFRCFLDB ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NFRCFLDB')
df = df_remove_indexCols(df)
logging.info(
    "NFRCOUT_HHLDRTN_HOMEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("NFRCOUT_HHLDRTN_HOMEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1198 & END:1198.###
'''SAS Comment:*THE FOLLOWING WAS ADDED FOR BALANCING PURPOSES ONLY; '''
### SAS Source Code Line Numbers START:1200 & END:1202.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from MICMB_NFRCRTN ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'MICMB_NFRCRTN')
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1204 & END:1204.###
'''SAS Comment:*MI IPM/LEGACY COMBINED TOTAL RETAINED; '''
### SAS Source Code Line Numbers START:1205 & END:1208.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from MICMB_NFRCRTN WHERE curr_rtncnt > 0", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'MICMB_NFRCRTN')
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1211 & END:1211.###
'''SAS Comment:*REGIONAL STATES INFORCE BREAKDOWN; '''
### SAS Source Code Line Numbers START:1212 & END:1215.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'IA'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1217 & END:1220.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'IL'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')

df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1222 & END:1225.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'IN'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1227 & END:1230.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'KY'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')

df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1232 & END:1235.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'MN'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1237 & END:1240.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'OH'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')

df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1242 & END:1245.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'WI'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1247 & END:1250.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN where state = 'WV'", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1253 & END:1253.###
'''SAS Comment:*REGIONAL STATES TOTAL RETAINED; '''
### SAS Source Code Line Numbers START:1254 & END:1258.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from NONMI_NFRCRTN WHERE curr_rtncnt > 0", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'NONMI_NFRCRTN')
# Keep columns in the taget df data in datafram.
df = df[['state', 'seqpolno', 'form', 'rfage', 'hmage']]
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1261 & END:1261.###
'''SAS Comment:*MI IPM TOTAL RETAINED; '''
### SAS Source Code Line Numbers START:1262 & END:1266.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from MIIPM_NFRCRTN where curr_rtncnt > 0", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'MIIPM_NFRCRTN')
# Keep columns in the taget df data in datafram.
df = df[['state', 'seqpolno', 'form', 'rfage', 'hmage']]
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1269 & END:1269.###
'''SAS Comment:*LEGACY TOTAL RETAINED; '''
### SAS Source Code Line Numbers START:1270 & END:1274.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from LGCY_NFRCRTN where curr_rtncnt > 0", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'LGCY_NFRCRTN')
# Keep columns in the taget df data in datafram.
df = df[['seqpolno', 'form', 'hmage', 'rfage']]
df = df_remove_indexCols(df)
logging.info("STATEDB created successfully with {} records".format(len(df)))
# Push results data frame to Sqlite DB
df.to_sql("STATEDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1280 & END:1280.###
'''SAS Comment:*** Writes SAS Log to a text file ***; '''
### SAS Source Code Line Numbers START:1281 & END:1281.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
%SaveLog(NAME=RetentionRatio_HomeDB,DIR=T:\Shared\Acturial\BISLogs\RetentionRatio\)
'''

### SAS Source Code Line Numbers START:1283 & END:1283.###
'''SAS Comment:*** Close the stored compiled macro catalog and clear the previous libref  ***; '''
### SAS Source Code Line Numbers START:1286 & END:1286.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME _ALL_ CLEAR;
'''

### SAS Source Code Line Numbers START:1288 & END:1293.###

'''WARNING SAS commnet block detected.
Any SAS steps within the block are converted to python code but commented.
# Sql Code Start and End Lines - 0&0 #
"""***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************"""
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """/* TITLE 'UNKNOWN AGENT TYPES'; PROC PRINT DATA=WORK.UNKAGTDB */"""
    sql = mcrResl(sql)
    tgtSqliteTable = ""
    procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
except:
   e = sys.exc_info()[0]
   logging.error('Table creation/update is failed.')
   logging.error('Error - {}'.format(e))
'''
