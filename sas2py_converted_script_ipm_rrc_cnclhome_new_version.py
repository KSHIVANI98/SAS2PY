# -*- coding: utf-8 -*-
r'''
Created on: Mon 14 Dec 20 18:09:19

Author: SAS2PY Code Conversion Tool

SAS Input File: gw_ipm_cnclhome
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
import itertools
import sys
import re
import sqlite3
import pandas as pd
import psutil
import os
import gc
import dateutil.parser
import datetime
from functools import reduce, partial
import numpy as np
from sas7bdat import SAS7BDAT
import warnings
warnings.filterwarnings("ignore")
from google.cloud import bigquery

# Seting up logging info #

config_file = None
yaml_file = None

try:
    config_file = open('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/config.yaml', 'r+')
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

log_file_name = os.path.basename(__file__).lstrip('sas2py_converted_script_')[:-3]
logging.basicConfig(filename= yaml_file['logs'] + os.sep + log_file_name + '.log',level=logging.INFO,format='%(asctime)s - '+ log_file_name.upper()+' %(levelname)s - %(message)s')

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


def procSql_standard_Exec(sqliteDb, sql, tgtSqliteTable):
    if '_sqlitesorted' in tgtSqliteTable:
        tgtSqliteTable = tgtSqliteTable.replace('_sqlitesorted', '')
    try:
        sqliteConnection = sqlite3.connect(sqliteDb)
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
'''


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
    # tabNm = [x for x in globals() if globals()[x] is dfName][0]
    logging.info('Table {} created successfully with {} records and {} columns.'.format(
        tablename.upper(), rows, cols))


'''
def df_creation_logging(dfName):
    rows = len(dfName.index)
    cols = len(dfName.columns)
    tabNm = [x for x in globals() if globals()[x] is dfName][0]
    # logging data frame creation
    logging.info('Table {} created successfully with {} records and {} columns.'.format(
        tabNm.upper(), rows, cols))'''
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
    sql = "select * from cncldb"
    logging.info('Getting table cncldb from sqlitedb')	
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
'''SAS Comment:*     NAME:  MONTHLY, HOME POLICY CANCELLATION RATIO                   ; '''
### SAS Source Code Line Numbers START:5 & END:5.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:6 & END:6.###
'''SAS Comment:*   SYSTEM:  LEGACY & IPM                                              ; '''
### SAS Source Code Line Numbers START:7 & END:7.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:8 & END:8.###
'''SAS Comment:* FUNCTION:  THIS PROGRAM PROCESSES THE CURRENT AND PRIOR MONTH HOME   ; '''
### SAS Source Code Line Numbers START:9 & END:9.###
'''SAS Comment:*            INFORCE FILES AND CALCULATES THE CANCELLATION RATIO       ; '''
### SAS Source Code Line Numbers START:10 & END:10.###
'''SAS Comment:*            ONLY FOR POLICIES THAT ARE NOT RENEWING. THIS PROCESS     ; '''
### SAS Source Code Line Numbers START:11 & END:11.###
'''SAS Comment:*            IS THE OPPOSITE OF THE RENEWAL RETENTION PROCESS.         ; '''
### SAS Source Code Line Numbers START:12 & END:12.###
'''SAS Comment:*            THE INFORMATION IS LOADED TO A SAS DATABASE. THE          ; '''
### SAS Source Code Line Numbers START:13 & END:13.###
'''SAS Comment:*            DATABASE IS INPUT TO A SEPARATE PROGRAM TO THE EXCEL      ; '''
### SAS Source Code Line Numbers START:14 & END:14.###
'''SAS Comment:*            SPREADSHEET REPORTS FOR EACH STATE.                       ; '''
### SAS Source Code Line Numbers START:15 & END:15.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:16 & END:16.###
'''SAS Comment:*            ANOTHER PROGRAM WILL PRODUCE SEPARATE SPREADSHEETS FOR    ; '''
### SAS Source Code Line Numbers START:17 & END:17.###
'''SAS Comment:*            VARIOUS DIMENSIONS SUCH AS PREMIER AND MUTLIPRODUCT       ; '''
### SAS Source Code Line Numbers START:18 & END:18.###
'''SAS Comment:*            INDICATORS, HOME AGE, ROOF AGE, FORM CODE, AND MEMBERSHIP ; '''
### SAS Source Code Line Numbers START:19 & END:19.###
'''SAS Comment:*            INDICATOR. EACH OF THESE DIMENSIONS ARE ASSIGNED A        ; '''
### SAS Source Code Line Numbers START:20 & END:20.###
'''SAS Comment:*            SEQUENCE NUMBER ON THE DATABASE TO REPRESENT THE ORDER IN ; '''
### SAS Source Code Line Numbers START:21 & END:21.###
'''SAS Comment:*            WHICH THEY ARE PRESENTED IN THE EXCEL SPREADSHEET FOR EACH; '''
### SAS Source Code Line Numbers START:22 & END:22.###
'''SAS Comment:*            PARTICULAR STATE. EACH SHEET HAS 3 SEPARATE AREAS UNDER   ; '''
### SAS Source Code Line Numbers START:23 & END:23.###
'''SAS Comment:*            WHICH INFORCE/RETENTION COUNTS AND RATIO PERCENTAGE IS    ; '''
### SAS Source Code Line Numbers START:24 & END:24.###
'''SAS Comment:*            REPORTED - PRIOR MONTH NONRENEWAL INFORCE COUNT, THE      ; '''
### SAS Source Code Line Numbers START:25 & END:25.###
'''SAS Comment:*            CURRENT MONTH NONRENEWAL INFORCE RETENTION COUNT, AND THE ; '''
### SAS Source Code Line Numbers START:26 & END:26.###
'''SAS Comment:*            CALCULATED CANCELLATION RATIO. THE PRIOR MONTH IS THE     ; '''
### SAS Source Code Line Numbers START:27 & END:27.###
'''SAS Comment:*            STANDARD FOR COMPARISON.                                  ; '''
### SAS Source Code Line Numbers START:28 & END:28.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:29 & END:29.###
'''SAS Comment:*            EACH DIMENSION HAS VALUES/RANGES REPORTED ACROSS ALL THREE; '''
### SAS Source Code Line Numbers START:30 & END:30.###
'''SAS Comment:*            AREAS OF THE REPORT. SEE THE EXAMPLE BELOW FOR PREMIER    ; '''
### SAS Source Code Line Numbers START:31 & END:31.###
'''SAS Comment:*            INDICATOR - DIMENSION SEQUENCE = 1:                       ; '''
### SAS Source Code Line Numbers START:32 & END:32.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:33 & END:33.###
'''SAS Comment:* PRIOR INFORCE          CURRENT RETENTION         RETENTION RATIO     ; '''
### SAS Source Code Line Numbers START:34 & END:34.###
'''SAS Comment:*LOW   MED  HIGH         LOW   MED  HIGH           LOW   MED    HIGH   ; '''
### SAS Source Code Line Numbers START:35 & END:35.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:36 & END:36.###
'''SAS Comment:*            THE DATABASE RETAINS THE INFORCE AND RETENTION COUNTS AND ; '''
### SAS Source Code Line Numbers START:37 & END:37.###
'''SAS Comment:*            CALCULATED RATIO FOR EACH MONTH, SYSTEM, PRODUCT, AGENT   ; '''
### SAS Source Code Line Numbers START:38 & END:38.###
'''SAS Comment:*            TYPE, STATE, MIGR_IND, DIMENSION, AND DIMENSION VALUE. THE; '''
### SAS Source Code Line Numbers START:39 & END:39.###
'''SAS Comment:*            DIMENSION VALUES ARE THE REPORT COLUMNS. THERE IS A       ; '''
### SAS Source Code Line Numbers START:40 & END:40.###
'''SAS Comment:*            SEQUENCE NUMBER ASSOCIATED WITH EACH DIMENSION AND        ; '''
### SAS Source Code Line Numbers START:41 & END:41.###
'''SAS Comment:*            DIMENSION VALUE FOR REPORTING PURPOSES. THE DATABASE      ; '''
### SAS Source Code Line Numbers START:42 & END:42.###
'''SAS Comment:*            STRUCTURE IS COLUMNAR ALSO KNOWN AS LONG. THE REPORT      ; '''
### SAS Source Code Line Numbers START:43 & END:43.###
'''SAS Comment:*            PROGRAM WILL CONVERT THE COLUMNAR (LONG) STRUCTURE TO A   ; '''
### SAS Source Code Line Numbers START:44 & END:44.###
'''SAS Comment:*            ROW (WIDE)STRUCTURE.                                      ; '''
### SAS Source Code Line Numbers START:45 & END:45.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:46 & END:46.###
'''SAS Comment:*    INPUT:  MONTH END DATE CARD (IUT015M)                             ; '''
### SAS Source Code Line Numbers START:47 & END:47.###
'''SAS Comment:*            LEGACY HOME INFORCE CONVERSION (created in renewals)      ; '''
### SAS Source Code Line Numbers START:48 & END:48.###
'''SAS Comment:*            IPM HOME INFORCE CONVERSION (created in renewal process)  ; '''
### SAS Source Code Line Numbers START:49 & END:49.###
'''SAS Comment:*            CNCLRAT_HOMEDB                                            ; '''
### SAS Source Code Line Numbers START:50 & END:50.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:51 & END:51.###
'''SAS Comment:*   OUTPUT:  CNCLRAT_HOMEDB - MONTHLY CANCELLATION RATIO DB            ; '''
### SAS Source Code Line Numbers START:52 & END:52.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:53 & END:53.###
'''SAS Comment:*DEPENDENCIES: MAINFRAME LEGACY AND IPM INFORCE FTP                    ; '''
### SAS Source Code Line Numbers START:54 & END:54.###
'''SAS Comment:*              MAINFRAME DATECARD (IUT015M) FTP                        ; '''
### SAS Source Code Line Numbers START:55 & END:55.###
'''SAS Comment:*              RENEWAL RATIO MUST RUN TO CREATE CONVERSION INPUT FILES ; '''
### SAS Source Code Line Numbers START:56 & END:56.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:57 & END:57.###
'''SAS Comment:*   BALANCING: - BALANCE CURRENT AND PRIOR LEGACY/IPM INFORCE          ; '''
### SAS Source Code Line Numbers START:58 & END:58.###
'''SAS Comment:*                LEGACY ONLY, IPM ONLY AND REGIONAL                    ; '''
### SAS Source Code Line Numbers START:59 & END:59.###
'''SAS Comment:*              - BALANCE BREAKDOWNS TO CREATION OF SEPARATE            ; '''
### SAS Source Code Line Numbers START:60 & END:60.###
'''SAS Comment:*                DATABASES                                             ; '''
### SAS Source Code Line Numbers START:61 & END:61.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:62 & END:62.###
'''SAS Comment:*              FOR MICHIGAN, LEGACY PLUS IPM INFORCE WILL BALANCE TO   ; '''
### SAS Source Code Line Numbers START:63 & END:63.###
'''SAS Comment:*              LEGACY AND IPM COMBINED. HOWEVER, THE RETENTION WILL NOT; '''
### SAS Source Code Line Numbers START:64 & END:64.###
'''SAS Comment:*              THE REASON IS A POLICY COULD HAVE MIGRATED FROM LEGACY  ; '''
### SAS Source Code Line Numbers START:65 & END:65.###
'''SAS Comment:*              TO IPM. SO, IT WON'T BE CAPTURED UNDER RETAINED ON THE  ; '''
### SAS Source Code Line Numbers START:66 & END:66.###
'''SAS Comment:*              THE REPORTS FOR THE SEPARATE SYSTEMS, BUT IT WILL BE    ; '''
### SAS Source Code Line Numbers START:67 & END:67.###
'''SAS Comment:*              CAPTURED ON THE COMBINED REPORT.                        ; '''
### SAS Source Code Line Numbers START:68 & END:68.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:69 & END:69.###
'''SAS Comment:*    NOTES:  THIS JOB CREATED FOR P&PD.                                ; '''
### SAS Source Code Line Numbers START:70 & END:70.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:71 & END:71.###
'''SAS Comment:*            CANNOT RUN THIS PROGRAM MULTIPLE TIMES WITHOUT DELETING   ; '''
### SAS Source Code Line Numbers START:72 & END:72.###
'''SAS Comment:*            ALL WORK FILES. THE EASIEST WAY TO DO THIS IS TO QUIT SAS ; '''
### SAS Source Code Line Numbers START:73 & END:73.###
'''SAS Comment:*            AND GET BACK IN AND RERUN. THIS PROGRAM CREATES A WORK    ; '''
### SAS Source Code Line Numbers START:74 & END:74.###
'''SAS Comment:*            FILE FOR WHICH DATA IS APPENDED TO THROUGH MULTIPLE       ; '''
### SAS Source Code Line Numbers START:75 & END:75.###
'''SAS Comment:*            ITERATIONS OF A MACRO.                                    ; '''
### SAS Source Code Line Numbers START:76 & END:76.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:77 & END:77.###
'''SAS Comment:*            REMEMBER THIS PROCESS CREATES AN ACTUAL DATABASE. THE     ; '''
### SAS Source Code Line Numbers START:78 & END:78.###
'''SAS Comment:*            DATABASE IS AUTOMATICALLY BACKED UP IN THIS PROCESS.      ; '''
### SAS Source Code Line Numbers START:79 & END:79.###
'''SAS Comment:*            THIS MUST BE TAKEN INTO CONSIDERATION FOR RERUNS.         ; '''
### SAS Source Code Line Numbers START:80 & END:80.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:81 & END:81.###
'''SAS Comment:*            THIS PROGRAM DOES NOT HAVE ANY MANUAL PARMS TO UPDATE.    ; '''
### SAS Source Code Line Numbers START:82 & END:82.###
'''SAS Comment:*            THE DATECARD FOR THE CURRENT MONTH IS FTP'D FROM THE      ; '''
### SAS Source Code Line Numbers START:83 & END:83.###
'''SAS Comment:*            MAINFRAME TO DIRECTORY, T\Shared\Acturial\FTPINBOUND.     ; '''
### SAS Source Code Line Numbers START:84 & END:84.###
'''SAS Comment:*            THE PROGRAM READS THE CURRENT MONTH DATE AND CALCULATES   ; '''
### SAS Source Code Line Numbers START:85 & END:85.###
'''SAS Comment:*            DATE FOR THE PRIOR MONTH. THESE DATES ARE THEN USED TO    ; '''
### SAS Source Code Line Numbers START:86 & END:86.###
'''SAS Comment:*            ACCESS DATE QUALIFIED FILES. THE CURRENT MONTH END FILES  ; '''
### SAS Source Code Line Numbers START:87 & END:87.###
'''SAS Comment:*            ARE USED TO CALCULATE RETENTION BASED ON THE INFORCE      ; '''
### SAS Source Code Line Numbers START:88 & END:88.###
'''SAS Comment:*            COUNTS FOR THE PRIOR MONTH.                               ; '''
### SAS Source Code Line Numbers START:89 & END:89.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:90 & END:90.###
'''SAS Comment:*            THE LEGACY AND IPM HOME INFORCE FILES FOR THE CURRENT     ; '''
### SAS Source Code Line Numbers START:91 & END:91.###
'''SAS Comment:*            QUARTER ARE FTP'D FROM THE MAINFRAME TO THE FTPINBOUND    ; '''
### SAS Source Code Line Numbers START:92 & END:92.###
'''SAS Comment:*            DIRECTORY IN BINARY FORMAT NAMED, LGCYBNRY AND IPMBNRY    ; '''
### SAS Source Code Line Numbers START:93 & END:93.###
'''SAS Comment:*            RESPECTIVELY. THIS PROGRAM EXECUTES THE CIMPORT PROCEDURE ; '''
### SAS Source Code Line Numbers START:94 & END:94.###
'''SAS Comment:*            TO CREATE SAS FILES FROM THE BINARY TEXT FILES. THESE SAS ; '''
### SAS Source Code Line Numbers START:95 & END:95.###
'''SAS Comment:*            FILES ARE CALLED LGHMNFRC (LEGACY) AND FINALOUT2 (IPM)    ; '''
### SAS Source Code Line Numbers START:96 & END:96.###
'''SAS Comment:*            BASED ON THE NAMES GIVEN IN THE MAINFRAME CODE. LGHMNFRC  ; '''
### SAS Source Code Line Numbers START:97 & END:97.###
'''SAS Comment:*            AND FINALOUT2 ARE THEN INPUT & MANIPULATED TO CREATE DATE ; '''
### SAS Source Code Line Numbers START:98 & END:98.###
'''SAS Comment:*            QUALIFIED SAS FILES FOR THE CURRENT QUARTER TO BE USED TO ; '''
### SAS Source Code Line Numbers START:99 & END:99.###
'''SAS Comment:*            CALCULATE RETENTION. THE FILE CREATED THIS RUN WILL BE    ; '''
### SAS Source Code Line Numbers START:100 & END:100.###
'''SAS Comment:*            USED IN THE SAME PERIOD NEXT YEAR AS THE INFORCE FILE.    ; '''
### SAS Source Code Line Numbers START:101 & END:101.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:102 & END:102.###
'''SAS Comment:*            THE PROGRAM IS STRUCTURED TO DO THE FOLLOWING:            ; '''
### SAS Source Code Line Numbers START:103 & END:103.###
'''SAS Comment:* PART 1 - READ MICHIGAN AND REGIONAL CONVERSION FILES FOR THE         ; '''
### SAS Source Code Line Numbers START:104 & END:104.###
'''SAS Comment:*          CURRENT AND PRIOR MONTH. COMPARE FILES TO DETERMINE         ; '''
### SAS Source Code Line Numbers START:105 & END:105.###
'''SAS Comment:*          POLICIES RETAINED.                                          ; '''
### SAS Source Code Line Numbers START:106 & END:106.###
'''SAS Comment:* PART 2 - DETERMINE DIMENSION VALUES AND CREATE SUMMARY ROWS          ; '''
### SAS Source Code Line Numbers START:107 & END:107.###
'''SAS Comment:* PART 3 - CREATE ROWS ON THE TEMPORARY DATABASE FOR ANY MISSING       ; '''
### SAS Source Code Line Numbers START:108 & END:108.###
'''SAS Comment:*          DIMENSIONS. THIS IS NECESSARY FOR FINAL REPORTING.          ; '''
### SAS Source Code Line Numbers START:109 & END:109.###
'''SAS Comment:* PART 4 - BACKUP THE ACTUAL DATABASE AND CREATE THE NEW. THE CURRENT  ; '''
### SAS Source Code Line Numbers START:110 & END:110.###
'''SAS Comment:*          MONTH DATA IS ADDED TO THE EXISTING DATABASE.               ; '''
### SAS Source Code Line Numbers START:111 & END:111.###
'''SAS Comment:* PART 5 - PRODUCE BALANCING COUNTS.                                   ; '''
### SAS Source Code Line Numbers START:112 & END:112.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:113 & END:113.###
'''SAS Comment:*            THE FINAL OUTPUT IS A SAS DATABASE, CNCLRAT_HOMEDB. THIS  ; '''
### SAS Source Code Line Numbers START:114 & END:114.###
'''SAS Comment:*            DATABASE IS INPUT TO THE REPORT WRITER PROGRAM TO PRODUCE ; '''
### SAS Source Code Line Numbers START:115 & END:115.###
'''SAS Comment:*            THE FINAL EXCEL SPREADSHEETS AND TO THE GRAPH PROGRAM.    ; '''
### SAS Source Code Line Numbers START:116 & END:116.###
'''SAS Comment:*            THIS PROGRAM CREATES A BACKUP OF THE DATABASE EACH RUN.   ; '''
### SAS Source Code Line Numbers START:117 & END:117.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:118 & END:118.###
'''SAS Comment:************************ REVISION LOG *********************************; '''
### SAS Source Code Line Numbers START:119 & END:119.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:120 & END:120.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:121 & END:121.###
'''SAS Comment:*   DATE        INIT     DESCRIPTION OF CHANGE                         ; '''
### SAS Source Code Line Numbers START:122 & END:122.###
'''SAS Comment:*   10/14       PLT      NEW PROGRAM                                   ; '''
### SAS Source Code Line Numbers START:123 & END:123.###
'''SAS Comment:*   03/15       PLT      Breakout EA agent type.                       ; '''
### SAS Source Code Line Numbers START:124 & END:124.###
'''SAS Comment:*   09/15       PLT      New premier ranges.                           ; '''
### SAS Source Code Line Numbers START:125 & END:125.###
'''SAS Comment:*                      - Changes to renewal processing. Now using GL   ; '''
### SAS Source Code Line Numbers START:126 & END:126.###
'''SAS Comment:*                        dates. This change impacts non-renewing.      ; '''
### SAS Source Code Line Numbers START:127 & END:127.###
'''SAS Comment:*   01/16       PLT    - New prior claim categories.                   ; '''
### SAS Source Code Line Numbers START:128 & END:128.###
'''SAS Comment:*                      - Correct issue where entries for agent types   ; '''
### SAS Source Code Line Numbers START:129 & END:129.###
'''SAS Comment:*                        SSA & EA were not created.                    ; '''
### SAS Source Code Line Numbers START:130 & END:130.###
'''SAS Comment:*   02/18       PLT    - Breakout MSC & HB, but keep MSC/HB as well    ; '''
### SAS Source Code Line Numbers START:131 & END:131.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:132 & END:132.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
OPTIONS MPRINT SYMBOLGEN MSTORED SASMSTORE=StoreMac missing='.';
'''

### SAS Source Code Line Numbers START:133 & END:133.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME StoreMac 'T:\Shared\Acturial\BISMacros\SAS';
'''

### SAS Source Code Line Numbers START:134 & END:134.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME CNVFILE 'T:\Shared\Acturial\BISProd\CommonDataSources\Home\Inforce\';
'''

### SAS Source Code Line Numbers START:135 & END:135.###
'''SAS Comment:*LIBNAME CNVFILE 'T:\Shared\Acturial\BISProd\RenewalRatio\InputOutput\'; '''
### SAS Source Code Line Numbers START:137 & END:137.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME CNCLDB 'T:\Shared\Acturial\BISProd\CancelRatio\InputOutput\';
'''

### SAS Source Code Line Numbers START:140 & END:140.###
'''SAS Comment:*FILENAME IUT015M 'T:\Shared\Acturial\BISProd\CommonDataSources\Datecards\IUT015M_1508.TXT'; '''
### SAS Source Code Line Numbers START:142 & END:304.###

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


### SAS Source Code Line Numbers START:305 & END:305.###
'''SAS Comment:************************************END OF PROC FORMAT******************************************; '''
### SAS Source Code Line Numbers START:307 & END:307.###
'''SAS Comment:************************************END OF PROC FORMAT******************************************; '''
### SAS Source Code Line Numbers START:309 & END:309.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:310 & END:310.###
'''SAS Comment:****PROCESSING BEGINS HERE****PROCESSING BEGINS HERE****PROCESSING BEGINS HERE****              ; '''
### SAS Source Code Line Numbers START:311 & END:311.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:312 & END:312.###
'''SAS Comment:*IF THE CONVERSION FILES ARE GOOD AND IT IS NECESSARY TO RERUN TO RECREATE THE DATABASE,        ; '''
### SAS Source Code Line Numbers START:313 & END:313.###
'''SAS Comment:*COMMENT OUT EVERYTHING IN PART 1 AFTER READING IN THE DATECARD. THE DATECARD IS USED IN THE    ; '''
### SAS Source Code Line Numbers START:314 & END:314.###
'''SAS Comment:*OTHER SECTIONS OF THE CODE.                                                                    ; '''
### SAS Source Code Line Numbers START:317 & END:317.###
'''SAS Comment:***THIS SECTION PROCESSES THE CURRENT LEGACY AND IPM INFORCE FILES ONLY AND CREATES DATE        ; '''
### SAS Source Code Line Numbers START:318 & END:318.###
'''SAS Comment:***QUALIFIED FILES OF EACH. THE DATE QUALIFIED FILES ARE PROCESSED IN PART II AND ARE COMPARED  ; '''
### SAS Source Code Line Numbers START:319 & END:319.###
'''SAS Comment:***TO THE PRIOR MONTH DATE QUALIFIED FILES TO DETERMINE THE RETENTION.                          ; '''
### SAS Source Code Line Numbers START:321 & END:321.###
'''SAS Comment:*CURRENT QUARTER END DATE CARD; '''
### SAS Source Code Line Numbers START:323 & END:323.###
'''SAS Comment:*THIS IS WHAT THE INPUT DATECARD LOOKS LIKE; '''
### SAS Source Code Line Numbers START:324 & END:324.###
'''SAS Comment:*IUT015  2013033020130628; '''
### SAS Source Code Line Numbers START:325 & END:366.###
"""ERROR: Unable to convert the below SAS block/code into python """
"""
data _null_;
 infile iut015m;
 input @17 mnthnd_ccyymm 6. @17 mnthnd_ccyy 4. @21 mnthnd_mm 2. @17 mnthnd_ccyymmdd YYMMDD10. ;
 ***DEFINE CURRDTE AND PREVDTE AS INTEGER FOR COMPARISON LATER ;
 call symput('curr_ccyymm',put(mnthnd_ccyymm,z6.));
 call symput('curr_ccyy',put(mnthnd_ccyy,z4.));
 call symput('curr_mm',put(mnthnd_mm,z2.));
 *format the month end date as date9 (i.e. 30jun2014) for use in the intnx function;
 format mnthnd date9.;
 mnthnd = mnthnd_ccyymmdd;
 put mnthnd=;
 *calculate date one month prior to current month end date (CCYYMM);
 amnthago = intnx('month',mnthnd,-1);
 *nxtmnth = sasdate;
 *convert the full date into CCYYMM to create a global variable;
 chardte = put(amnthago,yymmn6.);
 numdte = input(chardte,6.);
 put numdte= chardte=;
 call symput('amnthago',put(numdte,z6.));
 *calculate date two months prior to current month end (CCYYMM);
 twomnthsago = intnx('month',mnthnd,-2);
 *convert the full date into CCYYMM to create a global variable;
 chardte = put(twomnthsago,yymmn6.);
 numdte = input(chardte,6.);
 put numdte= chardte=;
 call symput('twomnthsago',put(numdte,z6.));
 run;
"""

file_str = open(
    r"/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/IUT015M.TXT").read()
mnthnd_ccyymm = file_str[16:22]
mnthnd_ccyy = file_str[16:20]
mnthnd_mm = file_str[20:22]
mnthnd_ccyymmdd = file_str[16:24]


global curr_ccyymm, curr_ccyy, curr_mm

curr_ccyymm = datetime.datetime.strptime(
    mnthnd_ccyymm, "%Y%m").strftime("%Y%m").strip()
curr_ccyy = datetime.datetime.strptime(
    mnthnd_ccyy, "%Y").strftime("%Y").strip()
curr_mm = datetime.datetime.strptime(mnthnd_mm, "%m").strftime("%m").strip()
# mnthnd = pd.to_datetime(mnthnd_ccyymmdd)
mnthnd = datetime.datetime.strptime(mnthnd_ccyymmdd, '%Y%m%d')
a_month = dateutil.relativedelta.relativedelta(months=1)
result = mnthnd-a_month
amnthago = result.replace(day=1)
# chardte = pd.to_datetime(amnthago,"%Y%m")
chardte = amnthago.strftime('%Y%m')
numdte = chardte

amnthago = datetime.datetime.strptime(numdte, "%Y%m").strftime("%Y%m").strip()

# twomnthsago = intnx('month',mnthnd,-2)
two_month = dateutil.relativedelta.relativedelta(months=2)
two_result = mnthnd-two_month
twomnthsago = two_result.replace(day=1)
# chardte = pd.to_datetime(twomnthsago,"%Y%m")
chardte = twomnthsago.strftime('%Y%m')
numdte = chardte
twomnthsago = datetime.datetime.strptime(
    numdte, "%Y%m").strftime("%Y%m").strip()
# amnthago=202102

sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/mihmcnv_{}.sas7bdat'.format(curr_ccyymm)) as reader:
df = reader.to_data_frame()
df.to_sql("mihmcnv_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace')
sqliteConnection.close()


sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/rgnhmcnv_{}.sas7bdat'.format(curr_ccyymm)) as reader:
df = reader.to_data_frame()
df.to_sql("rgnhmcnv_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace')
sqliteConnection.close()


sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/mihmcnv_{}.sas7bdat'.format(amnthago)) as reader:
df = reader.to_data_frame()
df.to_sql("mihmcnv_{}".format(amnthago),
              con=sqliteConnection, if_exists='replace')
sqliteConnection.close()


sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/rgnhmcnv_{}.sas7bdat'.format(amnthago)) as reader:
df = reader.to_data_frame()
df.to_sql("rgnhmcnv_{}".format(amnthago),
              con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

# TODO - Make sure file names are dynamic
# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_csv(
    # '/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/mihmcnv_{}.csv'.format(curr_ccyymm))
# df.to_sql("mihmcnv_{}".format(curr_ccyymm),
          # con=sqliteConnection, if_exists='replace')
# sqliteConnection.close()
# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_csv(
    # '/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/rgnhmcnv_{}.csv'.format(curr_ccyymm))
# df.to_sql("rgnhmcnv_{}".format(curr_ccyymm),
          # con=sqliteConnection, if_exists='replace')
# sqliteConnection.close()

# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_csv(
    # '/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/mihmcnv_{}.csv'.format(amnthago))
# df.to_sql("mihmcnv_{}".format(amnthago),
          # con=sqliteConnection, if_exists='replace')
# sqliteConnection.close()
# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_csv(
    # '/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/rgnhmcnv_{}.csv'.format(amnthago))
# df.to_sql("rgnhmcnv_{}".format(amnthago),
          # con=sqliteConnection, if_exists='replace')
# sqliteConnection.close()

### SAS Source Code Line Numbers START:368 & END:368.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:369 & END:369.###
'''SAS Comment:*Process prior month inforce file. Set counts for both total inforce and non renewing.          ; '''
### SAS Source Code Line Numbers START:370 & END:370.###
'''SAS Comment:*Compare the file to the current month to determine retention.                                  ; '''
### SAS Source Code Line Numbers START:371 & END:371.###
'''SAS Comment:*NFRCCNT1 = TOTAL INFORCE and NFRCNT2 = NON-RENEWING INFORCE (determined by rnwgcnt = 0)        ; '''
### SAS Source Code Line Numbers START:372 & END:372.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:373 & END:373.###
'''SAS Comment:*MICHIGAN; '''
### SAS Source Code Line Numbers START:374 & END:374.###
'''SAS Comment:*Create a Michigan work database of inforce policies from prior month.             ; '''
### SAS Source Code Line Numbers START:375 & END:383.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from mihmcnv_{}".format(amnthago), sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'mihmcnv'))
df['nfrccnt1'] = df.nfrccnt
df['system'] = df.syscd
df['mnthnd'] = curr_ccyymm
# Push results data frame to Sqlite DB
logging.info("mi_prinfrc created successfully with {} records".format(len(df)))
df.to_sql("mi_prinfrc", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:385 & END:385.###
'''SAS Comment:*Read MI current inforce; '''
### SAS Source Code Line Numbers START:386 & END:389.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
********************************************************************************* '''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from mihmcnv_{}".format(curr_ccyymm), sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'mihmcnv')
# Push results data frame to Sqlite DB
logging.info("mi_currnfrc created successfully with {} records".format(len(df)))
df.to_sql("mi_currnfrc", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:391 & END:391.###
'''SAS Comment:*Compare MI inforce from a month ago to current inforce; '''
### SAS Source Code Line Numbers START:392 & END:402.###

# Sql Code Start and End Lines - 392&402 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
****************************************************** '''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS mi_nfrcrtn;create table mi_nfrcrtn as select a.* ,b.nfrccnt as rtncnt1 from mi_prinfrc
        a left join mi_currnfrc b on b.syscd = a.syscd and b.seqpolno = a.seqpolno"""
    sql = mcrResl(sql)
    tgtSqliteTable = "mi_nfrcrtn"
    procSql_standard_Exec(sqliteDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:405 & END:405.###
'''SAS Comment:*REGIONAL; '''
### SAS Source Code Line Numbers START:406 & END:406.###
'''SAS Comment:*Create a regional work database of inforce policies from prior month.             ; '''
### SAS Source Code Line Numbers START:407 & END:415.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from rgnhmcnv_{}".format(amnthago), sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'rgnhmcnv')
df['nfrccnt1'] = df.nfrccnt
df['system'] = df.syscd
df['mnthnd'] = curr_ccyymm
# Push results data frame to Sqlite DB
logging.info(
    "rgnl_prinfrc created successfully with {} records".format(len(df)))
df.to_sql("rgnl_prinfrc", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:417 & END:417.###
'''SAS Comment:*Read current regional inforce; '''
### SAS Source Code Line Numbers START:418 & END:420.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from rgnhmcnv_{}".format(curr_ccyymm), sqliteConnection)
df = df_rgnhmcnv1
# handling data frame column case senstivity.#
df_lower_colNames(df, 'rgnhmcnv_{}'.format(curr_ccyymm))
# Push results data frame to Sqlite DB
logging.info(
    "rgnl_currnfrc created successfully with {} records".format(len(df)))
df.to_sql("rgnl_currnfrc", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:423 & END:423.###
'''SAS Comment:*Compare inforce from a month ago to current month inforce; '''
### SAS Source Code Line Numbers START:424 & END:434.###

# Sql Code Start and End Lines - 424&434 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS rgnl_nfrcrtn;create table rgnl_nfrcrtn as select a.* ,b.nfrccnt as rtncnt1 from
        rgnl_prinfrc a left join rgnl_currnfrc b on b.state = a.state and
        b.seqpolno = a.seqpolno"""
    sql = mcrResl(sql)
    tgtSqliteTable = "rgnl_nfrcrtn"
    procSql_standard_Exec(sqliteDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:436 & END:436.###
'''SAS Comment:*Concatenate Michigan and regional inforce retained and process non-renewing policies to obtain count; '''
### SAS Source Code Line Numbers START:437 & END:444.###
'''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
if rnwgcnt = 0 then nfrccnt2 = nfrccnt;if rnwgcnt = 0 and rtncnt1 = 1 then rtncnt2 = nfrccnt;'''
# Converting source mi_nfrcrtn data into datafram.
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
mi_nfrcrtn = pd.read_sql_query("select * from mi_nfrcrtn ", sqliteConnection)
# Converting source rgnl_nfrcrtn data into datafram.
rgnl_nfrcrtn = pd.read_sql_query(
    "select * from rgnl_nfrcrtn ", sqliteConnection)
# Concatenate the source data frames
cmb_nfrcrtnd = pd.concat([mi_nfrcrtn, rgnl_nfrcrtn],
                         ignore_index=True, sort=False)
# Push results data frame to Sqlite DB
cmb_nfrcrtnd.loc[cmb_nfrcrtnd.rnwgcnt == 0, 'nfrccnt2'] = df.nfrccnt
cmb_nfrcrtnd.loc[(cmb_nfrcrtnd.rnwgcnt == 0) & (
    cmb_nfrcrtnd.rtncnt1 == 1), 'rtncnt2'] = df.nfrccnt
cmb_nfrcrtnd = df_remove_indexCols(cmb_nfrcrtnd)
# df_creation_logging(cmb_nfrcrtnd)
logging.info(
    "cmb_nfrcrtnd created successfully with {} records".format(len(cmb_nfrcrtnd)))
cmb_nfrcrtnd.to_sql("cmb_nfrcrtnd", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()
# SAS Source Code Line Numbers START:446 & END:446.###
'''SAS Comment:***END OF MACRO PROCESSING    END OF MACRO PROCESSING     END OF MACRO PROCESSING               ; '''
# SAS Source Code Line Numbers START:449 & END:449.###
'''SAS Comment:**************************************PART 2****************************************************; '''
# SAS Source Code Line Numbers START:450 & END:450.###
'''SAS Comment:*CREATE THE APPROPRIATE ROWS ON THE DATABASE                                                    ; '''
# SAS Source Code Line Numbers START:451 & END:451.###
'''SAS Comment:*As of March 2015, there are now two categories for SSA and EA. They are broken out separately  ; '''
# SAS Source Code Line Numbers START:452 & END:452.###
'''SAS Comment:*and rolled up into CAPTIVE/EA. This was done for the flexibility to break them out or group    ; '''
# SAS Source Code Line Numbers START:453 & END:453.###
'''SAS Comment:*them into one category. However, you must take care to add them to total fields only one time  ; '''
# SAS Source Code Line Numbers START:454 & END:454.###
'''SAS Comment:*throughout the code.; '''
# SAS Source Code Line Numbers START:456 & END:801.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from cmb_nfrcrtnd ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'cmb_nfrcrtnd')
df['valseq'] = df['agttypcd'].apply(AGTTYP_SEQ)
df['agttyp'] = df['valseq'].apply(AGTTYP_VAL)
df_unknown = df.loc[df.agttyp == 'Unknown']
df_unknown = df_remove_indexCols(df_unknown)
logging.info("unkagtdb created with {} records".format(len(df_unknown)))
df_unknown.to_sql("unkagtdb", con=sqliteConnection, if_exists='replace')

df.loc[df.agttyp == 'Unknown', 'agttyp'] = 'HB'
df.loc[df.agttyp == 'Unknown', 'valseq'] = 3

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

df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA'])) & (df.system == 'I')]
df_homenfrc['agtsave'] = df_homenfrc['agttyp']
df_homenfrc['agttyp'] = 'Captive/EA'
df_homenfrc = df_remove_indexCols(df_homenfrc)
logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
count = count + 1
df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB'])) & (df.system == 'I')]
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

df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA'])) & (df.state != 'MI')]
df_homenfrc['agtsave'] = df_homenfrc['agttyp']
df_homenfrc['agttyp'] = 'Captive/EA'
df_homenfrc = df_remove_indexCols(df_homenfrc)
temp = temp + len(df_homenfrc)
df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB'])) & (df.state != 'MI')]
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
conditions = [df[col] == '0', df[col] == '1', df[col] == '2', df[col] == '3']
choices1 = [1, 2, 3, 4]
choices2 = ['0', '1', '2', '3']
df['valseq'] = np.select(conditions, choices1)
df['dimval'] = np.select(conditions, choices2)
df_homenfrc = df.loc[df.system == 'I']
df_homenfrc = df_remove_indexCols(df_homenfrc)
logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
count = count + 1
df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA'])) & (df.system == 'I')]
df_homenfrc['agtsave'] = df_homenfrc['agttyp']
df_homenfrc['agttyp'] = 'Captive/EA'
df_homenfrc = df_remove_indexCols(df_homenfrc)
logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
count = count + 1
df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB'])) & (df.system == 'I')]
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

df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA'])) & (df.system == 'I')]
df_homenfrc['agtsave'] = df_homenfrc['agttyp']
df_homenfrc['agttyp'] = 'Captive/EA'
df_homenfrc = df_remove_indexCols(df_homenfrc)
logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
count = count + 1
df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB'])) & (df.system == 'I')]
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

df_homenfrc = df.loc[(df['agttyp'].isin(['SSA', 'EA'])) & (df.system == 'I')]
df_homenfrc['agtsave'] = df_homenfrc['agttyp']
df_homenfrc['agttyp'] = 'Captive/EA'
df_homenfrc = df_remove_indexCols(df_homenfrc)
logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
count = count + 1
df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')

df_homenfrc = df.loc[(df['agttyp'].isin(['MSC', 'HB'])) & (df.system == 'I')]
df_homenfrc['agtsave'] = df_homenfrc['agttyp']
df_homenfrc['agttyp'] = 'MSC/HB'
df_homenfrc = df_remove_indexCols(df_homenfrc)
logging.info('{} homenfrc - {}'.format(count, len(df_homenfrc)))
count = count + 1
df_homenfrc.to_sql("homenfrc", con=sqliteConnection, if_exists='append')
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:802 & END:802.###
'''SAS Comment:***********************************************************************************************************; '''
### SAS Source Code Line Numbers START:804 & END:804.###
'''SAS Comment:***********************************************************************************************************; '''
### SAS Source Code Line Numbers START:807 & END:808.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
PROC PRINT DATA=WORK.UNKAGTDB;
RUN;
'''

### SAS Source Code Line Numbers START:810 & END:810.###
'''SAS Comment:*SUMMARIZE INFORCE AND RETENTION COUNT BY AGENT TYPE; '''
### SAS Source Code Line Numbers START:811 & END:818.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
PROC SUMMARY DATA=WORK.HOMENFRC NWAY;
CLASS mnthnd SYSTEM PRODUCT STATE AGTTYP DIMSEQ VALSEQ;
ID DIM DIMVAL;
VAR nfrccnt1 rtncnt1 nfrccnt2 rtncnt2;
OUTPUT OUT=WORK.DIMSUM (DROP=_TYPE_ _FREQ_)
SUM=
;
RUN;
'''
# TODO - Didn't have all variables in group by
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
df = pd.read_sql_query("select * from HOMENFRC", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'HOMENFRC')
str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
agg_cols = {'dim', 'dimval', 'nfrccnt1', 'rtncnt1', 'nfrccnt2', 'rtncnt2'}
final_cols = list(agg_cols.intersection(str_cols))
df[final_cols] = df[final_cols].fillna(value='')
df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq', 'valseq']).agg(
    {'dim': max, 'dimval': max, 'nfrccnt1': sum, 'rtncnt1': sum, 'nfrccnt2': sum, 'rtncnt2': sum}).reset_index()
df_creation_logging(df, "DIMSUM")
df.to_sql("DIMSUM", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_sql_query("select * from HOMENFRC", sqliteConnection)
# #handling data frame column case senstivity.
# df_lower_colNames(df, 'HOMENFRC')
# grouped_df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq', 'valseq']).agg({'dim':max, 'dimval':max, 'nfrccnt1':sum, 'rtncnt1':sum , 'nfrccnt2':sum, 'rtncnt2':sum}).reset_index()
# df_creation_logging(grouped_df, "DIMSUM")
# grouped_df.to_sql("DIMSUM",con=sqliteConnection,if_exists='replace')
# sqliteConnection.close()

### SAS Source Code Line Numbers START:821 & END:821.###
'''SAS Comment:*CREATE ROW TOTALS BY AGTTYP; '''
### SAS Source Code Line Numbers START:822 & END:829.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
PROC SUMMARY DATA=WORK.DIMSUM NWAY;
CLASS mnthnd SYSTEM PRODUCT STATE AGTTYP DIMSEQ;
ID VALSEQ DIM DIMVAL;
VAR nfrccnt1 rtncnt1 nfrccnt2 rtncnt2;
OUTPUT OUT=WORK.ROWSUM (DROP=_TYPE_ _FREQ_)
SUM=
;
RUN;
'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
df = pd.read_sql_query("select * from DIMSUM", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'DIMSUM')
str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
agg_cols = {'valseq', 'dim', 'dimval',
            'nfrccnt1', 'rtncnt1', 'nfrccnt2', 'rtncnt2'}
final_cols = list(agg_cols.intersection(str_cols))
df[final_cols] = df[final_cols].fillna(value='')
df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq']).agg(
    {'valseq': max, 'dim': max, 'dimval': max, 'nfrccnt1': sum, 'rtncnt1': sum, 'nfrccnt2': sum, 'rtncnt2': sum}).reset_index()
df_creation_logging(df, "ROWSUM")
df.to_sql("ROWSUM", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_sql_query("select * from DIMSUM", sqliteConnection)
##handling data frame column case senstivity.#
# df_lower_colNames(df, 'HOMENFRC')
# df['valseq'] = df['valseq'].astype('Int64')
# TODO - Add ID VAR
# grouped_df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq']).agg({'valseq':max, 'nfrccnt1':sum, 'rtncnt1':sum , 'nfrccnt2':sum, 'rtncnt2':sum}).reset_index()
# df_creation_logging(grouped_df, "ROWSUM")
# grouped_df.to_sql("ROWSUM",con=sqliteConnection,if_exists='replace')
# sqliteConnection.close()

### SAS Source Code Line Numbers START:832 & END:832.###
'''SAS Comment:*CREATE THE "TOTAL" DIMVAL; '''
### SAS Source Code Line Numbers START:833 & END:862.###
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

df.loc[df['dimseq'] == '5', 'valseq'] = 4
# ***Start manual effort here...
# ELSE IF DIMSEQ = '5' THEN VALSEQ = 4;
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

# Push results data frame to Sqlite DB
logging.info("ROWSUM2 created successfully with {} records".format(len(df)))
df.to_sql("ROWSUM2", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()

#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:866 & END:866.###
'''SAS Comment:*CONCATENATE THE "TOTAL" DIMVAL WITH THE OTHER DIMENSION VALUES FOR EACH AGTTYP; '''
## SAS Source Code Line Numbers START:867 & END:869.###
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source WORK.DIMSUM data into datafram.
DIMSUM = pd.read_sql_query("select * from DIMSUM ", sqliteConnection)
# Converting source WORK.ROWSUM2 data into datafram.
ROWSUM2 = pd.read_sql_query("select * from ROWSUM2 ", sqliteConnection)
# Concatenate the source data frames
SUMDB = pd.concat([DIMSUM, ROWSUM2], ignore_index=True, sort=False)
# Push results data frame to Sqlite DB
SUMDB = df_remove_indexCols(SUMDB)
df_creation_logging(SUMDB, 'SUMDB')
SUMDB.to_sql("SUMDB", con=sqliteConnection, if_exists='replace')

'''SAS Comment:*ROLL UP SUMMARIES FOR EACH AGENT TYPE TO CREATE THE "ALL AGENT" CATEGORY; '''
## SAS Source Code Line Numbers START:878 & END:886.###
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
df = pd.read_sql_query(
    "select * from SUMDB WHERE AGTTYP NOT IN ('SSA', 'EA', 'MSC', 'HB')", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'SUMDB')
str_cols = set(df.select_dtypes(include = ['object', 'string']).columns)
agg_cols = {'dim', 'dimval', 'nfrccnt1', 'rtncnt1', 'nfrccnt2', 'rtncnt2'}
final_cols = list(agg_cols.intersection(str_cols))
df[final_cols] = df[final_cols].fillna(value = '')
grouped_df = df.groupby(['mnthnd', 'system', 'product', 'state', 'dimseq', 'valseq']).agg(
    {'dim' : max, 'dimval' : max, 'nfrccnt1': sum, 'rtncnt1': sum, 'nfrccnt2': sum, 'rtncnt2': sum}).reset_index()
df_creation_logging(grouped_df, "AGTSUM")
grouped_df.to_sql("AGTSUM", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

### SAS Source Code Line Numbers START:890 & END:890.###
'''SAS Comment:*CREATE THE "All Agents" AGTTYP; '''
### SAS Source Code Line Numbers START:891 & END:894.###
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
# Push results data frame to Sqlite DB
logging.info("AGTSUM2 created successfully with {} records".format(len(df)))
df.to_sql("AGTSUM2", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:898 & END:898.###
'''SAS Comment:*CONCATENATE "All Agents" AGTTYP WITH THE OTHER AGTTYPs; '''
### SAS Source Code Line Numbers START:899 & END:901.###
# Converting source AGTSUM2 data into datafram.
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
AGTSUM2 = pd.read_sql_query("select * from AGTSUM2 ", sqliteConnection)
# Converting source SUMDB data into datafram.
SUMDB = pd.read_sql_query("select * from SUMDB ", sqliteConnection)
# Concatenate the source data frames
NEWSUM = pd.concat([AGTSUM2, SUMDB], ignore_index=True, sort=False)
# Push results data frame to Sqlite DB
NEWSUM = df_remove_indexCols(NEWSUM)
df_creation_logging(NEWSUM, 'NEWSUM')
NEWSUM.to_sql("NEWSUM", con=sqliteConnection, if_exists='replace')

### SAS Source Code Line Numbers START:905 & END:905.###
'''SAS Comment:*Calculate the retention ratio for all rows; '''
### SAS Source Code Line Numbers START:906 & END:918.###
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
df['rtnratio1'] = df.rtncnt1 / df.nfrccnt1
df['cnclratio1'] = 1 - df.rtnratio1
df['rtnratio2'] = df.rtncnt2 / df.nfrccnt2
df['cnclratio2'] = 1 - df.rtnratio2
df['migrind'] = 'N'
# Push results data frame to Sqlite DB
logging.info("NFRCDB created successfully with {} records".format(len(df)))
df = df.drop(columns='level_0')
df.to_sql("NFRCDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:920 & END:922.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 920&922 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
# sql = """DROP TABLE IF EXISTS NFRCDB_sqlitesorted; CREATE TABLE NFRCDB_sqlitesorted AS SELECT * FROM NFRCDB ORDER BY
# STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE NFRCDB;ALTER TABLE
# NFRCDB_sqlitesorted RENAME TO NFRCDB"""
# sql = mcrResl(sql)
# tgtSqliteTable = "NFRCDB_sqlitesorted"
# procSql_standard_Exec(procSql_standard_Exec,sql,tgtSqliteTable)
# except:
# e = sys.exc_info()[0]
# logging.error('Table creation/update is failed.')
# logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:925 & END:925.###
'''SAS Comment:*Create an interim, inforce database. This data must be further manipulated to create rows for  ; '''
### SAS Source Code Line Numbers START:926 & END:926.###
'''SAS Comment:*missing valseq values across the various dimensions.                                           ; '''
### SAS Source Code Line Numbers START:927 & END:948.###

# Sql Code Start and End Lines - 927&948 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS NTRMOUT; CREATE TABLE NTRMOUT AS SELECT mnthnd ,SYSTEM ,PRODUCT ,AGTTYP ,STATE
        ,MIGRIND ,DIM ,DIMSEQ ,DIMVAL ,VALSEQ ,NFRCCNT1 ,RTNCNT1 ,CNCLRATIO1 ,NFRCCNT2
        ,RTNCNT2 ,CNCLRATIO2 FROM NFRCDB"""
    sql = mcrResl(sql)
    tgtSqliteTable = "NTRMOUT"
    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:950 & END:953.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
PROC APPEND BASE=nfrcrtnd DATA=NTRMOUT;


run;
'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
NTRMOUT = pd.read_sql_query("select * from NTRMOUT", sqliteConnection)
df_creation_logging(NTRMOUT, "nfrcrtnd")
NTRMOUT.to_sql("nfrcrtnd", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
### SAS Source Code Line Numbers START:954 & END:954.###
'''SAS Comment:****************************PART 3***FIX THE DB*************************************************; '''
### SAS Source Code Line Numbers START:956 & END:958.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 956&958 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
# sql = """DROP TABLE IF EXISTS nfrcrtnd_sqlitesorted;CREATE TABLE nfrcrtnd_sqlitesorted AS SELECT * FROM nfrcrtnd ORDER BY
# mnthnd,system,state,agttyp,dimseq,valseq;DROP TABLE nfrcrtnd;ALTER TABLE
# nfrcrtnd_sqlitesorted RENAME TO nfrcrtnd"""
# sql = mcrResl(sql)
# tgtSqliteTable = "nfrcrtnd_sqlitesorted"
# procSql_standard_Exec(procSql_standard_Exec,sql,tgtSqliteTable)
# except:
# e = sys.exc_info()[0]
# logging.error('Table creation/update is failed.')
# logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:960 & END:960.###
'''SAS Comment:*There may be some dimensions that do not have counts for every valseq. This code will create   ; '''
### SAS Source Code Line Numbers START:961 & END:961.###
'''SAS Comment:*missing valseq numbers and zero out the counts. This is required for the report writer to put  ; '''
### SAS Source Code Line Numbers START:962 & END:962.###
'''SAS Comment:*the counts in the right column of the spreadsheet. There must be rows for each valseq for each ; '''
### SAS Source Code Line Numbers START:963 & END:963.###
'''SAS Comment:*dimension. The DIMVAL is not used in the report writer. Therefore, this field is left blank    ; '''
### SAS Source Code Line Numbers START:964 & END:964.###
'''SAS Comment:*when the row is created.                                                                       ; '''
### SAS Source Code Line Numbers START:966 & END:996.###
'''*********************************************************************************
Below python code is to execute SAS data step with BY varaible in python
*********************************************************************************'''
# TODO - retain
'''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
retain;frstdim = first.dimseq;else cnt = cnt + 1;seqhold = valseq;output;if seqhold = cnt then return;else do until (seqhold = cnt);nfrccnt1 = .;rtncnt1 = .;cnclratio1 = .;nfrccnt2 = .;rtncnt2 = .;cnclratio2 = .;dimval = ' ';valseq = cnt;output;cnt = cnt + 1;end;'''


# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source NFRCOUT data into datafram.
df = pd.read_sql_query("select * from nfrcrtnd ", sqliteConnection)
# lowering all column names#Generate first and last temporary indicators in the given data frames.
df_lower_colNames(df)
var_list = ['mnthnd', 'system', 'state', 'agttyp', 'dimseq'] 
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
            temp['rtncnt1'] = np.nan
            temp['cnclratio1'] = np.nan
            temp['nfrccnt2'] = np.nan
            temp['rtncnt2'] = np.nan
            temp['cnclratio2'] = np.nan
            temp['dimval'] = np.nan
            temp['valseq'] = val
            temp_df = temp_df.append(temp)
fxdnfrc = pd.concat([df, temp_df],ignore_index=True,sort=False)
fxdnfrc = df_remove_indexCols(fxdnfrc)
df_creation_logging(fxdnfrc, "fxdnfrc")
# Push results data frame to Sqlite DB
fxdnfrc.to_sql("fxdnfrc", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
'''*******************************End of Merge Process**************************************************'''

### SAS Source Code Line Numbers START:997 & END:997.###
'''SAS Comment:*END FIX DB   END FIX DB     END FIX DB     END FIX DB    END FIX DB     END FIX DB; '''
### SAS Source Code Line Numbers START:1000 & END:1000.###
'''SAS Comment:**************************************PART 4****************************************************; '''
### SAS Source Code Line Numbers START:1001 & END:1001.###
'''SAS Comment:*Backup current database; '''
### SAS Source Code Line Numbers START:1002 & END:1004.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
sql = """select * from {}.{}.cnclrat_homedb """.format(project_id, output_dataset)
df_cnclrat_homedb = client.query(sql).to_dataframe()
df_cnclrat_homedb.to_gbq(destination_table = output_dataset + '.' + 'cncldb_cnclrat_homedb_backup', project_id = project_id, if_exists = 'replace')
'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/cnclrat_homedb.sas7bdat') as reader:
    df = reader.to_data_frame()
    df_lower_colNames(df, 'cnclrat_homedb')
	df['mnthnd'] = df.mnthnd.astype('Int64')
    df = df.loc[df.mnthnd < int(curr_ccyymm)]
    df.to_sql("cncldb_cnclrat_homedb",
              con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
'''

# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# # reading the file from csv
# df = pd.read_csv(
    # "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_cnclhome/data/cncldb_cnclrat_homedb.csv")
# # lowering all columns
# df_lower_colNames(df, 'cncldb_cnclrat_homedb')
# # logging info
# df_creation_logging(df, "cncldb_cnclrat_homedb")
# # putting into the sqliteDB
# df.to_sql("cncldb_cnclrat_homedb", con=sqliteConnection,
          # if_exists='replace', index=True)
# sqliteConnection.close()

# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
'''df = pd.read_sql_query(
    "select * from cncldb_cnclrat_homedb ", sqliteConnection)'''
df = df_cnclrat_homedb
df['mnthnd'] = df.mnthnd.astype('Int64')
df = df.loc[df.mnthnd < int(curr_ccyymm)]
# handling data frame column case senstivity.#
df_lower_colNames(df, 'cncldb_cnclrat_homedb')
# Push results data frame to Sqlite DB
logging.info(
    "cncldb_cnclrat_homedb_backup created successfully with {} records".format(len(df)))
df.to_sql("cncldb_cnclrat_homedb_backup",
          con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1007 & END:1007.###

'''
### SAS Source Code Line Numbers START:1008 & END:1013.###
**WARNING:Below steps are not included in logic calculation. Please amend them manually.
drop frstdim cnt seqhold;'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source cncldb.cnclrat_homedb data into datafram.
'''cncldb_cnclrat_homedb = pd.read_sql_query(
    "select * from cncldb_cnclrat_homedb ", sqliteConnection)'''
cncldb_cnclrat_homedb = df_cnclrat_homedb
# Converting source work.fxdnfrc data into datafram.
fxdnfrc = pd.read_sql_query("select * from fxdnfrc ", sqliteConnection)
# Concatenate the source data frames
cncldb = pd.concat([cncldb_cnclrat_homedb, fxdnfrc],
                   ignore_index=True, sort=False)
cncldb = cncldb.drop(columns = ['frstdim', 'cnt', 'seqhold'])
# Push results data frame to Sqlite DB
df = df_remove_indexCols(cncldb)
df_creation_logging(cncldb, "cncldb")
del df_cnclrat_homedb
cncldb.to_sql("cncldb", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

sqliteToBQ(output_tables)

### SAS Source Code Line Numbers START:1015 & END:1017.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 1015&1017 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
    # sql = """DROP TABLE IF EXISTS  cncldb_sqlitesorted; CREATE TABLE cncldb_sqlitesorted AS SELECT * FROM cncldb ORDER BY
        # mnthnd,system,product,state,agttyp,dimseq,valseq;DROP TABLE cncldb;ALTER
        # TABLE cncldb_sqlitesorted RENAME TO cncldb"""
    # sql = mcrResl(sql)
    # tgtSqliteTable = "cncldb_sqlitesorted"
    # procSql_standard_Exec(procSql_standard_Exec, sql, tgtSqliteTable)
# except:
    # e = sys.exc_info()[0]
    # logging.error('Table creation/update is failed.')
    # logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:1019 & END:1019.###
'''SAS Comment:*Output the final database; '''
### SAS Source Code Line Numbers START:1020 & END:1022.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from cncldb ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'cncldb')
# Push results data frame to Sqlite DB
df = df.drop(columns='level_0')
logging.info(
    "cncldb_cnclrat_homedb created successfully with {} records".format(len(df)))
df.to_sql("cncldb_cnclrat_homedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()

#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1024 & END:1024.###
'''SAS Comment:**************************************PART 5****************************************************; '''
### SAS Source Code Line Numbers START:1025 & END:1025.###
'''SAS Comment:*THE FOLLOWING WAS ADDED FOR BALANCING PURPOSES ONLY; '''
### SAS Source Code Line Numbers START:1028 & END:1032.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'L'; ", sqliteConnection)
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Close connection to Sqlite work data base
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq.to_sql(
    "legacy_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

rtncnt1_freq.to_sql("legacy_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

nfrccnt2_freq.to_sql(
    "legacy_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

rtncnt2_freq.to_sql("legacy_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1035 & END:1039.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'MI'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()


nfrccnt1_freq.to_sql(
    "mi_ipm_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt1_freq.to_sql("mi_ipm_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt2_freq.to_sql(
    "mi_ipm_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("mi_ipm_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1042 & END:1046.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state != 'MI'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt1_freq.to_sql(
    "reg_ipm_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

rtncnt1_freq.to_sql("reg_ipm_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt2_freq.to_sql(
    "reg_ipm_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("reg_ipm_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1049 & END:1053.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'IA'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt1_freq.to_sql(
    "iowa_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

rtncnt1_freq.to_sql("iowa_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt2_freq.to_sql(
    "iowa_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("iowa_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1056 & END:1060.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'IN'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt1_freq.to_sql(
    "indiana_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt1_freq.to_sql("indiana_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

nfrccnt2_freq.to_sql(
    "indiana_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("indiana_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1063 & END:1067.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'IL'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

nfrccnt1_freq.to_sql(
    "illinois_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt1_freq.to_sql("illinois_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt2_freq.to_sql(
    "illinois_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("illinois_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1070 & END:1074.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'KY'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt1_freq.to_sql(
    "kentucky_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

rtncnt1_freq.to_sql("kentucky_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

nfrccnt2_freq.to_sql(
    "kentucky_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("kentucky_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1077 & END:1081.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'MN'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt1_freq.to_sql(
    "minnesota_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt1_freq.to_sql("minnesota_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

nfrccnt2_freq.to_sql(
    "minnesota_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("minnesota_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1084 & END:1088.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'OH'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt1_freq.to_sql(
    "ohio_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt1_freq.to_sql("ohio_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt2_freq.to_sql(
    "ohio_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("ohio_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1091 & END:1095.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'WI'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

nfrccnt1_freq.to_sql(
    "wisconsin_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt1_freq.to_sql("wisconsin_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt2_freq.to_sql(
    "wisconsin_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("wisconsin_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1098 & END:1102.###
### PROC FREQ Step Starts Here ###
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source ins_home data into datafram.
cmb_nfrcrtnd = pd.read_sql_query(
    "SELECT nfrccnt1,rtncnt1,nfrccnt2,rtncnt2 FROM cmb_nfrcrtnd where syscd = 'I' and state = 'WV'; ", sqliteConnection)
# Close connection to Sqlite work data base
df_lower_colNames(cmb_nfrcrtnd, 'cmb_nfrcrtnd')
# Creating PRCO FREQ results in python for source column :NFRCCNT1.
nfrccnt1_freq = cmb_nfrcrtnd['nfrccnt1'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt1': 'Frequency', 'index': 'nfrccnt1'})
nfrccnt1_freq["Percent"] = 100*nfrccnt1_freq['Frequency'] / \
    nfrccnt1_freq['Frequency'].sum()
nfrccnt1_freq['Cumulative Frequency'] = nfrccnt1_freq['Frequency'].cumsum()
nfrccnt1_freq['cum_perc'] = 100 * \
    nfrccnt1_freq['Cumulative Frequency']/nfrccnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt1_freq.to_sql(
    "west_virginia_nfrccnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT1.
rtncnt1_freq = cmb_nfrcrtnd['rtncnt1'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
    rtncnt1_freq['Frequency'].sum()
rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
rtncnt1_freq['cum_perc'] = 100 * \
    rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt1_freq.to_sql("west_virginia_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :NFRCCNT2.
nfrccnt2_freq = cmb_nfrcrtnd['nfrccnt2'].value_counts().to_frame(
).reset_index().rename(columns={'nfrccnt2': 'Frequency', 'index': 'nfrccnt2'})
nfrccnt2_freq["Percent"] = 100*nfrccnt2_freq['Frequency'] / \
    nfrccnt2_freq['Frequency'].sum()
nfrccnt2_freq['Cumulative Frequency'] = nfrccnt2_freq['Frequency'].cumsum()
nfrccnt2_freq['cum_perc'] = 100 * \
    nfrccnt2_freq['Cumulative Frequency']/nfrccnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
nfrccnt2_freq.to_sql(
    "west_virginia_nfrccnt2_freq", con=sqliteConnection, if_exists='replace')

# Creating PRCO FREQ results in python for source column :RTNCNT2.
rtncnt2_freq = cmb_nfrcrtnd['rtncnt2'].value_counts().to_frame(
).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
    rtncnt2_freq['Frequency'].sum()
rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
rtncnt2_freq['cum_perc'] = 100 * \
    rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

# Push results data frame to Sqlite DB
rtncnt2_freq.to_sql("west_virginia_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

# Close connection to Sqlite work data base
sqliteConnection.close()
#End of PROC FREQ Step #

### SAS Source Code Line Numbers START:1106 & END:1106.###
'''SAS Comment:*** Writes SAS Log to a text file ***; '''
### SAS Source Code Line Numbers START:1107 & END:1107.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
%SaveLog(NAME=CancelRatio_Homedb,DIR=T:\Shared\Acturial\BISLogs\CancelRatio\)
'''

### SAS Source Code Line Numbers START:1109 & END:1109.###
'''SAS Comment:*** Close the stored compiled macro catalog and clear the previous libref  ***; '''
### SAS Source Code Line Numbers START:1113 & END:1113.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
libname _all_ clear;
'''
