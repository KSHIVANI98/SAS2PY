# -*- coding: utf-8 -*-
r'''
Created on: Mon 14 Dec 20 18:09:22
Author: SAS2PY Code Conversion Tool
SAS Input File: gw_ipm_rnwlhome
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
import warnings
import dateutil.parser


# Seting up logging info #

config_file = None
yaml_file = None

try:
    config_file = open('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/config.yaml', 'r+')
    yaml_file = yaml.load(config_file)
except Exception as e:
    print("Error reading config file | ERROR : ", e)
finally:
    config_file.close()

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
'''SAS Comment:*     NAME:  MONTHLY, HOME POLICY RENEWAL RETENTION RATIO              ; '''
### SAS Source Code Line Numbers START:5 & END:5.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:6 & END:6.###
'''SAS Comment:*   SYSTEM:  LEGACY & IPM                                              ; '''
### SAS Source Code Line Numbers START:7 & END:7.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:8 & END:8.###
'''SAS Comment:* FUNCTION:  THIS PROGRAM PROCESSES INFORCE FOR TWO PERIODS PRIOR TO   ; '''
### SAS Source Code Line Numbers START:9 & END:9.###
'''SAS Comment:*            THE CURRENT MONTH END TO DETERMINE THE RECORDS RENEWING   ; '''
### SAS Source Code Line Numbers START:10 & END:10.###
'''SAS Comment:*            IN THE MONTH FOLLOWING. THESE RECORDS ARE THEN COMPARED   ; '''
### SAS Source Code Line Numbers START:11 & END:11.###
'''SAS Comment:*            TO THE CURRENT MONTH FOR RETENTION. THE INFORMATION IS    ; '''
### SAS Source Code Line Numbers START:12 & END:12.###
'''SAS Comment:*            LOADED TO A SAS DATABASE. THE DATABASE IS INPUT TO A      ; '''
### SAS Source Code Line Numbers START:13 & END:13.###
'''SAS Comment:*            SEPARATE PROGRAM TO PRODUCE EXCEL SPREADSHEETS AND GRAPHS ; '''
### SAS Source Code Line Numbers START:14 & END:14.###
'''SAS Comment:*            FOR EACH STATE.                                           ; '''
### SAS Source Code Line Numbers START:15 & END:15.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:16 & END:16.###
'''SAS Comment:*            THE SPREADSHEET PROGRAM PRODUCES SEPARATE SPREADSHEETS FOR; '''
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
'''SAS Comment:*            REPORTED - PRIOR YEAR REPORTING PERIOD INFORCE COUNT, THE ; '''
### SAS Source Code Line Numbers START:25 & END:25.###
'''SAS Comment:*            CURRENT REPORTING PERIOD RETENTION COUNT, AND THE         ; '''
### SAS Source Code Line Numbers START:26 & END:26.###
'''SAS Comment:*            CALCULATED RETENTION RATIO. THE PRIOR YEAR REPORTING      ; '''
### SAS Source Code Line Numbers START:27 & END:27.###
'''SAS Comment:*            PERIOD IS THE STANDARD FOR COMPARISON.                    ; '''
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
'''SAS Comment:*            LEGACY HOME INFORCE EXTRACT                               ; '''
### SAS Source Code Line Numbers START:48 & END:48.###
'''SAS Comment:*            IPM HOME INFORCE EXTRACT                                  ; '''
### SAS Source Code Line Numbers START:49 & END:49.###
'''SAS Comment:*            LEGACY AGENT LIST                                         ; '''
### SAS Source Code Line Numbers START:50 & END:50.###
'''SAS Comment:*            IPM AGENT LIST                                            ; '''
### SAS Source Code Line Numbers START:51 & END:51.###
'''SAS Comment:*            RTNRNWL_HOMEDB                                            ; '''
### SAS Source Code Line Numbers START:52 & END:52.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:53 & END:53.###
'''SAS Comment:*   OUTPUT:  RTNRNWL_HOMEDB - RENEWAL HOME INFORCE/RETENTION DB        ; '''
### SAS Source Code Line Numbers START:54 & END:54.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:55 & END:55.###
'''SAS Comment:*DEPENDENCIES: MAINFRAME LEGACY AND IPM INFORCE FTP                    ; '''
### SAS Source Code Line Numbers START:56 & END:56.###
'''SAS Comment:*              MAINFRAME DATECARD (IUT015M) FTP                        ; '''
### SAS Source Code Line Numbers START:57 & END:57.###
'''SAS Comment:*              WEBFOCUS AGENT LIST TRANSFER AND CONVERSION TO SAS DB   ; '''
### SAS Source Code Line Numbers START:58 & END:58.###
'''SAS Comment:*              LEGACY AGENT LIST TRANSFER AND CONVERSION TO SAS DB     ; '''
### SAS Source Code Line Numbers START:59 & END:59.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:60 & END:60.###
'''SAS Comment:*   BALANCING: - BALANCE CURRENT AND PRIOR LEGACY/IPM INFORCE          ; '''
### SAS Source Code Line Numbers START:61 & END:61.###
'''SAS Comment:*              NOTE:  INVALID AGENT TYPES WILL BE PRINTED IN THE LOG.  ; '''
### SAS Source Code Line Numbers START:62 & END:62.###
'''SAS Comment:*              TRACK TO SEE WHY THEY'RE INVALID TO DETERMINE WHAT      ; '''
### SAS Source Code Line Numbers START:63 & END:63.###
'''SAS Comment:*              SHOULD BE DONE IF ANYTHING. OTHERWISE, THE AGENT WILL BE; '''
### SAS Source Code Line Numbers START:64 & END:64.###
'''SAS Comment:*              SLOTTED UNDER UNKNOWN.                                  ; '''
### SAS Source Code Line Numbers START:65 & END:65.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:66 & END:66.###
'''SAS Comment:*              FOR MICHIGAN, LEGACY PLUS IPM INFORCE WILL BALANCE TO   ; '''
### SAS Source Code Line Numbers START:67 & END:67.###
'''SAS Comment:*              LEGACY AND IPM COMBINED. HOWEVER, THE RETENTION WILL NOT; '''
### SAS Source Code Line Numbers START:68 & END:68.###
'''SAS Comment:*              THE REASON IS A POLICY COULD HAVE MIGRATED FROM LEGACY  ; '''
### SAS Source Code Line Numbers START:69 & END:69.###
'''SAS Comment:*              TO IPM. SO, IT WON'T BE CAPTURED UNDER RETAINED ON THE  ; '''
### SAS Source Code Line Numbers START:70 & END:70.###
'''SAS Comment:*              THE REPORTS FOR THE SEPARATE SYSTEMS, BUT IT WILL BE    ; '''
### SAS Source Code Line Numbers START:71 & END:71.###
'''SAS Comment:*              CAPTURED ON THE COMBINED REPORT.                        ; '''
### SAS Source Code Line Numbers START:72 & END:72.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:73 & END:73.###
'''SAS Comment:*    NOTES:  THIS JOB CREATED FOR P&PD.                                ; '''
### SAS Source Code Line Numbers START:74 & END:74.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:75 & END:75.###
'''SAS Comment:*            CANNOT RUN THIS PROGRAM MULTIPLE TIMES WITHOUT DELETING   ; '''
### SAS Source Code Line Numbers START:76 & END:76.###
'''SAS Comment:*            ALL WORK FILES. THE EASIEST WAY TO DO THIS IS TO QUIT SAS ; '''
### SAS Source Code Line Numbers START:77 & END:77.###
'''SAS Comment:*            AND GET BACK IN AND RERUN. THIS PROGRAM CREATES A WORK    ; '''
### SAS Source Code Line Numbers START:78 & END:78.###
'''SAS Comment:*            FILE FOR WHICH DATA IS APPENDED TO THROUGH MULTIPLE       ; '''
### SAS Source Code Line Numbers START:79 & END:79.###
'''SAS Comment:*            ITERATIONS OF A MACRO.                                    ; '''
### SAS Source Code Line Numbers START:80 & END:80.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:81 & END:81.###
'''SAS Comment:*            REMEMBER THIS PROCESS CREATES AN ACTUAL DATABASE. THE     ; '''
### SAS Source Code Line Numbers START:82 & END:82.###
'''SAS Comment:*            DATABASE IS AUTOMATICALLY BACKED UP IN THIS PROCESS.      ; '''
### SAS Source Code Line Numbers START:83 & END:83.###
'''SAS Comment:*            THIS MUST BE TAKEN INTO CONSIDERATION FOR RERUNS.         ; '''
### SAS Source Code Line Numbers START:84 & END:84.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:85 & END:85.###
'''SAS Comment:*            THIS PROGRAM DOES NOT HAVE ANY MANUAL PARMS TO UPDATE.    ; '''
### SAS Source Code Line Numbers START:86 & END:86.###
'''SAS Comment:*            THE DATECARD FOR THE CURRENT QUARTER IS FTP'D FROM THE    ; '''
### SAS Source Code Line Numbers START:87 & END:87.###
'''SAS Comment:*            MAINFRAME TO DIRECTORY, T\Shared\Acturial\FTPINBOUND.     ; '''
### SAS Source Code Line Numbers START:88 & END:88.###
'''SAS Comment:*            THE PROGRAM READS THE CURRENT QUARTER DATE AND CALCULATES ; '''
### SAS Source Code Line Numbers START:89 & END:89.###
'''SAS Comment:*            DATE FOR THE SAME QUARTER FROM THE PRIOR YEAR. THESE DATES; '''
### SAS Source Code Line Numbers START:90 & END:90.###
'''SAS Comment:*            ARE THEN USED TO ACCESS DATE QUALIFIED FILES. THE CURRENT ; '''
### SAS Source Code Line Numbers START:91 & END:91.###
'''SAS Comment:*            QUARTER FILES ARE USED TO CALCULATE RETENTION BASED ON THE; '''
### SAS Source Code Line Numbers START:92 & END:92.###
'''SAS Comment:*            INFORCE COUNTS FOR THE SAME PERIOD IN THE PRIOR YEAR.     ; '''
### SAS Source Code Line Numbers START:93 & END:93.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:94 & END:94.###
'''SAS Comment:*            THE LEGACY AND IPM HOME INFORCE FILES FOR THE CURRENT     ; '''
### SAS Source Code Line Numbers START:95 & END:95.###
'''SAS Comment:*            MONTH ARE FTP'D FROM THE MAINFRAME TO THE FTPINBOUND      ; '''
### SAS Source Code Line Numbers START:96 & END:96.###
'''SAS Comment:*            DIRECTORY IN BINARY FORMAT NAMED, LGCYBNRY AND IPMBNRY    ; '''
### SAS Source Code Line Numbers START:97 & END:97.###
'''SAS Comment:*            RESPECTIVELY. THE CIMPORT PROGRAM CREATES SAS FILES FROM  ; '''
### SAS Source Code Line Numbers START:98 & END:98.###
'''SAS Comment:*            THE BINARY TEXT FILES. THESE SAS FILES ARE CALLED LGHMNFRC; '''
### SAS Source Code Line Numbers START:99 & END:99.###
'''SAS Comment:*            (LEGACY) AND FINALOUT2 (IPM) BASED ON THE NAMES GIVEN IN  ; '''
### SAS Source Code Line Numbers START:100 & END:100.###
'''SAS Comment:*            THE MAINFRAME CODE.                                       ; '''
### SAS Source Code Line Numbers START:101 & END:101.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:102 & END:102.###
'''SAS Comment:*            THE PROGRAM IS STRUCTURED TO DO THE FOLLOWING:            ; '''
### SAS Source Code Line Numbers START:103 & END:103.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:104 & END:104.###
'''SAS Comment:*            THE FINAL OUTPUT IS A SAS DATABASE, RNWLRTN_HOMEDB. THIS  ; '''
### SAS Source Code Line Numbers START:105 & END:105.###
'''SAS Comment:*            DATABASE IS INPUT TO THE REPORT WRITER PROGRAM TO PRODUCE ; '''
### SAS Source Code Line Numbers START:106 & END:106.###
'''SAS Comment:*            THE FINAL EXCEL SPREADSHEETS. THIS PROGRAM CREATES A      ; '''
### SAS Source Code Line Numbers START:107 & END:107.###
'''SAS Comment:*            BACKUP OF THE DATABASE EACH RUN.                          ; '''
### SAS Source Code Line Numbers START:108 & END:108.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:109 & END:109.###
'''SAS Comment:************************ REVISION LOG *********************************; '''
### SAS Source Code Line Numbers START:110 & END:110.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:111 & END:111.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:112 & END:112.###
'''SAS Comment:*   DATE        INIT     DESCRIPTION OF CHANGE                         ; '''
### SAS Source Code Line Numbers START:113 & END:113.###
'''SAS Comment:*   02/15       PLT      NEW PROGRAM. Implemented 02-15                ; '''
### SAS Source Code Line Numbers START:114 & END:114.###
'''SAS Comment:*   03/15       PLT      Add EA agent type.                            ; '''
### SAS Source Code Line Numbers START:115 & END:115.###
'''SAS Comment:*   09/15       PLT    - New premier breakout.                         ; '''
### SAS Source Code Line Numbers START:116 & END:116.###
'''SAS Comment:*                      - Renewal calculation changes. Use GL date to   ; '''
### SAS Source Code Line Numbers START:117 & END:117.###
'''SAS Comment:*                      - determine renewals.                           ; '''
### SAS Source Code Line Numbers START:118 & END:118.###
'''SAS Comment:*   01/16       PLT    - New prior claim categories.                   ; '''
### SAS Source Code Line Numbers START:119 & END:119.###
'''SAS Comment:*                      - Correct issue where entries for agent types   ; '''
### SAS Source Code Line Numbers START:120 & END:120.###
'''SAS Comment:*                        SSA & EA were not created.                    ; '''
### SAS Source Code Line Numbers START:121 & END:121.###
'''SAS Comment:*   02/18       PLT    - Breakout MSC & HB, but keep MSC/HB as well    ; '''
### SAS Source Code Line Numbers START:122 & END:122.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:123 & END:123.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
OPTIONS MPRINT SYMBOLGEN MSTORED SASMSTORE=StoreMac missing='.';
'''

### SAS Source Code Line Numbers START:125 & END:125.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME StoreMac 'T:\Shared\Acturial\BISMacros\SAS';
'''

### SAS Source Code Line Numbers START:126 & END:126.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME NFRCFILE 'T:\Shared\Acturial\BISProd\CommonDataSources\Home\Inforce\';
'''

### SAS Source Code Line Numbers START:127 & END:127.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
LIBNAME NFRCOUT 'T:\Shared\Acturial\BISProd\RenewalRatio\InputOutput\';
'''

### SAS Source Code Line Numbers START:129 & END:129.###
'''SAS Comment:*FILENAME IUT015M 'T:\Shared\Acturial\BISProd\CommonDataSources\Datecards\IUT015M_1508.TXT'; '''
### SAS Source Code Line Numbers START:132 & END:296.###

# SAS Comment:*NOTE:  VALUE CLAUSES WITH FORMAT NAMES THAT BEGIN WITH A DOLLAR SIGN ARE CHARACTER.
# SAS Comment:*THE DISTINCTION BETWEEN NUMERIC AND CHARACTER APPLIES TO THE COLUMN TO WHICH THE
# SAS Comment:*FORMAT WILL BE APPLIED -- NOT THE DISPLAYED VALUE.
# SAS Comment:*INVALUE IS AN INFORMAT
# SAS Comment:*FOR ALL THE RETENTION RATIO REPORT DIMENSIONS: PREMIER INDICATOR, MULTIPRODUCT INDICATOR, HOME
# SAS Comment:*AGE, ROOF AGE, AND FORM CODE, WE CREATE A FORMAT TO REPRESENT THE DIMENSION SEQUENCE AND
# SAS Comment:*THE DIMENSION VALUE. THE SEQUENCE IS THE ORDER IN WHICH THE VALUE WILL APPEAR ON THE FINAL
# SAS Comment:*REPORT. THE VALUE IS THE TITLE THAT WILL APPEAR ON THE REPORT.


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
    elif inPut in ['N', '0', ' ', 'nan']:
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
    elif inPut in ['N', ' ', 'nan']:
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


### SAS Source Code Line Numbers START:297 & END:297.###
'''SAS Comment:************************************END OF PROC FORMAT******************************************; '''
### SAS Source Code Line Numbers START:299 & END:299.###
'''SAS Comment:************************************END OF PROC FORMAT******************************************; '''
### SAS Source Code Line Numbers START:301 & END:301.###
'''SAS Comment:***********************************MACRO BEGINS HERE*******************************************; '''
### SAS Source Code Line Numbers START:302 & END:302.###
'''SAS Comment:*This section is done two times to process the inforce files from two months ago and one month ; '''
### SAS Source Code Line Numbers START:303 & END:303.###
'''SAS Comment:*ago for records that are renewing the month following the file date. The renewing inforce is  ; '''
### SAS Source Code Line Numbers START:304 & END:304.###
'''SAS Comment:*matched against two subsequent months to determine the records retained in each of the        ; '''
### SAS Source Code Line Numbers START:305 & END:305.###
'''SAS Comment:*subsequent months. Basically, we obtain a count for those that renewed on time. For the month ; '''
### SAS Source Code Line Numbers START:306 & END:306.###
'''SAS Comment:*following the renewal month, we pick up lapse renewals. However, some that renewed on time    ; '''
### SAS Source Code Line Numbers START:307 & END:307.###
'''SAS Comment:*could also be cancelled.                                                                      ; '''
### SAS Source Code Line Numbers START:308 & END:308.###
'''SAS Comment:***********************************MACRO BEGINS HERE*******************************************; '''
### SAS Source Code Line Numbers START:309 & END:348.###

''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''

"""ERROR: Unable to convert the below SAS block/code into python
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
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
file_str = open(
    r"/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/IUT015M.TXT").read()
mnthnd_ccyymm = file_str[16:22]
mnthnd_ccyy = file_str[16:20]
mnthnd_mm = file_str[20:22]
mnthnd_ccyymmdd = file_str[16:24]


global curr_ccyymm, curr_ccyy, curr_mm, mnthnd, amnthago, twomnthsago

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

def prcsrnwg(nfrcrnwg_filedte, matchind, rnwgmnth):

    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/mihmcnv_{}.sas7bdat'.format(nfrcrnwg_filedte)) as reader:
        df = reader.to_data_frame()
        df.to_sql("nfrcfile_mihmcnv_{}".format(nfrcrnwg_filedte),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()

    # Open connection to Sqlite work data base
    '''sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/nfrcfile_mihmcnv_{}.csv".format(nfrcrnwg_filedte))
    # lowering all columns
    df_lower_colNames(df, 'nfrcfile_mihmcnv_{}'.format(nfrcrnwg_filedte))
    # logging info
    df_creation_logging(df, "nfrcfile_mihmcnv_{}".format(nfrcrnwg_filedte))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_mihmcnv_{}".format(nfrcrnwg_filedte),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_mihmcnv_{} ".format(nfrcrnwg_filedte), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_mihmcnv_{}'.format(nfrcrnwg_filedte))
    # where rnwgcnt = 1; # Manual effort require.
    df = df.loc[df['rnwgcnt'] == 1]
    df['system'] = df.syscd
    df['mnthnd'] = rnwgmnth
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "mi_nfrcrnwg created successfully with {} records".format(len(df)))
    df.to_sql("mi_nfrcrnwg", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*Create a regional work database of inforce policies renewing the following month; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/rgnhmcnv_{}.sas7bdat'.format(nfrcrnwg_filedte)) as reader:
        df = reader.to_data_frame()
        df.to_sql("nfrcfile_rgnhmcnv_{}".format(nfrcrnwg_filedte),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()

    # Open connection to Sqlite work data base
    '''sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/nfrcfile_rgnhmcnv_{}.csv".format(nfrcrnwg_filedte))
    # lowering all columns
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}'.format(nfrcrnwg_filedte))
    # logging info
    df_creation_logging(df, "nfrcfile_rgnhmcnv_{}".format(nfrcrnwg_filedte))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_rgnhmcnv_{}".format(nfrcrnwg_filedte),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_rgnhmcnv_{} ".format(nfrcrnwg_filedte), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}'.format(nfrcrnwg_filedte))
    # where rnwgcnt = 1; # Manual effort require.
    df = df.loc[df['rnwgcnt'] == 1]
    df['system'] = df.syscd
    df['mnthnd'] = rnwgmnth
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "rgnl_nfrcrnwg created successfully with {} records".format(len(df)))
    df.to_sql("rgnl_nfrcrnwg", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*When matchind is equal 2, the inforce file being processed is two months prior to the current  ; '''
    '''SAS Comment:*month end. The file contains only policies renewing the month following. So, we compare the    ; '''
    '''SAS Comment:*renewing inforce file to the actual renewing month and to the current month inforce files to   ; '''
    '''SAS Comment:*determine retained for both those periods.                                                               ; '''
    '''SAS Comment:*                                                                                               ; '''
    '''SAS Comment:*When the matchind is equal 1, the inforce file being processed is one month prior to the       ; '''
    '''SAS Comment:*current month end. The file contains only policies renewing in the current month. So, it is    ; '''
    '''SAS Comment:*only compared to the current month inforce.                                                    ; '''
    '''SAS Comment:*                                                                                               ; '''
    '''SAS Comment:*The date qualifier represents the time period from the current month end.                      ; '''

    '''Python Indentation Required. Please check below lines and correct accordingly.'''

    if(matchind) == '2':
        join_twomnths(amnthago)
    else:
        join_onemnth()


'''Uncomment to execute the below sas macro'''
# prcsrnwg(<< Provide require args here >>)

### SAS Source Code Line Numbers START:349 & END:349.###
'''SAS Comment:***********************************MACRO ENDS HERE**********************************************; '''
### SAS Source Code Line Numbers START:351 & END:351.###
'''SAS Comment:*Compares the inforce renewing file (from two months prior to the current month end) to         ; '''
### SAS Source Code Line Numbers START:352 & END:352.###
'''SAS Comment:*the renewing month inforce file. For example, if current month is June, then the file two      ; '''
### SAS Source Code Line Numbers START:353 & END:353.###
'''SAS Comment:*months prior is April. Here the April file containing inforce policies renewing in May is      ; '''
### SAS Source Code Line Numbers START:354 & END:354.###
'''SAS Comment:*compared to the May inforce and the June inforce files. Matches for May are policies that      ; '''
### SAS Source Code Line Numbers START:355 & END:355.###
'''SAS Comment:*renewed on time. Matches for June pick up lapse policies.                                      ; '''
### SAS Source Code Line Numbers START:356 & END:486.###

''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''


def join_twomnths(dtequal):
    '''SAS Comment:*MICHIGAN; '''
    '''SAS Comment:*Read MI inforce from a month ago; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/mihmcnv_{}.sas7bdat'.format(dtequal)) as reader:
        df = reader.to_data_frame()
        df.to_sql("nfrcfile_mihmcnv_{}".format(dtequal),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()

    '''# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/nfrcfile_mihmcnv_{}.csv".format(dtequal))
    # lowering all columns
    df_lower_colNames(df, 'nfrcfile_mihmcnv_{}'.format(dtequal))
    # logging info
    df_creation_logging(df, "nfrcfile_mihmcnv_{}".format(dtequal))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_mihmcnv_{}".format(dtequal),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_mihmcnv_{} ".format(dtequal), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_mihmcnv_{}'.format(dtequal))
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "mi_nfrcrtnd_{} created successfully with {} records".format(dtequal, len(df)))
    df.to_sql("mi_nfrcrtnd_{}".format(dtequal),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*Read MI current inforce.; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/mihmcnv_{}.sas7bdat'.format(curr_ccyymm)) as reader:
        df = reader.to_data_frame()
        df.to_sql("nfrcfile_mihmcnv_{}".format(curr_ccyymm),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()

        '''# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/nfrcfile_mihmcnv_{}.csv".format(curr_ccyymm))
    # lowering all columns
    df_lower_colNames(df, 'nfrcfile_mihmcnv_{}'.format(curr_ccyymm))
    # logging info
    df_creation_logging(df, "nfrcfile_mihmcnv_{}".format(curr_ccyymm))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_mihmcnv_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_mihmcnv_{} ".format(curr_ccyymm), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_mihmcnv_{}'.format(curr_ccyymm))
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "mi_nfrcrtnd_{} created successfully with {} records".format(curr_ccyymm, len(df)))
    df.to_sql("mi_nfrcrtnd_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*Compare renewing from two months ago to inforce from a month ago; '''

    # Sql Code Start and End Lines - 30&40 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS nfrcrtn;create table nfrcrtn as select a.*,b.nfrccnt as rtncnt1 from mi_nfrcrnwg
            a left join mi_nfrcrtnd_{} b on b.syscd = a.syscd and b.seqpolno = a.seqpolno""".format(dtequal)
        sql = mcrResl(sql)
        tgtSqliteTable = "nfrcrtn"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:*input the Michigan work database created in sql above; '''

    # Sql Code Start and End Lines - 44&55 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS mi_nfrcrtn;create table mi_nfrcrtn as select a.*,b.nfrccnt as rtncnt2 from nfrcrtn
            a left join mi_nfrcrtnd_{} b on b.syscd = a.syscd and b.seqpolno =
            a.seqpolno""".format(curr_ccyymm)
        sql = mcrResl(sql)
        tgtSqliteTable = "mi_nfrcrtn"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    ### PROC FREQ Step Starts Here ###
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source ins_home data into datafram.
    mi_nfrcrtn = pd.read_sql_query(
        "SELECT nfrccnt,rtncnt1,rtncnt2 FROM mi_nfrcrtn where syscd = 'L' ", sqliteConnection)
    # Close connection to Sqlite work data base
    # sqliteConnection.close()
    # Creating PRCO FREQ results in python for source column :NFRCCNT.
    df_lower_colNames(mi_nfrcrtn, 'mi_nfrcrtn')
    nfrccnt_freq = mi_nfrcrtn['nfrccnt'].value_counts().to_frame().reset_index(
    ).rename(columns={'nfrccnt': 'Frequency', 'index': 'nfrccnt'})
    nfrccnt_freq["Percent"] = 100*nfrccnt_freq['Frequency'] / \
        nfrccnt_freq['Frequency'].sum()
    nfrccnt_freq['Cumulative Frequency'] = nfrccnt_freq['Frequency'].cumsum()
    nfrccnt_freq['cum_perc'] = 100 * \
        nfrccnt_freq['Cumulative Frequency']/nfrccnt_freq['Frequency'].sum()

    nfrccnt_freq.to_sql(
        "Legacy_nfrccnt_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT1.
    rtncnt1_freq = mi_nfrcrtn['rtncnt1'].value_counts().to_frame().reset_index(
    ).rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
    rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
        rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
    rtncnt1_freq['cum_perc'] = 100 * \
        rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq.to_sql(
        "Legacy_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT2.
    rtncnt2_freq = mi_nfrcrtn['rtncnt2'].value_counts().to_frame().reset_index(
    ).rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
    rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
        rtncnt2_freq['Frequency'].sum()
    rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
    rtncnt2_freq['cum_perc'] = 100 * \
        rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

    rtncnt2_freq.to_sql(
        "Legacy_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #End of PROC FREQ Step #
    ### PROC FREQ Step Starts Here ###
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source ins_home data into datafram.
    mi_nfrcrtn = pd.read_sql_query(
        "SELECT nfrccnt,rtncnt1,rtncnt2 FROM mi_nfrcrtn where syscd = 'I' ", sqliteConnection)
    # Close connection to Sqlite work data base
    # sqliteConnection.close()
    # Creating PRCO FREQ results in python for source column :NFRCCNT.
    df_lower_colNames(mi_nfrcrtn, 'mi_nfrcrtn')
    nfrccnt_freq = mi_nfrcrtn['nfrccnt'].value_counts().to_frame().reset_index(
    ).rename(columns={'nfrccnt': 'Frequency', 'index': 'nfrccnt'})
    nfrccnt_freq["Percent"] = 100*nfrccnt_freq['Frequency'] / \
        nfrccnt_freq['Frequency'].sum()
    nfrccnt_freq['Cumulative Frequency'] = nfrccnt_freq['Frequency'].cumsum()
    nfrccnt_freq['cum_perc'] = 100 * \
        nfrccnt_freq['Cumulative Frequency']/nfrccnt_freq['Frequency'].sum()
    nfrccnt_freq.to_sql(
        "IPM_nfrccnt_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT1.
    rtncnt1_freq = mi_nfrcrtn['rtncnt1'].value_counts().to_frame().reset_index(
    ).rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
    rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
        rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
    rtncnt1_freq['cum_perc'] = 100 * \
        rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq.to_sql(
        "IPM_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT2.
    rtncnt2_freq = mi_nfrcrtn['rtncnt2'].value_counts().to_frame().reset_index(
    ).rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
    rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
        rtncnt2_freq['Frequency'].sum()
    rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
    rtncnt2_freq['cum_perc'] = 100 * \
        rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

    rtncnt2_freq.to_sql(
        "IPM_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #End of PROC FREQ Step #
    '''SAS Comment:*REGIONAL; '''
    '''SAS Comment:*Read regional inforce from a month ago; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/rgnhmcnv_{}.sas7bdat'.format(dtequal)) as reader:
        df = reader.to_data_frame()
        df.to_sql("nfrcfile_rgnhmcnv_{}".format(dtequal),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()

        '''# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/nfrcfile_rgnhmcnv_{}.csv".format(dtequal))
    # lowering all columns
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}'.format(dtequal))
    # logging info
    df_creation_logging(df, "nfrcfile_rgnhmcnv_{}".format(dtequal))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_rgnhmcnv_{}".format(dtequal),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_rgnhmcnv_{} ".format(dtequal), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}'.format(dtequal))
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "rgnl_nfrcrtnd_{} created successfully with {} records".format(dtequal, len(df)))
    df.to_sql("rgnl_nfrcrtnd_{}".format(dtequal),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*Read MI current inforce;'''
    # title Join Two Months - Legacy counts after matching
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/rgnhmcnv_{}.sas7bdat'.format(curr_ccyymm)) as reader:
        df = reader.to_data_frame()
        df.to_sql("nfrcfile_rgnhmcnv_{}".format(curr_ccyymm),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()

    '''# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # reading the file from csv
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/nfrcfile_rgnhmcnv_{}.csv".format(curr_ccyymm))
    # lowering all columns
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}'.format(curr_ccyymm))
    # logging info
    df_creation_logging(df, "nfrcfile_rgnhmcnv_{}".format(curr_ccyymm))
    # putting into the sqliteDB
    df.to_sql("nfrcfile_rgnhmcnv_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_rgnhmcnv_{} ".format(curr_ccyymm), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}'.format(curr_ccyymm))
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "rgnl_nfrcrtnd_{} created successfully with {} records".format(curr_ccyymm, len(df)))
    df.to_sql("rgnl_nfrcrtnd_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    # title 'Join Two Months - MI IPM counts after matching'

    # Sql Code Start and End Lines - 97&107 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS nfrcrtn;create table nfrcrtn as select a.*,b.nfrccnt as rtncnt1 from
            rgnl_nfrcrnwg a left join rgnl_nfrcrtnd_{} b on b.state =
            a.state and b.seqpolno = a.seqpolno""".format(dtequal)
        sql = mcrResl(sql)
        tgtSqliteTable = "nfrcrtn"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:*input the regional work database created in sql above; '''

    # Sql Code Start and End Lines - 110&120 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS rgnl_nfrcrtn;create table rgnl_nfrcrtn as select a.*,b.nfrccnt as rtncnt2 from
            nfrcrtn a left join rgnl_nfrcrtnd_{} b on b.state = a.state and
            b.seqpolno = a.seqpolno""".format(curr_ccyymm)
        sql = mcrResl(sql)
        tgtSqliteTable = "rgnl_nfrcrtn"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    ### PROC FREQ Step Starts Here ###
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source ins_home data into datafram.
    rgnl_nfrcrtn = pd.read_sql_query(
        "SELECT nfrccnt,rtncnt1,rtncnt2 FROM rgnl_nfrcrtn", sqliteConnection)
    # Close connection to Sqlite work data base
    df_lower_colNames(rgnl_nfrcrtn, 'rgnl_nfrcrtn')
    # Creating PRCO FREQ results in python for source column :NFRCCNT.
    nfrccnt_freq = rgnl_nfrcrtn['nfrccnt'].value_counts().to_frame(
    ).reset_index().rename(columns={'nfrccnt': 'Frequency', 'index': 'nfrccnt'})
    nfrccnt_freq["Percent"] = 100*nfrccnt_freq['Frequency'] / \
        nfrccnt_freq['Frequency'].sum()
    nfrccnt_freq['Cumulative Frequency'] = nfrccnt_freq['Frequency'].cumsum()
    nfrccnt_freq['cum_perc'] = 100 * \
        nfrccnt_freq['Cumulative Frequency']/nfrccnt_freq['Frequency'].sum()

    nfrccnt_freq.to_sql(
        "IPM2_nfrccnt_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT1.
    rtncnt1_freq = rgnl_nfrcrtn['rtncnt1'].value_counts().to_frame(
    ).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
    rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
        rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
    rtncnt1_freq['cum_perc'] = 100 * \
        rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq.to_sql(
        "IPM2_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT2.
    rtncnt2_freq = rgnl_nfrcrtn['rtncnt2'].value_counts().to_frame(
    ).reset_index().rename(columns={'rtncnt2': 'Frequency', 'index': 'rtncnt2'})
    rtncnt2_freq["Percent"] = 100*rtncnt2_freq['Frequency'] / \
        rtncnt2_freq['Frequency'].sum()
    rtncnt2_freq['Cumulative Frequency'] = rtncnt2_freq['Frequency'].cumsum()
    rtncnt2_freq['cum_perc'] = 100 * \
        rtncnt2_freq['Cumulative Frequency']/rtncnt2_freq['Frequency'].sum()

    rtncnt2_freq.to_sql(
        "IPM2_rtncnt2_freq", con=sqliteConnection, if_exists='replace')

    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #End of PROC FREQ Step #
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source mi_nfrcrtn data into datafram.
    mi_nfrcrtn = pd.read_sql_query(
        "select * from mi_nfrcrtn ", sqliteConnection)
    df_lower_colNames(mi_nfrcrtn, 'mi_nfrcrtn')
    # Converting source rgnl_nfrcrtn data into datafram.
    rgnl_nfrcrtn = pd.read_sql_query(
        "select * from rgnl_nfrcrtn ", sqliteConnection)
    df_lower_colNames(rgnl_nfrcrtn, 'rgnl_nfrcrtn')
    # Concatenate the source data frames
    cmb_nfrcrtn1 = pd.concat([mi_nfrcrtn, rgnl_nfrcrtn],
                             ignore_index=True, sort=False)

    cmb_nfrcrtn1.to_sql(
        "cmb_nfrcrtn1", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
    # Read MI current inforce
    # input the regional work database created in sql above
    # title 'Join Two Months - Regional IPM counts after matching'
    # proc freq data = rgnl_nfrcrtn


'''Uncomment to execute the below sas macro'''
# join_twomnths(<< Provide require args here >>)

### SAS Source Code Line Numbers START:487 & END:487.###
'''SAS Comment:*END OF MACRO JOIN_TWOMNTHS     END OF MACRO JOIN_TWOMNTHS    END OF MACRO JOIN_TWOMNTHS        ; '''
### SAS Source Code Line Numbers START:489 & END:489.###
'''SAS Comment:*join one month always compares the inforce renewing file to the current inforce file.          ; '''
### SAS Source Code Line Numbers START:490 & END:567.###

''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''


def join_onemnth():
    '''SAS Comment:*MICHIGAN; '''
    '''SAS Comment:*Read MI current inforce; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_mihmcnv_{} ".format(curr_ccyymm), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_mihmcnv_{}'.format(curr_ccyymm))
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "mi_nfrcrtnd_{} created successfully with {} records".format(curr_ccyymm, len(df)))
    df.to_sql("mi_nfrcrtnd_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*Compare renewing from a month ago to current inforce; '''

    # Sql Code Start and End Lines - 17&27 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS mi_nfrcrtn;create table mi_nfrcrtn as select a.*,b.nfrccnt as rtncnt1 from mi_nfrcrnwg a
            left join mi_nfrcrtnd_{} b on b.syscd = a.syscd and b.seqpolno =
            a.seqpolno""".format(curr_ccyymm)
        sql = mcrResl(sql)
        tgtSqliteTable = "mi_nfrcrtn"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    ### PROC FREQ Step Starts Here ###
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source ins_home data into datafram.
    mi_nfrcrtn = pd.read_sql_query(
        "SELECT nfrccnt,rtncnt1 FROM mi_nfrcrtn where syscd = 'L' ", sqliteConnection)
    # Close connection to Sqlite work data base
    # sqliteConnection.close()
    # Creating PRCO FREQ results in python for source column :NFRCCNT.
    df_lower_colNames(mi_nfrcrtn, 'mi_nfrcrtn')
    nfrccnt_freq = mi_nfrcrtn['nfrccnt'].value_counts().to_frame().reset_index(
    ).rename(columns={'nfrccnt': 'Frequency', 'index': 'nfrccnt'})
    nfrccnt_freq["Percent"] = 100*nfrccnt_freq['Frequency'] / \
        nfrccnt_freq['Frequency'].sum()
    nfrccnt_freq['Cumulative Frequency'] = nfrccnt_freq['Frequency'].cumsum()
    nfrccnt_freq['cum_perc'] = 100 * \
        nfrccnt_freq['Cumulative Frequency']/nfrccnt_freq['Frequency'].sum()


    nfrccnt_freq.to_sql(
        "legacy2_nfrccnt_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT1.
    rtncnt1_freq = mi_nfrcrtn['rtncnt1'].value_counts().to_frame().reset_index(
    ).rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
    rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
        rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
    rtncnt1_freq['cum_perc'] = 100 * \
        rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

    rtncnt1_freq.to_sql(
        "legacy2_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #End of PROC FREQ Step #
    ### PROC FREQ Step Starts Here ###
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source ins_home data into datafram.
    mi_nfrcrtn = pd.read_sql_query(
        "SELECT nfrccnt,rtncnt1 FROM mi_nfrcrtn where syscd = 'I' ", sqliteConnection)
    df_lower_colNames(mi_nfrcrtn, 'mi_nfrcrtn')
    # Close connection to Sqlite work data base
    # sqliteConnection.close()
    # Creating PRCO FREQ results in python for source column :NFRCCNT.
    nfrccnt_freq = mi_nfrcrtn['nfrccnt'].value_counts().to_frame().reset_index(
    ).rename(columns={'nfrccnt': 'Frequency', 'index': 'nfrccnt'})
    nfrccnt_freq["Percent"] = 100*nfrccnt_freq['Frequency'] / \
        nfrccnt_freq['Frequency'].sum()
    nfrccnt_freq['Cumulative Frequency'] = nfrccnt_freq['Frequency'].cumsum()
    nfrccnt_freq['cum_perc'] = 100 * \
        nfrccnt_freq['Cumulative Frequency']/nfrccnt_freq['Frequency'].sum()

    nfrccnt_freq.to_sql(
        "IPM3_nfrccnt_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT1.
    rtncnt1_freq = mi_nfrcrtn['rtncnt1'].value_counts().to_frame().reset_index(
    ).rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
    rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
        rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
    rtncnt1_freq['cum_perc'] = 100 * \
        rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

    rtncnt1_freq.to_sql(
        "IPM3_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #End of PROC FREQ Step #
    '''SAS Comment:*REGIONAL; '''
    # title 'Join One Month - Legacy counts after matching'
    '''SAS Comment:*Read current regional inforce; '''
    # title 'Join One Month - MI IPM counts after matching'
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from nfrcfile_rgnhmcnv_{} ".format(curr_ccyymm), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'nfrcfile_rgnhmcnv_{}')
    # Push results data frame to Sqlite DB
    df = df_remove_indexCols(df)
    logging.info(
        "rgnl_nfrcrtnd_{} created successfully with {} records".format(curr_ccyymm, len(df)))
    df.to_sql("rgnl_nfrcrtnd_{}".format(curr_ccyymm),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*Compare renewing from a month ago to current month inforce; '''

    # Sql Code Start and End Lines - 56&66 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS rgnl_nfrcrtn;create table rgnl_nfrcrtn as select a.*,b.nfrccnt as rtncnt1 from
            rgnl_nfrcrnwg a left join rgnl_nfrcrtnd_{} b on b.state =
            a.state and b.seqpolno = a.seqpolno""".format(curr_ccyymm)
        sql = mcrResl(sql)
        tgtSqliteTable = "rgnl_nfrcrtn"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    ### PROC FREQ Step Starts Here ###
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source ins_home data into datafram.
    rgnl_nfrcrtn = pd.read_sql_query(
        "SELECT nfrccnt,rtncnt1 FROM rgnl_nfrcrtn ", sqliteConnection)
    # Close connection to Sqlite work data base
    # sqliteConnection.close()
    # Creating PRCO FREQ results in python for source column :NFRCCNT.
    df_lower_colNames(rgnl_nfrcrtn, 'rgnl_nfrcrtn')
    nfrccnt_freq = rgnl_nfrcrtn['nfrccnt'].value_counts().to_frame(
    ).reset_index().rename(columns={'nfrccnt': 'Frequency', 'index': 'nfrccnt'})
    nfrccnt_freq["Percent"] = 100*nfrccnt_freq['Frequency'] / \
        nfrccnt_freq['Frequency'].sum()
    nfrccnt_freq['Cumulative Frequency'] = nfrccnt_freq['Frequency'].cumsum()
    nfrccnt_freq['cum_perc'] = 100 * \
        nfrccnt_freq['Cumulative Frequency']/nfrccnt_freq['Frequency'].sum()

    nfrccnt_freq.to_sql(
        "IPM4_nfrccnt_freq", con=sqliteConnection, if_exists='replace')

    # Creating PRCO FREQ results in python for source column :RTNCNT1.
    rtncnt1_freq = rgnl_nfrcrtn['rtncnt1'].value_counts().to_frame(
    ).reset_index().rename(columns={'rtncnt1': 'Frequency', 'index': 'rtncnt1'})
    rtncnt1_freq["Percent"] = 100*rtncnt1_freq['Frequency'] / \
        rtncnt1_freq['Frequency'].sum()
    rtncnt1_freq['Cumulative Frequency'] = rtncnt1_freq['Frequency'].cumsum()
    rtncnt1_freq['cum_perc'] = 100 * \
        rtncnt1_freq['Cumulative Frequency']/rtncnt1_freq['Frequency'].sum()

    rtncnt1_freq.to_sql(
        "IPM4_rtncnt1_freq", con=sqliteConnection, if_exists='replace')

    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #End of PROC FREQ Step #
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source mi_nfrcrtn data into datafram.
    mi_nfrcrtn = pd.read_sql_query(
        "select * from mi_nfrcrtn ", sqliteConnection)
    df_lower_colNames(mi_nfrcrtn, 'mi_nfrcrtn')
    # Converting source rgnl_nfrcrtn data into datafram.
    rgnl_nfrcrtn = pd.read_sql_query(
        "select * from rgnl_nfrcrtn ", sqliteConnection)
    # Concatenate the source data frames
    df_lower_colNames(rgnl_nfrcrtn, 'rgnl_nfrcrtn')
    cmb_nfrcrtn2 = pd.concat([mi_nfrcrtn, rgnl_nfrcrtn],
                             ignore_index=True, sort=False)
    # Push results data frame to Sqlite DB
    cmb_nfrcrtn2 = df_remove_indexCols(cmb_nfrcrtn2)
    df_creation_logging(cmb_nfrcrtn2, "cmb_nfrcrtn2")
    cmb_nfrcrtn2.to_sql(
        "cmb_nfrcrtn2", con=sqliteConnection, if_exists='replace')
    # Compare renewing from a month ago to current month inforce
    # title 'Join One Month - Regional IPM counts after matching'
    # proc freq data = rgnl_nfrcrtn
    sqliteConnection.close()


'''Uncomment to execute the below sas macro'''
# join_onemnth(<< Provide require args here >>)

### SAS Source Code Line Numbers START:568 & END:568.###
'''SAS Comment:*END OF MACRO JOIN_ONEMNTH     END OF MACRO JOIN_ONEMNTH    END OF MACRO JOIN_ONEMNTH          ; '''
### SAS Source Code Line Numbers START:571 & END:571.###
'''SAS Comment:***********************************MACROS ENDS HERE*********************************************; '''
### SAS Source Code Line Numbers START:575 & END:575.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:576 & END:576.###
'''SAS Comment:****PROCESSING BEGINS HERE****PROCESSING BEGINS HERE****PROCESSING BEGINS HERE****              ; '''
### SAS Source Code Line Numbers START:577 & END:577.###
'''SAS Comment:**************************************PART 1****************************************************; '''
### SAS Source Code Line Numbers START:579 & END:579.###
'''SAS Comment:*CURRENT MONTH END DATE CARD; '''
### SAS Source Code Line Numbers START:581 & END:581.###
'''SAS Comment:*THIS IS WHAT THE INPUT DATECARD LOOKS LIKE; '''
### SAS Source Code Line Numbers START:582 & END:582.###
'''SAS Comment:*IUT015  2013033020130628; '''
### SAS Source Code Line Numbers START:583 & END:624.###


### SAS Source Code Line Numbers START:628 & END:628.###
'''SAS Comment:**************************************PART 2****************************************************; '''
### SAS Source Code Line Numbers START:629 & END:629.###
'''SAS Comment:*Execute macro to process the inforce files for two months prior to current month end. Pull off ; '''
### SAS Source Code Line Numbers START:630 & END:630.###
'''SAS Comment:*only the records that are renewing the following month. Breakout the files by system code.     ; '''
### SAS Source Code Line Numbers START:631 & END:631.###
'''SAS Comment:*MATCHIND is a local macro variable to be used only in that macro. The variable denotes whether ; '''
### SAS Source Code Line Numbers START:632 & END:632.###
'''SAS Comment:*to match the master inforce file to 1 or 2 months of inforce renewal files. The RNWGMNTH parm  ; '''
### SAS Source Code Line Numbers START:633 & END:633.###
'''SAS Comment:*is used to populate the MNTHND field on the database.                                          ; '''
### SAS Source Code Line Numbers START:634 & END:634.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
prcsrnwg(nfrcrnwg_filedte=twomnthsago, matchind='2', rnwgmnth=amnthago)

### SAS Source Code Line Numbers START:635 & END:635.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
prcsrnwg(nfrcrnwg_filedte=amnthago, matchind='1', rnwgmnth=curr_ccyymm)

### SAS Source Code Line Numbers START:637 & END:637.###
'''SAS Comment:***END OF MACRO PROCESSING    END OF MACRO PROCESSING     END OF MACRO PROCESSING               ; '''
### SAS Source Code Line Numbers START:640 & END:640.###
'''SAS Comment:**************************************PART 3****************************************************; '''
### SAS Source Code Line Numbers START:641 & END:641.###
'''SAS Comment:*CREATE THE APPROPRIATE ROWS ON THE DATABASE                                                    ; '''
### SAS Source Code Line Numbers START:642 & END:642.###
'''SAS Comment:*As of March 2015, there are now two categories for SSA and EA. They are broken out separately  ; '''
### SAS Source Code Line Numbers START:643 & END:643.###
'''SAS Comment:*and rolled up into CAPTIVE/EA. This was done for the flexibility to break them out or group    ; '''
### SAS Source Code Line Numbers START:644 & END:644.###
'''SAS Comment:*them into one category. However, you must take care to add them to total fields only one time  ; '''
### SAS Source Code Line Numbers START:645 & END:645.###
'''SAS Comment:*throughout the code.; '''
### SAS Source Code Line Numbers START:647 & END:996.###
"""ERROR: Unable to convert the below SAS block/code into python """
'''
DATA HOMENFRC UNKAGTDB;
 SET cmb_nfrcrtn1 cmb_nfrcrtn2;
 FORMAT DIMVAL $40. DIM $25.;
 *AGENT TYPE;
 VALSEQ=INPUT(AGTTYPCD,AGTTYP_SEQ.);
 AGTTYP=PUT(VALSEQ,AGTTYP_VAL.);
 IF AGTTYP = 'Unknown' THEN DO;
 OUTPUT UNKAGTDB;
 AGTTYP='HB';
 VALSEQ = 6;
 END;
 '''
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
cmb_nfrcrtn1 = pd.read_sql_query(
    "select * from cmb_nfrcrtn1 ", sqliteConnection)
cmd_nfrcrtn2 = pd.read_sql_query(
    "select * from cmb_nfrcrtn2 ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(cmb_nfrcrtn1,'cmb_nfrcrtn1')
df_lower_colNames(cmd_nfrcrtn2,'cmd_nfrcrtn2')
df = pd.concat([cmb_nfrcrtn1, cmd_nfrcrtn2], ignore_index=True, sort=False)
df = df_remove_indexCols(df)
df_lower_colNames(df,'cmb_nfrcrtn1, cmb_nfrcrtn2')
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


df['dim'] = 'Multi-Product'
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
logging.info('{} homenfrc - {}'.format(count, len(df)))
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
logging.info('{} homenfrc - {}'.format(count, len(df)))
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
logging.info('{} homenfrc - {}'.format(count, len(df)))
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
logging.info('{} homenfrc - {}'.format(count, len(df)))
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
"""
### SAS Source Code Line Numbers START:997 & END:997.###
'''SAS Comment:************************************************************************************************; '''
### SAS Source Code Line Numbers START:999 & END:999.###
'''SAS Comment:************************************************************************************************; '''
### SAS Source Code Line Numbers START:1002 & END:1003.###
"""
'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
PROC PRINT DATA=UNKAGTDB;
RUN;
'''


### SAS Source Code Line Numbers START:1005 & END:1005.###
'''SAS Comment:*SUMMARIZE INFORCE AND RETENTION COUNT BY AGENT TYPE; '''
### SAS Source Code Line Numbers START:1006 & END:1013.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
PROC SUMMARY DATA=HOMENFRC NWAY;
CLASS mnthnd SYSTEM PRODUCT STATE AGTTYP DIMSEQ VALSEQ;
ID DIM DIMVAL;
VAR nfrccnt rtncnt1 rtncnt2;
OUTPUT OUT=DIMSUM (DROP=_TYPE_ _FREQ_)
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
agg_cols = {'dim', 'dimval', 'nfrccnt', 'rtncnt1', 'rtncnt2'}
final_cols = list(agg_cols.intersection(str_cols))
df[final_cols] = df[final_cols].fillna(value='')
df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq', 'valseq']).agg(
    {'dim': max, 'dimval': max, 'nfrccnt': sum, 'rtncnt1': sum, 'rtncnt2': sum}).reset_index()
df_creation_logging(df, "DIMSUM")
df.to_sql("DIMSUM", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_sql_query("select * from HOMENFRC", sqliteConnection)
#handling data frame column case senstivity.#
# df_lower_colNames(df, 'HOMENFRC')
# grouped_df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq', 'valseq']).agg(
# {'dim': max, 'dimval': max, 'nfrccnt': sum, 'rtncnt1': sum, 'rtncnt2': sum}).reset_index()
'''grp_lst = ['mnthnd', 'system', 'product', 'state', 'agttyp',
           'dimseq', 'valseq']  # Take the columns in Class
id_list = ['dim', 'dimval']  # Take the columns in ID
grouped_df = pd.DataFrame()
cnt = 0
df_0 = df.drop(columns=grp_lst)
df_0_summ = df_0[['nfrccnt', 'rtncnt1', 'rtncnt2']].sum()

for i in range(1, len(grp_lst)+1):
    for j in itertools.combinations(grp_lst, i):
        cnt = cnt + 1
        df1 = df.groupby(list(j))[
            ['nfrccnt', 'rtncnt1', 'rtncnt2']].sum().reset_index()
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
grouped_df = grouped_df[grp_lst+id_list + ['nfrccnt', 'rtncnt1', 'rtncnt2']]'''

# df_creation_logging(grouped_df, "DIMSUM")
# grouped_df.to_sql("DIMSUM", con=sqliteConnection, if_exists='replace')
# sqliteConnection.close()

'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq',
                 'valseq', 'dim', 'dimval'])[['nfrccnt', 'rtncnt1', 'rtncnt2']].sum()
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "homenfrc created successfully with {} records".format(len(df)))
df.to_sql("homenfrc", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()'''


### SAS Source Code Line Numbers START:1016 & END:1016.###
'''SAS Comment:*CREATE ROW TOTALS BY AGTTYP; '''
### SAS Source Code Line Numbers START:1017 & END:1024.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
PROC SUMMARY DATA=DIMSUM NWAY;
CLASS mnthnd SYSTEM PRODUCT STATE AGTTYP DIMSEQ;
ID VALSEQ DIM DIMVAL;
VAR nfrccnt rtncnt1 rtncnt2;
OUTPUT OUT=ROWSUM (DROP=_TYPE_ _FREQ_)
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
            'nfrccnt', 'rtncnt1', 'rtncnt2'}
final_cols = list(agg_cols.intersection(str_cols))
df[final_cols] = df[final_cols].fillna(value='')
df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq']).agg(
    {'valseq': max, 'dim': max, 'dimval': max, 'nfrccnt': sum, 'rtncnt1': sum,  'rtncnt2': sum}).reset_index()
df_creation_logging(df, "ROWSUM")
df.to_sql("ROWSUM", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_sql_query("select * from DIMSUM", sqliteConnection)
##handling data frame column case senstivity.#
# df_lower_colNames(df, 'DIMSUM')
# grouped_df = df.groupby(['mnthnd', 'system', 'product', 'state', 'agttyp', 'dimseq']).agg(
# {'valseq': max, 'dim': max, 'dimval': max, 'nfrccnt': sum, 'rtncnt1': sum, 'rtncnt2': sum}).reset_index()
'''grp_lst = ['mnthnd', 'system', 'product', 'state',
           'agttyp', 'dimseq']  # Take the columns in Class
id_list = ['valseq', 'dim', 'dimval']  # Take the columns in ID
grouped_df = pd.DataFrame()
cnt = 0
df_0 = df.drop(columns=grp_lst)
df_0_summ = df_0[['nfrccnt', 'rtncnt1', 'rtncnt2']].sum()

for i in range(1, len(grp_lst)+1):
    for j in itertools.combinations(grp_lst, i):
        cnt = cnt + 1
        df1 = df.groupby(list(j))[
            ['nfrccnt', 'rtncnt1', 'rtncnt2']].sum().reset_index()
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
grouped_df = grouped_df[grp_lst+id_list + ['nfrccnt', 'rtncnt1', 'rtncnt2']]'''
# df_creation_logging(grouped_df, "ROWSUM")
# grouped_df.to_sql("ROWSUM", con=sqliteConnection, if_exists='replace')
# sqliteConnection.close()

'''sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from dimsum ",sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df,'dimsum')
df=df.groupby(['mnthnd','system','product','state','agttyp','dimseq','valseq','dim','dimval'])[['nfrccnt','rtncnt1','rtncnt2']].sum()
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info("sql table name created successfully with {} records".format(len(df)))
df.to_sql("dimsum",con=sqliteConnection,if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()'''

### SAS Source Code Line Numbers START:1027 & END:1027.###
'''SAS Comment:*CREATE THE "TOTAL" DIMVAL; '''
### SAS Source Code Line Numbers START:1028 & END:1057.###
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
df.loc[df['dimseq'].isin(['1', '2']), 'valseq'] = 4
# End manual effort.***'''


# ***Start manual effort here...
# IF DIMSEQ IN ('3', '4') THEN VALSEQ = 8;
df.loc[df['dimseq'].isin(['3', '4']), 'valseq'] = 8
# End manual effort.***'''


# ***Start manual effort here...
# IF STATE NOT = 'MI' AND DIMSEQ = '2' THEN VALSEQ = 3;
df.loc[(df.state != 'MI') & (df.dimseq == '2'), 'valseq'] = 3

# End manual effort.***'''


# ***Start manual effort here...
# IF STATE = 'MI' AND DIMSEQ = '5' THEN VALSEQ = 5;df.loc[(df.state !='MI') & (df.dimseq=='5'),'valseq']
df.loc[(df['state'] == 'MI') & (df['dimseq'] == '5'), 'valseq'] = 5
# End manual effort.***'''


# ***Start manual effort here...
# ELSE IF DIMSEQ = '5' THEN VALSEQ = 4;
df.loc[df['dimseq'] == '5', 'valseq'] = 4
# End manual effort.***'''


# ***Start manual effort here...
# IF DIMSEQ IN ('6', '18') THEN VALSEQ = 3;
df.loc[df['dimseq'].isin(['6', '18']), 'valseq'] = 3
# End manual effort.***'''


# ***Start manual effort here...
# IF DIMSEQ IN ('7', '17') THEN VALSEQ = 5;
df.loc[df['dimseq'].isin(['7', '17']), 'valseq'] = 5
# End manual effort.***'''


# ***Start manual effort here...
# IF DIMSEQ = '9' THEN VALSEQ = 6;
df.loc[df['dimseq'] == '9', 'valseq'] = 6
# End manual effort.***'''

# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "ROWSUM2 created successfully with {} records".format(len(df)))
df.to_sql("ROWSUM2", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1061 & END:1061.###
'''SAS Comment:*CONCATENATE THE "TOTAL" DIMVAL WITH THE OTHER DIMENSION VALUES FOR EACH AGTTYP; '''
### SAS Source Code Line Numbers START:1062 & END:1064.###
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source DIMSUM data into datafram.
DIMSUM = pd.read_sql_query("select * from DIMSUM ", sqliteConnection)
df_lower_colNames(DIMSUM, 'DIMSUM')
# Converting source ROWSUM2 data into datafram.
ROWSUM2 = pd.read_sql_query("select * from ROWSUM2 ", sqliteConnection)
df_lower_colNames(ROWSUM2, 'ROWSUM2')
# Concatenate the source data frames
SUMDB = pd.concat([DIMSUM, ROWSUM2], ignore_index=True, sort=False)
# Push results data frame to Sqlite DB
SUMDB = df_remove_indexCols(SUMDB)
df_creation_logging(SUMDB, "SUMDB")
SUMDB.to_sql("SUMDB", con=sqliteConnection, if_exists='replace')

### SAS Source Code Line Numbers START:1066 & END:1068.###
sqliteConnection.close()
''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 1066&1068 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
#     sql = """DROP TABLE IF EXISTS SUMDB_sqlitesorted;CREATE TABLE SUMDB_sqlitesorted AS SELECT * FROM SUMDB ORDER BY
#         STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE SUMDB;ALTER TABLE
#         SUMDB_sqlitesorted RENAME TO SUMDB"""
#     sql = mcrResl(sql)
#     tgtSqliteTable = "SUMDB_sqlitesorted"
#     procSql_standard_Exec(sqliteDb,sql,tgtSqliteTable)
# except:
#    e = sys.exc_info()[0]
#    logging.error('Table creation/update is failed.')
#    logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:1072 & END:1072.###
'''SAS Comment:*ROLL UP SUMMARIES FOR EACH AGENT TYPE TO CREATE THE "ALL AGENT" CATEGORY; '''
### SAS Source Code Line Numbers START:1073 & END:1073.###
'''SAS Comment:*The SSA & EA agent types are accounted for in the Captive/EA category; '''
### SAS Source Code Line Numbers START:1074 & END:1082.###

"""WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress."""

'''
Please find below Please find below SAS code lines.
PROC SUMMARY DATA=SUMDB NWAY;
where agttyp not in ('SSA', 'EA', 'MSC', 'HB');
CLASS mnthnd SYSTEM PRODUCT STATE DIMSEQ VALSEQ;
ID DIM DIMVAL;
VAR NFRCCNT RTNCNT1 rtncnt2;
OUTPUT OUT=AGTSUM (DROP=_TYPE_ _FREQ_)
SUM=
;
RUN;
'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
df = pd.read_sql_query(
    "select * from SUMDB where agttyp not in ('SSA', 'EA', 'MSC', 'HB')", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'SUMDB')
str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
agg_cols = {'dim', 'dimval', 'nfrccnt', 'rtncnt1', 'rtncnt2'}
final_cols = list(agg_cols.intersection(str_cols))
df[final_cols] = df[final_cols].fillna(value='')
df = df.groupby(['mnthnd', 'system', 'product', 'state', 'dimseq', 'valseq']).agg(
    {'dim': max, 'dimval': max, 'nfrccnt': sum, 'rtncnt1': sum, 'rtncnt2': sum}).reset_index()
df_creation_logging(df, "AGTSUM")
df.to_sql("AGTSUM", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# df = pd.read_sql_query(
# "select * from SUMDB where agttyp not in ('SSA', 'EA', 'MSC', 'HB')", sqliteConnection)
##handling data frame column case senstivity.#
# df_lower_colNames(df, 'SUMDB')
# grouped_df = df.groupby(['mnthnd', 'system', 'product', 'state', 'dimseq', 'valseq']).agg(
# {'dim': max, 'dimval': max, 'nfrccnt': sum, 'rtncnt1': sum, 'rtncnt2': sum}).reset_index()
'''grp_lst = ['mnthnd', 'system', 'product', 'state', 'agttyp',
           'dimseq', 'valseq']  # Take the columns in Class
id_list = ['dim', 'dimval']  # Take the columns in ID
grouped_df = pd.DataFrame()
cnt = 0
df_0 = df.drop(columns=grp_lst)
df_0_summ = df_0[['nfrccnt', 'rtncnt1', 'rtncnt2']].sum()

for i in range(1, len(grp_lst)+1):
    for j in itertools.combinations(grp_lst, i):
        cnt = cnt + 1
        df1 = df.groupby(list(j))[
            ['nfrccnt', 'rtncnt1', 'rtncnt2']].sum().reset_index()
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
grouped_df = grouped_df[grp_lst+id_list + ['nfrccnt', 'rtncnt1', 'rtncnt2']]'''
# df_creation_logging(grouped_df, "AGTSUM")
# grouped_df.to_sql("AGTSUM", con=sqliteConnection, if_exists='replace')
# sqliteConnection.close()

### SAS Source Code Line Numbers START:1086 & END:1086.###
'''SAS Comment:*CREATE THE "All Agents" AGTTYP; '''
### SAS Source Code Line Numbers START:1087 & END:1090.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
********************************************************************************* '''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from AGTSUM ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'AGTSUM')
df['agttyp'] = 'All Agents'
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "AGTSUM2 created successfully with {} records".format(len(df)))
df.to_sql("AGTSUM2", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1094 & END:1094.###
'''SAS Comment:*CONCATENATE "All Agents" AGTTYP WITH THE OTHER AGTTYPs; '''
### SAS Source Code Line Numbers START:1095 & END:1097.###
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source AGTSUM2 data into datafram.
AGTSUM2 = pd.read_sql_query("select * from AGTSUM2 ", sqliteConnection)
df_lower_colNames(AGTSUM2, 'AGTSUM2')
# Converting source SUMDB data into datafram.
SUMDB = pd.read_sql_query("select * from SUMDB ", sqliteConnection)
df_lower_colNames(SUMDB, 'SUMDB')
# Concatenate the source data frames
NEWSUM = pd.concat([AGTSUM2, SUMDB], ignore_index=True, sort=False)
# Push results data frame to Sqlite DB
NEWSUM = df_remove_indexCols(NEWSUM)
df_creation_logging(NEWSUM, "NEWSUM")
NEWSUM.to_sql("NEWSUM", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

### SAS Source Code Line Numbers START:1101 & END:1101.###
'''SAS Comment:*Calculate the retention ratio for all rows; '''
### SAS Source Code Line Numbers START:1102 & END:1108.###
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
df['rtnratio1'] = df['rtncnt1'] / df['nfrccnt']
df['rtnratio2'] = df['rtncnt2'] / df['nfrccnt']
df['migrind'] = 'N'
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "NFRCDB created successfully with {} records".format(len(df)))
df.to_sql("NFRCDB", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1110 & END:1112.###
'''
Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
Sometimes this step isnt necessary based on the scenario of execution,hence it can be commented out if you want.'''
# Sql Code Start and End Lines - 1110&1112 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
#     sql = """DROP TABLE IF EXISTS NFRCDB_sqlitesorted;CREATE TABLE NFRCDB_sqlitesorted AS SELECT * FROM NFRCDB ORDER BY
#         STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE NFRCDB;ALTER TABLE
#         NFRCDB_sqlitesorted RENAME TO NFRCDB"""
#     sql = mcrResl(sql)
#     tgtSqliteTable = "NFRCDB_sqlitesorted"
#     procSql_standard_Exec(sqliteDb,sql,tgtSqliteTable)
# except:
#    e = sys.exc_info()[0]
#    logging.error('Table creation/update is failed.')
#    logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:1115 & END:1115.###
'''SAS Comment:*Create an interim, inforce database. This data must be further manipulated to create rows for  ; '''
### SAS Source Code Line Numbers START:1116 & END:1116.###
'''SAS Comment:*missing valseq values across the various dimensions.                                       ; '''
### SAS Source Code Line Numbers START:1117 & END:1137.###
'''
# Sql Code Start and End Lines - 1117&1137 #
***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''

# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS NTRMOUT;CREATE TABLE NTRMOUT AS SELECT mnthnd ,SYSTEM ,PRODUCT ,AGTTYP ,STATE
        ,MIGRIND ,DIM ,DIMSEQ ,DIMVAL ,VALSEQ ,NFRCCNT ,RTNCNT1 ,RTNRATIO1 ,rtncnt2
        ,rtnratio2 FROM NFRCDB"""
    sql = mcrResl(sql)
    tgtSqliteTable = "NTRMOUT"
    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:1143 & END:1143.###
'''SAS Comment:****************************PART 4***FIX THE DB*************************************************; '''
### SAS Source Code Line Numbers START:1145 & END:1147.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
# appending the data
'''PROC APPEND BASE=WORK.rnwlout DATA=WORK.NTRMOUT;'''
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source NFRCOUT data into datafram.
NFRCOUT = pd.read_sql_query("select * from NTRMOUT ", sqliteConnection)
NFRCOUT.to_sql("rnwlout", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()


# Sql Code Start and End Lines - 1145&1147 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
#     sql = """DROP TABLE IF EXISTS rnwlout_sqlitesorted;CREATE TABLE rnwlout_sqlitesorted AS SELECT * FROM rnwlout ORDER BY
#         mnthnd,system,state,agttyp,dimseq,valseq;DROP TABLE rnwlout;ALTER TABLE
#         rnwlout_sqlitesorted RENAME TO rnwlout"""
#     sql = mcrResl(sql)
#     tgtSqliteTable = "rnwlout_sqlitesorted"
#     procSql_standard_Exec(sqliteDb,sql,tgtSqliteTable)
# except:
#    e = sys.exc_info()[0]
#    logging.error('Table creation/update is failed.')
#    logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:1149 & END:1149.###
'''SAS Comment:*There may be some dimensions that do not have counts for every valseq. This code will create   ; '''
### SAS Source Code Line Numbers START:1150 & END:1150.###
'''SAS Comment:*missing valseq numbers and zero out the counts. This is required for the report writer to put  ; '''
### SAS Source Code Line Numbers START:1151 & END:1151.###
'''SAS Comment:*the counts in the right column of the spreadsheet. There must be rows for each valseq for each ; '''
### SAS Source Code Line Numbers START:1152 & END:1152.###
'''SAS Comment:*dimension. The DIMVAL is not used in the report writer. Therefore, this field is left blank    ; '''
### SAS Source Code Line Numbers START:1153 & END:1153.###
'''SAS Comment:*when the row is created.                                                                       ; '''
### SAS Source Code Line Numbers START:1155 & END:1184.###
'''*********************************************************************************
Below python code is to execute SAS data step with BY varaible in python
*********************************************************************************'''

'''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
retain;frstdim = first.dimseq;else cnt = cnt + 1;seqhold = valseq;output;if seqhold = cnt then return;else do until (seqhold = cnt);nfrccnt = .;rtncnt1 = .;rtnratio1 = .;rtncnt2 = .;rtnratio2 = .;dimval = ' ';valseq = cnt;output;cnt = cnt + 1;end;'''
# Open connection to Sqlite work data base
#  Shradha working on fxdnfrc

# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source NFRCOUT data into datafram.
df = pd.read_sql_query("select * from rnwlout", sqliteConnection)
# lowering all column names#Generate first and last temporary indicators in the given data frames.
df_lower_colNames(df)
var_list = ['mnthnd', 'system', 'state', 'agttyp', 'dimseq']
df = df.sort_values(by=var_list)
df['IsFirst'], df['IsLast'] = [False, False]
df.loc[df.groupby(var_list)['IsFirst'].head(1).index, 'IsFirst'] = True
df.loc[df.groupby(var_list)['IsLast'].tail(1).index, 'IsLast'] = True
df['cnt'] = df.groupby(var_list).cumcount() + 1
df_grouped = df.groupby(var_list)['valseq'].max()
# Output first occurance values in data to the target data frame.
df['frstdim'] = df['IsFirst']
df['seqhold'] = df['valseq']
df = df.drop(columns=['IsFirst', 'IsLast'])
df_new = df.set_index(var_list)
d = df_grouped.to_dict()
temp_df = pd.DataFrame(columns=df.columns)
out = []
for index, new_df in df_new.groupby(level=list(range(len(var_list)))):
    for val in range(1, d[index] + 1):
        if val not in new_df['valseq'].to_list():
            ungroup_df = new_df.reset_index()
            temp = ungroup_df.iloc[0]
            temp['nfrccnt'] = np.nan
            temp['rtncnt1'] = np.nan
            temp['rtnratio1'] = np.nan
            temp['rtncnt2'] = np.nan
            temp['rtnratio2'] = np.nan
            temp['dimval'] = np.nan
            temp['valseq'] = val
            temp_df = temp_df.append(temp)
fxdnfrc = pd.concat([df, temp_df], ignore_index=True, sort=False)
fxdnfrc = df_remove_indexCols(fxdnfrc)
df_creation_logging(fxdnfrc, "fxdnfrc")
# Push results data frame to Sqlite DB
fxdnfrc.to_sql("fxdnfrc", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()

# Close connection to Sqlite work data base
sqliteConnection.close()


'''sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source rnwlout data into datafram.
rnwlout = pd.read_sql_query("select * from rnwlout ",sqliteConnection)
df_lower_colNames(rnwlout,'rnwlout') #lowering all column names#Generate first and last temporary indicators in the given data frames.
df = df.sort_values(by = ['mnthnd','system','state','agttyp','dimseq','valseq'])
rnwlout_grouped = rnwlout
rnwlout_grouped['IsFirst'] ,rnwlout_grouped['IsLast'] = [False, False]
rnwlout_grouped.loc[rnwlout_grouped.groupby(['mnthnd','system','state','agttyp','dimseq','valseq'])['IsFirst'].head(1).index, 'IsFirst'] = True
rnwlout_grouped.loc[rnwlout_grouped.groupby(['mnthnd','system','state','agttyp','dimseq','valseq'])['IsLast'].tail(1).index, 'IsLast'] = True
# Output first occurance values in data to the target data frame.
fxdnfrc = rnwlout_grouped[(rnwlout_grouped['IsFirst'])]# Drop indicator tmp columns
fxdnfrc = fxdnfrc.drop(columns=['IsFirst','IsLast'])

# Push results data frame to Sqlite DB
fxdnfrc = df_remove_indexCols(fxdnfrc)
df_creation_logging(fxdnfrc)
fxdnfrc.to_sql("fxdnfrc",con=sqliteConnection,if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
'''
'''*******************************End of Merge Process**************************************************'''

### SAS Source Code Line Numbers START:1185 & END:1185.###
'''SAS Comment:*END FIX DB   END FIX DB     END FIX DB     END FIX DB    END FIX DB     END FIX DB; '''
### SAS Source Code Line Numbers START:1188 & END:1188.###
'''SAS Comment:*Backup current database; '''
### SAS Source Code Line Numbers START:1189 & END:1191.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
with SAS7BDAT("/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/rtnrnwl_homedb.sas7bdat") as reader:
    df = reader.to_data_frame()
    df_lower_colNames(df, 'rtnrnwl_homedb')
    df['mnthnd'] = df.mnthnd.astype('Int64')
    df = df.loc[df.mnthnd < int(curr_ccyymm)]
    df.to_sql("nfrcout_rtnrnwl_homedb",
              con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
# sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# reading the file from csv
# df = pd.read_csv(
    # "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_rnwlhome/data/nfrcout_rtnrnwl_homedb.csv")
# lowering all columns
# df_lower_colNames(df, 'nfrcout_rtnrnwl_homedb')
# logging info
# df_creation_logging(df, "nfrcout_rtnrnwl_homedb")
# putting into the sqliteDB
# df.to_sql("nfrcout_rtnrnwl_homedb", con=sqliteConnection,
    # if_exists='replace', index=True)
# sqliteConnection.close()

# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query(
    "select * from nfrcout_rtnrnwl_homedb ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'nfrcout_rtnrnwl_homedb')
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "nfrcout_rtnrnwl_homedb_backup created successfully with {} records".format(len(df)))
df.to_sql("nfrcout_rtnrnwl_homedb_backup",
          con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1194 & END:1194.###
'''SAS Comment:**************************************PART 5****************************************************; '''
### SAS Source Code Line Numbers START:1195 & END:1195.###
'''SAS Comment:*This sort basically deletes the row from a month ago each run as it is a partial row. It does  ; '''
### SAS Source Code Line Numbers START:1196 & END:1196.###
'''SAS Comment:*not contain the renewal retention one month expired data. Therefore, it is deleted and         ; '''
### SAS Source Code Line Numbers START:1197 & END:1197.###
'''SAS Comment:*recreated each month when the data is available.                                               ; '''
### SAS Source Code Line Numbers START:1198 & END:1201.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 1198&1201 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
try:
    sql = """DROP TABLE IF EXISTS nfrcout.rtnrnwl_homedb_sqlitesorted;CREATE TABLE nfrcout_rtnrnwl_homedb_sqlitesorted AS SELECT * FROM
        nfrcout_rtnrnwl_homedb where mnthnd < {} ORDER BY
        mnthnd,SYSTEM,PRODUCT,STATE,AGTTYP,DIMSEQ,VALSEQ;DROP TABLE
        nfrcout_rtnrnwl_homedb;ALTER TABLE nfrcout_rtnrnwl_homedb_sqlitesorted RENAME TO
        nfrcout_rtnrnwl_homedb""".format(amnthago)
    sql = mcrResl(sql)
    tgtSqliteTable = "nfrcout_rtnrnwl_homedb_sqlitesorted"
    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
except:
    e = sys.exc_info()[0]
    logging.error('Table creation/update is failed.')
    logging.error('Error - {}'.format(e))


### SAS Source Code Line Numbers START:1204 & END:1204.###

'''WARNING SAS commnet block detected.
Any SAS steps within the block are converted to python code but commented.'''

# Sql Code Start and End Lines - 0&0 #
"""***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************"""
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
# try:
#     sql = """*Concatenate the database with the fixed inforce file for the current run"""
#     sql = mcrResl(sql)
#     tgtSqliteTable = ""
#     procSql_standard_Exec(sqliteDb,sql,tgtSqliteTable)
# except:
#    e = sys.exc_info()[0]
#    logging.error('Table creation/update is failed.')
#    logging.error('Error - {}'.format(e))

### SAS Source Code Line Numbers START:1205 & END:1209.###
'''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
drop frstdim cnt seqhold;'''
# Converting source nfrcout.rtnrnwl_homedb data into datafram.
# ToDo
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
nfrcout_rtnrnwl_homedb = pd.read_sql_query(
    "select * from nfrcout_rtnrnwl_homedb ", sqliteConnection)
df_lower_colNames(nfrcout_rtnrnwl_homedb, 'nfrcout_rtnrnwl_homedb')
# Converting source fxdnfrc data into datafram.
fxdnfrc = pd.read_sql_query("select * from fxdnfrc ", sqliteConnection)
df_lower_colNames(fxdnfrc, 'fxdnfrc')
# Concatenate the source data frames
rnwldb = pd.concat([nfrcout_rtnrnwl_homedb, fxdnfrc],
                   ignore_index=True, sort=False)
# droping COLUMNS
rnwldb = rnwldb.drop(['frstdim', 'cnt', 'seqhold'], axis=1)
# Push results data frame to Sqlite DB
rnwldb = df_remove_indexCols(rnwldb)
df_creation_logging(rnwldb, "rnwldb")
rnwldb.to_sql("rnwldb", con=sqliteConnection, if_exists='replace')
sqliteConnection.close()

### SAS Source Code Line Numbers START:1211 & END:1213.###

''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
 Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

# Sql Code Start and End Lines - 1211&1213 #
'''***************************************************
Below Python Code Executes The Standard SAS PROC SQL.
******************************************************'''
# Connections to Sqlite DB and fetch all data from source table to process
# Please check if any SAS functions are not converted in SqLite query.
'''try:
    sql = """DROP TABLE IF EXISTS rnwldb_sqlitesorted;CREATE TABLE rnwldb_sqlitesorted AS SELECT * FROM rnwldb ORDER BY
        mnthnd,system,product,state,agttyp,dimseq,valseq;DROP TABLE rnwldb;ALTER
        TABLE rnwldb_sqlitesorted RENAME TO rnwldb"""
    sql = mcrResl(sql)
    tgtSqliteTable = "rnwldb_sqlitesorted"
    procSql_standard_Exec(sqliteDb,sql,tgtSqliteTable)
except:
   e = sys.exc_info()[0]
   logging.error('Table creation/update is failed.')
   logging.error('Error - {}'.format(e))'''


### SAS Source Code Line Numbers START:1215 & END:1215.###
'''SAS Comment:*Output the final database; '''
### SAS Source Code Line Numbers START:1216 & END:1218.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# ToDo
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from rnwldb ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'rnwldb')
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "nfrcout_rtnrnwl_homedb created successfully with {} records".format(len(df)))
df.to_sql("nfrcout_rtnrnwl_homedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1221 & END:1221.###
'''SAS Comment:**************************************PART 6****************************************************; '''
### SAS Source Code Line Numbers START:1222 & END:1222.###
'''SAS Comment:*THE FOLLOWING WAS ADDED FOR BALANCING PURPOSES ONLY; '''
### SAS Source Code Line Numbers START:1224 & END:1224.###
'''SAS Comment:*Inforce renewal counts for two months ago. The column MNTHND reflects the renewal month, not   ; '''
### SAS Source Code Line Numbers START:1225 & END:1225.###
'''SAS Comment:*the inforce file examined for renewal.                                                         ; '''
### SAS Source Code Line Numbers START:1226 & END:1226.###
'''SAS Comment:*The SSA and EA are accounted for in the CAPTIVE/EA category. They exist rolled into CAPTIVE/EA ; '''
### SAS Source Code Line Numbers START:1227 & END:1227.###
'''SAS Comment:*and separately. This was done for flexibility to be able to report them broken out or grouped  ; '''
### SAS Source Code Line Numbers START:1228 & END:1228.###
'''SAS Comment:*into a single category. But, for balancing, you only want to count them once. Therefore, the   ; '''
### SAS Source Code Line Numbers START:1229 & END:1229.###
'''SAS Comment:*separate categories of SSA and EA are excluded. The total is captured in the CAPTIVE/EA category; '''
### SAS Source Code Line Numbers START:1230 & END:1230.###
'''SAS Comment:*where SSA and EA are combined.; '''
### SAS Source Code Line Numbers START:1231 & END:1239.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'MI') & (df.mnthnd == amnthago) & (df.syscd == 'L') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]
# where state = 'MI' and mnthnd = &amnthago and syscd = 'L' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1241 & END:1249.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'MI') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]
# where state = 'MI' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1251 & END:1259.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state != 'MI') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]
# where state not = 'MI' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1261 & END:1261.###
'''SAS Comment:*Regional breakdown; '''
### SAS Source Code Line Numbers START:1262 & END:1270.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'IA') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'IA' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1272 & END:1280.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'IL') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'IL' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1282 & END:1290.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'IN') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]
# where state = 'IN' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#


### SAS Source Code Line Numbers START:1292 & END:1300.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'KY') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'KY' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DBdf = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df = df_remove_indexCols(df)
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1302 & END:1310.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'MN') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]
# where state = 'MN' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1312 & END:1320.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'OH') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'OH' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1322 & END:1330.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'WI') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'WI' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1332 & END:1340.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'WV') & (df.mnthnd == amnthago) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'WV' and mnthnd = &amnthago and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1342 & END:1342.###
'''SAS Comment:*Inforce renewal counts for a month ago; '''
### SAS Source Code Line Numbers START:1343 & END:1351.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'MI') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'L') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'MI' and mnthnd = &curr_ccyymm and syscd = 'L' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1353 & END:1361.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'MI') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'MI' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1363 & END:1371.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state != 'MI') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state not = 'MI' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1374 & END:1374.###
'''SAS Comment:*Regional breakdown; '''
### SAS Source Code Line Numbers START:1375 & END:1383.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'IA') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'IA' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1385 & END:1393.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'IL') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'IL' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1395 & END:1403.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'IN') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'IN' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1405 & END:1413.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'KY') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'KY' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1415 & END:1423.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'MN') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'MN' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1425 & END:1433.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'OH') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'OH' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1435 & END:1443.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'WI') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'WI' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1445 & END:1453.###
'''*********************************************************************************
Below python code is to execute standard SAS data step
*********************************************************************************'''
# Please Note - If any library references remove them accordingly post your code analysis.#
# Open connection to Sqlite work data base
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
# Converting source df data into datafram.
df = pd.read_sql_query("select * from homenfrc ", sqliteConnection)
# handling data frame column case senstivity.#
df_lower_colNames(df, 'homenfrc')
df = df.loc[(df.state == 'WV') & (df.mnthnd == curr_ccyymm) & (df.syscd == 'I') & (
    df.dimseq == 1) & (~df.agttyp.isin(['SSA', 'EA', 'MSC', 'HB']))]

# where state = 'WV' and mnthnd = &curr_ccyymm and syscd = 'I' and dimseq = 1 and agttyp not in ('SSA', 'EA', 'MSC', 'HB'); # Manual effort require.
# Push results data frame to Sqlite DB
df = df_remove_indexCols(df)
logging.info(
    "statedb created successfully with {} records".format(len(df)))
df.to_sql("statedb", con=sqliteConnection, if_exists='replace')
# Close connection to Sqlite work data base
sqliteConnection.close()
#*******************************End of Data Step Process**************************************************#

### SAS Source Code Line Numbers START:1458 & END:1458.###
'''SAS Comment:*** Writes SAS Log to a text file ***; '''
### SAS Source Code Line Numbers START:1459 & END:1459.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
%SaveLog(NAME=RenewalRatio_Homedb,DIR=T:\Shared\Acturial\BISLogs\RenewalRatio\)
'''

### SAS Source Code Line Numbers START:1461 & END:1461.###
'''SAS Comment:*** Close the stored compiled macro catalog and clear the previous libref  ***; '''
### SAS Source Code Line Numbers START:1465 & END:1465.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
libname _all_ clear;
'''
