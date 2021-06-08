# -*- coding: utf-8 -*-
r'''
Created on: Mon 14 Dec 20 18:09:21

Author: SAS2PY Code Conversion Tool

SAS Input File: gw_ipm_hhldrtn regional auto
SAS File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS_SRC_CDE

Generated Python File: Sas2PyConvertedScript_Out
Python File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS2PY_TRANSLATED
'''

''' Importing necessary standard Python 3 modules

Please uncomment the commented modules if necessary. '''
# import pyodbc
# import teradata
# import textwrap
# import subprocess


''' Importing necessary project specific core utility python modules.'''
'''Please update the below path according to your project specification where core SAS to Python code conversion core modules stored'''
import logging
from sas2py_func_lib_repo_acg import *
from sas2py_code_converter_funcs_acg import *
import sys
import re
import itertools
from sas7bdat import SAS7BDAT
import sqlite3
import psutil
import os
import gc
import pandas as pd
import numpy as np
from functools import reduce, partial
sys.path.insert(
    1, r'C:\Users\vegopi\PycharmProjects\SAS2pyRepo\Sas2PyUtilCore')
# from sas2py_sqlite3_db_funcs_lib import *


# Seting up logging info #
'''You can redirect program log to a file,please provide log name and path you want and uncomment the line
logging.basicConfig(filename='<<log name here>>',level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')'''
logging.basicConfig(filename='/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/logs/ipm_rrc_hhldrtn_regional_auto.log',
                    level=logging.INFO, format='%(asctime)s - '+'GW_IPM_HHLDRTN REGIONAL AUTO'+' %(levelname)s - %(message)s')

''' Creating temporary sqlite working DB to store all temporay stating results '''
SQLitePythonWorkDb = '/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/ipm_rrc_hhldrtn_regional_auto.db'
sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)

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
'''SAS Comment:************************ REVISION LOG *********************************; '''
### SAS Source Code Line Numbers START:3 & END:3.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:4 & END:4.###
'''SAS Comment:*                                                                      ; '''
### SAS Source Code Line Numbers START:5 & END:5.###
'''SAS Comment:*   DATE        INIT     DESCRIPTION OF CHANGE                         ; '''
### SAS Source Code Line Numbers START:6 & END:6.###
'''SAS Comment:*   01/17       PLT      Added HHLD Incidents dimension                ; '''
### SAS Source Code Line Numbers START:7 & END:7.###
'''SAS Comment:***********************************************************************; '''
### SAS Source Code Line Numbers START:8 & END:8.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
libname Agent "T:\Shared\Acturial\Special Projects\Research\Agents\Business Intelligence";
'''

### SAS Source Code Line Numbers START:10 & END:10.###

'''WARNING Keyword SAS Code identified unable to convert due to functionality development for this step is in progress.
Please find below Please find below SAS code lines.
options mprint;
'''

### SAS Source Code Line Numbers START:12 & END:1868.###

''' WARNING  SAS User Defined Macro Identified. Macro has been re-written in python. Code validation and intendation is required.'''


def Retention(Y1, Y2, GY1, GY2):
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from retain_Y2", sqliteConnection)
    df_lower_colNames(df, 'retain_Y2')
    df['agent'] = df['agent'].astype('Int64')
    df = df_remove_indexCols(df)
    logging.info(
        "Retain{} created successfully with {} records".format(Y2, len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("Retain{}".format(Y2), con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
	
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df1 = pd.read_sql_query(
        "select * from Retain{}".format(Y2), sqliteConnection)
    try:
	    df1 = df_remove_indexCols(df1)
    except:
	    pass
    df1.to_sql("Retain{}_temp".format(Y2), con=sqliteConnection, if_exists='replace')
    # handling data frame column case senstivity.#
    df_lower_colNames(df1, 'Retain{}'.format(Y2))
    # Converting source df data into datafram.
    df2 = pd.read_sql_query(
        "select * from GWretain{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df2, 'GWretain{}'.format(Y2))
    df = pd.concat([df1, df2], ignore_index=True, sort=False)
    # df['agey1'] = df['agey1'].apply(np.floor)
    # df['ageo1'] = df['ageo1'].apply(np.floor)
    # End manual effort.***'''
    df['agenttype'] = "Unknown"
    df['agent'] = df['agent'].astype(str)
    df.loc[df['agent'].isin(['1', '7', '8', '01', '07', '08', '1.0', '7.0', '8.0']), 'agenttype'] = 'Captive'
    # ***Start manual effort here...
    df.loc[df.agent.isin(['9', '09', '9.0']), 'agenttype'] = 'EA'
    # else if Agent = 9 then AgentType="EA";
    # End manual effort.***'''

    # ***Start manual effort here...
    df.loc[df['agent'].isin(['4', '04', '4.0', '3', '03', '3.0']), 'agenttype'] = 'MSC/HB'
    # else if Agent in (3 4) then AgentType="MSC/HB";
    # End manual effort.***'''
    # ***Start manual effort here...
    df['aaa_ec'] = df['aaa_ec'].astype(str)
    df.loc[(df['mvstate'].isin(['KY', 'WV', 'OH']))
           & (df.aaa_ec.isin(['1', '1.0'])) & ~(df['agent'].isin(['1', '7', '8', '01', '07', '08', '1.0', '7.0', '8.0', '9', '09', '9.0', '4', '04', '4.0', '3', '03', '3.0'])), 'agenttype'] = 'EC'
    df.loc[(df['agent'].isin(['2', '02', '2.0', '6', '06', '6.0'])) & ~((df['mvstate'].isin(
        ['KY', 'WV', 'OH'])) & (df.aaa_ec.isin(['1', '1.0']))), 'agenttype'] = 'IA'


    df.loc[df.agenttype == 'Unknown', 'agenttype'] = 'MSC/HB'
	
    df['coverage'] = "Mixed Full Liability Comp/Collision"
    # df['coverage']=["Full Coverage" if a =="VehCent" else "Liability Only" if b==0 else "Comp/Collision Only" if c==99 else "Mixed Full Liability" if x==1 else "Mixed Full Comp/Collision" if (d==1 & b==99) else "Mixed Liability Comp/Collision" if e!=1 for a,b,c,d,e in zip(df['CovSum'],df['CovMax'],df['CovAvg'],df['CovMax'],df['CovMin'],df['VehFullCov'])]
    df.loc[df.covsum == df.vehcnt, 'coverage'] = "Full Coverage"
    df.loc[((df.covmax == 0) | (df.covmax == 0.0)) &  (df['coverage'] == "Mixed Full Liability Comp/Collision"), 'coverage'] = "Liability Only"
    df.loc[((df.covavg == 99) | (df.covavg == 99.0)) &  (df['coverage'] == "Mixed Full Liability Comp/Collision"), 'coverage'] = "Comp/Collision Only"
    df.loc[((df.covmax == 1) | (df.covmax == 1.0)) &  (df['coverage'] == "Mixed Full Liability Comp/Collision"), 'coverage'] = "Mixed Full Liability"
    df.loc[(df.covmin.isin([1, 1.0])) & (df.covmax.isin([99, 99.0])) &  (df['coverage'] == "Mixed Full Liability Comp/Collision"),'coverage'] = "Mixed Full Comp/Collision"
    df.loc[~(df.vehfullcov.isin([1, 1.0])) &  (df['coverage'] == "Mixed Full Liability Comp/Collision"), 'coverage'] = "Mixed Liability Comp/Collision"

    df.loc[(df.coverage == "Full Coverage") | (df.coverage ==
                                               "Mixed Full Comp/Collision"), 'coverage'] = "Full Cov"
    df.loc[df.coverage == "Liability Only", 'coverage'] = "Lia Only"
    df.loc[(df.coverage == "Comp/Collision Only") | (df.coverage == "Mixed Full Liability") | (df.coverage == "Mixed Liability Comp/Collision")
           | (df.coverage == "Mixed Full Liability Comp/Collision"), 'coverage'] = "Mixed"

    # ***Start manual effort here...
    # else PremierGrp="High";
    df['premier'] = df['premier'].astype('Int64')
    df['premiergrp'] = ['Low' if x == 1 else 'Med' if x ==
                        2 else 'High' for x in df['premier']]

    df['premier2'] = df['premier2'].apply(np.int64)
    df['premiergrp2'] = ['Low' if x == 1 else 'Mid-Low' if x ==
                         2 else 'Mid-High' if x == 3 else 'High' for x in df['premier2']]

    df['multiprod'] = ['No' if x == 0 else 'Yes' for x in df['mulprod']]

    df['mem'] = ['No' if x == 0 else 'Yes' for x in df['member']]

    df['pifd'] = df['pifd'].astype(str)
    df['pif'] = ['No' if ((x == '0') | (x == '0.0')) else 'Yes' for x in df['pifd']]

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


    df.loc[(df['mvstate'].isin(['MN'])), 'priorinsstatus'] = "N/A"
    df.loc[(df['mvstate'].isin(['MN'])) & (df['prins'].isin(
        [1212, 1222])), 'priorinsstatus'] = "<100/300"
    df.loc[(df['mvstate'].isin(['MN'])) & (
        df['prins'].isin([1112, 1122])), 'priorinsstatus'] = "30/60"
    df.loc[(df['mvstate'].isin(['MN'])) & (df['prins'].isin(
        [1312, 1322])), 'priorinsstatus'] = ">=100/300"
    # ***Start manual effort here...

    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA',
                                'IN', 'NE', 'ND'])) & (df.system == 'I'), 'priorinsstatus'] = "N/A"
    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA', 'IN', 'NE', 'ND'])) & (
        df['prins'].isin([1212, 1222])) & (df.system == 'I'), 'priorinsstatus'] = "<100/300"
    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA', 'IN', 'NE', 'ND'])) & (
        df['prins'].isin([1112, 1122])) & (df.system == 'I'), 'priorinsstatus'] = "25/50"
    df.loc[(df['mvstate'].isin(['WI', 'KY', 'OH', 'TN', 'GA', 'IN', 'NE', 'ND'])) & (
        df['prins'].isin([1312, 1322])) & (df.system == 'I'), 'priorinsstatus'] = ">=100/300"
    # if mvstate in ('WI' 'KY' 'OH' 'TN' 'GA' 'IN' 'NE' 'ND') and system='I' then do;
    # if prins in (1212 1222) then PriorInsStatus="<100/300";
    # else if prins in (1112 1122) then PriorInsStatus="25/50";
    # else if prins in (1312 1322) then PriorInsStatus=">=100/300";
    # else PriorInsStatus="N/A";
    # end;
    # End manual effort.***

    # ***Start manual effort here...
    df.loc[(df['mvstate'].isin(['GA', 'TN'])) & (df.system == 'G'), 'priorinsstatus'] = "N/A"
    df.loc[(df['mvstate'].isin(['GA', 'TN'])) & (
        df['prins'].isin([1212])) & (df.system == 'G'), 'priorinsstatus'] = "<100/300"
    df.loc[(df['mvstate'].isin(['GA', 'TN'])) & (
        df['prins'].isin([1112])) & (df.system == 'G'), 'priorinsstatus'] = "<=25/50"
    df.loc[(df['mvstate'].isin(['GA', 'TN'])) & (
        df['prins'].isin([1312])) & (df.system == 'G'), 'priorinsstatus'] = ">=100/300"
    # df = df.loc[df.mvstate == 'IL']
    # if mvstate = 'IL' then do;
    # if prins in (1212 1222) then PriorInsStatus="<100/300";
    # else if prins in (1112 1122) then PriorInsStatus="<=25/50";
    # else if prins in (1312 1322) then PriorInsStatus=">=100/300";
    # else PriorInsStatus="N/A";
    # end;
    df.loc[(df['mvstate'].isin(['IL'])), 'priorinsstatus'] = "N/A"
    df.loc[(df['mvstate'].isin(['IL'])) & (df['prins'].isin(
        [1212, 1222])), 'priorinsstatus'] = "<100/300"
    df.loc[(df['mvstate'].isin(['IL'])) & (
        df['prins'].isin([1112, 1122])), 'priorinsstatus'] = "<=25/50"
    df.loc[(df['mvstate'].isin(['IL'])) & (df['prins'].isin(
        [1312, 1322])), 'priorinsstatus'] = ">=100/300"

    df.loc[(df['mvstate'].isin(['FL'])), 'priorinsstatus'] = "N/A"
    df.loc[(df['mvstate'].isin(['FL'])) & (df['prins'].isin(
        [1612, 1712])), 'priorinsstatus'] = "<100/300"
    df.loc[(df['mvstate'].isin(['FL'])) & (df['prins'].isin(
        [1212, 1312, 1412, 1512])), 'priorinsstatus'] = "<=25/50"
    df.loc[(df['mvstate'].isin(['FL'])) & (df['prins'].isin(
        [1812, 1912, 2012, 2112, 2212])), 'priorinsstatus'] = ">=100/300"
    # df = df.loc[df.mvstate == 'FL']

    # End manual effort.***
    df.loc[df.productgen == 'Gen1', 'priorinsstatus'] = ''

    # df.loc[df.system == 'I']
    df.loc[(df['system'] == 'I') &
           ((df.agey1 == np.nan) | (df.agey1.isnull()) | (df.agey1 == '') | (df.agey1 == ' ')), 'agey1'] = df['agey']
    df.loc[(df['system'] == 'I') & ((df.agey1 == np.nan) | (df.agey1.isnull()) | (df.agey1 == '') | (df.agey1 == ' ')),
           'agey1'] = df['assigneddrvy']
    df.loc[(df['system'] == 'G'), 'agey1'] = df['agey']
    # if AgeY1>=65 then AgeYoungest=">64"; # Manual effort require.
    df['agey1'] = pd.to_numeric(df['agey1'])
    df['ageyoungest'] = [">64" if x >= 65 else "45-64" if x >=
                         45 else "30-44" if x >= 30 else "25-29" if x >= 25 else "<25" for x in df['agey1']]
    # ***Start manual effort here...
    # ***Start manual effort here...

    # df = df.loc[df.system == 'I']
    df.loc[(df['system'] == 'I') &
           ((df.ageo1 == np.nan) | (df.ageo1.isnull()) | (df.ageo1 == '') | (df.ageo1 == ' ')), 'ageo1'] = df['ageo']
    df.loc[(df['system'] == 'I') & ((df.ageo1 == np.nan) | (df.ageo1.isnull()) | (df.ageo1 == '') | (df.ageo1 == ' ')),
           'ageo1'] = df['assigneddrvage']
    df.loc[(df['system'] == 'G'), 'ageo1'] = df['ageo']

    # ***Start manual effort here...
    df['ageo1'] = pd.to_numeric(df['ageo1'])
    df['ageoldest'] = ["<25" if x < 25 else "25-29" if x <
                       30 else "30-44" if x < 45 else "45-64" if x < 65 else ">64" for x in df['ageo1']]

    # else if AgeO1<30 then AgeOldest="25-29";
    # End manual effort.***'''
    df['vhlevel']=df['vhlevel'].fillna('')
    df['vhlevelw'] = ['N/A' if ((x ==
                      np.nan) | (x == '') | (x == ' ') | (x == 'N/A'))  else int(x) for x in df['vhlevel']]

    df['vhlevelw'] = [('0' + str(x)) if len(str(x)) == 1  else str(x) for x in df['vhlevelw']]
    # ***Start manual effort here...
    # else VHLevelW=put(vhlevel,z2.);
    # End manual effort.***'''

    # if productgen="Gen1" then VHLevelW=""; # Manual effort require.
    df.loc[df.productgen == 'Gen1', 'vhlevelw'] = ''

    # if vhlevelB1=. then VHLevelB="N/A"; # Manual effort require.
    df['vhlevelb1']=df['vhlevelb1'].fillna('')

    df['vhlevelb'] = ['N/A' if ((x ==
                      np.nan) | (x == '') | (x == ' ') | (x == 'N/A'))  else int(x) for x in df['vhlevelb1']]

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
    df['eft'] = ['Yes' if x == 1 else 'No' if x ==
                 0 else "N/A" for x in df['eft1']]
    # ***Start manual effort here...
    # else if EFT1=0 then EFT="No ";

    # ***Start manual effort here...
    # else EFT="";
    # End manual effort.***'''
    # df['surchrgpts']=df['surchrgpts'].fillna(0)
    df['surchrgpts'] = df['surchrgpts'].astype('Int64')
    # df['cved']=df['cved'].fillna(0)
    df['cved'] = df['cved'].astype('Int64')
    df['nohhincdt'] = df['nohhincdt'].astype('Int64')
    # df['incidents']=df['incidents'].astype('Int64')
    df.loc[(df['productgen'] == "Gen2") & (df['nohhincdt'] == 0)
           & (df['surchrgpts'] == 0), 'incidents'] = 1
    df.loc[(df['productgen'] == "Gen2") & ((df['nohhincdt'] > 2)
           | (df['cved'] == 99)) & df['incidents'] != 1, 'incidents'] = 5
    df.loc[(df['productgen'] == "Gen2") & (df['cved'] < 99) & (
        df['nohhincdt'] == 1) & (df['surchrgpts'] == 0) & ~df['incidents'].isin([1, 5]), 'incidents'] = 2
    df.loc[(df['productgen'] == "Gen2") & (df['cved'] < 99) & (
        df['nohhincdt'] < 2) & (df['surchrgpts'] > 0) & ~df['incidents'].isin([1, 5, 2]), 'incidents'] = 3
    df.loc[(df['productgen'] == "Gen2") & (df['cved'] < 99)
           & (df['nohhincdt'] == 2) & ~df['incidents'].isin([1, 5, 2, 3]), 'incidents'] = 4
    # length AAADrive $40; # Manual effort require.
    # if AAADriveDisc=1 then AAADrive="Yes"; # Manual effort require.
    df['aaadrivedisc'] = df['aaadrivedisc'].astype(str)
    df['aaadrive'] = ["Yes" if ((x == '1') | (
        x == '1.0')) else "No" for x in df['aaadrivedisc']]
    df.loc[df.productgen == 'Gen1', 'aaadrive'] = ""
    # ***Start manual effort here...
    # else AAADrive="No";
    # End manual effort.***'''
    df = df[['mem', 'mvstate', 'incidents', 'productgen', 'policy', 'retention', 'agenttype', 'coverage', 'cved', 'premiergrp', 'premiergrp2', 'multiprod',
             'tenure', 'priorinsstatus', 'ageyoungest', 'ageoldest',  'vhlevelw', 'vhlevelb1', 'vhlevelb', 'noveh', 'vehagen', 'eft', 'pif', 'aaadrive', 'system']]
    df = df_remove_indexCols(df)
    logging.info(
        "Retain{} created successfully with {} records".format(Y2, len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("Retain{}".format(Y2), con=sqliteConnection,
              if_exists='replace')
    sqliteConnection.close()
    # Converting source df data into datafram.


	
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from Retain{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retain{}'.format(Y2))
    df = df.rename(columns={"retention": "rtncnt", "mvstate": "state"})
    df['qtrnd'] = Y2
    df['product'] = 'AUT'
    df['migrind'] = 'N'
    # length agttyp $10; # Manual effort require.
    df['agttyp'] = df['agenttype']
    # df['tenure'] = df['tenure'].astype(str)
    df['cved'] = df['cved'].astype(str)
    df['vhlevelb'] = df['vhlevelb'].astype(str)
    df['vhlevelw'] = df['vhlevelw'].astype(str)
    df['policy'] = df['policy'].astype(np.int64)
    df['incidents'] = df['incidents'].astype('Int64')

    # length dim $25; # Manual effort require.
    df['dim'] = ' '
    # length dimval $40; # Manual effort require.
    df['dimval'] = ' '
    # length tenuretxt $40; # Manual effort require.

    # ***Start manual effort here...
    # if system = 'I' then do;
    # df = df.loc[df.system == 'I']
    df.loc[(df['system'] == 'I') & ((df['tenure'] == np.nan) | (df['tenure'] == 0)| df.tenure.isnull() | df.tenure == 0.0), 'tenuretxt'] = '0'
    df.loc[(df['system'] == 'I') & ((df['tenure'] == 1)), 'tenuretxt'] = '1'
    df.loc[(df['system'] == 'I') & (df['tenure'] == 2), 'tenuretxt'] = '2'
    df.loc[(df['system'] == 'I') & ((df['tenure'] == 3)
                                    | (df['tenure'] == 4)), 'tenuretxt'] = '3-4'
    df.loc[(df['system'] == 'I') & (df['tenure'] >= 5), 'tenuretxt'] = '5+'
    # if Tenure=. or tenure=0 then tenuretxt='0';
    # else if tenure=1 then tenuretxt='1';
    # else if tenure=2 then tenuretxt='2';
    # else if tenure=3 or tenure=4 then tenuretxt='3-4';
    # else if tenure>=5 then tenuretxt='5+';
    # length CVEDtxt $40;
    # if CVED=0 then CVEDtxt='0';
    # df['cvedtxt'] = ['0' if x == 0 else '1' if x == 1 else '2' if x == 2 else '3' if x ==
    #                 3 else '4' if x == 4 else 5 if x == 5 else '99' if x == 99 else x for x in df['cved']]
    df['cvedtxt'] = ['0' if x == '0.0' else '1' if x == '1.0' else '2' if x == '2.0' else '3' if x ==
                     '3.0' else '4' if x == '4.0' else '5' if x == '5.0' else '99' if x == '99.0' else x for x in df['cved']]

    # else if CVED=1 then CVEDtxt='1';
    # else if CVED=2 then CVEDtxt='2';
    # else if CVED=3 then CVEDtxt='3';
    # else if CVED=4 then CVEDtxt='4';
    # else if CVED=5 then CVEDtxt='5';
    # else if CVED=99 then CVEDtxt='99';
    # end;
    # End manual effort.***

    # ***Start manual effort here...
    # if system = 'G' then do;
    df.loc[(df['system'] == 'G') & (df['tenure'].isnull())
           | (df['tenure'] == 0), 'tenuretxt'] = '0'
    df.loc[(df['system'] == 'G') & (df['tenure'] == 1), 'tenuretxt'] = '1'
    df.loc[(df['system'] == 'G') & (df['tenure'].isin(
        [1.5, 2, 2.5])), 'tenuretxt'] = '1.5-2.5'
    df.loc[(df['system'] == 'G') & (df['tenure'].isin(
        [3, 3.5, 4, 4.5])), 'tenuretxt'] = '3-4.5'
    df.loc[(df['system'] == 'G') & (df['tenure'] > 4.5), 'tenuretxt'] = '4.5+'
    # if tenure=. or tenure=0 then tenuretxt='0';
    # else if tenure=1 then tenuretxt='1';
    # else if tenure in (1,1.5,2,2.5) then tenuretxt='1.5-2.5';
    # else if tenure in (3,3.5,4,4.5) then tenuretxt='3-4.5';
    # else if tenure>4.5 then tenuretxt='4.5+';
    # end;
    # End manual effort.***

    # length VHlevelBtxt $40; # Manual effort require.
    # if VHlevelB='N/A' then VHlevelBtxt='N/A'; # Manual effort require.

    df.loc[df['vhlevelb'] == 'N/A', 'vhlevelbtxt'] = 'N/A'
    df.loc[(df['vhlevelb'].isin(['1', '01', '02', '2', '3', '03'])), 'vhlevelbtxt'] = '1-3'
    df.loc[(df['vhlevelb'].isin(['4', '04', '5', '05',
                                 '6', '06', '7', '07'])), 'vhlevelbtxt'] = '4-7'
    df.loc[(df['vhlevelb'].isin(['08', '09', '10', '11']) & (
        (df['state'] == "MN") | (df['system'] == 'G'))), 'vhlevelbtxt'] = '8-11'
    df.loc[(df['vhlevelb'].isin(['08', '09']) & (df['state'] != "MN")
            & (df['system'] != "G")), 'vhlevelbtxt'] = '8-9'

    df.loc[df['vhlevelw'] == 'N/A', 'vhlevelwtxt'] = 'N/A'
    df.loc[(df['vhlevelw'].isin(['1', '01', '02', '2', '3', '03'])), 'vhlevelwtxt'] = '1-3'
    df.loc[(df['vhlevelw'].isin(['4', '04', '5', '05',
                                 '6', '06', '7', '07'])), 'vhlevelwtxt'] = '4-7'
    df.loc[(df['vhlevelw'].isin(['08', '8', '9', '09', '10', '11']) & (
        (df['state'] == "MN") | (df['system'] == 'G'))), 'vhlevelwtxt'] = '8-11'
    df.loc[(df['vhlevelw'].isin(['08', '09']) & (df['state'] != "MN")
            & (df['system'] != "G")), 'vhlevelwtxt'] = '8-9'

    # Rename columns in the target df data in datafram.
    df = df.rename(columns={"retention": "rtncnt", "mvstate": "state"})
    df = df_remove_indexCols(df)
    logging.info(
        "Retainpop&Y2 created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("Retainpop{}".format(Y2),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''
    proc summary data = retainpop & Y2;
    class state system agttyp premiergrp;
    var rtncnt;
    id qtrnd product migrind;
    output out = summaryPremier (rename=_freq_=nfrccnt)sum = ;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]

    df_creation_logging(grouped_df, "summaryPremier")
    grouped_df.to_sql("summaryPremier", con=sqliteConnection,
                      if_exists='replace')
    sqliteConnection.close()
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from summaryPremier ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'summaryPremier')
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'ALL AGENTS'
    # if PremierGrp=' ' then PremierGrp='Total'; # Manual effort require.
    df.loc[df['premiergrp'].isnull(), 'premiergrp'] = 'Total'
    # select(PremierGrp); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 4
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == 'Low' else 2 if x == 'Med' else 3 if x ==
                    'High' else 4 if x == 'Total' else 9 for x in df['premiergrp']]
    # End manual effort.***

    df['dimval'] = df['premiergrp']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Premier'
    df['dimseq'] = 1
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["premiergrp"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryPremier created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryPremier", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    # output out=driver (drop=_type_ _freq_) max(bday)=BdayY min(bday)=BdayO

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1027&1029 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryPremier_sqlitesorted;CREATE TABLE summaryPremier_sqlitesorted AS SELECT * FROM summaryPremier ORDER
            BY state,agttyp,valseq;DROP TABLE summaryPremier;ALTER TABLE
            summaryPremier_sqlitesorted RENAME TO summaryPremier"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryPremier_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryPremier; '''
    '''SAS Comment:*  title 'summaryPremier'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Mulitproduct processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp MultiProd;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryMultiProd (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'multiprod']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryMultiprod")
    grouped_df.to_sql("summaryMultiprod", con=sqliteConnection,
                      if_exists='replace')
    sqliteConnection.close()

    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from summaryMultiprod ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'summaryMultiprod')
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'multiprod']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if MultiProd=' ' then MultiProd='Total'; # Manual effort require.
    df.loc[df['multiprod'].isnull(), 'multiprod'] = 'Total'
    # select(MultiProd); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==
                    'No' else 3 if x == 'Total' else 9 for x in df['multiprod']]

    # End manual effort.***

    df['dimval'] = df['multiprod']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Multiproduct'
    df['dimseq'] = 2
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["multiprod"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryMultiprod created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryMultiprod", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1063&1065 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryMultiProd_sqlitesorted;CREATE TABLE summaryMultiProd_sqlitesorted AS SELECT * FROM summaryMultiProd
            ORDER BY state,agttyp,valseq;DROP TABLE summaryMultiProd;ALTER TABLE
            summaryMultiProd_sqlitesorted RENAME TO summaryMultiProd"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryMultiProd_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    # output out=sumpts (drop=_type_ _freq_) sum=
    '''SAS Comment:*proc print data=summaryMultiProd; '''
    '''SAS Comment:*  title 'summaryMultiProd'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Coverage processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=Retainpop&Y2;
    class state system agttyp Coverage;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryCoverage (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'coverage']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryCoverage")
    grouped_df.to_sql("summaryCoverage", con=sqliteConnection,
                      if_exists='replace')
    sqliteConnection.close()

    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************
    '''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from summaryCoverage ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'summaryCoverage')
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'coverage']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
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
    # end;'''
    df['valseq'] = [1 if x == 'Full Cov' else 2 if x == 'Lia Only' else 3 if x ==
                    'Mixed' else 4 if x == 'Total' else 9 for x in df['coverage']]

    # End manual effort.***

    df['dimval'] = df['coverage']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Coverage'
    df['dimseq'] = 3
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["coverage"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryCoverage created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryCoverage", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1100&1102 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryCoverage_sqlitesorted;CREATE TABLE summaryCoverage_sqlitesorted AS SELECT * FROM summaryCoverage ORDER
            BY state,agttyp,valseq;DROP TABLE summaryCoverage;ALTER TABLE
            summaryCoverage_sqlitesorted RENAME TO summaryCoverage"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryCoverage_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
       '''
    '''
    output out=GWretain&Y2 (drop=_type_ rename=_freq_=VehCnt)
    max(agent AAA_EC VehFullCov premier premier2 mulprod tenure CVED retention Member prins AssignedDrvAge AgeO vhlevel EFT1 PIFD nohhincdt AAADriveDisc)=
    sum(cov)=CovSum mean(cov)=CovAvg max(cov)=CovMax min(cov AssignedDrvAge AgeY vhlevel VehAge)=CovMin AssignedDrvY AgeY vhlevelB1 VehAgeN1
    '''
    '''SAS Comment:*proc print data=summaryCoverage; '''
    '''SAS Comment:*  title 'summaryCoverage'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Vehicle # processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=Retainpop&Y2;
    class state system agttyp NoVeh;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryNoVeh (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'noveh']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryNoveh")
    grouped_df.to_sql("summaryNoveh", con=sqliteConnection,
                      if_exists='replace')
    sqliteConnection.close()

    '''*********************************************************************************
    below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from summaryNoveh ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'summaryNoveh')
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'noveh']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
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
    # end;'''
    df['valseq'] = [1 if x == '1' else 2 if x == '2' else 3 if x ==
                    '3' else 4 if x == '>3' else 5 if x == 'Total' else 9 for x in df['noveh']]
    # End manual effort.***

    df['dimval'] = df['noveh']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Vehicle #'
    df['dimseq'] = 4
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["noveh"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryNoveh created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryNoveh", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1138&1140 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryNoVeh_sqlitesorted;CREATE TABLE summaryNoVeh_sqlitesorted AS SELECT * FROM summaryNoVeh ORDER BY
            state,agttyp,valseq;DROP TABLE summaryNoVeh;ALTER TABLE
            summaryNoVeh_sqlitesorted RENAME TO summaryNoVeh"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryNoVeh_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryNoVeh; '''
    '''SAS Comment:*  title 'summaryNoVeh'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    ''' try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Newest Veh Age processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=Retainpop&Y2;
    class state system agttyp VehAgeN;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryVehAgeN  (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'vehagen']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'vehagen']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if VehAgeN=' ' then VehAgeN='Total'; # Manual effort require.
    df.loc[df.vehagen == ' ', 'vehagen'] = 'Total'
    # select(VehAgeN); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 4
    df['valseq'] = 5
    df['valseq'] = 6
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == '0-1' else 2 if x == '2-5' else 3 if x == '6-10' else 4 if x ==
                    '11-15' else 5 if '>15' else 6 if x == 'Total' else 9 for x in df['vehagen']]

    # End manual effort.***

    df['dimval'] = df['vehagen']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
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

    # Sql Code Start and End Lines - 1177&1179 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryVehAgeN_sqlitesorted;CREATE TABLE summaryVehAgeN_sqlitesorted AS SELECT * FROM summaryVehAgeN ORDER
            BY state,agttyp,valseq;DROP TABLE summaryVehAgeN;ALTER TABLE
            summaryVehAgeN_sqlitesorted RENAME TO summaryVehAgeN"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryVehAgeN_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryVehAgeN; '''
    '''SAS Comment:*  title 'summaryVehAgeN'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Membership processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=Retainpop&Y2;
    class state system agttyp Mem;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryMem (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'mem']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryMem")
    grouped_df.to_sql("summaryMem", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'mem']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.

    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if Mem=' ' then Mem='Total'; # Manual effort require.

    df.loc[df['mem'].isnull(), 'mem'] = 'Total'
    # select(Mem); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==
                    'No' else 3 if x == 'Total' else 9 for x in df['mem']]

    # End manual effort.***

    df['dimval'] = df['mem']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Membership'
    df['dimseq'] = 6
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["mem"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryMem created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryMem", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1213&1215 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryMem_sqlitesorted;CREATE TABLE summaryMem_sqlitesorted AS SELECT * FROM summaryMem ORDER BY
            state,agttyp,valseq;DROP TABLE summaryMem;ALTER TABLE summaryMem_sqlitesorted
            RENAME TO summaryMem"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryMem_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryMem; '''
    '''SAS Comment:*  title 'summaryMem'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Oldest Driver processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp AgeOldest;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryAgeOldest (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'ageoldest']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryAgeOldest")
    grouped_df.to_sql("summaryAgeOldest", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'ageoldest']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
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
    # end;'''
    df['valseq'] = [1 if x == '<25' else 2 if x == '25-29' else 3 if x == '30-44' else 4 if x ==
                    '45-64' else 5 if x == '>64' else 6 if x == 'Total' else 9 for x in df['ageoldest']]

    # End manual effort.***

    df['dimval'] = df['ageoldest']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Oldest Driver'
    df['dimseq'] = 7
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["ageoldest"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryAgeOldest created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryAgeOldest", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1252&1254 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryAgeOldest_sqlitesorted;CREATE TABLE summaryAgeOldest_sqlitesorted AS SELECT * FROM summaryAgeOldest
            ORDER BY state,agttyp,valseq;DROP TABLE summaryAgeOldest;ALTER TABLE
            summaryAgeOldest_sqlitesorted RENAME TO summaryAgeOldest"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryAgeOldest_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryAgeOldest; '''
    '''SAS Comment:*  title 'summaryAgeOldest'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Youngest Driver processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp AgeYoungest;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryAgeYoungest (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'ageyoungest']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryAgeYoungest")
    grouped_df.to_sql("summaryAgeYoungest", con=sqliteConnection,
                      if_exists='replace')
    sqliteConnection.close()

    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from summaryAgeYoungest", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'summaryAgeYoungest')
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'ageyoungest']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
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
    # end;'''
    df['valseq'] = [1 if x == '<25' else 2 if x == '25-29' else 3 if x == '30-44' else 4 if x ==
                    '45-64' else 5 if x == '>64' else 6 if x == 'Total' else 9 for x in df['ageyoungest']]
    # End manual effort.***

    df['dimval'] = df['ageyoungest']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
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

    # Sql Code Start and End Lines - 1291&1293 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryAgeYoungest_sqlitesorted;CREATE TABLE summaryAgeYoungest_sqlitesorted AS SELECT * FROM summaryAgeYoungest
            ORDER BY state,agttyp,valseq;DROP TABLE summaryAgeYoungest;ALTER TABLE
            summaryAgeYoungest_sqlitesorted RENAME TO summaryAgeYoungest"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryAgeYoungest_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryAgeYoungest; '''
    '''SAS Comment:*  title 'summaryAgeYoungest'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Tenure processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp tenuretxt;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryTenuretxt (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'tenuretxt']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryTenuretxt")
    grouped_df.to_sql("summaryTenuretxt", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'tenuretxt']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if tenuretxt=' ' then tenuretxt='Total'; # Manual effort require.
    df.loc[df['tenuretxt'].isnull(), 'tenuretxt'] = 'Total'
    # select(tenuretxt); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 3
    df['valseq'] = 4
    df['valseq'] = 4
    df['valseq'] = 5
    df['valseq'] = 5
    df['valseq'] = 6
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == '0' else 2 if x == '1' else 3 if x == '2' else 3 if x == '1.5-2.5' else 4 if x ==
                    '3-4' else 4 if x == '3-4.5' else 5 if x == '5+' else 5 if x == '4.5+' else 6 if x == 'Total' else 9 for x in df['tenuretxt']]

    # End manual effort.***

    df['dimval'] = df['tenuretxt']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Tenure'
    df['dimseq'] = 9
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["tenuretxt"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryTenuretxt created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryTenuretxt", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1333&1335 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryTenuretxt_sqlitesorted;CREATE TABLE summaryTenuretxt_sqlitesorted AS SELECT * FROM summaryTenuretxt
            ORDER BY state,agttyp,valseq;DROP TABLE summaryTenuretxt;ALTER TABLE
            summaryTenuretxt_sqlitesorted RENAME TO summaryTenuretxt"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryTenuretxt_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryTenuretxt; '''
    '''SAS Comment:*  title 'summaryTenuretxt'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* EFT processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp EFT;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryEFT (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'eft']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryEFT")
    grouped_df.to_sql("summaryEFT", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'eft']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if EFT=' ' then EFT='Total'; # Manual effort require.
    df.loc[df['eft'].isnull(), 'eft'] = 'Total'
    # select(EFT); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 4
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == 'Yes' else 2 if x == 'No' else 3 if x ==
                    'N/A' else 4 if x == 'Total' else 9 for x in df['eft']]

    # End manual effort.***

    df['dimval'] = df['eft']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'EFT'
    df['dimseq'] = 10
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["eft"])
    df = df_remove_indexCols(df)
    logging.info(
        " summaryEFT created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryEFT", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1371&1373 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryEFT_sqlitesorted;CREATE TABLE summaryEFT_sqlitesorted AS SELECT * FROM summaryEFT ORDER BY
            state,agttyp,valseq;DROP TABLE summaryEFT;ALTER TABLE summaryEFT_sqlitesorted
            RENAME TO summaryEFT"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryEFT_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryEFT; '''
    '''SAS Comment:*  title 'summaryEFT'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* CVED processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp CVEDtxt;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryCVED (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{} where cvedtxt != 'nan'".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'cvedtxt']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryCVED")
    grouped_df.to_sql("summaryCVED", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'cvedtxt']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
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
    # end;'''
    df['valseq'] = [1 if x == '5' else 2 if x == '4' else 3 if x == '3' else 4 if x == '2' else 5 if x ==
                    '1' else 6 if x == '0' else 7 if x == '99' else 8 if x == 'Total' else 9 for x in df['cvedtxt']]

    # End manual effort.***

    df['dimval'] = df['cvedtxt']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'CVED'
    df['dimseq'] = 11
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["cvedtxt"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryCVED created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryCVED", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1412&1414 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryCVED_sqlitesorted;CREATE TABLE summaryCVED_sqlitesorted AS SELECT * FROM summaryCVED ORDER BY
            state,agttyp,valseq;DROP TABLE summaryCVED;ALTER TABLE summaryCVED_sqlitesorted
            RENAME TO summaryCVED"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryCVED_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryCVED; '''
    '''SAS Comment:*  title 'summaryCVED'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Prior Insurance processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp PriorInsStatus;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryPriorIns (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{} where priorinsstatus != ''".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'priorinsstatus']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryPriorIns")
    grouped_df.to_sql("summaryPriorIns", con=sqliteConnection,
                      if_exists='replace')
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
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
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

    # Sql Code Start and End Lines - 1503&1505 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryPriorIns_sqlitesorted;CREATE TABLE summaryPriorIns_sqlitesorted AS SELECT * FROM summaryPriorIns ORDER
            BY state,agttyp,valseq;DROP TABLE summaryPriorIns;ALTER TABLE
            summaryPriorIns_sqlitesorted RENAME TO summaryPriorIns"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryPriorIns_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryPriorIns; '''
    '''SAS Comment:*  title 'summaryPriorIns'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Best Vehicle History processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp VHlevelBtxt;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryVHlevelB (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{} where vhlevelbtxt != ''".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelbtxt']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryVHlevelB")
    grouped_df.to_sql("summaryVHlevelB", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelbtxt']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
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
    # end;'''
    df['valseq'] = [1 if x == 'N/A' else 2 if x == '1-3' else 3 if x ==
                    '4-7' else 4 if x in ['8-9', '8-11'] else 5 if 'Total' else 9 for x in df['vhlevelbtxt']]

    # End manual effort.***

    df['dimval'] = df['vhlevelbtxt']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
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

    # Sql Code Start and End Lines - 1542&1544 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryVHlevelB_sqlitesorted;CREATE TABLE summaryVHlevelB_sqlitesorted AS SELECT * FROM summaryVHlevelB ORDER
            BY state,agttyp,valseq;DROP TABLE summaryVHlevelB;ALTER TABLE
            summaryVHlevelB_sqlitesorted RENAME TO summaryVHlevelB"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryVHlevelB_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryVHlevelB; '''
    '''SAS Comment:*  title 'summaryVHlevelB'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run; """
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:* Worst Vehicle History processing; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp VHlevelWtxt;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryVHlevelW (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{} where vhlevelwtxt != ''".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelwtxt']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryVHlevelW")
    grouped_df.to_sql("summaryVHlevelW", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'vhlevelwtxt']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

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
    # end;'''
    df['valseq'] = [1 if x == 'N/A' else 2 if x == '1-3' else 3 if x ==
                    '4-7' else 4 if x in ['8-9', '8-11'] else 5 if 'Total' else 9 for x in df['vhlevelwtxt']]

    # End manual effort.***

    df['dimval'] = df['vhlevelwtxt']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
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

    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryVHlevelW_sqlitesorted;CREATE TABLE summaryVHlevelW_sqlitesorted AS SELECT * FROM summaryVHlevelW ORDER
            BY state,agttyp,valseq;DROP TABLE summaryVHlevelW;ALTER TABLE
            summaryVHlevelW_sqlitesorted RENAME TO summaryVHlevelW"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryVHlevelW_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*proc print data=summaryVHlevelW; '''
    '''SAS Comment:*  title 'summaryVHlevelW'; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*run;"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    # Sql Code Start and End Lines - 1581&1583 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp PIF;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryPIF (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{} where pif != ''".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'pif']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryPIF")
    grouped_df.to_sql("summaryPIF", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'pif']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if PIF=' ' then PIF='Total'; # Manual effort require.

    df.loc[df['pif'].isnull(), 'pif'] = 'Total'
    # select(PIF); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==
                    'No' else 3 if x == 'Total' else 9 for x in df['pif']]

    # End manual effort.***

    df['dimval'] = df['pif']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Paid In Full'
    df['dimseq'] = 15
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["pif"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryPIF created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryPIF", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
    # Sql Code Start and End Lines - 1617&1619 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE OIF EXISTS summaryPIF_sqlitesorted;CREATE TABLE summaryPIF_sqlitesorted AS SELECT * FROM summaryPIF ORDER BY
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
    '''SAS Comment:* Premier processing - second grouping; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp premiergrp2;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryPremier2 (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp2']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryPremier2")
    grouped_df.to_sql("summaryPremier2", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'premiergrp2']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
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
    # end;'''
    df['valseq'] = [1 if x == 'Low' else 2 if x == 'Mid-Low' else 3 if x ==
                    'Mid-High' else 4 if x == 'High' else 5 if x == 'Total' else 9 for x in df['premiergrp2']]
    # End manual effort.***

    df['dimval'] = df['premiergrp2']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Premier2'
    df['dimseq'] = 16
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["premiergrp2"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryPremier2 created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryPremier2", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
    # Sql Code Start and End Lines - 1651&1653 #
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
    Any SAS steps within the block are converted to python code but commented.'''
    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
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
    proc summary data=retainpop&Y2;
    where state not = 'KY';
    class state system agttyp incidents;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryIncidents (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{} where state not in ('KY') and incidents != ''".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'incidents']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryIncidents")
    grouped_df.to_sql("summaryIncidents", con=sqliteConnection,
                      if_exists='replace')
    sqliteConnection.close()

    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from summaryIncidents ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'summaryIncidents')
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'incidents']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if Incidents=' ' then Incidents='Total'; # Manual effort require.
    df.loc[df['incidents'].isnull(), 'incidents'] = 'Total'
    # select(Incidents); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 4
    df['valseq'] = 5
    df['valseq'] = 6
    # end;'''
    df['valseq'] = [1 if x == '1' else 2 if x == '2' else 3 if x == '3' else 4 if x ==
                    '4' else 5 if x == '5' else 6 if x == 'Total' else x for x in df['incidents']]

    # End manual effort.***

    df['dimval'] = df['incidents']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'Incidents'
    df['dimseq'] = 17
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["incidents"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryIncidents created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryIncidents", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''
    # Sql Code Start and End Lines - 1690&1692 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryIncidents_sqlitesorted;CREATE TABLE summaryIncidents_sqlitesorted AS SELECT * FROM summaryIncidents
            ORDER BY state,agttyp,valseq;DROP TABLE summaryIncidents;ALTER TABLE
            summaryIncidents_sqlitesorted RENAME TO summaryIncidents"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryIncidents_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:* AAA Drive; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=retainpop&Y2;
    class state system agttyp AAADrive;
    var rtncnt;
    id qtrnd product migrind;
    output out=summaryAAADrive (rename=_freq_=nfrccnt)sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retainpop{} where aaadrive != ''".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retainpop{}'.format(Y2))
    # Take the columns in Class
    grp_lst = ['state', 'agttyp', 'system', 'aaadrive']
    id_list = ['qtrnd', 'product', 'migrind']  # Take the columns in ID
    grouped_df = pd.DataFrame()
    cnt = 0
    df_0 = df.drop(columns=grp_lst)
    df_0_summ = df[['rtncnt']].sum()
    for i in range(1, len(grp_lst)+1):
        for j in itertools.combinations(grp_lst, i):
            cnt = cnt + 1
            df1 = df.groupby(list(j))[
                ['rtncnt']].sum().reset_index()
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
            count_df = df.value_counts(list(j)).reset_index(name='c')
            resdf = resdf.merge(count_df, on = list(j), how = 'left').drop_duplicates().rename(columns = {'c' : 'nfrccnt'})

            grouped_df = grouped_df.append(resdf, ignore_index=True)
    df_0_summ['nfrccnt'] = len(df)
    grouped_df = grouped_df.append(df_0_summ, ignore_index=True)

    grouped_df = grouped_df[grp_lst+id_list + ['rtncnt', 'nfrccnt']]
    df_creation_logging(grouped_df, "summaryAAADrive")
    grouped_df.to_sql("summaryAAADrive", con=sqliteConnection,
                      if_exists='replace')
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
    # if _TYPE_ in (12 13 14 15); # Manual effort require.
    grp_lst = ['state', 'agttyp', 'system', 'aaadrive']
    df = df.loc[(~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & (df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & (df[grp_lst[3]].isnull())) |
                (~(df[grp_lst[0]].isnull()) & ~(df[grp_lst[1]].isnull()) & ~(df[grp_lst[2]].isnull()) & ~(df[grp_lst[3]].isnull()))]

    # df.loc[df['__type__'].isin(['12', '13', '14', '15'])]
    # if agttyp=' ' then agttyp='All Agents'; # Manual effort require.
    df.loc[df['agttyp'].isnull(), 'agttyp'] = 'All Agents'
    # if AAADrive=' ' then AAADrive='Total'; # Manual effort require.
    df.loc[df['aaadrive'].isnull(), 'aaadrive'] = 'Total'
    # select(AAADrive); # Manual effort require.
    '''df['valseq'] = 1
    df['valseq'] = 2
    df['valseq'] = 3
    df['valseq'] = 9
    # end;'''
    df['valseq'] = [1 if x == 'Yes' else 2 if x ==
                    'No' else 3 if x == 'Total' else 9 for x in df['aaadrive']]

    # End manual effort.***

    df['dimval'] = df['aaadrive']
    df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    df['dim'] = 'AAADrive'
    df['dimseq'] = 18
    # Drop columns in the target df data in datafram.
    df = df.drop(columns=["aaadrive"])
    df = df_remove_indexCols(df)
    logging.info(
        "summaryAAADrive created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("summaryAAADrive", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS summaryAAADrive_sqlitesorted;CREATE TABLE summaryAAADrive_sqlitesorted AS SELECT * FROM summaryAAADrive ORDER
            BY state,agttyp,system,valseq;DROP TABLE summaryAAADrive;ALTER TABLE
            summaryAAADrive_sqlitesorted RENAME TO summaryAAADrive"""
        sql = mcrResl(sql)
        tgtSqliteTable = "summaryAAADrive_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''

    #Below python code is to execute standard SAS data step#
    '''*********************************************************************************'''
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
    Incidents = pd.read_sql_query(
        "select * from summaryIncidents", sqliteConnection)
    AAADrive = pd.read_sql_query(
        "select * from summaryAAADrive", sqliteConnection)
    # Concatenate the source data frames
    summary = pd.concat([Premier, MultiProd, Coverage, NoVeh, VehAgeN, Mem, AgeOldest, AgeYoungest, Tenuretxt,
                         EFT, CVED, PriorIns, VHlevelB, VHlevelW, PIF, Premier2, Incidents, AAADrive], ignore_index=True, sort=False)
    df = df_remove_indexCols(df)
    logging.info(
        "finalsummary{} created successfully with {} records".format(Y2, len(summary)))
    # Push results data frame to Sqlite DB
    summary.to_sql("finalsummary{}".format(
        Y2), con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # retain qtrnd system product agttyp state migrind dim dimseq dimval valseq nfrccnt rtncnt rtnratio; # Manual effort require.
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from finalsummary{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df = df[['qtrnd', 'system', 'product', 'agttyp', 'state', 'migrind',
             'dim', 'dimseq', 'dimval', 'valseq', 'nfrccnt', 'rtncnt', 'rtnratio']]
    df_lower_colNames(df, 'finalsummary{}'.format(Y2))
    df = df_remove_indexCols(df)
    logging.info(
        "finalsummary{} created successfully with {} records".format(Y2, len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("finalsummary{}".format(Y2),
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
        "select * from finalsummary{} where agttyp in ('Captive', 'EA')".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'finalsummary{}'.format(Y2))
    # Converting source df data into datafram.
    # df = pd.read_sql_query("select * from in ", sqliteConnection)
    # handling data frame column case senstivity.#
    # df_lower_colNames(df, 'in')
    df['agttyp'] = 'Captive/EA '
    df = df_remove_indexCols(df)
    logging.info(
        "finalsummaryCaptiveEA{} created successfully with {} records".format(Y2, len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("finalsummaryCaptiveEA{}".format(Y2),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''output out=summaryVHlevelB (rename=_freq_=nfrccnt)
    sum='''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary nway data=finalsummaryCaptiveEA&Y2;
    class state system agttyp dimseq valseq;
    var nfrccnt rtncnt;
    id qtrnd product agttyp state migrind dim dimseq dimval valseq;
    output out=finalsummary2CaptiveEA sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from finalsummaryCaptiveEA{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'finalsummaryCaptiveEA{}'.format(Y2))
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
    # Take the columns in Class
    agg_cols = {'qtrnd', 'product', 'migrind', 'rtncnt', 'nfrccnt',
                'agttyp', 'stste', 'dim', 'dimseq', 'dimval', 'valseq'}
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value='')
    df = df.groupby(['state', 'system', 'agttyp', 'dimseq', 'valseq']).agg(
        {'qtrnd': max, 'product': max, 'migrind': max, 'dim': max, 'dimval' : max, 'rtncnt': sum, 'nfrccnt' : sum}).reset_index()
    df_creation_logging(df, "finalsummary2CaptiveEA{}".format(Y2))
    df.to_sql("finalsummary2CaptiveEA{}".format(Y2),
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
        "select * from finalsummary2CaptiveEA{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'finalsummary2CaptiveEA{}'.format(Y2))
    # df['rtnratio'] = df['rtncnt']/df['nfrccnt']
    # retain qtrnd system product agttyp state migrind dim dimseq dimval valseq nfrccnt rtncnt rtnratio; # Manual effort require.
    # Drop columns in the target df data in datafram.
    #df = df.drop(columns=["_freq_"])
    df = df_remove_indexCols(df)
    logging.info(
        "finalsummary2CaptiveEA created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("finalsummary2CaptiveEA{}".format(Y2),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source work.finalsummary data into datafram.
    finalsummary = pd.read_sql_query(
        "select * from finalsummary{}".format(Y2), sqliteConnection)
    # Converting source work.finalsummary2CaptiveEA&Y2 data into datafram.
    finalsummary2CaptiveEA = pd.read_sql_query(
        "select * from finalsummary2CaptiveEA{}".format(Y2), sqliteConnection)
    # Concatenate the source data frames
    finalsummaryout = pd.concat(
        [finalsummary, finalsummary2CaptiveEA], ignore_index=True, sort=False)
    finalsummaryout = df_remove_indexCols(finalsummaryout)
    df_creation_logging(finalsummaryout, "finalsummaryout{}".format(Y2))
    # Push results data frame to Sqlite DB
    finalsummaryout.to_sql("finalsummaryout{}".format(
        Y2), con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1780&1782 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS finalsummaryout_sqlitesorted; CREATE TABLE finalsummaryout_sqlitesorted AS SELECT * FROM
            finalsummaryout ORDER BY system, state, agttyp, dimseq, valseq; DROP TABLE
            finalsummaryout; ALTER TABLE finalsummaryout_sqlitesorted RENAME
            TO finalsummaryout"""
        sql = mcrResl(sql)
        tgtSqliteTable = mcrResl("finalsummaryout_sqlitesorted")
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
 
        
   
    '''*********************************************************************************
    Below python code is to execute SAS data step with BY varaible in python
    *********************************************************************************'''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.
    retain;frstdim = first.dimseq; else cnt = cnt + 1;seqhold = valseq;output;if seqhold = cnt then return;else do until (seqhold = cnt);nfrccnt1 = .;rtncnt1 = .;cnclratio1 = .;nfrccnt2 = .;rtncnt = .;cnclratio2 = .;dimval = ' ';valseq = cnt;output;cnt = cnt + 1;end;
    '''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source finalsummaryout&InfMon data into datafram.
    df = pd.read_sql_query(
        "select * from finalsummaryout{}".format(Y2), sqliteConnection)
    # lowering all column names#Generate first and last temporary indicators in the given data frames.
    df_lower_colNames(df, 'finalsummaryout{}'.format(Y2))
    var_list = ['system', 'state', 'agttyp', 'dimseq','valseq'] 
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
        for val in np.arange(1, d[index]-1):
            if val not in new_df['seqhold'].to_list():
                ungroup_df = new_df.reset_index()
                temp = ungroup_df.iloc[0]
                temp['nfrccnt'] = np.nan
                temp['rtncnt'] = np.nan
                temp['rtnratio'] = np.nan	    
                temp['dimval'] = np.nan
                temp['valseq'] = val
                temp_df = temp_df.append(temp)
    fixempty = pd.concat([df, temp_df],ignore_index=True,sort=False)
    fixempty = df_remove_indexCols(fixempty)
    if 'level_0' in fixempty.columns:
        fixempty = fixempty.drop(columns=["level_0"])
    # Push results data frame to Sqlite DB
    logging.info(
        "fixempty{} created successfully with {} records".format(Y2, len(fixempty)))
    fixempty.to_sql("fixempty{}".format(Y2),
                    con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    '''
    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """*** first run; """
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))

    output out=summaryVHlevelW (rename=_freq_=nfrccnt)
    sum=

    WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """/*proc sort data = fixempty; by system product state agttyp dimseq
            valseqdata outfile.hhldrtn_regautodb; set fixempty; drop frstdim cnt
            seqhold*/"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))'''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented.'''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """*** first run; """
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''SAS Comment:*** subsequent runs; '''
    '''SAS Comment:*create backup; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/hhldrtn_regautodb.sas7bdat') as reader:
        df = reader.to_data_frame()
        df_lower_colNames(df, 'hhldrtn_regautodb')
        df.to_sql("hhldrtn_regautodb",
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()

    # Open connection to Sqlite work data base
    '''sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/hhldrtn_regautodb.csv".format(Y1))
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'hhldrtn_regautodb')
    df_creation_logging(df, "hhldrtn_regautodb")
    df.to_sql("hhldrtn_regautodb", con=sqliteConnection,
              if_exists='replace', index=True)
    sqliteConnection.close()'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from hhldrtn_regautodb", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'hhldrtn_regautodb')
    df = df_remove_indexCols(df)
    logging.info(
        "hhldrtn_regautodb_backup created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("hhldrtn_regautodb_backup",
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*combine fix database and production database;
    **WARNING:Below steps are not included in logic calculation. Please amend them manually.
    drop frstdim cnt seqhold;'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source outfile.hhldrtn_regautodb data into datafram.
    hhldrtn_regautodb = pd.read_sql_query(
        "select * from hhldrtn_regautodb ", sqliteConnection)
    # Converting source work.fixemptydata into datafram.
    fixempty = pd.read_sql_query("select * from fixempty{}".format(Y2), sqliteConnection)
    # Concatenate the source data frames
    outfiledb = pd.concat(
        [hhldrtn_regautodb, fixempty], ignore_index=True, sort=False)
    df = df_remove_indexCols(df)
    if 'level_0' in outfiledb.columns:
        outfiledb = outfiledb.drop(columns="level_0")
    df_creation_logging(outfiledb, "outfiledb")
    # Push results data frame to Sqlite DB
    outfiledb.to_sql("outfiledb", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
    '''
    Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
    Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 1845&1847 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''
    try:
        sql = """DROP TABLE IF EXISTS outfiledb_sqlitesorted; CREATE TABLE outfiledb_sqlitesorted AS SELECT * FROM outfiledb ORDER
            BY qtrnd, system, product, state, agttyp, dimseq, valseq; DROP TABLE
            outfiledb; ALTER TABLE outfiledb_sqlitesorted RENAME TO outfiledb"""
        sql = mcrResl(sql)
        tgtSqliteTable = "outfiledb_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb,sql,tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))
    '''
    '''SAS Comment:*update production database for current month; '''
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
    df = df_remove_indexCols(df)
    logging.info(
        "hhldrtn_regautodb created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("hhldrtn_regautodb",
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    #*******************************End of Data Step Process**************************************************#
    '''
    output out=summaryPremier2 (rename=_freq_=nfrccnt)
    sum=
    SAS Comment:*/*** subsequent runs;
    *proc print data=summaryPremier2
    *  title 'summaryPremier2'
    *run
    * Incidents - Added 01-17 - KY excluded because we cannot surcharge there
    proc summary data=retainpop&Y2
    output out=summaryIncidents (rename=_freq_=nfrccnt)
    sum=
    proc summary data=retainpop&Y2
    output out=summaryAAADrive (rename=_freq_=nfrccnt)
    sum=
    ************************************************************************************************************
    proc summary nway data=finalsummaryCaptiveEA&Y2
    output out=finalsummary2CaptiveEA
    sum=
    proc sort data=work.finalsummaryout
    ****WRITE TO DATABASE****************************************************************************************
    *** first run
    /*
    proc sort data=work.fixempty
    */
    *** first run
    *** subsequent runs
    *create backup
    *combine fix database and production database
    *sort combined fix and production database
    proc sort data=work.outfiledb
    *update production database for current month
    */
    *** subsequent runs
    '''


'''Uncomment to execute the below sas macro'''
# Retention(<< Provide require args here >>)

### SAS Source Code Line Numbers START:1870 & END:1870.###
'''NOTE: SAS User Defined Macro Execution. Python UDF Execution.
Please Validate Before Execution.'''
Retention(Y1=202002, Y2=202102, GY1=20200229, GY2=20210228)
