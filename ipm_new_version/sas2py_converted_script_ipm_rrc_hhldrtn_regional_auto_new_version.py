# -*- coding: utf-8 -*-
r'''
Created on: Mon 14 Dec 20 18:09:21

Author: SAS2PY Code Conversion Tool

SAS Input File: gw_ipm_hhldrtn regional auto
SAS File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS_SRC_CDE

Generated Python File: Sas2PyConvertedScript_Out
Python File Location: C:\Users\vegopi\Desktop\sas2py_framework\Sas2Py_Repo\SAS2PY_TRANSLATED
'''

''' Importing necessary project specific core utility python modules.'''
'''Please update the below path according to your project specification where core SAS to Python code conversion core modules stored'''
import yaml
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
from google.cloud import bigquery

# Seting up logging info #

config_file = None
yaml_file = None

try:
    config_file = open('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/config.yaml', 'r+')
    yaml_file = yaml.load(config_file)
except Exception as e:
    print("Error reading config file | ERROR : ", e)
finally:
    config_file.close()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/data02/keys/lab-e42-bq.json"

Y1 = yaml_file['Y1']
Y2 = yaml_file['Y2']
GY1 = yaml_file['GY1']
GY2 = yaml_file['GY2']
project_id  = yaml_file['gcp_project_id']
output_dataset = yaml_file['gcp_output_dataset_id']
gcp_gw_info_auto = yaml_file['gcp_gw_info_auto ']
gen1_dataset_id = yaml_file['gcp_gen1_dataset_id']
gen2_dataset_id = yaml_file['gcp_gen2_dataset_id']
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
            
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#.
	client=bigquery.Client()
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen1_dataset_id, Y1)
    df_inforce_gen1 = client.query(sql).to_dataframe()
	'''
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/inforce{}.sas7bdat'.format(Y1)) as reader:
        df = reader.to_data_frame()
        df.to_sql("inforce{}".format(Y1),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()'''

    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    '''df = pd.read_sql_query(
        "select * from inforce{}".format(Y1), sqliteConnection)'''
	df = df_inforce_gen1
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'inforce')
    df['system'] = 'I'
    df['termincep'] = df['termincep'].astype(str)
    # Keep columns in the taget df data in datafram.
    df = df[['cdtscore', 'hhclient', 'primaryclass', 'policy', 'mvstate', 'system', 'inception', 'termincep',
             'cp_prm', 'billplan', 'seqagtno', 'bi_prm', 'mvyear', 'memberind', 'mltprdind', 'duedate', 'client', 'cl_prm']]
    df = df_remove_indexCols(df)
    logging.info(
        "g1old created successfully with {} records".format(len(df)))
    del df_inforce_gen1
	# Push results data frame to Sqlite DB
    df.to_sql("g1old", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
    Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 20&22 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """DROP TABLE IF EXISTS g1old_sqlitesorted;CREATE TABLE g1old_sqlitesorted AS SELECT * FROM g1old ORDER BY
            mvstate,hhclient,descending,cdtscore;DROP TABLE g1old;ALTER TABLE
            g1old_sqlitesorted RENAME TO g1old"""
        sql = mcrResl(sql)
        tgtSqliteTable = "g1old_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''*********************************************************************************
    Below python code is to execute SAS data step with BY varaible in python
    *********************************************************************************'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source g1old data into datafram.
    g1old = pd.read_sql_query("select * from g1old ", sqliteConnection)
    # lowering all column names#Generate first and last temporary indicators in the given data frames.
    df_lower_colNames(g1old)
    g1old_grouped = g1old
    g1old = g1old.sort_values(by=['mvstate', 'hhclient'])
    g1old_grouped['IsFirst'], g1old_grouped['IsLast'] = [False, False]
    g1old_grouped.loc[g1old_grouped.groupby(['mvstate', 'hhclient'])[
        'IsFirst'].head(1).index, 'IsFirst'] = True
    g1old_grouped.loc[g1old_grouped.groupby(['mvstate', 'hhclient'])[
        'IsLast'].tail(1).index, 'IsLast'] = True
    # Output first occurance values in data to the target data frame.
    # Drop indicator tmp columns
    g1keepold = g1old_grouped[(g1old_grouped['IsFirst'])]
    g1keepold = g1keepold.drop(columns=['IsFirst', 'IsLast'])
    df = df_remove_indexCols(g1keepold)
    df_creation_logging(g1keepold, "g1keepold")
    # Push results data frame to Sqlite DB
    g1keepold.to_sql("g1keepold", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''

    # Sql Code Start and End Lines - 30&37 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.

    try:
        sql = """DROP TABLE IF EXISTS g1hhldold;create table g1hhldold as select a.* from g1old a, g1keepold b where
            a.policy=b.policy"""
        sql = mcrResl(sql)
        tgtSqliteTable = "g1hhldold"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:*AJS: Repeat the same process to dedup Gen2 file by HHCLIENT; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen2_dataset_id, Y1)
    df_inforce_gen2 = client.query(sql).to_dataframe()
	'''
	# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/inforcegenii{}.sas7bdat'.format(Y1)) as reader:
        df = reader.to_data_frame()
        df.to_sql("inforcegenii{}".format(Y1),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()'''

    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    '''df = pd.read_sql_query(
        "select * from inforcegenii{}".format(Y1), sqliteConnection)'''
	del df_inforce_gen2
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'inforce')
    df['system'] = 'I'
    df = df_remove_indexCols(df)
    logging.info(
        "g2old created successfully with {} records".format(len(df)))
    del df_inforce_gen2
	# Push results data frame to Sqlite DB
    df.to_sql("g2old", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 45&47 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """DROP TABLE IF EXISTS g2old_sqlitesorted;CREATE TABLE g2old_sqlitesorted AS SELECT * FROM g2old ORDER BY
            mvstate,hhclient,descending,cdtscore;DROP TABLE g2old;ALTER TABLE
            g2old_sqlitesorted RENAME TO g2old"""
        sql = mcrResl(sql)
        tgtSqliteTable = "g2old_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
       e = sys.exc_info()[0]
       logging.error('Table creation/update is failed.')
       logging.error('Error - {}'.format(e))'''

    '''*********************************************************************************
    Below python code is to execute SAS data step with BY varaible in python
    *********************************************************************************'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source g2old data into datafram.
    g2old = pd.read_sql_query("select * from g2old ", sqliteConnection)
    # lowering all column names#Generate first and last temporary indicators in the given data frames.
    df_lower_colNames(df, 'g2old')
    g2old_grouped = g2old
    g2old = g2old.sort_values(by=['mvstate', 'hhclient'])
    g2old_grouped['IsFirst'], g2old_grouped['IsLast'] = [False, False]
    g2old_grouped.loc[g2old_grouped.groupby(['mvstate', 'hhclient'])[
        'IsFirst'].head(1).index, 'IsFirst'] = True
    g2old_grouped.loc[g2old_grouped.groupby(['mvstate', 'hhclient'])[
        'IsLast'].tail(1).index, 'IsLast'] = True
    # Output first occurance values in data to the target data frame.
    # Drop indicator tmp columns
    g2keepold = g2old_grouped[(g2old_grouped['IsFirst'])]
    g2keepold = g2keepold.drop(columns=['IsFirst', 'IsLast'])
    df = df_remove_indexCols(g2keepold)
    df_creation_logging(g2keepold, "g2keepold")
    # Push results data frame to Sqlite DB
    g2keepold.to_sql("g2keepold", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''

    # Sql Code Start and End Lines - 55&62 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS gen2old;create table gen2old as select a.* from g2old a, g2keepold b where
            a.policy=b.policy"""
        sql = mcrResl(sql)
        tgtSqliteTable = "gen2old"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:*AJS: Merge the Gen1 and Gen2 files.  If the HHCLIENT exists in both Gen1 and Gen2, we want to use the Gen2 data for the household.; '''
    '''SAS Comment:*This merge will effectively create a new Gen1 dataset to use for report dimension processing.  These HHCLIENT records DO NOT appear in Gen2.; '''
    '''*********************************************************************************
    Below python code is to execute SAS data step merge step in python
    *********************************************************************************'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source g1hhldold data into datafram.
    g1hhldold = pd.read_sql_query("select * from g1hhldold ", sqliteConnection)
    df_lower_colNames(g1hhldold, 'g1hhldold')  # lowering all column names
    # Adding indicator columns in source g1hhldold data in datafram.
    g1hhldold['a'] = True
    # Converting source gen2old data into datafram.
    gen2old = pd.read_sql_query("select * from gen2old ", sqliteConnection)
    df_lower_colNames(gen2old, 'gen2old')
    gen2old = gen2old[['mvstate', 'hhclient', 'policy', 'seqpolno', 'riskseq', 'system']]
    # lowering all column names
    # Adding indicator columns in source gen2old data in datafram.
    gen2old['b'] = True
    # Creating dictionary of data frames and indicator values
    dfMergeDict = {'a': g1hhldold, 'b': gen2old}
    # Merge the given two data frames.
    mrgResultTmpDf = reduce(
        partial(sas2pyMergedfs, on=['mvstate', 'hhclient']), dfMergeDict.values())
    # Update NaN values to False in the merged output.
    mrgResultTmpDf[list(dfMergeDict.keys())] = mrgResultTmpDf[list(
        dfMergeDict.keys())].fillna(False)
    mrgResultTmpDf[list(dfMergeDict.keys())] = mrgResultTmpDf[list(
        dfMergeDict.keys())].astype(bool)
    mrgResultTmpDf = df_remove_indexCols(mrgResultTmpDf)

    # Records only from left given data source on key colomns
    gen1old = mrgResultTmpDf[(mrgResultTmpDf['a']) & ~(mrgResultTmpDf['b'])]
    # Drop indicator tmp columns
    gen1old = gen1old.drop(columns=list(dfMergeDict.keys()))
    gen1old = df_remove_indexCols(gen1old)
    df_creation_logging(gen1old, "gen1old")
    # Push results data frame to Sqlite DB
    gen1old.to_sql("gen1old", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''
    '''SAS Comment:*************************************************************************************************************; '''
    '''SAS Comment:*** AJS: RegNew - Regional Year 2; '''
    '''SAS Comment:*Dedup Gen1 file by HHCLIENT.  Same household (determined by HHCLIENT) may have more than one policy (different number).; '''
    '''SAS Comment:*Want one policy, the one w/ highest premier if difference exists; '''
    '''SAS Comment:*Sort, take first hhclient record to create "keep" dataset and then join back to original dataset,; '''
    '''SAS Comment:*in order to keep all vehicle records associated w/ policy for further report processing; '''
    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

     #*******************************End of Data Step Process**************************************************#

    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen1_dataset_id, Y2)
    df_inforce_gen1 = client.query(sql).to_dataframe()
	'''
	# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/inforce{}.sas7bdat'.format(Y2)) as reader:
        df = reader.to_data_frame()
        df.to_sql("inforce{}".format(Y2),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()'''

    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    '''df = pd.read_sql_query(
        "select * from inforce{}".format(Y2), sqliteConnection)'''
	df = df_inforce_gen1
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'inforce')
    df['system'] = 'I'
    df['termincep'] = df['termincep'].astype(str)
    # Keep columns in the taget df data in datafram.
    df = df[['cdtscore', 'hhclient', 'primaryclass', 'policy', 'mvstate', 'system', 'inception', 'termincep',
             'cp_prm', 'billplan', 'seqagtno', 'bi_prm', 'mvyear', 'memberind', 'mltprdind', 'duedate', 'client', 'cl_prm']]
    df = df_remove_indexCols(df)
    del df_inforce_gen1
	# Close connection to Sqlite work data base
    df.to_sql("g1new", con=sqliteConnection, if_exists='replace')
    logging.info(
        "g1new created successfully with {} records".format(len(df)))
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    # Sql Code Start and End Lines - 85&87 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """DROP TABLE IF EXISTS g1new_sqlitesorted;CREATE TABLE g1new_sqlitesorted AS SELECT * FROM g1new ORDER BY
            mvstate,hhclient,descending,cdtscore;DROP TABLE g1new;ALTER TABLE
            g1new_sqlitesorted RENAME TO g1new"""
        sql = mcrResl(sql)
        tgtSqliteTable = "g1new_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))'''

    '''*********************************************************************************
    Below python code is to execute SAS data step with BY varaible in python
    *********************************************************************************'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source g1new data into datafram.
    g1new = pd.read_sql_query("select * from g1new ", sqliteConnection)
    # lowering all column names#Generate first and last temporary indicators in the given data frames.
    df_lower_colNames(g1new)
    g1new_grouped = g1new
    g1new = g1new.sort_values(by=['mvstate', 'hhclient'])
    g1new_grouped['IsFirst'], g1new_grouped['IsLast'] = [False, False]
    g1new_grouped.loc[g1new_grouped.groupby(['mvstate', 'hhclient'])[
        'IsFirst'].head(1).index, 'IsFirst'] = True
    g1new_grouped.loc[g1new_grouped.groupby(['mvstate', 'hhclient'])[
        'IsLast'].tail(1).index, 'IsLast'] = True
    # Output first occurance values in data to the target data frame.
    # Drop indicator tmp columns
    g1keepnew = g1new_grouped[(g1new_grouped['IsFirst'])]
    g1keepnew = g1keepnew.drop(columns=['IsFirst', 'IsLast'])
    g1keepnew = df_remove_indexCols(g1keepnew)
    df_creation_logging(g1keepnew, "g1keepnew")
    # Push results data frame to Sqlite DB
    g1keepnew.to_sql("g1keepnew", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''

    # Sql Code Start and End Lines - 95&102 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS g1hhldnew;create table g1hhldnew as select a.* from g1new a, g1keepnew b where
            a.policy=b.policy"""
        sql = mcrResl(sql)
        tgtSqliteTable = "g1hhldnew"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:*AJS: Repeat the same process to dedup Gen2 file by HHCLIENT; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sql = """select * from {}.{}.inforce where mondate = {}""".format(project_id, gen2_dataset_id, Y2)
    df_inforce_gen2 = client.query(sql).to_dataframe()
	'''
	# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/inforcegenii{}.sas7bdat'.format(Y2)) as reader:
        df = reader.to_data_frame()
        df.to_sql("inforcegenii{}".format(Y2),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()'''

    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    '''df = pd.read_sql_query(
        "select * from inforcegenii{}".format(Y2), sqliteConnection)'''
	df = df_inforce_gen2
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'inforce')
    df['system'] = 'I'
    df = df_remove_indexCols(df)
    del df_inforce_gen2
	df.to_sql("g2new", con=sqliteConnection, if_exists='replace')
    logging.info(
        "g2new created successfully with {} records".format(len(df)))
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#

    # Sql Code Start and End Lines - 110&112 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """DROP TABLE IF EXISTS g2new_sqlitesorted;CREATE TABLE g2new_sqlitesorted AS SELECT * FROM g2new ORDER BY
            mvstate,hhclient,descending,cdtscore;DROP TABLE g2new;ALTER TABLE
            g2new_sqlitesorted RENAME TO g2new"""
        sql = mcrResl(sql)
        tgtSqliteTable = "g2new_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))'''

    '''*********************************************************************************
    Below python code is to execute SAS data step with BY varaible in python
    *********************************************************************************'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source g2new data into datafram.
    g2new = pd.read_sql_query("select * from g2new ", sqliteConnection)
    # lowering all column names#Generate first and last temporary indicators in the given data frames.
    df_lower_colNames(g2new)
    g2new_grouped = g2new
    g2new = g2new.sort_values(by=['mvstate', 'hhclient'])
    g2new_grouped['IsFirst'], g2new_grouped['IsLast'] = [False, False]
    g2new_grouped.loc[g2new_grouped.groupby(['mvstate', 'hhclient'])[
        'IsFirst'].head(1).index, 'IsFirst'] = True
    g2new_grouped.loc[g2new_grouped.groupby(['mvstate', 'hhclient'])[
        'IsLast'].tail(1).index, 'IsLast'] = True
    # Output first occurance values in data to the target data frame.
    # Drop indicator tmp columns
    g2keepnew = g2new_grouped[(g2new_grouped['IsFirst'])]
    g2keepnew = g2keepnew.drop(columns=['IsFirst', 'IsLast'])
    g2keepnew = df_remove_indexCols(g2keepnew)
    df_creation_logging(g2keepnew, "g2keepnew")
    # Push results data frame to Sqlite DB
    g2keepnew.to_sql("g2keepnew", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''

    # Sql Code Start and End Lines - 120&127 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS gen2new;create table gen2new as select a.* from g2new a, g2keepnew b where
            a.policy=b.policy"""
        sql = mcrResl(sql)
        tgtSqliteTable = "gen2new"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment: *Merge the Gen1 and Gen2 files.  If the HHCLIENT exists in both Gen1 and Gen2, we want to use the Gen2 data for the household.; '''
    '''SAS Comment: *This merge will effectively create a new Gen1 dataset to use for report dimension processing.  These HHCLIENT records DO NOT appear in Gen2.; '''
    '''*********************************************************************************
    Below python code is to execute SAS data step merge step in python
    *********************************************************************************'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source g1hhldnew data into datafram.
    g1hhldnew = pd.read_sql_query("select * from g1hhldnew ", sqliteConnection)
    df_lower_colNames(g1hhldnew, 'g1hhldnew')  # lowering all column names
    # Adding indicator columns in source g1hhldnew data in datafram.
    g1hhldnew['a'] = True
    # Converting source gen2new data into datafram.
    gen2new = pd.read_sql_query("select * from gen2new ", sqliteConnection)
    df_lower_colNames(gen2new, 'gen2new')  # lowering all column names
    # Adding indicator columns in source gen2new data in datafram.

    gen2new = gen2new[['mvstate', 'hhclient', 'policy', 'seqpolno', 'riskseq', 'system']]
    gen2new['b'] = True
    # Creating dictionary of data frames and indicator values
    dfMergeDict = {'a': g1hhldnew, 'b': gen2new}
    # Merge the given two data frames.
    mrgResultTmpDf = reduce(
        partial(sas2pyMergedfs, on=['mvstate', 'hhclient']), dfMergeDict.values())
    # Update NaN values to False in the merged output.
    mrgResultTmpDf[list(dfMergeDict.keys())] = mrgResultTmpDf[list(
        dfMergeDict.keys())].fillna(False)
    mrgResultTmpDf[list(dfMergeDict.keys())] = mrgResultTmpDf[list(
        dfMergeDict.keys())].astype(bool)
    mrgResultTmpDf = df_remove_indexCols(mrgResultTmpDf)

    # Records only from left given data source on key colomns
    gen1new = mrgResultTmpDf[(mrgResultTmpDf['a']) & ~(mrgResultTmpDf['b'])]
    # Drop indicator tmp columns
    gen1new = gen1new.drop(columns=list(dfMergeDict.keys()))
    gen1new = df_remove_indexCols(gen1new)
    df_creation_logging(gen1new, "gen1new")
    # Push results data frame to Sqlite DB
    gen1new.to_sql("gen1new", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''
    '''SAS Comment: read in Guidewire INF data; '''
    '''SAS Comment: gen2 fields kept: mvstate policy client hhclient cdtscore seqagtno inception termincep duedate mltprdind bi_prm cp_prm cl_prm MEMBERIND primaryclass mvyear billplan system); '''
    '''SAS Comment: Year 1 - "old"; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """/*data gwold_src (keep=PRIM_STATE_CD POL_NO policy ACCT_KEY Z_PREM_LVL seqagtno
            ORIG_EFF_DATE POL_EFF_DATE POL_EXP_DATE MULT_PROD_DSIC_FL MULT_PROD_TYP_STAT_FL
            MULT_PROD_TYP_STAT_GATNBI_WPREM COMP_WPREM COLL_WPREM MEM_SHIP_FL MODEL_YR
            BILL_PLAN_CD system PAY_IN_FULL_DISC_FL POL_TENURE P_IN_STAT_CD Veh_Age
            DISC_EFT_DISC_FL AAA_DRV_DISC_FLAGE_OF_OLD_DRVR_HHLD AGE_OF_YOUNG_DRVR_HHLD);set
            gwauto.o_gw_pa_in_force_GY1;system = 'G';policy = POL_NO,
            4,9)),9.)*/"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))'''

    # Sql Code Start and End Lines - 153&161 #
    sql = """select * from {}.{}.o_gw_pa_in_force where mondate = {}""".format(project_id, gcp_gw_info_auto, GY1)
    df_gw_pa_in_force1 = client.query(sql).to_dataframe()
	
	# Open connection to Sqlite work data base#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
	df = df_gw_pa_in_force1
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'o_gw_pa_in_force')
    df = df_remove_indexCols(df)
    del df_gw_pa_in_force1
	logging.info(
        "o_gw_pa_in_force_{} created successfully with {} records".format(GY1, len(df)))
	df.to_sql("o_gw_pa_in_force_{}".format(GY1), con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
	
	'''
	# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/o_gw_pa_in_force_{}.sas7bdat'.format(GY1)) as reader:
        df = reader.to_data_frame()
        df.to_sql("o_gw_pa_in_force_{}".format(GY1),
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()'''

    '''# Converting source df data into datafram.
    df = pd.read_csv(
        "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/o_gw_pa_in_force_{}.csv".format(GY1))
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'o_gw_pa_in_force_{}'.format(GY1))
    df_creation_logging(df, "o_gw_pa_in_force_{}".format(GY1))
    df.to_sql("o_gw_pa_in_force_{}".format(GY1), con=sqliteConnection,
                  if_exists='replace', index=True)
    sqliteConnection.close()'''
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS gwold_src;create table gwold_src as select prim_state_cd as mvstate, pol_no,
            substr(POL_NO, 4,9) as policy, acct_key, z_prem_lvl as
            cdtscore, seqagtno, abbragt, orig_eff_date as inception, pol_eff_date as
            termincep,pol_exp_date as duedate, mult_prod_dsic_fl as mltprdind,
            mult_prod_typ_stat_fl, mult_prod_typ_stat_gatn, bi_wprem as bi_prm, comp_wprem
            as comp_prm, coll_wprem as coll_prm,mem_ship_fl as memberind, model_yr as
            mvyear, bill_plan_cd as billplan, pay_in_full_disc_fl as pifind, pol_tenure as
            tenure, p_in_stat_cd as prinscde, veh_age,disc_eft_disc_fl, aaa_drv_disc_fl,
            age_of_old_drvr_hhld, age_of_young_drvr_hhld, veh_hist_lvl_cd as
            vhlevel, 'G' as system from o_gw_pa_in_force_{}""".format(GY1)
        sql = mcrResl(sql)
        tgtSqliteTable = "gwold_src"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
		

    ''' Conversion of PROC SORT into Python code as it creates new sorted table in the sqllite db.
     Some times this step isn't necessary based on the scenario of execution,hence it can be commented out if you want.'''

    # Sql Code Start and End Lines - 163&165 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql="""DROP TABLE IF EXISTS gwold_src_sqlitesorted;CREATE TABLE gwold_src_sqlitesorted AS SELECT * FROM gwold_src ORDER BY
            mvstate,acct_key,descending,cdtscore,pol_no;DROP TABLE gwold_src;ALTER TABLE
            gwold_src_sqlitesorted RENAME TO gwold_src"""
        sql=mcrResl(sql)
        tgtSqliteTable="gwold_src_sqlitesorted"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e=sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))'''

    '''*********************************************************************************
    Below python code is to execute SAS data step with BY varaible in python
    *********************************************************************************'''

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source gwold_src data into datafram.
    gwold_src = pd.read_sql_query("select * from gwold_src ", sqliteConnection)
    # lowering all column names#Generate first and last temporary indicators in the given data frames.
    df_lower_colNames(gwold_src, 'gwold_src')
    gwold_src_grouped = gwold_src
    gwold_src = gwold_src.sort_values(by=['mvstate', 'acct_key'])
    gwold_src_grouped['IsFirst'], gwold_src_grouped['IsLast'] = [False, False]
    gwold_src_grouped.loc[gwold_src_grouped.groupby(['mvstate', 'acct_key'])[
        'IsFirst'].head(1).index, 'IsFirst'] = True
    gwold_src_grouped.loc[gwold_src_grouped.groupby(['mvstate', 'acct_key'])[
        'IsLast'].tail(1).index, 'IsLast'] = True
    # Output first occurance values in data to the target data frame.
    # Drop indicator tmp columns
    gwkeepold = gwold_src_grouped[(gwold_src_grouped['IsFirst'])]
    gwkeepold = gwkeepold.drop(columns=['IsFirst', 'IsLast'])
    gwkeepold = df_remove_indexCols(gwkeepold)
    df_creation_logging(gwkeepold, "gwkeepold")
    # Push results data frame to Sqlite DB
    gwkeepold.to_sql("gwkeepold", con=sqliteConnection,
                     if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''

    # Sql Code Start and End Lines - 173&179 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS gwold;create table gwold as select a.* from gwold_src a, gwkeepold b where
            a.pol_no=b.pol_no"""
        sql = mcrResl(sql)
        tgtSqliteTable = "gwold"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:*Year 2 - "new"; '''

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """/*data gwnew_src (keep=PRIM_STATE_CD POL_NO policy ACCT_KEY Z_PREM_LVL seqagtno
            ORIG_EFF_DATE POL_EFF_DATE POL_EXP_DATE MULT_PROD_DSIC_FL MULT_PROD_TYP_STAT_FL
            MULT_PROD_TYP_STAT_GATNBI_WPREM COMP_WPREM COLL_WPREM MEM_SHIP_FL MODEL_YR
            BILL_PLAN_CD system PAY_IN_FULL_DISC_FL POL_TENURE P_IN_STAT_CD Veh_Age
            DISC_EFT_DISC_FL AAA_DRV_DISC_FLAGE_OF_OLD_DRVR_HHLD AGE_OF_YOUNG_DRVR_HHLD);set
            gwauto.o_gw_pa_in_force_&GY2;system = 'G';policy = (POL_NO,
            4,9)),9.)*/"""
        sql = mcrResl(sql)
        tgtSqliteTable = ""
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))'''
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    sql = """select * from {}.{}.o_gw_pa_in_force where mondate = {}""".format(project_id, gcp_gw_info_auto, GY2)
    df_gw_pa_in_force2 = client.query(sql).to_dataframe()
	
	# Open connection to Sqlite work data base#
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
	df = df_gw_pa_in_force2
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'o_gw_pa_in_force')
    df = df_remove_indexCols(df)
    del df_gw_pa_in_force2
	logging.info(
        "o_gw_pa_in_force_{} created successfully with {} records".format(GY2, len(df)))
	df.to_sql("o_gw_pa_in_force_{}".format(GY2), con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
    '''
	# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_csv(
	    "/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/o_gw_pa_in_force_{}.csv".format(GY2))
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'o_gw_pa_in_force_{}'.format(GY2))
    df_creation_logging(df, "o_gw_pa_in_force_{}".format(GY2))
    df.to_sql("o_gw_pa_in_force_{}".format(GY2), con=sqliteConnection,
		  if_exists='replace', index=True)
    sqliteConnection.close()'''

    try:
	    sql = """DROP TABLE IF EXISTS gwnew_src;create table gwnew_src as select prim_state_cd as mvstate, pol_no,
            substr(POL_NO, 4,9) as policy, acct_key, z_prem_lvl as
            cdtscore, seqagtno, abbragt, orig_eff_date as inception, pol_eff_date as
            termincep,pol_exp_date as duedate, mult_prod_dsic_fl as mltprdind,
            mult_prod_typ_stat_fl, mult_prod_typ_stat_gatn, bi_wprem as bi_prm, comp_wprem as comp_prm, coll_wprem as coll_prm,mem_ship_fl as memberind, model_yr as mvyear, bill_plan_cd as billplan, pay_in_full_disc_fl as pifind, pol_tenure as tenure, p_in_stat_cd as prinscde, veh_age,disc_eft_disc_fl, aaa_drv_disc_fl, age_of_old_drvr_hhld, age_of_young_drvr_hhld, veh_hist_lvl_cd as vhlevel, 'G' as system from o_gw_pa_in_force_{}""".format(GY2)
	    sql = mcrResl(sql)
	    tgtSqliteTable = "gwnew_src"
	    procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
	    e = sys.exc_info()[0]
	    logging.error('Table creation/update is failed.')
	    logging.error('Error - {}'.format(e))

    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source gwnew_src data into datafram.
    gwnew_src = pd.read_sql_query("select * from gwnew_src ", sqliteConnection)
    # lowering all column names#Generate first and last temporary indicators in the given data frames.
    df_lower_colNames(gwnew_src, 'gwnew_src')
    gwnew_src_grouped = gwnew_src
    gwnew_src = gwnew_src.sort_values(by=['mvstate', 'acct_key'])
    gwnew_src_grouped['IsFirst'], gwnew_src_grouped['IsLast'] = [False, False]
    gwnew_src_grouped.loc[gwnew_src_grouped.groupby(['mvstate', 'acct_key'])[
        'IsFirst'].head(1).index, 'IsFirst'] = True
    gwnew_src_grouped.loc[gwnew_src_grouped.groupby(['mvstate', 'acct_key'])[
        'IsLast'].tail(1).index, 'IsLast'] = True
    # Output first occurance values in data to the target data frame.
    # Drop indicator tmp columns
    gwkeepnew = gwnew_src_grouped[(gwnew_src_grouped['IsFirst'])]
    gwkeepnew = gwkeepnew.drop(columns=['IsFirst', 'IsLast'])
    gwkeepnew = df_remove_indexCols(gwkeepnew)
    df_creation_logging(gwkeepnew, "gwkeepnew")
    # Push results data frame to Sqlite DB
    gwkeepnew.to_sql("gwkeepnew", con=sqliteConnection,
                     if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*******************************End of Merge Process**************************************************'''

    # Sql Code Start and End Lines - 212&218 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS gwnew;create table gwnew as select a.* from gwnew_src a, gwkeepnew b where
            a.pol_no=b.pol_no"""
        sql = mcrResl(sql)
        tgtSqliteTable = "gwnew"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:*************************************************************************************************************; '''
    '''SAS Comment:*** ORIGINAL programming; '''

    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    '''SAS Comment:/***** Gen1 *****/ '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from gen1old ", sqliteConnection)
    # handling data frame column case senstivity.#
    df = df[['mvstate', 'policy', 'client', 'hhclient', 'cdtscore', 'seqagtno', 'inception', 'termincep', 'duedate',
             'mltprdind', 'bi_prm', 'cp_prm', 'cl_prm', 'memberind', 'primaryclass', 'mvyear', 'billplan', 'system']]
    df_lower_colNames(df, 'gen1old')
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

    '''WARNING: Below SAS step has not converted in this release.
    data=gen1new nway;
    class mvstate policy;
    output out=gen1_retain (drop=_type_ _freq_);
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from gen1new", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'gen1new')
    df = df_remove_indexCols(df)
    df_0 = df.groupby(["mvstate", "policy"]).size().reset_index()
    df_1 = df_0.drop(columns=0)
    df_creation_logging(df_1, "gen1_retain")
    df_1.to_sql("gen1_retain", con=sqliteConnection, if_exists='replace')

    # Sql Code Start and End Lines - 325&328 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS GEN1_TEMP;create table Gen1_temp as select a.*,b.policy as retain from Gen1 a left join
            gen1_retain b on a.mvstate=b.mvstate and a.policy=b.policy"""
        sql = mcrResl(sql)
        tgtSqliteTable = "Gen1_temp"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    '''SAS Comment:* Track migration; '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from Gen1_temp", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Gen1_Temp')
    df = df_remove_indexCols(df)
    logging.info(
        "Gen1 created successfully with {} records".format(len(df)))
    df.to_sql("Gen1", con=sqliteConnection, if_exists='replace')

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=gen2new nway;
    class mvstate client;
    output out=migrate (drop=_type_ _freq_);
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from gen2new", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'gen2new')
    df = df_remove_indexCols(df)
    df_summ = df.groupby(['mvstate', 'client']).size().reset_index()
    df_min = df_summ.drop(columns=0)
    df_creation_logging(df_min, "migrate")
    df_min.to_sql("migrate", con=sqliteConnection, if_exists='replace')

    # Sql Code Start and End Lines - 337&340 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS Gen1_temp2;create table Gen1_temp2 as select a.*,b.client as migrate1 from gen1 a left join
            migrate b on a.mvstate=b.mvstate and a.client=b.client"""
        sql = mcrResl(sql)
        tgtSqliteTable = "Gen1_temp2"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from Gen1_temp2", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Gen1_Temp2')
    df = df_remove_indexCols(df)
    logging.info(
        "Gen1 created successfully with {} records".format(len(df)))
    df.to_sql("Gen1", con=sqliteConnection, if_exists='replace')

    '''WARNING SAS commnet block detected.
    Any SAS steps within the block are converted to python code but commented. '''

    # Sql Code Start and End Lines - 0&0 #
    """***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************"""
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    '''try:
        sql = """/*proc summary data=regnew.inforcegenii nway;class mvstate hhclient;output
            out=migrate (drop=_type_ _freq_) create table Gen1 as select a.*,b.hhclient as
            migrate2 from gen1 a left join migrate bon a.mvstate=b.mvstate and
            a.hhclient=b.hhclient*/"""
        sql = mcrResl(sql)
        tgtSqliteTable = "Gen1"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))'''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from gen2old ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'gen2old')
    # Drop columns from source df data in datafram.
    df = df.drop(columns="product")
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

    # Keep columns in the taget df data in datafram.
    df = df[['mvstate', 'policy', 'clmviolno', 'cdtscore', 'seqagtno', 'maxtendte', 'inception', 'termincep', 'mltprdind', 'bi_prm', 'comp_prm', 'cmpf_prm', 'coll_prm', 'memberind', 'prinscde', 'ybrthdte', 'obirthdte', 'vhlevel', 'mvyear', 'productgen', 'cov', 'premier', 'premier2', 'mulprod', 'member', 'cved', 'tenure', 'prins', 'ageo', 'agey', 'vehage', 'eft1', 'pifind', 'pifd', 'nohhincdt', 'aaadrivedisc', 'system']]
    logging.info("Gen2 created successfully with {} records".format(len(df)))
    df = df_remove_indexCols(df)
    # Push results data frame to Sqlite DB
    df.to_sql("Gen2", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:* Track retention; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=gen2new nway;
    class mvstate policy;
    output out=gen2_retain (drop=_type_ _freq_);
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from gen2new", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'gen2new')
    df = df_remove_indexCols(df)
    df_3 = df.groupby(['mvstate', 'policy']).size().reset_index()
    df_4 = df_3.drop(columns=0)
    df_creation_logging(df_4, "gen2_retain")
    df_4.to_sql("gen2_retain", con=sqliteConnection, if_exists='replace')
    # Sql Code Start and End Lines - 457&460 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS Gen2_temp;create table Gen2_temp as select a.*,b.policy as retain from gen2 a left join
            gen2_retain b on a.mvstate=b.mvstate and a.policy=b.policy"""
        sql = mcrResl(sql)
        tgtSqliteTable = "Gen2_temp"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from Gen2_temp", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Gen2_Temp')
    df = df_remove_indexCols(df)
    logging.info(
        "Gen2 created successfully with {} records".format(len(df)))
    df.to_sql("Gen2", con=sqliteConnection, if_exists='replace')

    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from gwold ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'gwold')
    # if seqagtno^=.; # Manual effort require.
    df = df.loc[(df.seqagtno != np.nan) | (df.seqagtno != '') | (df.seqagtno != ' ') | ~(df.seqagtno.isnull())]
    df = df.loc[(df.policy != np.nan) | (df.policy != '') | (df.policy != ' ') | ~(df.policy.isnull())]
    # if policy^=.; # Manual effort require.
    df['bi_prm'] = df['bi_prm'].astype(np.float64)
    df['comp_prm'] = df['comp_prm'].astype(np.float64)
    df['coll_prm'] = df['coll_prm'].astype(np.float64)
    indexNames = df[(df['bi_prm'].isin([0, np.nan, 0.0]) | df['bi_prm'].isnull()) & ((df['comp_prm'].isin([0, np.nan, 0.0])) | df['comp_prm'].isnull()) & ((df['coll_prm'].isin([0, np.nan])) | df['coll_prm'].isnull()) ].index
    df.drop(indexNames, inplace=True)
    # if bi_prm in (0 .) and comp_prm in (0 .) and cmpf_prm in (0 .) and coll_prm in (0 .) then delete; # Manual effort require.
    df['productgen'] = 'Gen2'
    # ***Start manual effort here...
    # if bi_prm not in (0 .) and sum(0,comp_prm,cmpf_prm,coll_prm)^=0 then Cov=1;

    df['cdtscore'] = df['cdtscore'].apply(str)
    df['cov'] = [1 if (~((a in [0, 0.0, np.nan]) | (np.isnan(a))) & ~(((np.isnan(b) & np.isnan(d)) | ((b+d) in [0, 0.0, np.nan])))) else 0 if (((b in [0, 0.0, np.nan]) | (np.isnan(b))) & ((d in [0, 0.0, np.nan]) | (np.isnan(d)))) else 99 for a, b, d in zip(df['bi_prm'], df['comp_prm'], df['coll_prm'])]
	
    df['productgen'] = "SA"

    # ***Start manual effort here...
    # if bi_prm not in (0 .) and sum(0,comp_prm,coll_prm)^=0 then Cov=1;
    # df['cov'] = [1 if (x not in [0, np.nan]) & ((y+z) != 0) else 0 if (y in [0, np.nan]) & (
    #    z in [0, np.nan]) else 99 for x, y, z in zip(df['bi_prm'], df['comp_prm'], df['coll_prm'])]
    df['cdtscore'] = df['cdtscore'].apply(str)


    # ***Start manual effort here...
    # else Cov=99;
    # End manual effort.***'''

    # ***Start manual effort here...
    # if cdtscore in ('R4', 'R6', 'R8', 'T0', 'T2', 'T4', 'T6', 'T8') then Premier2=4;
    # End manual effort.***'''

    # ***Start manual effort here...
    # else if cdtscore in ('J6', 'J8', 'L0', 'L2', 'L4', 'L6', 'L8', 'N0', 'N2', 'N4', 'N6', 'N8', 'P0', 'P2', 'P4', 'P6', 'P8', 'R0', 'R2') then Premier2=3;
    # End manual effort.***'''

    # ***Start manual effort here...
    # else if cdtscore in ('D8', 'F0', 'F2', 'F4', 'F6', 'F8', 'H0', 'H2', 'H4', 'H6', 'H8', 'J0', 'J2', 'J4') then Premier2=2;
    # End manual effort.***'''

    # ***Start manual effort here...
    # else Premier2=1;
    df['premier2'] = [4 if x in ['R4', 'R6', 'R8', 'T0', 'T2', 'T4', 'T6', 'T8'] else 3 if x in ['J6', 'J8', 'L0', 'L2', 'L4', 'L6', 'L8', 'N0', 'N2', 'N4', 'N6', 'N8', 'P0', 'P2', 'P4', 'P6', 'P8', 'R0', 'R2']
                      else 2 if x in ['D8', 'F0', 'F2', 'F4', 'F6', 'F8', 'H0', 'H2', 'H4', 'H6', 'H8', 'J0', 'J2', 'J4'] else 1 for x in df['cdtscore']]
    # End manual effort.***'''
    # df.loc[(df['cdtscore'].isin(['R4', 'R6', 'R8', 'T0', 'T2', 'T4', 'T6', 'T8'])),'premier2']=4
    # df.loc[(df['cdtscore'].isin(['J6', 'J8', 'L0', 'L2', 'L4', 'L6', 'L8', 'N0', 'N2', 'N4', 'N6', 'N8', 'P0', 'P2', 'P4', 'P6', 'P8', 'R0', 'R2'])),'premier2']=3

    # ***Start manual effort here...
    # df.loc[(df['mvstate'].isin(['FL'])), 'Mulprod']= 0
    # df.loc[(df['mvstate'].isin(['FL'])) & (
    # if findw(mult_prod_typ_stat_fl,'Life')>0 then Mulprod=1;
    # else Mulprod=0;

    df.loc[df.mvstate == 'FL', 'mulprod'] = 0
    df.loc[((df.mvstate == 'FL') & (df.mult_prod_typ_stat_fl.str.contains(' Life '))), 'mulprod'] = 1

    df.loc[df.mvstate.isin(['GA', 'TN']), 'mulprod'] = 1
    df.loc[((df.mvstate.isin(['GA', 'TN'])) & (df.mult_prod_typ_stat_fl.isin(['Home/Condo Ownership','None']))), 'mulprod'] = 0


    # if mult_prod_typ_stat_gatn in ('Home/Condo Ownership','None') then Mulprod=0;
    # else Mulprod=1;
    # end;
    # End manual effort.***

    # ***Start manual effort here...
    # if PIFIND in ("" "N") then PIFD=0;
    df['pifind'] = df['pifind'].apply(str)
    df['pifd'] = [0 if x in ['nan', '', ' ', "N"] else 1 for x in df['pifind']]
    # End manual effort.***'''

    # ***Start manual effort here...
    # else PIFD=1;
    # End manual effort.***'''

    # ***Start manual effort here...
    # if MEMBERIND in ("Y") then Member=1;
    df['memberind'] = df['memberind'].apply(str)
    df['member'] = [1 if x == "Y" else 0 for x in df['memberind']]
    # End manual effort.***'''

    # ***Start manual effort here...
    # else Member=0;
    # End manual effort.***'''

    # if Tenure=. then tenure=0; # Manual effort require.
    df['tenure'] = df['tenure'].apply(str)
    df.loc[((df.tenure.isnull()) | (df.tenure.isin(['nan', '', ' ']))), 'tenure'] = 0
    df['prins'] = df['prinscde']
    df['ageo'] = df['age_of_old_drvr_hhld']
    df['agey'] = df['age_of_young_drvr_hhld']

    # ***Start manual effort here...
    # if vhlevel in (0 99) then vhlevel=.;
    #df.loc[df['vhlevel'].isin([0, 99]), 'vhlevel'] = np.nan
    # End manual effort.***'''
    df['vhlevel'] = df['vhlevel'].astype(str)
    df.loc[df['vhlevel'].isin(['0', '99', '0.0', '99.0']), 'vhlevel'] = np.nan
    df['vehage'] = df['veh_age']

    # ***Start manual effort here...
    # if DISC_EFT_DISC_FL = 'Y' then EFT1=1;
    # End manual effort.***'''

    # ***Start manual effort here...
    # else EFT1=0;
    df['eft1'] = [1 if x == 'Y' else 0 for x in df['disc_eft_disc_fl']]
    # End manual effort.***'''

    # if AAA_DRV_DISC_FL='Y' then AAADriveDisc=1; # Manual effort require.
    df['aaadrivedisc'] = [1 if x == 'Y' else 0 for x in df['aaa_drv_disc_fl']]

    # ***Start manual effort here...
    # else AAADriveDisc=0;
    # End manual effort.***'''
    #df['agent'] = df['agent'].apply(np.int64)
    # select (abbragt); # Manual effort require.
 
    df['agent'] = [1 if x == 'salesAndServiceAgent' else 1 if x == 'memberRepresentative' else 7 if x == 'salesAgt' else 1 if x == 'salesAndServiceRepresentative' else 8 if x ==
                   'generalAgent' else 3 if x == 'contactCenter' else 2 if x == 'independentAgent' else 9 if x == 'entreprenurialAgent' else 4 if x in ['houseBook', 'AAA.com', 'bookroll'] else 0 for x in df['abbragt']]
    # df['agent']=0
    # df.loc[df['abbragt']=='salesAndServiceAgent','agent']=1
    # df.loc[df['abbragt']=='memberRepresentative','agent']=1
    # df.loc[df['abbragt']=='salesAgt','agent']=7
    # df.loc[df['abbragt']=='salesAndServiceRepresentative','agent']=1
    # df.loc[df['abbragt']=='generalAgent','agent']=8
    #df.loc[df['abbragt']=='contactCenter' ,'agent']=3
    #df.loc[df['abbragt']=='independentAgent' ,'agent']=2
    #df.loc[df['abbragt']=='entreprenurialAgent' ,'agent']=9
    # df.loc[df['abbragt'].isin(['houseBook', 'AAA.com', 'bookroll']) ,'agent']=4'''
    # End manual effort.***
    # dict = {'salesAndServiceAgent': 1}
    # Keep columns in the taget df data in datafram.
    logging.info("GW created successfully with {} records".format(len(df)))
    df = df_remove_indexCols(df)
    # Push results data frame to Sqlite DB
    df.to_sql("GW", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:* Track retention; '''
    '''WARNING: Below SAS step has not converted in this release.
    data=gwnew nway;
    class mvstate policy;
    output out=gw_retain (drop=_type_ _freq_);
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from gwnew", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'gwnew')
    df = df_remove_indexCols(df)
    df_6 = df.groupby(['mvstate', 'policy']).size().reset_index()
    df_7 = df_6.drop(columns=0)
    df_creation_logging(df_7, "gw_retain")
    df_7.to_sql("gw_retain", con=sqliteConnection, if_exists='replace')

    # Sql Code Start and End Lines - 588&591 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS GW_temp;create table GW_temp as select a.*,b.policy as retain from GW a left join gw_retain
            b on a.mvstate=b.mvstate and a.policy=b.policy"""
        sql = mcrResl(sql)
        tgtSqliteTable = "GW_temp"
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    df = pd.read_sql_query("select * from GW_temp", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'GW_Temp')
    df = df_remove_indexCols(df)
    logging.info(
        "GW created successfully with {} records".format(len(df)))
    df.to_sql("GW", con=sqliteConnection, if_exists='replace')

    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df1 = pd.read_sql_query("select * from Gen1 ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df1, 'Gen1')
    # Drop columns from source df data in datafram.
    df1 = df1.drop(columns=["termincep", "inception"])
    # Converting source df data into datafram.
    df2 = pd.read_sql_query("select * from Gen2 ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df2, 'Gen2')
    # Drop columns from source df data in datafram.
    df2 = df2.drop(columns=["termincep", "inception"])
    # if retain==migrate1. then Retention=0; # Manual effort require.
    # ***Start manual effort here...
    # else Retention=1;
    df = pd.concat([df1, df2], ignore_index=True, sort=False)
    # End manual effort.***'''
    df['cov'] = df['cov'].astype(str)
    df['retain'] = df['retain'].astype(str)
    df['migrate1'] = df['migrate1'].astype(str)
    df['retention'] = [0 if ((x == y) & (x in ['nan', '', ' '])) else 1 for x,
                       y in zip(df["retain"], df["migrate1"])]
    df['vehfullcov'] = [1 if x in ['1', '1.0'] else 0 for x in df['cov']]
    df['nohhincdt'] = [0 if x == 'Gen1' else 1 for x in df['productgen']]

    # if cov=1 then VehFullCov=1; # Manual effort require.

    # ***Start manual effort here...
    # else VehFullCov=0;
    # End manual effort.***'''

    # ***Start manual effort here...
    # if productgen = 'Gen1' then nohhincdt = 0;
    # End manual effort.***'''

    # Keep columns in the taget df data in datafram.
    df = df[['vehage', 'retention', 'member', 'cved', 'policy', 'eft1', 'pifind', 'seqagtno', 'cov', 'aaadrivedisc', 'prins', 'vehfullcov', 'nohhincdt',
             'mvstate', 'premier', 'premier2', 'productgen', 'assigneddrvage', 'mulprod', 'tenure', 'ageo', 'system', 'agey', 'vhlevel', 'pifd']]
    df = df_remove_indexCols(df)
    logging.info(
        "Retain{} created successfully with {} records".format(Y2, len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("Retain{}".format(Y2), con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*** AJS - Updated to change to identify Joint Venture agents by community code in the first agent table (ignore EC Excel file); '''

    def Regional8(Y1):
        # Open connection to Sqlite work data base
        sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
        # Converting source df data into datafram.
        sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
        with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/agent{}.sas7bdat'.format(Y1)) as reader:
            df = reader.to_data_frame()
            df.to_sql("agent{}".format(Y1),
                      con=sqliteConnection, if_exists='replace')
            sqliteConnection.close()
        sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
        df = pd.read_sql_query(
            "select * from agent{}".format(Y1), sqliteConnection)
        # handling data frame column case senstivity.#
        df_lower_colNames(df, 'agent{}'.format(Y1))
        df = df[['seql_no_', 'agent_type', 'community']]
        # df['agent']=df['agent'].astype('Int64')
        df['agent'] = df['agent_type']
        df = df_remove_indexCols(df)
        logging.info(
            "agent created successfully with {} records".format(len(df)))
        # Push results data frame to Sqlite DB
        df.to_sql("agent", con=sqliteConnection, if_exists='replace')
        # Close connection to Sqlite work data base
        sqliteConnection.close()
        #*******************************End of Data Step Process**************************************************#
    Regional8(202002)

    # Sql Code Start and End Lines - 618&621 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS retain_temp;create table retain_temp as select a.*,b.agent, b.community from Retain{} a left
            join agent b on a.seqagtno=b.seql_no_""".format(Y2)
        sql = mcrResl(sql)
        tgtSqliteTable = mcrResl("retain_temp")
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from retain_temp", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'retain_temp')
    df = df_remove_indexCols(df)
    logging.info(
        "Retain{} created successfully with {} records".format(Y2, len(df)))
    df.to_sql("Retain{}".format(Y2), con=sqliteConnection, if_exists='replace')
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from Retain{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retain{}'.format(Y2))
    df['community'] = df['community'].astype(str)
    # ***Start manual effort here...
    if not df.empty:
        df.loc[df['community'].isin(
            ['O002', '0002', 'O089', '0089', 'K006', 'V004']), 'aaa_ec'] = 1
    else:
        df['aaa_ec'] = np.nan
    # if community in ('O002', 'O089', 'K006', 'V004') then aaa_ec=1;
    # End manual effort.***'''
    df['seqagtno'] = df['seqagtno'].astype(str)
    # if seqagtno=379346 then agent=3; # Manual effort require.
    df.loc[df.seqagtno == '379346', 'agent'] = 3
    df = df_remove_indexCols(df)
    logging.info(
        "Retain{} created successfully with {} records".format(Y2, len(df)))
    df.to_sql("Retain{}".format(Y2), con=sqliteConnection, if_exists='replace')

    '''SAS Comment:*DRIVER is gen1 and DRIVERGENII is gen2; '''
    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=Retain&Y2 nway;
    class mvstate system productgen policy;
    var agent aaa_ec cov vehfullcov premier premier2 mulprod tenure cved retention Member prins assigneddrvage vhlevel VehAge eft1 pifd nohhincdt AAADriveDisc;
    output out=retain&Y2 (drop=_type_ rename=_freq_=VehCnt)max(agent AAA_EC VehFullCov premier premier2 mulprod tenure CVED retention Member prins AssignedDrvAge AgeO vhlevel EFT1 PIFD nohhincdt AAADriveDisc)=sum(cov)=CovSum mean(cov)=CovAvg max(cov)=CovMax min(cov AssignedDrvAge AgeY vhlevel VehAge)=CovMin AssignedDrvY AgeY vhlevelB1 VehAgeN1;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from Retain{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'Retain{}'.format(Y2))
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
    agg_cols = {'agent', 'aaa_ec', 'cov', 'vehfullcov', 'premier', 'premier2', 'mulprod', 'tenure', 'cved', 'retention',
                'member', 'prins', 'assigneddrvage', 'vhlevel', 'vehage', 'eft1', 'pifd', 'nohhincdt', 'aaadrivedisc'}
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value='')
    for i in df.columns:
        try:
            df[i] = df[i].apply(pd.to_numeric)
        except:
            pass

    df_summ = df.groupby(['mvstate', 'system', 'productgen', 'policy'], as_index=False).agg(
        agent=pd.NamedAgg(column='agent', aggfunc=max), aaa_ec=pd.NamedAgg(column='aaa_ec', aggfunc=max),
        vehfullcov=pd.NamedAgg(column='vehfullcov', aggfunc=max), premier=pd.NamedAgg(column='premier', aggfunc=max), premier2=pd.NamedAgg(column='premier2', aggfunc=max),
        mulprod=pd.NamedAgg(column='mulprod', aggfunc=max), tenure=pd.NamedAgg(column='tenure', aggfunc=max), cved=pd.NamedAgg(column='cved', aggfunc=max),
        retention=pd.NamedAgg(column='retention', aggfunc=max), member=pd.NamedAgg(column='member', aggfunc=max),
        prins=pd.NamedAgg(column='prins', aggfunc=max), assigneddrvage=pd.NamedAgg(column='assigneddrvage', aggfunc=max), assigneddrvy=pd.NamedAgg(column='assigneddrvage', aggfunc=min),
        ageo=pd.NamedAgg(column='ageo', aggfunc=max), vhlevel=pd.NamedAgg(column='vhlevel', aggfunc=max), vhlevelb1=pd.NamedAgg(column='vhlevel', aggfunc=min), eft1=pd.NamedAgg(column='eft1', aggfunc=max),
        pifd=pd.NamedAgg(column='pifd', aggfunc=max), aaadrivedisc=pd.NamedAgg(column='aaadrivedisc', aggfunc=max), nohhincdt=pd.NamedAgg(column='nohhincdt', aggfunc=max), covsum=pd.NamedAgg(column='cov', aggfunc=sum), covavg=pd.NamedAgg(column='cov', aggfunc=np.mean),
        covmax=pd.NamedAgg(column='cov', aggfunc=max), covmin=pd.NamedAgg(column='cov', aggfunc=min), agey=pd.NamedAgg(column='agey', aggfunc=min), vehagen1=pd.NamedAgg(column='vehage', aggfunc=min))
    df['policy_temp'] = df['policy']

    df_summ[['mvstate', 'system', 'productgen', 'policy', 'vehcnt']] = df.groupby(
        ['mvstate', 'system', 'productgen', 'policy'], as_index=False)['cov'].count()
    df_summ['agent'] = df_summ['agent'].astype('Int64')
    df_creation_logging(df_summ, 'Retain{}'.format(Y2))
    df_summ.to_sql("Retain{}".format(
        Y2), con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    client=bigquery.Client()
	sql = """select * from {}.{}.driver where mondate = {}""".format(project_id, gen1_dataset_id, Y1)
    df_driver_gen1 = client.query(sql).to_dataframe()
	'''
	# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/driver{}.sas7bdat'.format(Y1)) as reader:
        df = reader.to_data_frame()
        df.to_sql("driver{}".format(Y1),
                  con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()'''
	
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    '''df = pd.read_sql_query(
        "select * from driver{}".format(Y1), sqliteConnection)'''
    df = df_driver_gen1
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'driver')
    df['birthdte1'] = df['birthdte']
    df = df[['policy', 'drivtype', 'drvstatus', 'birthdte']]
    df = df_remove_indexCols(df)
    logging.info(
        "driver created successfully with {} records".format(len(df)))
    del df_driver_gen1
	# Push results data frame to Sqlite DB
    df.to_sql("driver", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    sql = """select * from {}.{}.driver where mondate = {}""".format(project_id, gen2_dataset_id, Y1)
    df_driver_gen2 = client.query(sql).to_dataframe()
	'''
	# Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/drivergenii{}.sas7bdat'.format(Y1)) as reader:
        df = reader.to_data_frame()
        df.to_sql("drivergenii{}".format(Y1),
                  con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()'''

    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df1 = pd.read_sql_query("select * from driver", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df1, 'driver')
    df1 = df1.drop(columns="birthdte")
    df1 = df1.rename(columns={"birthdte1": "birthdte"})
    # Converting source df data into datafram.
    '''df2 = pd.read_sql_query(
        "select * from drivergenii{}".format(Y1), sqliteConnection)'''
	df2 = df_driver_gen2
    # handling data frame column case senstivity.#
    df_lower_colNames(df2, 'drivergenii')
    df2 = df2[['policy', 'drivtype', 'drvstatus', 'birthdte']]
    df = pd.concat([df1, df2], ignore_index=True, sort=False)
    df['bday'] = df.birthdte
    df['policy'] = df['policy'].astype(float)
    #df['birthdte'] = pd.to_datetime(df['birthdte'])
    #df['bday'] = df.birthdte.dt.strftime('%y%m%d').astype(float)
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
    #*******************************End of Data Step Process**************************************************#

    '''if AAADRHHIND="Y" :
    AAADriveDisc=1
	    keep mvstate policy clmviolno cdtscore seqagtno maxtendte inception termincep mltprdind bi_prm comp_prm cmpf_prm coll_prm MEMBERIND prinscde ybrthdte obirthdte vhlevel mvyear
    productgen Cov Premier Premier2 Mulprod Member CVED Tenure prins AgeO AgeY VehAge EFT1 PIFIND PIFD nohhincdt AAADriveDisc system
    '''

    # Sql Code Start and End Lines - 192&200 #

    # Converting source df data into datafram.
    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=driver nway;
    class policy;
    var bday;
    output out=driver (drop=_type_ _freq_) max(bday)=BdayY min(bday)=BdayO;
    run;
    '''

    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from driver", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'driver')
    df = df_remove_indexCols(df)
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
    agg_cols = {'bday'}
    df['bday'] = df['bday'].astype(str)
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value='')
    df_summ = df.groupby(['policy'], as_index=False).agg(bdayy=pd.NamedAgg(
        column='bday', aggfunc=max), bdayo=pd.NamedAgg(column='bday', aggfunc=min))
    df_summ['bdayy_year'], df_summ['bdayy_month'], df_summ['bdayy_day'] = (df_summ['bdayy'].str.slice(0, 4).astype(
        float, errors='ignore'), df_summ['bdayy'].str.slice(4, 6).astype(float, errors='ignore'), df_summ['bdayy'].str.slice(6, 8).astype(float, errors='ignore'))
    df_summ['bdayo_year'], df_summ['bdayo_month'], df_summ['bdayo_day'] = (df_summ['bdayo'].str.slice(0, 4).astype(
        float, errors='ignore'), df_summ['bdayo'].str.slice(4, 6).astype(float, errors='ignore'), df_summ['bdayo'].str.slice(6, 8).astype(float, errors='ignore'))

    df_creation_logging(df_summ, 'driver')
    df_summ.to_sql("driver", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
    # sqliteConnection.close()

    '''WARNING: Below SAS step has not converted in this release.'''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from gen1old ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'gen1old')
    df = df[['policy', 'termincep']]
    df = df_remove_indexCols(df)
    #df['termincep1'] = df['termincep']
    logging.info(
        "termincep created successfully with {} records".format(len(df)))
    df = df_remove_indexCols(df)
    # Push results data frame to Sqlite DB
    df.to_sql("termincep", con=sqliteConnection, if_exists='replace')
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
    df1 = pd.read_sql_query("select * from termincep ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df1, 'termincep')
    # Rename columns in source df data in datafram.
    df1 = df1.rename(columns={"termincep1": "termincep"})
    # Drop columns from source df data in datafram.
    df1 = df1.drop(columns="termincep")
    # Converting source df data into datafram.
    df2 = pd.read_sql_query(
        "select policy,termincep from gen2old ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df2, 'gen2old')
    df = pd.concat([df1, df2], ignore_index=True, sort=False)
    df = df[['policy', 'termincep']]
    df['incep'] = df.termincep
    #df['termincep'] = pd.to_datetime(df['termincep'])
    #df['incep'] = df.termincep.dt.strftime('%y%m%d').astype(float)
    # df['incep'] = mdy(df['termincep'].str.slice(4, 6).astype(int), df['termincep'].str.slice(6, 8).astype(int), df['termincep'].str.slice(0, 4).astype(int))
    # Drop columns in the target df data in datafram.
    df = df.drop(columns="termincep")
    df = df_remove_indexCols(df)
    logging.info(
        "termincep created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("termincep", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
        '''
    proc summary data=termincep nway;
    class policy;
    var incep;
    output out=termincep (drop=_type_ _freq_) min=;
    run;
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
    df['incep']=df['incep'].astype(str)
    df['incep_year'], df['incep_month'], df['incep_day'] = (df['incep'].str.slice(0, 4).astype(float ,errors='ignore'), df['incep'].str.slice(4, 6).astype(float,errors='ignore'), df['incep'].str.slice(6, 8).astype(float,errors='ignore'))
    df_creation_logging(df, 'termincep')
    df.to_sql("termincep", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
    # Sql Code Start and End Lines - 673&680 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.

    try:
        sql = """DROP TABLE IF EXISTS retain_temp2;create table retain_temp2 as select a.*,
			CAST((c.incep_year)-(b.bdayy_year)+((c.incep_month)-(b.bdayy_month))/12+((c.incep_day)-(b.bdayy_day))/365 AS INT) as AgeY1,
			CAST((c.incep_year)-(b.bdayo_year)+((c.incep_month)-(b.bdayo_month))/12+((c.incep_day)-(b.bdayo_day))/365 AS INT) as AgeO1
			from Retain{} a left join driver b
			on a.policy=b.policy
			left join termincep c
			on a.policy=c.policy""".format(Y2)
        sql = mcrResl(sql)
        sql = mcrResl(sql)
        tgtSqliteTable = mcrResl("retain_temp2")
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from retain_temp2", sqliteConnection)
    df_lower_colNames(df, 'retain_temp2')
    df = df_remove_indexCols(df)
    logging.info(
        "Retain{} created successfully with {} records".format(Y2, len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("Retain{}".format(Y2), con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()

    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query(
        "select * from drivergenii{}".format(Y1), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'drivergenii{}'.format(Y1))
    # Keep columns in the taget df data in datafram.
    df = df[['policy', 'totalpts']]
    df = df_remove_indexCols(df)
    logging.info("ptsdb created successfully with {} records".format(len(df)))
    # Push results data frame to Sqlite DB
    df.to_sql("ptsdb", con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''SAS Comment:*01-17 - sum totalpts; '''

    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=ptsdb nway;
    class policy;
    var totalpts;
    output out=sumpts (drop=_type_ _freq_) sum=;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query("select * from ptsdb", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'ptsdb')
    df = df_remove_indexCols(df)
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
    agg_cols = {'totalpts'}
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value='')
    df = df.groupby(['policy']).agg({'totalpts': sum}).reset_index()
    df_creation_logging(df, 'sumpts')
    df.to_sql("sumpts", con=sqliteConnection, if_exists='replace')

    '''SAS Comment:*01-17 - summarize totalpts and change name to surchrgpts; '''

    # Sql Code Start and End Lines - 698&702 #
    '''***************************************************
    Below Python Code Executes The Standard SAS PROC SQL.
    ******************************************************'''
    # Connections to Sqlite DB and fetch all data from source table to process
    # Please check if any SAS functions are not converted in SqLite query.
    try:
        sql = """DROP TABLE IF EXISTS retain_Y2;create table retain_Y2 as select a.*, b.totalpts as surchrgpts from Retain{} a
            left join sumpts b on a.policy=b.policy""".format(Y2)
        sql = mcrResl(sql)
        tgtSqliteTable = mcrResl("retain_Y2")
        procSql_standard_Exec(SQLitePythonWorkDb, sql, tgtSqliteTable)
    except:
        e = sys.exc_info()[0]
        logging.error('Table creation/update is failed.')
        logging.error('Error - {}'.format(e))
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

    '''SAS Comment:****************************************************; '''
    '''SAS Comment:*GW HHLD summary and then add to retain ds below; '''
    '''*********************************************************************************
    Below python code is to execute standard SAS data step
    *********************************************************************************'''
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df = pd.read_sql_query("select * from GW ", sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'GW')
    # if retain=. then Retention=0; # Manual effort require.
    df['retain'] = df['retain'].astype(str)
    df['retention'] = [0 if x in ['nan', '', ' '] else 1 for x in df['retain']]

    # ***Start manual effort here...
    # else Retention=1;
    # End manual effort.***'''

    # if cov=1 then VehFullCov=1; # Manual effort require.
    df['cov'] = df['cov'].astype(str)
    df['vehfullcov'] = [1 if x in ['1', '1.0'] else 0 for x in df['cov']]

    # ***Start manual effort here...
    # else VehFullCov=0;
    # End manual effort.***'''

    # ***Start manual effort here...
    # if productgen = 'SA' then do;
    df.loc[df.productgen == 'SA', 'nohhincdt'] = 0
    df.loc[df.productgen == 'SA', 'cved'] = 0
    df.loc[df.productgen == 'SA', 'aaa_ec'] = 0
    df.loc[df.productgen == 'SA', 'premier'] = 0
    df.loc[df.productgen == 'SA', 'assigneddrvage'] = 0


    # Keep columns in the taget df data in datafram.

    df = df[['vehage', 'retention', 'member', 'agent', 'cved', 'policy', 'eft1', 'pifind', 'seqagtno', 'cov', 'aaadrivedisc', 'prins', 'vehfullcov', 'nohhincdt',
             'mvstate', 'premier', 'premier2', 'productgen', 'mulprod', 'tenure', 'ageo', 'aaa_ec', 'system', 'assigneddrvage', 'agey', 'vhlevel', 'pifd']]
    logging.info(
        "GWretain{} created successfully with {} records".format(Y2, len(df)))
    df = df_remove_indexCols(df)
    # Push results data frame to Sqlite DB
    df.to_sql("GWretain{}".format(Y2),
              con=sqliteConnection, if_exists='replace')
    # Close connection to Sqlite work data base
    sqliteConnection.close()
    #*******************************End of Data Step Process**************************************************#
    '''WARNING: Below SAS step has not converted in this release.
    proc summary data=GWretain&Y2 nway;
    class mvstate system productgen policy;
    var agent AAA_EC cov vehfullcov premier premier2 mulprod tenure CVED retention Member prins AssignedDrvAge vhlevel VehAge EFT1 PIFD nohhincdt AAADriveDisc;
    output out=GWretain&Y2 (drop=_type_ rename=_freq_=VehCnt)max(agent AAA_EC VehFullCov premier premier2 mulprod tenure CVED retention Member prins AssignedDrvAge AgeO vhlevel EFT1 PIFD nohhincdt AAADriveDisc)=sum(cov)=CovSum mean(cov)=CovAvg max(cov)=CovMax min(cov AssignedDrvAge AgeY vhlevel VehAge)=CovMin AssignedDrvY AgeY vhlevelB1 VehAgeN1;
    run;
    '''
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    df = pd.read_sql_query(
        "select * from GWretain{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df, 'GWretain{}'.format(Y2))
    str_cols = set(df.select_dtypes(include=['object', 'string']).columns)
    agg_cols = {'agent', 'aaa_ec', 'cov', 'vehfullcov', 'premier', 'premier2', 'mulprod', 'tenure', 'cved', 'retention',
                'member', 'prins', 'assigneddrvage', 'vhlevel', 'vehage', 'eft1', 'pifd', 'nohhincdt', 'aaadrivedisc'}
    final_cols = list(agg_cols.intersection(str_cols))
    df[final_cols] = df[final_cols].fillna(value='')
    for i in df.columns:
        try:
            df[i] = df[i].apply(pd.to_numeric)
        except:
            pass

    df_summ = df.groupby(['mvstate', 'system', 'productgen', 'policy'], as_index=False).agg(
        agent=pd.NamedAgg(column='agent', aggfunc=max), aaa_ec=pd.NamedAgg(column='aaa_ec', aggfunc=max),
        vehfullcov=pd.NamedAgg(column='vehfullcov', aggfunc=max), premier=pd.NamedAgg(column='premier', aggfunc=max), premier2=pd.NamedAgg(column='premier2', aggfunc=max),
        mulprod=pd.NamedAgg(column='mulprod', aggfunc=max), tenure=pd.NamedAgg(column='tenure', aggfunc=max), cved=pd.NamedAgg(column='cved', aggfunc=max),
        retention=pd.NamedAgg(column='retention', aggfunc=max), member=pd.NamedAgg(column='member', aggfunc=max),
        prins=pd.NamedAgg(column='prins', aggfunc=max), assigneddrvage=pd.NamedAgg(column='assigneddrvage', aggfunc=max), assigneddrvagey=pd.NamedAgg(column='assigneddrvage', aggfunc=min),
        ageo=pd.NamedAgg(column='ageo', aggfunc=max), vhlevel=pd.NamedAgg(column='vhlevel', aggfunc=max), vhlevelb1=pd.NamedAgg(column='vhlevel', aggfunc=min), eft1=pd.NamedAgg(column='eft1', aggfunc=max),
        pifd=pd.NamedAgg(column='pifd', aggfunc=max), aaadrivedisc=pd.NamedAgg(column='aaadrivedisc', aggfunc=max), nohhincdt=pd.NamedAgg(column='nohhincdt', aggfunc=max), covsum=pd.NamedAgg(column='cov', aggfunc=sum), covavg=pd.NamedAgg(column='cov', aggfunc=np.mean),
        covmax=pd.NamedAgg(column='cov', aggfunc=max), covmin=pd.NamedAgg(column='cov', aggfunc=min), agey=pd.NamedAgg(column='agey', aggfunc=min), vehagen1=pd.NamedAgg(column='vehage', aggfunc=min))
    df_summ[['mvstate', 'system', 'productgen', 'policy', 'vehcnt']] = df.groupby(
        ['mvstate', 'system', 'productgen', 'policy'], as_index=False)['cov'].count()
    df_creation_logging(df_summ, 'GWretain{}'.format(Y2))
    df_summ.to_sql("GWretain{}".format(
        Y2), con=sqliteConnection, if_exists='replace')

    '''SAS Comment:************************************************************************************************************; '''
    '''SAS Comment:*Categorize; '''
    '''**WARNING:Below steps are not included in logic calculation. Please amend them manually.
    length AgentType $10;AgentType="Unknown";if Agent in (1 7 8) then AgentType="Captive";else if Agent = 9 then AgentType="EA";else if Agent in (3 4) then AgentType="MSC/HB";else if mvstate in ('OH' 'KY' 'WV') and AAA_EC=1 then AgentType="EC";else if agent in (2 6) then AgentType="IA";if AgentType="Unknown" then AgentType="MSC/HB";length Coverage $40;Coverage="Mixed Full Liability Comp/Collision";if CovSum=VehCnt then Coverage="Full Coverage";else if CovMax=0 then Coverage="Liability Only";else if CovAvg=99 then Coverage="Comp/Collision Only";else if CovMax=1 then Coverage="Mixed Full Liability";else if CovMin=1 and CovMax=99 then Coverage="Mixed Full Comp/Collision";else if VehFullCov^=1 then Coverage="Mixed Liability Comp/Collision";if Coverage="Full Coverage" or Coverage="Mixed Full Comp/Collision" then Coverage="Full Cov";if Coverage="Liability Only" then Coverage="Lia Only";if Coverage="Comp/Collision Only" or Coverage="Mixed Full Liability" or Coverage="Mixed Liability Comp/Collision"or Coverage="Mixed Full Liability Comp/Collision" then Coverage="Mixed";length PremierGrp $40;if premier=1 then PremierGrp="Low";else if premier=2 then PremierGrp="Med";else PremierGrp="High";length PremierGrp2 $40;if premier2=1 then PremierGrp2="Low";else if premier2=2 then PremierGrp2="Mid-Low";else if premier2=3 then PremierGrp2="Mid-High";else PremierGrp2="High";length MultiProd $40;if mulprod=0 then MultiProd="No";else MultiProd="Yes";length Mem $40;if Member=0 then Mem="No";else Mem="Yes";length PIF $40;if PIFD=0 then PIF="No";else PIF="Yes";if productgen="Gen1" then PIF="";length PriorInsStatus $40;if mvstate in ('WV' 'IA') then do;if prins in (1212 1222) then PriorInsStatus="<100/300 ";else if prins in (1112 1122) then PriorInsStatus="20/40";else if prins in (1312 1322) then PriorInsStatus=">=100/300";else PriorInsStatus="N/A";end;if mvstate in ('MN') then do;if prins in (1212 1222) then PriorInsStatus="<100/300";else if prins in (1112 1122) then PriorInsStatus="30/60";else if prins in (1312 1322) then PriorInsStatus=">=100/300";else PriorInsStatus="N/A";end;if mvstate in ('WI' 'KY' 'OH' 'TN' 'GA' 'IN' 'NE' 'ND') and system = 'I' then do;if prins in (1212 1222) then PriorInsStatus="<100/300";else if prins in (1112 1122) then PriorInsStatus="25/50";else if prins in (1312 1322) then PriorInsStatus=">=100/300";else PriorInsStatus="N/A";end;if mvstate = 'IL' then do;if prins in (1212 1222) then PriorInsStatus="<100/300";else if prins in (1112 1122) then PriorInsStatus="<=25/50";else if prins in (1312 1322) then PriorInsStatus=">=100/300";else PriorInsStatus="N/A";end;if mvstate in ('GA' 'TN') and system = 'G' then do;if prins = 1212 then PriorInsStatus="<100/300";else if prins = 1112 then PriorInsStatus="<=25/50";else if prins = 1312 then PriorInsStatus=">=100/300";else PriorInsStatus="N/A";end;if mvstate = 'FL' then do;if prins in (1612 1712) then PriorInsStatus="<100/300";else if prins in (1212 1312 1412 1512) then PriorInsStatus="<=25/50";else if prins in (1812 1912 2012 2112 2212) then PriorInsStatus=">=100/300";else         PriorInsStatus="N/A";end;if productgen="Gen1" then PriorInsStatus="";if system = 'I' then do;if AgeY1=. then AgeY1=AgeY;if AgeY1=. then AgeY1=AssignedDrvY;end;else if system = 'G' then AgeY1=AgeY;length AgeYoungest $40;if AgeY1>=65 then AgeYoungest=">64";else if AgeY1>=45 then AgeYoungest="45-64";else if AgeY1>=30 then AgeYoungest="30-44";else if AgeY1>=25 then AgeYoungest="25-29";else AgeYoungest="<25";if system = 'I' then do;if AgeO1=. then AgeO1=AgeO;if AgeO1=. then AgeO1=AssignedDrvAge;end;else if system = 'G' then AgeO1=AgeO;length AgeOldest $40;if AgeO1<25 then AgeOldest="<25";else if AgeO1<30 then AgeOldest="25-29";else if AgeO1<45 then AgeOldest="30-44";else if AgeO1<65 then AgeOldest="45-64";else AgeOldest=">64";if vhlevel=. then VHLevelW="N/A";else VHLevelW=put(vhlevel,z2.);if productgen="Gen1" then VHLevelW="";if vhlevelB1=. then VHLevelB="N/A";else VHLevelB=put(vhlevelB1,z2.);if productgen="Gen1" then VHLevelB="";length NoVeh $40;if VehCnt>3 then NoVeh=">3";else if VehCnt>2 then NoVeh="3";else if VehCnt>1 then NoVeh="2";else NoVeh="1";length VehAgeN $40;if VehAgeN1>15 then VehAgeN=">15";else if VehAgeN1>10 then VehAgeN="11-15";else if VehAgeN1>5 then VehAgeN="6-10";else if VehAgeN1>1 then VehAgeN="2-5";else VehAgeN="0-1";length EFT $40;if EFT1=1 then EFT="Yes";else if EFT1=0 then EFT="No";else EFT="N/A";length Incidents $40;if productgen = 'Gen2' then do;if nohhincdt = 0 and surchrgpts = 0 then Incidents ="1";elseif nohhincdt > 2 or cved = 99 then Incidents = "5";elseif cved < 99 then do;if nohhincdt = 1 and surchrgpts = 0 then Incidents ="2";elseif nohhincdt < 2 and surchrgpts > 0 then Incidents ="3";elseif nohhincdt = 2 then Incidents ="4";end;end;length AAADrive $40;if AAADriveDisc=1 then AAADrive="Yes";else AAADrive="No";if productgen="Gen1" then AAADrive="";keep Mem mvstate productgen policy retention agenttype coverage cved premiergrp premiergrp2 multiprod tenure PriorInsStatus AgeYoungest AgeOldest VHLevelW VHLevelBNoVeh VehAgeN EFT PIF Incidents AAADrive system;'''
    '''SAS Comment:* Get active/assigned driver age from driver file; '''

    # Converting source retain&Y2 data into datafram.
    # Please Note - If any library references remove them accordingly post your code analysis.#
    # Open connection to Sqlite work data base
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    df1 = pd.read_sql_query(
        "select * from Retain{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df1, 'Retain{}'.format(Y2))
    # Converting source df data into datafram.
    df2 = pd.read_sql_query(
        "select * from GWretain{}".format(Y2), sqliteConnection)
    # handling data frame column case senstivity.#
    df_lower_colNames(df2, 'GWretain{}'.format(Y2))
    df = pd.concat([df1, df2], ignore_index=True, sort=False)
    #df['agey1'] = df['agey1'].apply(np.floor)
    #df['ageo1'] = df['ageo1'].apply(np.floor)
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
           & (df.aaa_ec.isin(['1', '01', '1.0'])) & ~(df['agent'].isin(['1', '7', '8', '01', '07', '08', '1.0', '7.0', '8.0', '9', '09', '9.0', '4', '04', '4.0', '3', '03', '3.0'])), 'agenttype'] = 'EC'
    df.loc[(df['agent'].isin(['2', '02', '2.0', '6', '06', '6.0'])) & ~((df['mvstate'].isin(
        ['KY', 'WV', 'OH'])) & (df.aaa_ec == 1)), 'agenttype'] = 'IA'


    df.loc[df.agenttype == 'Unknown', 'agenttype'] = 'MSC/HB'
	
    df['coverage'] = "Mixed Liability Full Comp/Collision"
    # df['coverage']=["Full Coverage" if a =="VehCent" else "Liability Only" if b==0 else "Comp/Collision Only" if c==99 else "Mixed Full Liability" if x==1 else "Mixed Full Comp/Collision" if (d==1 & b==99) else "Mixed Liability Comp/Collision" if e!=1 for a,b,c,d,e in zip(df['CovSum'],df['CovMax'],df['CovAvg'],df['CovMax'],df['CovMin'],df['VehFullCov'])]
    df.loc[df.covsum == df.vehcnt, 'coverage'] = "Full Coverage"
    df.loc[((df.covmax == 0) | (df.covmax == 0.0)) , 'coverage'] = "Liability Only"
    df.loc[(((df.covavg == 99) | (df.covavg == 99.0)) & ~((df.covmax == 0) | (df.covmax == 0.0))), 'coverage'] = "Comp/Collision Only"
    df.loc[(((df.covmax == 1) | (df.covmax == 1.0)) & ~(((df.covavg == 99) | (df.covavg == 99.0)) | ((df.covmax == 0) | (df.covmax == 0.0)))), 'coverage'] = "Mixed Full Liability"
    df.loc[((df.covmin.isin([1, 1.0])) & (df.covmax.isin([99, 99.0]))) & ~(((df.covmax == 1) | (df.covmax == 1.0)) | (((df.covavg == 99) | (df.covavg == 99.0)) | ((df.covmax == 0) | (df.covmax == 0.0))))
           ,'coverage'] = "Mixed Full Comp/Collision"
    df.loc[(~(df.vehfullcov.isin([1, 1.0])) & ~(((df.covmin.isin([1, 1.0])) & (df.covmax.isin([99, 99.0]))) | (((df.covmax == 1) | (df.covmax == 1.0)) | (((df.covavg == 99) | (df.covavg == 99.0)) | ((df.covmax == 0) | (df.covmax == 0.0)))))), 'coverage'] = "Mixed Liability Comp/Collision"

    df.loc[(df.coverage == "Full coverage") | (df.coverage ==
                                               "Mixed Full Comp/Collision"), 'coverage'] = "Full Cov"
    df.loc[df.coverage == "Liability Only", 'coverage'] = "Lia Only"
    df.loc[(df.coverage == "Comp/Collision Only") | (df.coverage == "Mixed Full Liability") | (df.coverage == "Mixed Liability Comp/Collision")
           | (df.coverage == "Mixed Full Liability Comp/Collision"), 'coverage'] = "Mixed"

    # ***Start manual effort here...
    # else PremierGrp="High";
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
    df['vhlevel']=pd.isnull(df['vhlevel'])
    #df['vhlevel']=df['vhlevel'].fillna(0).astype(np.int64)
    df['vhlevelw'] = ['N/A' if ((x ==
                      np.nan) | (x == '') | (x == ' '))  else int(x) for x in df['vhlevel']]

    df['vhlevelw'] = [('0' + str(x)) if len(str(x)) == 1  else str(x) for x in df['vhlevelw']]
    # ***Start manual effort here...
    # else VHLevelW=put(vhlevel,z2.);
    # End manual effort.***'''

    # if productgen="Gen1" then VHLevelW=""; # Manual effort require.
    df.loc[df.productgen == 'Gen1', 'vhlevelw'] = ''

    # if vhlevelB1=. then VHLevelB="N/A"; # Manual effort require.
    #df['vhlevelb1']=df['vhlevelb1'].fillna(0).astype('Int64')
    df['vhlevelb1']=pd.isnull(df['vhlevelb1'])
    df['vhlevelb'] = ['N/A' if ((x ==
                      np.nan) | (x == '') | (x == ' '))  else int(x) for x in df['vhlevelb1']]

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
    df.loc[(df['productgen'] == "Gen2") & (df['nohhincdt'] > 2)
           & (df['cved'] == 99) & ~((df['nohhincdt'] == 0)
           & (df['surchrgpts'] == 0)), 'incidents'] = 5
    df.loc[(df['productgen'] == "Gen2") & (df['cved'] < 99) & (
        df['nohhincdt'] == 1) & (df['surchrgpts'] == 0) & ~((df['nohhincdt'] > 2)& (df['cved'] == 99) | ((df['nohhincdt'] == 0)
           & (df['surchrgpts'] == 0))), 'incidents'] = 2
    df.loc[(df['productgen'] == "Gen2") & (df['cved'] < 99) & (
        df['nohhincdt'] < 2) & (df['surchrgpts'] > 0) & ~((df['nohhincdt'] > 2)& (df['cved'] == 99) | ((df['nohhincdt'] == 0)
           & (df['surchrgpts'] == 0))), 'incidents'] = 3
    df.loc[(df['productgen'] == "Gen2") & (df['cved'] < 99)
           & (df['nohhincdt'] == 2) & ~((df['nohhincdt'] > 2)& (df['cved'] == 99) | ((df['nohhincdt'] == 0)
           & (df['surchrgpts'] == 0))), 'incidents'] = 4
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
    sql = """select * from {}.{}.cnclratio_regautodb""".format(project_id, output_database)
    df_hhldrtn_regautodb = client.query(sql).to_dataframe()
    df_hhldrtn_regautodb.to_gbq(destination_table = output_dataset + '.' + 'cnclratio_regautodb_backup', project_id = project_id, if_exists='replace')

	'''
	sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    # Converting source df data into datafram.
    sqliteConnection = sqlite3.connect(SQLitePythonWorkDb)
    with SAS7BDAT('/data02/sas2py_poc/act/ipm_rrc/ipm_rrc_hhldrtn_regional_auto/data/hhldrtn_regautodb.sas7bdat') as reader:
        df = reader.to_data_frame()
        df_lower_colNames(df, 'hhldrtn_regautodb')
        df.to_sql("hhldrtn_regautodb",
                  con=sqliteConnection, if_exists='replace')
        sqliteConnection.close()'''

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
    '''df = pd.read_sql_query(
        "select * from hhldrtn_regautodb", sqliteConnection)'''
	df = df_hhldrtn_regautodb
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
    '''hhldrtn_regautodb = pd.read_sql_query(
        "select * from hhldrtn_regautodb ", sqliteConnection)'''
	hhldrtn_regautodb = df_hhldrtn_regautodb
    # Converting source work.fixemptydata into datafram.
    fixempty = pd.read_sql_query("select * from fixempty{}".format(Y2), sqliteConnection)
    # Concatenate the source data frames
    outfiledb = pd.concat(
        [hhldrtn_regautodb, fixempty], ignore_index=True, sort=False)
    df = df_remove_indexCols(df)
    if 'level_0' in outfiledb.columns:
        outfiledb = outfiledb.drop(columns="level_0")
    df_creation_logging(outfiledb, "outfiledb")
    del df_hhldrtn_regautodb
	# Push results data frame to Sqlite DB
    outfiledb.to_sql("outfiledb", con=sqliteConnection, if_exists='replace')
    sqliteConnection.close()
	
	sqliteToBQ(output_tables)
	
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
Retention(Y1, Y2, GY1, GY2)
