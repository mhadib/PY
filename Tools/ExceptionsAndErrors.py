
from weakref import finalize

import numpy as np
import pandas
import logging

import pyodbc
import requests
from datetime import datetime, timedelta
import sqlalchemy as sal
import json
import time


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/ExceptionsAndErrors.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:

    logger.log(level=logging.DEBUG, msg="start to write read sql")
    print("start to write read sql ")

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'user=sa;'
                          'password=Adib4626;'
                          'Trusted_connection=yes;')
    cursor = conn.cursor()
    cursor.execute('select top 1 cast(value as nvarchar(300)) as myColumn '
                                              'FROM ::fn_trace_getinfo(default) '
                                              'where property = 2 '
                                              'order by traceid desc')
    for i in cursor:
        profilerDirectory = "'"+i[0]+"'"
    # profilerDirectory = pandas.read_sql_query('select top 1 cast(value as nvarchar(300)) '
    #                                           'FROM ::fn_trace_getinfo(default) '
    #                                           'where property = 2 '
    #                                           'order by traceid desc', conn)


    sqlHistory = pandas.read_sql_query("select t.textData, t2.textData, t.TransactionID, t.LoginName, t.ApplicationName, t.NTUserName from (select * from fn_trace_gettable ( (SELECT top 1 cast([value] as nvarchar(max)) FROM sys.fn_trace_getinfo(NULL) where property = 2 order by 1 desc), default ) "
                                       "where error > 0 ) t join ( select * "
                                       "from fn_trace_gettable ( (SELECT top 1 cast([value] as nvarchar(max)) FROM sys.fn_trace_getinfo(NULL) where property = 2 order by 1 desc) , default ) ) t2 "
                                       "on t.TransactionID = t2.TransactionID where t.databaseName = 'adib' and t.ApplicationName not like N'%manage%' and t.loginname not like N'%jannejad%' ", conn)
    sqlHistory.to_excel('E:\\گزارش ارور ها\\EXCEPTIONSANDERRORS.xlsx', index=False)

    sqlHistory = pandas.read_sql_query("select t.textData, t2.textData, t.TransactionID, t.LoginName, t.ApplicationName, t.NTUserName from (select * from fn_trace_gettable ( (SELECT top 1 cast([value] as nvarchar(max)) FROM sys.fn_trace_getinfo(NULL) where property = 2 order by 1 desc), default ) "
                                       "where error > 0 ) t join ( select * "
                                       "from fn_trace_gettable ( (SELECT top 1 cast([value] as nvarchar(max)) FROM sys.fn_trace_getinfo(NULL) where property = 2 order by 1 desc) , default ) ) t2 "
                                       "on t.TransactionID = t2.TransactionID where t.databaseName = 'adib' and t.ApplicationName not like N'%manage%' and t.loginname  like N'%jannejad%' ", conn)
    sqlHistory.to_excel('E:\\گزارش ارور ها\\EXCEPTIONSANDERRORS_Jannejad.xlsx', index=False)

    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
