
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/JannejadExecutionTime.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:

    logger.log(level=logging.DEBUG, msg="start to write read sql")
    print("start to write read sql ")

    conn = pyodbc.connect('Driver=SQL Server;Server=localhost;Database=Adib;User Id=sa;Password=Adib4626;')
    sqlHistory = pandas.read_sql_query("select substring(textData,charindex(N'from',textdata)+4,100) text, duration/1000000, loginname FROM fn_trace_gettable((SELECT top 1 cast([value] as nvarchar(max)) FROM sys.fn_trace_getinfo(NULL) where property = 2 order by 1 desc), default) where loginname = N'Jannejad' and textData not like N'%[INFORMATION_SCHEMA].[TABLES]%' order by duration desc "
                                       , conn)
    sqlHistory.to_excel('E:\\گزارش ارور ها\\JannejadExecutionTime.xlsx', index=False)

    sqlHistory = pandas.read_sql_query("select textData, substring(textData,charindex(N'from',textdata)+4,100) text, duration/1000000, loginname FROM fn_trace_gettable((SELECT top 1 cast([value] as nvarchar(max)) FROM sys.fn_trace_getinfo(NULL) where property = 2 order by 1 desc), default) where loginname != N'Jannejad' and textData not like N'%[INFORMATION_SCHEMA].[TABLES]%' order by duration desc "
                                       , conn)
    sqlHistory.to_excel('E:\\گزارش ارور ها\\SQLInternalExecutionTime.xlsx', index=False)

    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
