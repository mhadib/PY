
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/SystemHealth.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:

    logger.log(level=logging.DEBUG, msg="start to write read sql")
    print("start to write read sql ")
    conn = pyodbc.connect('Driver=SQL Server;Server=localhost;Database=Adib;User Id=sa;Password=Adib4626;')
    sqlHistory = pandas.read_sql_query('select * from [dbo].[systemtest] with (nolock) ', conn)
    sqlHistory2 = pandas.read_sql_query('select * from [dbo].[systemtest2] with (nolock) ', conn)
    sqlHistory3 = pandas.read_sql_query('select * from [dbo].[systemtest3] with (nolock) ', conn)
    sqlHistory4 = pandas.read_sql_query('select * from [dbo].[systemtest4] with (nolock) ', conn)
    sqlHistory5 = pandas.read_sql_query('select * from [dbo].[systemtest5] with (nolock) ', conn)
    sqlHistory6 = pandas.read_sql_query('select * from [dbo].[systemtest6] with (nolock) ', conn)
    sqlHistory7 = pandas.read_sql_query('select * from [dbo].[systemtest7] with (nolock) ', conn)
    sqlHistory8 = pandas.read_sql_query('select * from [dbo].[systemtest8] with (nolock) ', conn)

    sqlHistory = pandas.concat([sqlHistory, sqlHistory2])
    sqlHistory = pandas.concat([sqlHistory, sqlHistory3])
    sqlHistory = pandas.concat([sqlHistory, sqlHistory4])
    sqlHistory = pandas.concat([sqlHistory, sqlHistory5])
    sqlHistory = pandas.concat([sqlHistory, sqlHistory6])
    sqlHistory = pandas.concat([sqlHistory, sqlHistory7])
    sqlHistory = pandas.concat([sqlHistory, sqlHistory8])

    sqlHistory.to_excel('E:\\چه گزارشاتی ساخته نشده به علاوه کد های فراخوانی و توضیحات مازاد بر کد\\SystemTest.xlsx', index=False)

    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
