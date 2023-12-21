
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/BalanceSheet/BalanSheetGheyreTalfighiLogGen.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:
    import sys
    import os
    path = os.path.abspath("E:\\PY\\Tools")
    sys.path.append(path)
    import ResolveExceptions as exHandling
    logger.log(level=logging.DEBUG, msg="start to write read sql")
    print("start to write read sql ")
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'user=sa;'
                          'password=Adib4626;'
                          'Trusted_connection=yes;')

    cursor = conn.cursor()
    # cursor.execute('	truncate table PreBalanceSheetGheyreTalfighi	insert into PreBalanceSheetGheyreTalfighi	select *	from VPreBalanceSheetGheyreTalfighi')
    # cursor.execute('	truncate table PreBalanceSheetTalfighi	insert into PreBalanceSheetTalfighi	select *	from VPreBalanceSheetTalfighi')
    # # cursor.execute('	truncate table [VBalanceSheetGheyreTalfighi]	insert into [VBalanceSheetGheyreTalfighi]	select *	from [VBalanceSheet_GheyreTalfighi_Talfighi_new]')
    # cursor.commit()
    cursor.execute('exec Clean_HistoricalData insert into PreProcessBalanceSheet select getdate()')
    cursor.commit()

    conn.close()
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'user=sa;'
                          'password=Adib4626;'
                          'Trusted_connection=yes;')
    sqlHistory = pandas.read_sql_query('select * from [dbo].[VBalanceSheetGheyreTalfighi] with (nolock)', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\BourseView\\BalanceSheet\\VBalanceSheetNahayi.xlsx', index=False)

    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
    logger.error(exHandling.ResolveExceptions(err))