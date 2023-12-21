
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Chartayi/VaBeMellatHistory.txt', level=logging.DEBUG,
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

    conn = pyodbc.connect('Driver=SQL Server;Server=localhost;Database=Adib;User Id=sa;Password=Adib4626;')
    test2df = pandas.read_excel(r'E:\PY\BourseView\چهارتایی\به_من_بگو_تاریخچه_کدوم_چهارتایی_رو_میخوای؟.xlsx', sheet_name='Sheet1')
    varibale = test2df['Value'][0]
    varibale = "N'"+varibale.strip()+"'"
    sqlHistory = pandas.read_sql_query('select * from HistoricalViewChartayi where symbolFA like '+varibale+' order by [dayMarketCap] desc', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\VaBeMellatHistory.xlsx', index=False)

    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
    logger.error(exHandling.ResolveExceptions(err))
