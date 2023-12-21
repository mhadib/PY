
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/SQLChangeHistory.txt', level=logging.DEBUG,
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

    sqlHistory = pandas.read_sql_query("with cte as ( " 
                                       "select a.*, lead(a.createdate, 1) over(PARTITION BY a.ObjectName ORDER BY a.CreateDate Desc) as OldDate "
                                       "from AUDIT_TABLE a ) "
                                       "select a.*, b.SQLCommand oldCommand "
                                       "from cte a "
                                       "left join AUDIT_TABLE b on a.ObjectName = b.ObjectName and b.CreateDate = a.OldDate", conn)
    sqlHistory.to_excel('E:\\SQLChangeHistory.xlsx', index=False)

    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
