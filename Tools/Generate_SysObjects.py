
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/Generate_SysObjects.txt', level=logging.DEBUG,
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
    sqlHistory = pandas.read_sql_query('select * from sys.objects order by modify_date desc', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\IDs.xlsx', index=False)

    print("end of write sql files ")
    logger.log(level=logging.DEBUG, msg="end of write sql files")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
