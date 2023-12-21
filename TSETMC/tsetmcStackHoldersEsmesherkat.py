
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/StackHolders/tsetmcStackHoldersEsmesherkat.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:
    import logging
    logging.basicConfig(filename='E:/گزارشات لاگ ها/StackHolders/tsetmcStackHoldersEsmesherkat.txt', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    logger = logging.getLogger(__name__)
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

    sqlHistory = pandas.read_sql_query(' select * from StackHoldersForLastDay1Mounth6MounthEsmeSherkat ', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\TSETMC\\tsetmcStackHoldersEsmesherkat.xlsx', index=False)

    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
    logger.error(exHandling.ResolveExceptions(err))

