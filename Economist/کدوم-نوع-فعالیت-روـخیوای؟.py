
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/کدوم-نوع-فعالیت-رو-میخوای.txt', level=logging.DEBUG,
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
    test2df = pandas.read_excel(r'E:\گزارشات اکسل\Economist\فعالیت-شرکت-های-بورسی.xlsx', sheet_name='Sheet1')
    varibale = test2df.loc[test2df['این را میخواهم'] == 1, ['فعالیت شرکت ']]
    # varibale = test2df['Value'][0]
    varibale = "N'"+varibale.strip()+"'"

    sqlHistory = pandas.read_sql_query(
        'select * from [dbo].[vboursPlus] '
        'where  rtrim(ltrim([فعالیت شرکت ])) in (select ltrim(rtrim(item)) from dbo.split('+varibale+',N\'،\'))  ' , conn)




    sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\FilteredBoursPlus.xlsx', index=False)


    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
