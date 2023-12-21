
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/BalanceSheet/VBalanceSheetNahayiHistory.txt', level=logging.DEBUG,
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

    print("start to write read sql ")
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'user=sa;'
                          'password=Adib4626;'
                          'Trusted_connection=yes;')
    test2df = pandas.read_excel(r'E:\PY\BourseView\بالانس_شیت\Be_Man_Begu_Tarikhche_Kodum_BalanceSheet_Ro_Mikhay.xlsx', sheet_name='Sheet1')
    varibale = test2df['Value'][0]
    varibale = "N'"+varibale.strip()+"'"

    sqlHistory = pandas.read_sql_query(
        'select * from [dbo].[VPreBalanceSheetTalfighi_Bourse_Drop_Bilion_Full_Report] '
        'where symbolFA like  ' + varibale + ' union all '
                                             'select * from [dbo].[VPreBalanceSheetTalfighi_FaraBourse_Drop_Bilion_Full_Report] '
                                             'where symbolFA like ' + varibale, conn)



    sqlHistory2 = pandas.read_sql_query('select * from [dbo].[VPreBalanceSheetGheyreTalfighi_Bourse_Drop_Bilion_Full_Report] '
                                       'where symbolFA like  '+varibale+' union all ' 
                                       'select * from [dbo].[VPreBalanceSheetGheyreTalfighi_FaraBourse_Drop_Bilion_Full_Report] '
                                       'where symbolFA like '+varibale, conn)

    with pandas.ExcelWriter("E:\\گزارشات اکسل\\BourseView\\BalanceSheet\\VBalanceSheetNahayiHistory.xlsx") as writer:
        sqlHistory2.to_excel(writer, index=False, sheet_name="غیر تلفیقی")
        sqlHistory.to_excel(writer, index=False, sheet_name="تلفیقی")


    print("end of write to excel ")
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
    logger.error(exHandling.ResolveExceptions(err))
