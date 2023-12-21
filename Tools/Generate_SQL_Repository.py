
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/Generate_SQL_Repository.txt', level=logging.DEBUG,
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
    cursor.execute('select o.name name, s.name schma, m.definition, c.Categury   '
                   'from sys.sql_modules m '
                   'join sys.objects o on m.object_id = o.object_id '
                   'join sys.schemas s on s.schema_id = o.schema_id '
                   'left join ObjectCateguries c on o.object_id = c.object_id '
                   'where o.name in (select ObjectName from AUDIT_TABLE where cast(Createdate as date) >= cast(dateadd(day, -1, getdate()) as date))')
    for i in cursor:
        profilerDirectory = "'"+i[0]+"'"
        with open('E:\\SQL\\'+i[3]+i[0]+'.sql', 'w') as f:
            f.write(i[2])
            f.close()

    print("end of write sql files ")
    logger.log(level=logging.DEBUG, msg="end of write sql files")
    conn.close()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
