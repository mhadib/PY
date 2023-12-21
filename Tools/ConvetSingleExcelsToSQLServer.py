from typing import re

import requests
import logging
import pandas
import pyodbc
from os import listdir
from os.path import isfile, join
def convert_fill(df):
    return df.stack().apply(pandas.to_numeric, errors='ignore').fillna(0).unstack()


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/Convert.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s %(lineno)d ')
logger=logging.getLogger(__name__)

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LOCALHOST;'
                      'Database=Adib;'
                      'UID:sa;'
                      'PWD:Adib4626')

cursor = conn.cursor()
excelHistory = pandas.read_excel(r'E:\\Moini\\Query.xlsx')
excelHistory = excelHistory.fillna(0)
# Insert DataFrame to Table
for row in excelHistory.itertuples():
    cursor.execute('''INSERT INTO Adib.dbo.ForConvert (SymboleFa, CompanyName, GroupName, InstrumentId, Esfand98)
                    VALUES (?,?,?,?,?)
                    ''',
        row[4],
        row[1],
        row[2],
        row[136],
        row[8],
             )
conn.commit()

