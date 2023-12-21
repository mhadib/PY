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



onlyfiles = [f for f in listdir("E:\\Moini\\Moini\\") if isfile(join("E:\\Moini\\Moini\\", f))]
history = pandas.DataFrame.from_dict(onlyfiles)
history = history.rename(columns={0: "m"})
history = history.query('m.str.contains("lchartayi")').query('m.str.contains("xlsx")').query(
            '~m.str.contains("-")').query('m.str.startswith("lcharta")').query('m.str.contains("20")').sort_values(
            by=['m'], ascending=False)
excelCounter = 0
tempForHistory2 = pandas.DataFrame();
conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'Trusted_Connection=yes;')
cursor = conn.cursor()
for key, item in history.iterrows():
    excelCounter = excelCounter + 1;
    if excelCounter < 3:
        print(excelCounter)
        print(item[0])
        excelHistory = pandas.read_excel(r'E:\\Moini\\Moini\\' + item[0])
        excelHistory = excelHistory.fillna(0)
        # Insert DataFrame to Table
        for row in excelHistory.itertuples():
            cursor.execute('''INSERT INTO Adib.dbo.HistoricalChartayi ([instrumentId] ,[ارزش دیروز بازار] ,[symbolFA],[1DaysVolume] ,[day] ,[3DaysVolume] ,[7DaysVolume] ,[14DaysVolume] ,[30DaysVolume] ,[180DaysVolume] ,[dayMarketCap] , [ارزش بازار] ,[رشد] ,[float] ,[ارزش سی روز گذشته بازار] ,[رشد سی روزه] ,[ارزش سه روز گذشته بازار] ,[رشد سه روزه] ,[ارزش هفت روز گذشته بازار] ,[رشد هفت روزه] ,[ارزش چهارده روز گذشته بازار] ,[رشد چهارده روزه],[فروش حقیقی],[خرید حقیقی],[فروش حقوقی],[خرید حقوقی])
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''',
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
        row[9],
        row[10],
        row[11],
        row[12],
        row[13],
        row[14],
        row[15],
        row[16],
        row[17],
        row[18],
        row[19],
        row[20],
        row[21],
        row[22],
        row[23],
        row[24],
        row[25],
        row[26],


             )
    conn.commit()

