import logging

import numpy
import pandas
import pyodbc
import requests
import codecs

import json, ast
import time
from os import listdir
from os.path import isfile, join

from datetime import datetime, timedelta
logging.basicConfig(filename='E:/گزارشات لاگ ها/Chartayi/lchartayitsetmcLog.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s %(lineno)d ')
logger=logging.getLogger(__name__)

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LOCALHOST;'
                      'Database=Adib;'
                      'UID:sa;'
                      'PWD:Adib4626')

# pyodbc.connect("Driver={SQL Server Native Client 11.0};"
#                       "Server=server_name;"
#                       "Database=db_name;"
#                       "Trusted_Connection=yes;")

def post():

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'UID:sa;'
                          'PWD:Adib4626')
    cursor = conn.cursor()
    r1 = requests.get('http://www.tsetmc.com/tsev2/data/ClientTypeAll.aspx')
    if r1.ok:
        cursor.execute('truncate table HaghighiHoghughi;')
        conn.commit()
        df = r1.content.decode("utf-8").split(";")
        for row in df:
            cursor.execute('insert into HaghighiHoghughi(val) select ?', row)
        conn.commit()

    r1 = requests.get('http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx?h=0&r=0')



    if r1.ok:
        cursor.execute('truncate table TsetmcTabular;')
        conn.commit()
        df = r1.content.decode("utf-8").split("@")
        for row in df[2].split(";"):
            cursor.execute('insert into TsetmcTabular(val) select ?', row)
        conn.commit()


    sqlHistory = pandas.read_sql_query('SELECT * FROM temp', conn)
    sqlHistory = sqlHistory.drop(columns=['symbolFA'])

    result = pandas.read_sql_query('select * from v_getLastTSETMCData', conn)

    result = pandas.merge(result, sqlHistory, left_on='instrumentId', right_on='instrumentId', how='outer')
    result = result.fillna(0)
    result['فروش حقوقی نود روز'] = result['فروش حقوقی نود روز'] + result['فروش حقوقی']
    result['فروش حقوقی سی روز'] = result['فروش حقوقی سی روز'] + result['فروش حقوقی']
    result['فروش حقوقی چهارده روز'] = result['فروش حقوقی چهارده روز'] + result['فروش حقوقی']
    result['فروش حقوقی هفت روز'] = result['فروش حقوقی هفت روز'] + result['فروش حقوقی']
    result['فروش حقوقی سه روز'] = result['فروش حقوقی سه روز'] + result['فروش حقوقی']

    result['فروش حقیقی سه روز'] = result['فروش حقیقی سه روز'] + result['فروش حقیقی']
    result['فروش حقیقی هفت روز'] = result['فروش حقیقی هفت روز'] + result['فروش حقیقی']
    result['فروش حقیقی چهارده روز'] = result['فروش حقیقی چهارده روز'] + result['فروش حقیقی']
    result['فروش حقیقی سی روز'] = result['فروش حقیقی سی روز'] + result['فروش حقیقی']
    result['فروش حقیقی نود روز'] = result['فروش حقیقی نود روز'] + result['فروش حقیقی']

    result['خرید حقوقی نود روز'] = result['خرید حقوقی نود روز'] + result['خرید حقوقی']
    result['خرید حقوقی سی روز'] = result['خرید حقوقی سی روز'] + result['خرید حقوقی']
    result['خرید حقوقی چهارده روز'] = result['خرید حقوقی چهارده روز'] + result['خرید حقوقی']
    result['خرید حقوقی هفت روز'] = result['خرید حقوقی هفت روز'] + result['خرید حقوقی']
    result['خرید حقوقی سه روز'] = result['خرید حقوقی سه روز'] + result['خرید حقوقی']

    result['خرید حقیقی سه روز'] = result['خرید حقیقی سه روز'] + result['خرید حقیقی']
    result['خرید حقیقی هفت روز'] = result['خرید حقیقی هفت روز'] + result['خرید حقیقی']
    result['خرید حقیقی چهارده روز'] = result['خرید حقیقی چهارده روز'] + result['خرید حقیقی']
    result['خرید حقیقی سی روز'] = result['خرید حقیقی سی روز'] + result['خرید حقیقی']
    result['خرید حقیقی نود روز'] = result['خرید حقیقی نود روز'] + result['خرید حقیقی']

    result['180DaysVolume'] = result['180DaysVolume'] + result['1DaysVolume']
    result['90DaysVolume'] = result['90DaysVolume'] + result['1DaysVolume']
    result['30DaysVolume'] = result['30DaysVolume'] + result['1DaysVolume']
    result['14DaysVolume'] = result['14DaysVolume'] + result['1DaysVolume']
    result['7DaysVolume'] = result['7DaysVolume'] + result['1DaysVolume']
    result['3DaysVolume'] = result['3DaysVolume'] + result['1DaysVolume']

    # if result['ارزش دیروز بازار'] != 0:
    result['رشد'] = (result['ارزش بازار'] - result['ارزش دیروز بازار']).divide(result['ارزش دیروز بازار'])
    # else:
    #     result['رشد'] = 0
    # if result['ارزش سی روز گذشته بازار'] != 0:
    result['رشد سی روزه'] = (result['ارزش بازار'] - result['ارزش سی روز گذشته بازار']).divide(
        result['ارزش سی روز گذشته بازار'])
    # else:
    #     result['رشد سی روزه'] = 0
    # if result['ارزش سه روز گذشته بازار'] != 0:
    result['رشد سه روزه'] = (result['ارزش بازار'] - result['ارزش سه روز گذشته بازار']).divide(
        result['ارزش سه روز گذشته بازار'])
    # else:
    #     result['ارزش سه روز گذشته بازار'] =0
    # if result['ارزش نود روز گذشته بازار'] !=0:
    result['رشد نود روزه'] = (result['ارزش بازار'] - result['ارزش نود روز گذشته بازار']).divide(
        result['ارزش نود روز گذشته بازار'])
    # else:
    #     result['رشد نود روزه'] = 0
    # if result['ارزش شش ماهه گذشته بازار'] !=0:
    result['رشد شش ماهه'] = (result['ارزش بازار'] - result['ارزش شش ماهه گذشته بازار']).divide(
        result['ارزش شش ماهه گذشته بازار'])
    # else:
    #     result['رشد شش ماهه'] = 0
    # if result['ارزش هفت روز گذشته بازار'] != 0:
    result['رشد هفت روزه'] = (result['ارزش بازار'] - result['ارزش هفت روز گذشته بازار']).divide(
        result['ارزش هفت روز گذشته بازار'])
    # else:
    #     result['رشد هفت روزه'] = 0
    # if result['ارزش چهارده روز گذشته بازار'] != 0:
    result['رشد چهارده روزه'] = (result['ارزش بازار'] - result['ارزش چهارده روز گذشته بازار']).divide(
        result['ارزش چهارده روز گذشته بازار'])
    # else:
    #     result['رشد چهارده روزه'] = 0
    result['لینک اول'] = pandas.Series(
        ["https://api.bourseview.com/v1/tickers?typeCodes=1000,4000,2000", "symbolFA", "instrumentId"])
    result['لینک دوم'] = pandas.Series(
        ["https://api.bourseview.com/v1/quotes?items=indinst&exchanges=IRTSENO,IRIFBNO,IRIFBOTC", "فروش حقیقی",
         "خرید حقیقی", "فروش حقوقی", "خرید حقوقی"])
    result['لینک چهارم'] = pandas.Series(
        ["https://api.bourseview.com/v1/quotes?items=share", "dayMarketCap", "ارزش بازار", "float"])

    print('befor create result2')
    timestr = time.strftime("%Y%m%d-%H%M%S")
    if result['day'][0] != int(time.strftime("%Y%m%d")):
        print('Out of day')
    ## return
    result2 = result[
        ['instrumentId', 'ارزش دیروز بازار', 'symbolFA', '1DaysVolume', 'day', '3DaysVolume', '7DaysVolume',
         '14DaysVolume',
         '30DaysVolume', '180DaysVolume', 'dayMarketCap', 'ارزش بازار', 'رشد', 'float', 'ارزش سی روز گذشته بازار',
         'رشد سی روزه', 'ارزش سه روز گذشته بازار', 'رشد سه روزه', 'ارزش هفت روز گذشته بازار', 'رشد هفت روزه',
         'ارزش چهارده روز گذشته بازار', 'رشد چهارده روزه', 'فروش حقیقی', 'خرید حقیقی', 'فروش حقوقی', 'خرید حقوقی',
         'فروش حقیقی چهارده روز', 'خرید حقیقی چهارده روز', 'فروش حقوقی چهارده روز', 'خرید حقوقی چهارده روز',
         'فروش حقیقی نود روز', 'خرید حقیقی نود روز', 'فروش حقوقی نود روز', 'خرید حقوقی نود روز', 'خرید حقوقی سه روز',
         'خرید حقوقی هفت روز', 'خرید حقیقی سه روز', 'خرید حقیقی هفت روز', 'فروش حقوقی سه روز', 'فروش حقیقی سه روز',
         'فروش حقوقی هفت روز', 'فروش حقیقی هفت روز', 'خرید حقوقی سی روز', 'خرید حقیقی سی روز', 'فروش حقوقی سی روز',
         'فروش حقیقی سی روز', '90DaysVolume', 'رشد نود روزه', 'رشد شش ماهه', 'لینک اول', 'لینک دوم',
         'لینک چهارم']]
    result3 = result[
        ['instrumentId', 'ارزش دیروز بازار', 'symbolFA', '1DaysVolume', 'day', '3DaysVolume', '7DaysVolume',
         '14DaysVolume', '30DaysVolume', '180DaysVolume', '90DaysVolume', 'dayMarketCap', 'ارزش بازار', 'رشد', 'float',
         '90DaysVolume', 'رشد نود روزه', 'رشد شش ماهه', 'لینک اول', 'لینک دوم', 'لینک چهارم']]

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'UID:sa;'
                          'PWD:Adib4626')
    cursor = conn.cursor()
    result2 = result2.replace(numpy.inf, numpy.nan)
    result2 = result2.fillna(0)
    # Insert DataFrame to Table
    print('insert into sql')
    for row in result2.itertuples():
        cursor.execute('''INSERT INTO Adib.dbo.HistoricalChartayiTSETMC ([instrumentId] ,[ارزش دیروز بازار] ,[symbolFA],[1DaysVolume] ,[day] ,[3DaysVolume] ,[7DaysVolume] ,[14DaysVolume] ,[30DaysVolume] ,[180DaysVolume] ,[dayMarketCap] , [ارزش بازار] ,[رشد] ,[float] ,[ارزش سی روز گذشته بازار] ,[رشد سی روزه] ,[ارزش سه روز گذشته بازار] ,[رشد سه روزه] ,[ارزش هفت روز گذشته بازار] ,[رشد هفت روزه] ,[ارزش چهارده روز گذشته بازار] ,[رشد چهارده روزه],[فروش حقیقی],[خرید حقیقی],[فروش حقوقی],[خرید حقوقی],[فروش حقیقی چهارده روز],[خرید حقیقی چهارده روز],[فروش حقوقی چهارده روز],[خرید حقوقی چهارده روز],  [فروش حقیقی نود روز], [خرید حقیقی نود روز], [فروش حقوقی نود روز], [خرید حقوقی نود روز], [خرید حقوقی سه روز], [خرید حقوقی هفت روز],[خرید حقیقی سه روز],[خرید حقیقی هفت روز],[فروش حقوقی سه روز],[فروش حقیقی سه روز],[فروش حقوقی هفت روز],[فروش حقیقی هفت روز],[خرید حقوقی سی روز],[خرید حقیقی سی روز],[فروش حقوقی سی روز],[فروش حقیقی سی روز],[90DaysVolume],[رشد نود روزه],[رشد شش ماهه])
                              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                       row[27],
                       row[28],
                       row[29],
                       row[30],
                       row[31],
                       row[32],
                       row[33],
                       row[34],
                       row[35],
                       row[36],
                       row[37],
                       row[38],
                       row[39],
                       row[40],
                       row[41],
                       row[42],
                       row[43],
                       row[44],
                       row[45],
                       row[46],
                       row[47],
                       row[48],
                       row[49],

                       )
    conn.commit()
    # cursor.execute('insert into startTriger select getdate()')
    # conn.commit()
    print('end of insert into sql')



post()