# coding=utf-8
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

import sqlalchemy

logging.basicConfig(filename='E:/گزارشات لاگ ها/Chartayi/Chartayi_Gen.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s %(lineno)d ')
logger=logging.getLogger(__name__)

try:
    import sys
    import os
    path = os.path.abspath("E:\\PY\\Tools")
    sys.path.append(path)
    import ResolveExceptions as exHandling
    shiftDayNumberToOld = 20
    environmentId = '82809'
    api_login = '09133135250'
    api_password = 'Adib4626'
    base_url = "https://api.bourseview.com"

    def doValuesOfTradePeriodicly(finalResult, finalResultMarketCap, legalRealFinalResult ):
        conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                              'Server=localhost;'
                              'Database=Adib;'
                              'Trusted_Connection=yes;')

        cursor = conn.cursor()


        sqlHistory = pandas.read_sql_query('SELECT * FROM Temp', conn)
        sqlHistory = sqlHistory.drop(columns=['symbolFA'])
        tsetmc = pandas.read_sql_query('SELECT * FROM v_getLastTSETMCData', conn)


        pandas.options.display.float_format = '{:.2f}'.format
        # authToken, cookies = login(api_login, api_password)
        # logoutall("https://api.bourseview.com/v2/logout-all", authToken, cookies)
        # authToken, cookies = login(api_login, api_password)
        choice = None

        #stockDataFrame = pandas.DataFrame.from_dict(get_sessions('https://api.bourseview.com/v2/tickers', authToken, cookies), orient="columns")
        # legalRealFinalResult = pandas.DataFrame()
        # dateToGetDate = datetime.today() #- timedelta(days=1)
        if (finalResult is None):
            finalResult = pandas.read_sql_query('SELECT InstrumentId, SymbolFA, [1DaysVolume], FinalPrice, [DAY] FROM VFinalpriceAndVolumes with (nolock) where cast(createdate as date) =cast(dateadd(day,0,getdate()) as date) ',
        conn)

        if (finalResultMarketCap is None):
            finalResultMarketCap = pandas.read_sql_query('SELECT InstrumentId, DayMarketCap, [ارزش بازار], [float] FROM VMarketCap where cast(createdate as date) = cast(dateadd(day,0,getdate()) as date) ', conn)

        if (legalRealFinalResult is None):
            legalRealFinalResult = pandas.read_sql_query('SELECT InstrumentId, [خرید حقیقی], [فروش حقیقی], [خرید حقوقی], [فروش حقوقی] FROM VLegalReal where cast(createdate as date) = cast(dateadd(day,0,getdate()) as date) ',
        conn)


        tseUses = '0'
        if (len(finalResult)==0 and int(time.strftime("%H")) < 14):
            tseUses = '1'
            result = tsetmc

        else :
            result = pandas.merge(finalResult, finalResultMarketCap, left_on='InstrumentId', right_on='InstrumentId', how='outer')
            result = pandas.merge(result, legalRealFinalResult, left_on='InstrumentId', right_on='InstrumentId', how='outer')
            result.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\BankDadeh.xlsx', index=False)
        result = pandas.merge(result, sqlHistory, left_on='InstrumentId', right_on='instrumentId',how='outer')
        result = result.fillna(0)
        result['فروش حقوقی نود روز'] = result['فروش حقوقی نود روز'] + result['فروش حقوقی']
        result['فروش حقوقی سی روز'] = result['فروش حقوقی سی روز'] + result['فروش حقوقی']
        result['فروش حقوقی چهارده روز'] = result['فروش حقوقی چهارده روز'] + result['فروش حقوقی']
        result['فروش حقوقی هفت روز'] = result['فروش حقوقی هفت روز'] + result['فروش حقوقی']
        result['فروش حقوقی سه روز'] = result['فروش حقوقی سه روز'] + result['فروش حقوقی']
        result['فروش حقوقی یک سال'] = result['فروش حقوقی یک سال'] + result['فروش حقوقی']
        result['فروش حقوقی 6 مرداد 99'] = result['فروش حقوقی شش مرداد 99'] + result['فروش حقوقی']


        result['فروش حقیقی سه روز'] = result['فروش حقیقی سه روز'] + result['فروش حقیقی']
        result['فروش حقیقی هفت روز'] = result['فروش حقیقی هفت روز'] + result['فروش حقیقی']
        result['فروش حقیقی چهارده روز'] = result['فروش حقیقی چهارده روز'] + result['فروش حقیقی']
        result['فروش حقیقی سی روز'] = result['فروش حقیقی سی روز'] + result['فروش حقیقی']
        result['فروش حقیقی نود روز'] = result['فروش حقیقی نود روز'] + result['فروش حقیقی']
        result['فروش حقیقی یک سال'] = result['فروش حقیقی یک سال'] + result['فروش حقیقی']
        result['فروش حقیقی 6 مرداد 99'] = result['فروش حقیقی شش مرداد 99'] + result['فروش حقیقی']

        result['خرید حقوقی نود روز'] = result['خرید حقوقی نود روز'] + result['خرید حقوقی']
        result['خرید حقوقی سی روز'] = result['خرید حقوقی سی روز'] + result['خرید حقوقی']
        result['خرید حقوقی چهارده روز'] = result['خرید حقوقی چهارده روز'] + result['خرید حقوقی']
        result['خرید حقوقی هفت روز'] = result['خرید حقوقی هفت روز'] + result['خرید حقوقی']
        result['خرید حقوقی سه روز'] = result['خرید حقوقی سه روز'] + result['خرید حقوقی']
        result['خرید حقوقی یک سال'] = result['خرید حقوقی یک سال'] + result['خرید حقوقی']
        result['خرید حقوقی 6 مرداد 99'] = result['خرید حقوقی شش مرداد 99'] + result['خرید حقوقی']


        result['خرید حقیقی سه روز'] = result['خرید حقیقی سه روز'] + result['خرید حقیقی']
        result['خرید حقیقی هفت روز'] = result['خرید حقیقی هفت روز'] + result['خرید حقیقی']
        result['خرید حقیقی چهارده روز'] = result['خرید حقیقی چهارده روز'] + result['خرید حقیقی']
        result['خرید حقیقی سی روز'] = result['خرید حقیقی سی روز'] + result['خرید حقیقی']
        result['خرید حقیقی نود روز'] = result['خرید حقیقی نود روز'] + result['خرید حقیقی']
        result['خرید حقیقی یک سال'] = result['خرید حقیقی یک سال'] + result['خرید حقیقی']
        result['خرید حقیقی 6 مرداد 99'] = result['خرید حقیقی شش مرداد 99'] + result['خرید حقیقی']

        result['6Mordad99DaysVolume'] = result['6Mordad99DaysVolume'] + result['1DaysVolume']
        result['365DaysVolume'] = result['365DaysVolume'] + result['1DaysVolume']
        result['180DaysVolume'] = result['180DaysVolume']+result['1DaysVolume']
        result['90DaysVolume'] = result['90DaysVolume'] + result['1DaysVolume']
        result['30DaysVolume'] = result['30DaysVolume'] + result['1DaysVolume']
        result['14DaysVolume'] = result['14DaysVolume'] + result['1DaysVolume']
        result['7DaysVolume'] = result['7DaysVolume'] + result['1DaysVolume']
        result['3DaysVolume'] = result['3DaysVolume'] + result['1DaysVolume']

        #if result['ارزش دیروز بازار'] != 0:
        result['رشد'] = (result['ارزش بازار'] - result['ارزش دیروز بازار']).divide(result['ارزش دیروز بازار'])
        # else:
        #     result['رشد'] = 0
        #if result['ارزش سی روز گذشته بازار'] != 0:
        result['رشد سی روزه'] = (result['ارزش بازار'] - result['ارزش سی روز گذشته بازار']).divide(result['ارزش سی روز گذشته بازار'])
        # else:
        #     result['رشد سی روزه'] = 0
        #if result['ارزش سه روز گذشته بازار'] != 0:
        result['رشد سه روزه'] = (result['ارزش بازار'] - result['ارزش سه روز گذشته بازار']).divide(result['ارزش سه روز گذشته بازار'])
        # else:
        #     result['ارزش سه روز گذشته بازار'] =0
        #if result['ارزش نود روز گذشته بازار'] !=0:
        result['رشد نود روزه'] = (result['ارزش بازار'] - result['ارزش نود روز گذشته بازار']).divide(result['ارزش نود روز گذشته بازار'])
        # else:
        #     result['رشد نود روزه'] = 0
        #if result['ارزش شش ماهه گذشته بازار'] !=0:
        result['رشد شش ماهه'] = (result['ارزش بازار'] - result['ارزش شش ماهه گذشته بازار']).divide(result['ارزش شش ماهه گذشته بازار'])
        # else:
        #     result['رشد شش ماهه'] = 0
        #if result['ارزش هفت روز گذشته بازار'] != 0:
        result['رشد هفت روزه'] = (result['ارزش بازار'] - result['ارزش هفت روز گذشته بازار']).divide(result['ارزش هفت روز گذشته بازار'])
        # else:
        #     result['رشد هفت روزه'] = 0
       # if result['ارزش چهارده روز گذشته بازار'] != 0:
        result['رشد چهارده روزه'] = (result['ارزش بازار'] - result['ارزش چهارده روز گذشته بازار']).divide(result['ارزش چهارده روز گذشته بازار'])
        # else:
        #     result['رشد چهارده روزه'] = 0
        result['رشد یک ساله'] = (result['ارزش بازار'] - result['ارزش یک سال گذشته بازار']).divide(result['ارزش یک سال گذشته بازار'])
        result['رشد شش مرداد 99'] = (result['ارزش بازار'] - result['ارزش 6 مرداد 99']).divide(result['ارزش 6 مرداد 99'])


        print('befor create result2')
        timestr = time.strftime("%Y%m%d-%H%M%S")
        if result['DAY'][0] != int(time.strftime("%Y%m%d")):
            print('Out of day')
           ## return
        result2 = result[['InstrumentId','ارزش دیروز بازار','SymbolFA','1DaysVolume','DAY','3DaysVolume','7DaysVolume','14DaysVolume',
                          '30DaysVolume','180DaysVolume','DayMarketCap', 'ارزش بازار','رشد','float','ارزش سی روز گذشته بازار',
                          'رشد سی روزه','ارزش سه روز گذشته بازار','رشد سه روزه','ارزش هفت روز گذشته بازار','رشد هفت روزه','ارزش چهارده روز گذشته بازار','رشد چهارده روزه','فروش حقیقی','خرید حقیقی','فروش حقوقی','خرید حقوقی','فروش حقیقی چهارده روز', 'خرید حقیقی چهارده روز', 'فروش حقوقی چهارده روز', 'خرید حقوقی چهارده روز',  'فروش حقیقی نود روز', 'خرید حقیقی نود روز', 'فروش حقوقی نود روز', 'خرید حقوقی نود روز', 'خرید حقوقی سه روز', 'خرید حقوقی هفت روز','خرید حقیقی سه روز','خرید حقیقی هفت روز','فروش حقوقی سه روز','فروش حقیقی سه روز','فروش حقوقی هفت روز','فروش حقیقی هفت روز','خرید حقوقی سی روز','خرید حقیقی سی روز','فروش حقوقی سی روز','فروش حقیقی سی روز','90DaysVolume','رشد نود روزه','رشد شش ماهه','رشد یک ساله','365DaysVolume','خرید حقیقی یک سال','خرید حقوقی یک سال','فروش حقیقی یک سال','فروش حقوقی یک سال','FinalPrice','رشد شش مرداد 99','فروش حقوقی 6 مرداد 99','خرید حقوقی 6 مرداد 99','فروش حقیقی 6 مرداد 99','خرید حقیقی 6 مرداد 99','6Mordad99DaysVolume']]
        result3 = result[['instrumentId','ارزش دیروز بازار','SymbolFA', '1DaysVolume', 'DAY', '3DaysVolume', '7DaysVolume','14DaysVolume', '30DaysVolume', '180DaysVolume','90DaysVolume', 'DayMarketCap', 'ارزش بازار', 'رشد', 'float', '90DaysVolume','رشد نود روزه','رشد شش ماهه']]
        result2.to_csv('E:\\PY\\BourseView\\CSVs\\lchartayi.txt', index=False )

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
            cursor.execute('''INSERT INTO Adib.dbo.HistoricalChartayi ([InstrumentId] ,[ارزش دیروز بازار] ,[symbolFA],[1DaysVolume] ,[day] ,[3DaysVolume] ,[7DaysVolume] ,[14DaysVolume] ,[30DaysVolume] ,[180DaysVolume] ,[dayMarketCap] , [ارزش بازار] ,[رشد] ,[float] ,[ارزش سی روز گذشته بازار] ,[رشد سی روزه] ,[ارزش سه روز گذشته بازار] ,[رشد سه روزه] ,[ارزش هفت روز گذشته بازار] ,[رشد هفت روزه] ,[ارزش چهارده روز گذشته بازار] ,[رشد چهارده روزه],[فروش حقیقی],[خرید حقیقی],[فروش حقوقی],[خرید حقوقی],[فروش حقیقی چهارده روز],[خرید حقیقی چهارده روز],[فروش حقوقی چهارده روز],[خرید حقوقی چهارده روز],  [فروش حقیقی نود روز], [خرید حقیقی نود روز], [فروش حقوقی نود روز], [خرید حقوقی نود روز], [خرید حقوقی سه روز], [خرید حقوقی هفت روز],[خرید حقیقی سه روز],[خرید حقیقی هفت روز],[فروش حقوقی سه روز],[فروش حقیقی سه روز],[فروش حقوقی هفت روز],[فروش حقیقی هفت روز],[خرید حقوقی سی روز],[خرید حقیقی سی روز],[فروش حقوقی سی روز],[فروش حقیقی سی روز],[90DaysVolume],[رشد نود روزه],[رشد شش ماهه],['رشد یک ساله'],['365DaysVolume'],['خرید حقیقی یک سال'],['خرید حقوقی یک سال'],['فروش حقیقی یک سال'],['فروش حقوقی یک سال'],[FinalPrice] ,['رشد شش مرداد 99'],['فروش حقوقی 6 مرداد 99'],  ['خرید حقوقی 6 مرداد 99'] , ['فروش حقیقی 6 مرداد 99']  ,['خرید حقیقی 6 مرداد 99'], ['6Mordad99DaysVolume'] )
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                           row[50],
                           row[51],
                           row[52],
                           row[53],
                           row[54],
                           row[55],
                           row[56],
                           row[57],
                           row[58],
                           row[59],
                           row[60],
                           row[61],
                           row[62],

                           )
        conn.commit()
        cursor.execute('insert into startTriger select getdate(),' + tseUses)
        cursor.close()
        conn.commit()
        conn.close()
        conn = pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'
        r'DATABASE=adib;'
        r'Trusted_Connection=yes;')

        sqlHistory = pandas.read_sql_query('select * from [dbo].vchartayi with (nolock)', conn)
        sqlHistory.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\vchartayiJani.xlsx', index=False)

        sqlHistory = pandas.read_sql_query('select * from VChartayiPreProcess with (nolock)', conn)
        sqlHistory.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\VChartayiPreProcess.xlsx', index=False)

        sqlHistory = pandas.read_sql_query('select distinct day from HistoricalChartayi with (nolock) order by day desc ', conn)
        sqlHistory.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\روزهای کاری.xlsx', index=False)


        print('end of insert into sql')

        result2.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\lchartayi.xlsx', index=False)


    try:
        if __name__ == "__main__":
            doValuesOfTradePeriodicly(None, None, None)
    except Exception as err:
        logger.error(err, exc_info=True)
        print(err)
except Exception as err14:
    print(err14)
    logger.error(err14, exc_info=True)
    logger.error(exHandling.ResolveExceptions(err))
