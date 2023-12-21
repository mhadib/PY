# coding=utf-8
import logging
import sqlalchemy as sal
import numpy
import pandas
import pyodbc
import requests
import codecs
import openpyxl
# from json_excel_converter import Converter
import json, ast
import time
from os import listdir
from os.path import isfile, join

from datetime import datetime, timedelta


logging.basicConfig(filename='E:/گزارشات لاگ ها/Chartayi/Chartayi_FinalPriceAndVolums.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s %(lineno)d ')
logger=logging.getLogger(__name__)

try:
    import sys
    import os
    path = os.path.abspath("E:\\PY\\Tools")
    sys.path.append(path)
    import ResolveExceptions as exHandling
    import BourseViewHelper as helper
    shiftDayNumberToOld = 20
    environmentId = '82809'
    api_login = '09133135250'
    api_password = 'Adib4626'
    base_url = "https://api.bourseview.com"

    def doValuesOfTradePeriodicly(i):

        pandas.options.display.float_format = '{:.2f}'.format
        authToken, cookies = helper.login(api_login, api_password)
        helper.logoutall("https://api.bourseview.com/v2/logout-all", authToken, cookies)
        authToken, cookies = helper.login(api_login, api_password)
        choice = None

        stockDataFrame = pandas.DataFrame.from_dict(helper.get_sessions('https://api.bourseview.com/v2/tickers', authToken, cookies), orient="columns")
        legalRealFinalResult = pandas.DataFrame()
        dateToGetDate = datetime.today() - timedelta(days=int(i))

        df = pandas.DataFrame.from_dict(helper.get_sessions('https://api.bourseview.com/v2/quotes?items=price&exchanges=IRIFBNO&date=['+dateToGetDate.strftime("%Y%m%d")+']', authToken, cookies), orient="columns")
        stockDataFrameHelper = pandas.DataFrame.from_dict(stockDataFrame['tickers'], orient="columns")

        pandas.DataFrame.from_dict(stockDataFrameHelper['tickers'][0], orient = "columns").query('ticker == "IRB3W0020211" ')
        finalResultIRIFBNO = pandas.DataFrame()
        for key, item in pandas.DataFrame.from_dict(df['tickers'], orient="columns").iterrows():
            dataframe8 = pandas.DataFrame.from_dict(item[0], orient="columns")
            result = pandas.DataFrame()
            #loop throw tickers
            for index7, row7 in dataframe8.iterrows():
                #add symboleFa and InstrumentId
                try:
                    result = pandas.DataFrame({'instrumentId': [row7['ticker']],'symbolFA':[pandas.DataFrame.from_dict(stockDataFrameHelper['tickers'][0], orient="columns").query('ticker == "'+row7['ticker']+'" ')['symbolFA'].values[0]]})
                except:
                    print(row7['ticker'])
                    continue
                t = 1
                value = 0
                itemCount = pandas.DataFrame.from_dict(row7['items'][0]['days']).count()['day']
                for key1, item1 in pandas.DataFrame.from_dict(row7['items'][0]['days']).iterrows():
                    value = value + row7['items'][0]['days'][itemCount - (t)]['items'][0]['values']['value']
                    final = row7['items'][0]['days'][itemCount - (t)]['items'][0]['values']['vwap']
                    if t == 1:
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),pandas.DataFrame.from_dict([{'1DaysVolume': value}],orient="columns")], axis=1, join='inner')
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),pandas.DataFrame.from_dict([{'FinalPrice': final}],orient="columns")], axis=1, join='inner')
                    try:
                        if (result['day'][0] < row7['items'][0]['days'][itemCount - (t)]['day']):
                            result['day'][0] = row7['items'][0]['days'][itemCount - (t)]['day']
                    except:
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),pandas.DataFrame.from_dict([{'day': row7['items'][0]['days'][itemCount - (t)]['day']}],orient="columns")], axis=1, join='inner')
                    t = t + 1
                finalResultIRIFBNO = pandas.concat([finalResultIRIFBNO, result])

                result = pandas.DataFrame()

        df = pandas.DataFrame.from_dict(helper.get_sessions(
            'https://api.bourseview.com/v2/quotes?items=price&exchanges=IRIFBOTC&date=[' + dateToGetDate.strftime(
                "%Y%m%d") + ']', authToken, cookies), orient="columns")
        stockDataFrameHelper = pandas.DataFrame.from_dict(stockDataFrame['tickers'], orient="columns")
        pandas.DataFrame.from_dict(stockDataFrameHelper['tickers'][0], orient="columns").query(
            'ticker == "IRO7BLIP0001" ')
        finalResultIRIFBOTC = pandas.DataFrame()
        for key, item in pandas.DataFrame.from_dict(df['tickers'], orient="columns").iterrows():
            dataframe8 = pandas.DataFrame.from_dict(item[0], orient="columns")
            result = pandas.DataFrame()
            # loop throw tickers
            for index7, row7 in dataframe8.iterrows():
                # add symboleFa and InstrumentId
                try:
                    result = pandas.DataFrame({'instrumentId': [row7['ticker']], 'symbolFA': [
                        pandas.DataFrame.from_dict(stockDataFrameHelper['tickers'][0], orient="columns").query(
                            'ticker == "' + row7['ticker'] + '" ')['symbolFA'].values[0]]})
                except:
                    print(row7['ticker'])
                    continue
                t = 1
                value = 0
                itemCount = pandas.DataFrame.from_dict(row7['items'][0]['days']).count()['day']
                for key1, item1 in pandas.DataFrame.from_dict(row7['items'][0]['days']).iterrows():
                    value = value + row7['items'][0]['days'][itemCount - (t)]['items'][0]['values']['value']
                    final = row7['items'][0]['days'][itemCount - (t)]['items'][0]['values']['vwap']
                    if t == 1:
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict([{'1DaysVolume': value}], orient="columns")],
                                               axis=1, join='inner')
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict([{'FinalPrice': final}], orient="columns")],
                                               axis=1, join='inner')
                    try:
                        if (result['day'][0] < row7['items'][0]['days'][itemCount - (t)]['day']):
                            result['day'][0] = row7['items'][0]['days'][itemCount - (t)]['day']
                    except:
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict(
                                                    [{'day': row7['items'][0]['days'][itemCount - (t)]['day']}],
                                                    orient="columns")], axis=1, join='inner')
                    t = t + 1
                finalResultIRIFBOTC = pandas.concat([finalResultIRIFBOTC, result])
                result = pandas.DataFrame()


        df = pandas.DataFrame.from_dict(helper.get_sessions(
            'https://api.bourseview.com/v2/quotes?items=price&exchanges=IRTSENO&date=[' + dateToGetDate.strftime(
                "%Y%m%d") + ']', authToken, cookies), orient="columns")
        stockDataFrameHelper = pandas.DataFrame.from_dict(stockDataFrame['tickers'], orient="columns")
        pandas.DataFrame.from_dict(stockDataFrameHelper['tickers'][0], orient="columns").query(
            'ticker == "IRO7BLIP0001" ')
        finalResultIRTSENO = pandas.DataFrame()
        for key, item in pandas.DataFrame.from_dict(df['tickers'], orient="columns").iterrows():
            dataframe8 = pandas.DataFrame.from_dict(item[0], orient="columns")
            result = pandas.DataFrame()
            # loop throw tickers
            for index7, row7 in dataframe8.iterrows():
                # add symboleFa and InstrumentId
                try:
                    result = pandas.DataFrame({'instrumentId': [row7['ticker']], 'symbolFA': [
                        pandas.DataFrame.from_dict(stockDataFrameHelper['tickers'][0], orient="columns").query(
                            'ticker == "' + row7['ticker'] + '" ')['symbolFA'].values[0]]})
                except:
                    print(row7['ticker'])
                    continue
                t = 1
                value = 0
                itemCount = pandas.DataFrame.from_dict(row7['items'][0]['days']).count()['day']
                for key1, item1 in pandas.DataFrame.from_dict(row7['items'][0]['days']).iterrows():
                    value = value + row7['items'][0]['days'][itemCount - (t)]['items'][0]['values']['value']
                    final = row7['items'][0]['days'][itemCount - (t)]['items'][0]['values']['vwap']
                    if t == 1:
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict([{'1DaysVolume': value}], orient="columns")],
                                               axis=1, join='inner')
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict([{'FinalPrice': final}], orient="columns")],
                                               axis=1, join='inner')

                    try:
                        if (result['day'][0] < row7['items'][0]['days'][itemCount - (t)]['day']):
                            result['day'][0] = row7['items'][0]['days'][itemCount - (t)]['day']
                    except:
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict(
                                                    [{'day': row7['items'][0]['days'][itemCount - (t)]['day']}],
                                                    orient="columns")], axis=1, join='inner')
                    t = t + 1
                finalResultIRTSENO = pandas.concat([finalResultIRTSENO, result])
                result = pandas.DataFrame()


        finalResult = pandas.concat([finalResultIRTSENO, finalResultIRIFBNO])
        finalResult = pandas.concat([finalResult, finalResultIRIFBOTC])
        # finalResult = finalResultIRIFBNO
        finalResult.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\PriceAndVolume.xlsx', index=False)





        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LOCALHOST;'
                              'Database=Adib;'
                              'UID:sa;'
                              'PWD:Adib4626')


        finalResult = finalResult.fillna(0)
        cursor = conn.cursor()
        for row in finalResult.itertuples():
            cursor.execute('''INSERT INTO Adib.dbo.FinalPriceAndVolumes (instrumentId, symbolFA, [1DaysVolume], FinalPrice,[day] )
                           VALUES (?,?,?,?,?)
                             ''',
                           row[1],
                           row[2],
                           row[3],
                           row[4],
                           row[5],
                           )
        conn.commit()

        # engin = sal.create_engine('mssql+pyodbc://sa:Adib4626@localhost/adib?driver=SQL+Server+Native+Client+11.0',
        #                           echo=True)
        # conn = engin.connect(charset='utf8')
        # finalResult.to_sql("FinalPriceAndVolumes", con=engin, index=False,
        #                             if_exists='append')
        conn.close()

        # Insert DataFrame to Table
        print('end of insert into sql')
        logger.log(level=logging.INFO, msg="end of insert into sql")
        return finalResult

    try:
        if __name__ == "__main__":
            doValuesOfTradePeriodicly(0)
    except Exception as err:
        logger.error(err, exc_info=True)
        print(err)
except Exception as err14:
    print(err14)
    logger.error(err14, exc_info=True)
    logger.error(exHandling.ResolveExceptions(err))
