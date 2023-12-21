# coding=utf-8
import logging
import sqlalchemy as sal
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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Chartayi/Chartayi_MarketCap.txt', level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s %(lineno)d ')
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
        dfMarketCap = pandas.DataFrame.from_dict(helper.get_sessions(
            'https://api.bourseview.com/v2/quotes?items=share&date=[' + dateToGetDate.strftime("%Y%m%d") + ']',
            authToken, cookies), orient="columns")

        finalResultMarketCap = pandas.DataFrame()
        try:
            for key, item in pandas.DataFrame.from_dict(dfMarketCap['tickers'], orient="columns").iterrows():
                dataframe8 = pandas.DataFrame.from_dict(item[0], orient="columns")
                result = pandas.DataFrame()
                # loop throw tickers
                for index7, row7 in dataframe8.iterrows():
                    # add symboleFa and InstrumentId
                    try:
                        result = pandas.DataFrame({'instrumentId': [row7['ticker']]})
                    except:
                        print(row7['ticker'])
                        continue
                    t = 0
                    for key1, item1 in pandas.DataFrame.from_dict(row7['items'][0]['days']).iterrows():
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                    pandas.DataFrame.from_dict(
                                                        [{'dayMarketCap': row7['items'][0]['days'][t]['day']}],
                                                        orient="columns")], axis=1, join='inner')
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict([{'ارزش بازار':
                                                                             row7['items'][0]['days'][t]['items'][0][
                                                                                 'values']['marketCap']}],
                                                                       orient="columns")], axis=1, join='inner')
                        result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                pandas.DataFrame.from_dict(
                                                    [{'float': row7['items'][0]['days'][t]['items'][0]['values']['float']}],
                                                    orient="columns")], axis=1, join='inner')
                        t = t + 1
                    finalResultMarketCap = pandas.concat([finalResultMarketCap, result])
                    result = pandas.DataFrame()
        except:
            print()

        finalResultMarketCap.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\MarketCap.xlsx', index=False)

        engin = sal.create_engine('mssql+pyodbc://sa:Adib4626@localhost/adib?driver=SQL+Server+Native+Client+11.0',
                                  echo=True)
        conn = engin.connect()
        finalResultMarketCap = finalResultMarketCap.fillna(0)
        finalResultMarketCap.to_sql("MarketCap", con=engin, index=False,
                                    if_exists='append')


        # Insert DataFrame to Table
        print('end of insert into sql')
        logger.log(level=logging.INFO, msg="end of insert into sql")
        return finalResultMarketCap
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
