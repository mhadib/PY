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


logging.basicConfig(filename='E:/گزارشات لاگ ها/Chartayi/Chartayi_LegalReal.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s %(lineno)d ')
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
        legalRealDF = pandas.DataFrame.from_dict(helper.get_sessions(
            'https://api.bourseview.com/v2/quotes?items=indinst&exchanges=IRTSENO,IRIFBNO,IRIFBOTC&date=[' + dateToGetDate.strftime(
                "%Y%m%d") + ']', authToken, cookies), orient="columns")

        for key, item in pandas.DataFrame.from_dict(legalRealDF['tickers'], orient="columns").iterrows():
            try:
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
                    for key12, item12 in pandas.DataFrame.from_dict(row7['items'][0]['days']).iterrows():
                        if (pandas.DataFrame.from_dict(item12['items'])['item'][0] == 'ind'):
                            for key13, item14 in pandas.DataFrame.from_dict(
                                    pandas.DataFrame.from_dict(item12['items'])['values'][0].items()).iterrows():
                                if (item14[0] == 'sellValue'):
                                    result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                            pandas.DataFrame.from_dict([{'فروش حقیقی': item14[1]}],
                                                                                       orient="columns")], axis=1,
                                                           join='inner')
                                if (item14[0] == 'buyValue'):
                                    result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                            pandas.DataFrame.from_dict([{'خرید حقیقی': item14[1]}],
                                                                                       orient="columns")], axis=1,
                                                           join='inner')
                        if (pandas.DataFrame.from_dict(item12['items'])['item'][1] == 'inst'):
                            for key13, item14 in pandas.DataFrame.from_dict(
                                    pandas.DataFrame.from_dict(item12['items'])['values'][1].items()).iterrows():
                                if (item14[0] == 'sellValue'):
                                    result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                            pandas.DataFrame.from_dict([{'فروش حقوقی': item14[1]}],
                                                                                       orient="columns")], axis=1,
                                                           join='inner')
                                if (item14[0] == 'buyValue'):
                                    result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                            pandas.DataFrame.from_dict([{'خرید حقوقی': item14[1]}],
                                                                                       orient="columns")], axis=1,
                                                           join='inner')
                    legalRealFinalResult = pandas.concat([legalRealFinalResult, result])
            except Exception as errlegalreal:
                print(errlegalreal)
            result = pandas.DataFrame()

        legalRealFinalResult.to_excel('E:\\گزارشات اکسل\\BourseView\\Chartayi\\legalReal.xlsx', index=False)

        engin = sal.create_engine('mssql+pyodbc://sa:Adib4626@localhost/adib?driver=SQL+Server+Native+Client+11.0',
                                  echo=True)
        conn = engin.connect()
        legalRealFinalResult = legalRealFinalResult.fillna(0)
        legalRealFinalResult.to_sql("LegalReal", con=engin, index=False,
                                    if_exists='append')


        # Insert DataFrame to Table
        print('end of insert into sql')
        logger.log(level=logging.INFO, msg="end of insert into sql")
        return legalRealFinalResult
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
