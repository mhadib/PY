
from weakref import finalize

import jdatetime
import numpy as np
import pandas
import logging

import pyodbc
import requests
from datetime import datetime, timedelta
from json_excel_converter import Converter
import sqlalchemy as sal
import json
import time


logging.basicConfig(filename='E:/گزارشات لاگ ها/BalanceSheet/BalanSheetGheyreTalfighiLog.txt', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:
    from numpy.compat import os_PathLike
    import sys
    import os
    path = os.path.abspath("E:\\PY\\Tools")
    sys.path.append(path)
    import ResolveExceptions as exHandling
    #test2df = pandas.read_excel (r'E:\adib\boors\analysis2.xlsm', sheet_name='filterdFirms')
    ### Parameters ###
    environmentId = '82809'
    api_login = '09133135250'
    api_password = 'Adib4626'
    base_url = "https://api.bourseview.com"

    #### FUNCTIONS ###
    def logoutall(base_url, auth_token, cookies):
        header_gs = {'X-MSTR-AuthToken': auth_token,
                     'Accept': 'application/json'}
        r = requests.post(base_url, headers=header_gs, cookies=cookies)
        if r.ok:
            print("Logout...")

        else:
            logger.error("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


    def login(base_url,api_login,api_password):
        print("Getting token...")
        data_get = {'username': api_login,
                    'password': api_password}
        r = requests.post(base_url + '/login', data=data_get)
        if r.ok:
            authToken = 'token'#r.headers['X-MSTR-AuthToken']
            cookies = dict(r.cookies)
            print("Token: " + authToken)
            return authToken, cookies
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))

    def get_sessions(base_url, auth_token, cookies):
        print("Checking session..."+base_url);
        header_gs = {'X-MSTR-AuthToken': auth_token,
                     'Accept': 'application/json'}
        r = requests.get(base_url, headers=header_gs, cookies=cookies)
        if r.ok:
            print("Authenticated...")

            conv = Converter()
            return json.loads('['+r.text+']')
        else:
            print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
            logger.error("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


    authToken, cookies = login(base_url, api_login, api_password)
    logoutall("https://api.bourseview.com/v2/logout-all", authToken, cookies)
    authToken, cookies = login(base_url, api_login, api_password)
    choice = None

    test2df = pandas.read_excel(r'E:\PY\BourseView\چند روز قبل را برای بالانس شیت ها از بورس ویو بگیر؟.xlsx', sheet_name='Sheet1')
    BeforeDayCount = test2df['Value'][0]
    dateToGetDate =jdatetime.datetime.now() - timedelta(80)#timedelta(days=np.float64(BeforeDayCount))
    ticker=""

    df = pandas.DataFrame.from_dict(get_sessions('https://api.bourseview.com/v2/stocks/mda/inventoryTurnover?date=['+dateToGetDate.strftime("%Y%m%d")+',null]&period=month&lang=fa&audit=false', authToken, cookies), orient="columns")
    for key, item in pandas.DataFrame.from_dict(df['tickers'][0]).iterrows():
        ticker = item[0]
        print(item[1])
        for key1, item1 in pandas.DataFrame.from_dict(item[1]).iterrows():
            print(item1)
            for key2, item2 in pandas.DataFrame.from_dict(item1[1]).iterrows():
                print(item2)
                for key3, item3 in pandas.DataFrame.from_dict(item2[0]).iterrows():
                    print(item3)
                    conn = pyodbc.connect('Driver={SQL Server};'
                                          'Server=LOCALHOST;'
                                          'Database=Adib;'
                                          'UID:sa;'
                                          'PWD:Adib4626')
                    cursor = conn.cursor()
                    cursor.execute('''INSERT INTO Adib.dbo.ProductionAndSell(ticker, publishTime, statementType, [audit], restate, codalTracingNo, [values], item, itemDetails, itemUnit)
                                                              VALUES (?,?,?,?,?,?,?,?,?,?)
                                                                ''',
                                       ticker,
                                       item2['publishTime'],
                                       item2['statementType'],
                                       item2['audit'],
                                       item2['restate'],
                                       item2['codalTracingNo'],
                                       str(item3['values']),
                                       str(item3['item']),
                                       str(item3['itemDetail']),
                                       str(item3['itemUnit'])
                                       )

                    conn.commit()

    print("end of final result ")

    print("end to write to files ")
except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
    logger.error(exHandling.ResolveExceptions(err))