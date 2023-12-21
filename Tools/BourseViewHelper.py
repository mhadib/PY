import numpy as np
import pandas
import logging
import requests
from datetime import datetime, timedelta

import sqlalchemy
import sqlalchemy as sal
import json
import time

def logoutall(base_url, auth_token, cookies):
    header_gs = {'X-MSTR-AuthToken': auth_token,
                 'Accept': 'application/json'}
    headers = {
        'Cookie': 'Authorization=' + auth_token
    }
    r = requests.post(base_url, headers=headers)
    if r.ok:
        print("Logout...")

    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


def login(base_url, api_login, api_password):
    print("Getting token...")
    data_get = {'username': api_login,
                'password': api_password}
    r = requests.post(base_url + '/login', data=data_get)
    if r.ok:
        authToken = 'token'  # r.headers['X-MSTR-AuthToken']
        cookies = dict(r.cookies)
        print("Token: " + authToken)
        return authToken, cookies
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


def extract_features(col: str, feat_new_names: dict):
    return (
        df[col]
        .apply(pandas.Series)
        .stack()
        .apply(pandas.Series)
        .reset_index()
        .drop(['level_1'], axis=1)
        .set_index('id')
        .rename(columns=feat_new_names)
    )


def login(username, password):
    body = {'username': username, 'password': password}
    resp = requests.post('https://api.bourseview.com/login', data=json.dumps(body))
    data = json.loads(resp.text)
    return data['token'], data['expiration']


def get_sessions(base_url, auth_token, params):
    print("Checking session...")
    headers = {
        'Cookie': 'Authorization=' + auth_token
    }
    # headers = {
    #     'Cookie': 'Authorization=' + token
    # }
    r = requests.get(base_url, headers=headers)
    print(base_url)
    if r.ok:

        return json.loads('[' + r.text + ']')
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


def get_sessions2(base_url, auth_token, params):
    print("Checking session...")
    header_gs = {'X-MSTR-AuthToken': auth_token,
                 'Accept': 'application/json'}
    headers = {
        'Cookie': 'Authorization=' + auth_token
    }
    r = requests.get(base_url, params=params, headers=headers)
    print(base_url)
    if r.ok:

        return json.loads('[' + r.text + ']')
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


def get_tickers_bourse(token, i):
    main_url = "https://api.bourseview.com/v2/tickers"
    headers = {	'Cookie': 'Authorization=' + token	}
    result = requests.get(url=main_url, params=None, headers=headers)
    json_data = result.json()
    tickerDF = pandas.json_normalize(json_data['tickers'])


    # overLapEvents = overLapEvents.append(eventsDF.loc[(eventsDF['description'].str.contains(roomTitle)) & (
    #             pandas.to_datetime(eventsDF['start.dateTime']) >= pandas.to_datetime(startTime)) & (pandas.to_datetime(
    #     eventsDF['end.dateTime']) <= pandas.to_datetime(endTime))])
    dd = tickerDF.loc[
        # (tickerDF['exchangeName'] == 'فرابورس ایران')&
        (tickerDF['status'] == 'active')
        & (tickerDF['nameFA'].str.contains('اختیار') == False)
        & (tickerDF['nameFA'].str.contains('اجاره') == False)
        & (tickerDF['exchangeName'].str.contains('انرژی') == False)
        # & (tickerDF['exchangeName'].str.contains('بورس کالا') == True)
        # & (tickerDF['industryIndex'].str.contains('IREXETF') == True)
        & (tickerDF['exchangeName'].str.contains('فرابورس') == False)
        & (tickerDF['ticker'].str.startswith('IROA') == False)
        & (tickerDF['ticker'].str.startswith('IRO9') == False)
        & (tickerDF['ticker'].str.startswith('IRO2') == False)
        & (tickerDF['ticker'].str.startswith('IROB') == False)
        & (tickerDF['ticker'].str.startswith('IROF') == False)
        & (tickerDF['ticker'].str.startswith('IRR') == False)
        & (tickerDF['ticker'].str.startswith('IREX') == False)
        & (tickerDF['nameFA'].str.contains('صکوک') == False)
        & (tickerDF['nameFA'].str.contains('مشارکت') == False)
        & (tickerDF['nameFA'].str.contains('مظنه') == False)
        & (tickerDF['nameFA'].str.contains('گواهی') == False)
        & (tickerDF['nameFA'].str.contains('خزانه') == False)
        & (tickerDF['nameFA'].str.contains('منفعت') == False)
        & (tickerDF['nameFA'].str.contains('مرابحه') == False)
        & (tickerDF['nameFA'].str.contains('تسهیلات') == False)
        & (tickerDF['nameFA'].str.contains('گام بانک') == False)
        & (tickerDF['nameFA'].str.contains('شاخص') == False)
        & (tickerDF['nameFA'].str.contains('تسهیلات') == False)
        & (tickerDF['nameFA'].str.contains('استان') == False)
        & (tickerDF['nameFA'].str.contains('سرمایه') == False)
        ]
    tickersId = dd['ticker'].iloc[i * 50: (i + 1) * 50].str.cat(sep=',')
    # tickersId = dd['ticker'].str.cat(sep=',')
    return tickersId

def get_tickers_farabourse(token, i):
    main_url = "https://api.bourseview.com/v2/tickers"
    headers = {'Cookie': 'Authorization=' + token}
    result = requests.get(url=main_url, params=None, headers=headers)
    json_data = result.json()
    tickerDF = pandas.json_normalize(json_data['tickers'])

    # overLapEvents = overLapEvents.append(eventsDF.loc[(eventsDF['description'].str.contains(roomTitle)) & (
    #             pandas.to_datetime(eventsDF['start.dateTime']) >= pandas.to_datetime(startTime)) & (pandas.to_datetime(
    #     eventsDF['end.dateTime']) <= pandas.to_datetime(endTime))])
    dd = tickerDF.loc[
        # (tickerDF['exchangeName'] == 'فرابورس ایران')&
        (tickerDF['status'] == 'active') & (
                tickerDF['nameFA'].str.contains('اختیار') == False) & (
                tickerDF['nameFA'].str.contains('اجاره') == False)
        & (tickerDF['exchangeName'].str.contains('انرژی') == False)
        & (tickerDF['exchangeName'].str.contains('بورس کالا') == False)
        & (tickerDF['exchangeName'].str.contains('فرابورس') == True)
        & (tickerDF['ticker'].str.startswith('IROA') == False)
        & (tickerDF['ticker'].str.startswith('IRO9') == False)
        & (tickerDF['ticker'].str.startswith('IRO2') == False)
        & (tickerDF['ticker'].str.startswith('IROB') == False)
        & (tickerDF['ticker'].str.startswith('IROF') == False)
        & (tickerDF['ticker'].str.startswith('IRR') == False)
        & (tickerDF['ticker'].str.startswith('IREX') == False)
        & (tickerDF['nameFA'].str.contains('صکوک') == False)
        & (tickerDF['nameFA'].str.contains('مشارکت') == False)
        & (tickerDF['nameFA'].str.contains('مظنه') == False)
        & (tickerDF['nameFA'].str.contains('گواهی') == False)
        & (tickerDF['nameFA'].str.contains('خزانه') == False)
        & (tickerDF['nameFA'].str.contains('منفعت') == False)
        & (tickerDF['nameFA'].str.contains('مرابحه') == False)
        & (tickerDF['nameFA'].str.contains('تسهیلات') == False)
        & (tickerDF['nameFA'].str.contains('گام بانک') == False)
        & (tickerDF['nameFA'].str.contains('شاخص') == False)
        & (tickerDF['nameFA'].str.contains('تسهیلات') == False)
        & (tickerDF['nameFA'].str.contains('استان') == False)
        & (tickerDF['nameFA'].str.contains('سرمایه') == False)
        ]

    tickersId = dd['ticker'].iloc[i*50: (i+1) * 50].str.cat(sep=',')
    # tickersId = dd['ticker'].str.cat(sep=',')
    return tickersId

def get_industries(token):
    main_url = "https://api.bourseview.com/v2/industries"
    headers = {	'Cookie': 'Authorization=' + token}
    result = requests.get(url=main_url, params=None, headers=headers)
    json_data = result.json()
    tickerDF = pandas.json_normalize(json_data['tse'])
    for key, item in pandas.DataFrame.from_dict(tickerDF, orient="columns").iterrows():
        # print(item['items'])
        dataframe8 = pandas.DataFrame.from_dict(item['items'], orient="columns")
        dataframe8 = dataframe8.fillna(0)
        print("start to write to sql ")

        engine = sqlalchemy.create_engine(
            "mssql+pyodbc://sa:Adib4626@localhost/adib?driver=ODBC+Driver+17+for+SQL+Server",
            echo=False, fast_executemany=True)
        dataframe8.to_sql("Industries", con=engine, index=False,
                        if_exists='append')
        print("end of write to sql ")


def get_oragh(token):
    main_url = "https://api.bourseview.com/v2/bonds/basics"
    headers = {	'Cookie': 'Authorization=' + token}
    result = requests.get(url=main_url, params=None, headers=headers)
    json_data = result.json()
    tickerDF = pandas.json_normalize(json_data['tickers'])
    for key, item in pandas.DataFrame.from_dict(tickerDF, orient="columns").iterrows():
        # print(item['items'])
        dataframe8 = pandas.DataFrame.from_dict(item['items'], orient="columns")
        dataframe8['ticker'] = item['ticker']
        dataframe8 = dataframe8.fillna(0)
        print("start to write to sql ")

        engine = sqlalchemy.create_engine(
            "mssql+pyodbc://sa:Adib4626@localhost/adib?driver=ODBC+Driver+17+for+SQL+Server",
            echo=False, fast_executemany=True)
        dataframe8.to_sql("Oragh", con=engine, index=False,
                        if_exists='append')
        print("end of write to sql ")




def get_tickers_and_write_to_db(token):
    main_url = "https://api.bourseview.com/v2/tickers"
    headers = {	'Cookie': 'Authorization=' + token					}
    result = requests.get(url=main_url, params=None, headers=headers)
    json_data = result.json()
    tickerDF = pandas.json_normalize(json_data['tickers'])
    # overLapEvents = overLapEvents.append(eventsDF.loc[(eventsDF['description'].str.contains(roomTitle)) & (
    #             pandas.to_datetime(eventsDF['start.dateTime']) >= pandas.to_datetime(startTime)) & (pandas.to_datetime(
    #     eventsDF['end.dateTime']) <= pandas.to_datetime(endTime))])

    tickerDF = tickerDF.fillna(0)

    # finalResult['day'] = finalResult['day'].apply(lambda x: "'" + str(x) + "'")

    print("start to write to sql ")
    # engin = sal.create_engine('mssql+pyodbc://sa:Adib4626@localhost/adib?driver=SQL+Server+Native+Client+11.0', echo=True)
    # conn = engin.connect()
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc://sa:Adib4626@localhost/adib?driver=ODBC+Driver+17+for+SQL+Server",
        echo=False, fast_executemany=True)
    tickerDF.to_sql("Company", con=engine, index=False,
                       if_exists='append')
    print("end of write to sql ")

    # dd = tickerDF.loc[
    #     # (tickerDF['exchangeName'] == 'فرابورس ایران')&
    #     (tickerDF['status'] == 'active') & (
    #         tickerDF['nameFA'].str.contains('اختیار') == False) & (
    #                           tickerDF['nameFA'].str.contains('اجاره') == False)
    #     & (tickerDF['exchangeName'].str.contains('انرژی') == False)
    #     & (tickerDF['exchangeName'].str.contains('بورس کالا') == False)
    #     & (tickerDF['exchangeName'].str.contains('فرابورس') == False)
    #     & (tickerDF['ticker'].str.startswith('IROA') == False)
    #     & (tickerDF['ticker'].str.startswith('IRO9') == False)
    #     & (tickerDF['ticker'].str.startswith('IRO2') == False)
    #     & (tickerDF['ticker'].str.startswith('IROB') == False)
    #     & (tickerDF['ticker'].str.startswith('IROF') == False)
    #     & (tickerDF['ticker'].str.startswith('IRR') == False)
    #     & (tickerDF['ticker'].str.startswith('IREX') == False)
    #     & (tickerDF['nameFA'].str.contains('صکوک') == False)
    #     & (tickerDF['nameFA'].str.contains('مشارکت') == False)
    #     & (tickerDF['nameFA'].str.contains('مظنه') == False)
    #     & (tickerDF['nameFA'].str.contains('گواهی') == False)
    #     & (tickerDF['nameFA'].str.contains('خزانه') == False)
    #     & (tickerDF['nameFA'].str.contains('منفعت') == False)
    #     & (tickerDF['nameFA'].str.contains('مرابحه') == False)
    #     & (tickerDF['nameFA'].str.contains('تسهیلات') == False)
    #     & (tickerDF['nameFA'].str.contains('گام بانک') == False)
    #     & (tickerDF['nameFA'].str.contains('شاخص') == False)
    #     & (tickerDF['nameFA'].str.contains('تسهیلات') == False)
    #     & (tickerDF['nameFA'].str.contains('استان') == False)
    #     & (tickerDF['nameFA'].str.contains('سرمایه') == False)
    #     ]

    # tickersId = dd['ticker'].str.cat(sep=',')

