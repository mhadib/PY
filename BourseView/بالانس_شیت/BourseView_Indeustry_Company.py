# coding=utf-8
from weakref import finalize

import numpy as np
import pandas
import logging
import requests
from datetime import datetime, timedelta

import sqlalchemy
import sqlalchemy as sal
import json
import time


logging.basicConfig(filename='E:/گزارشات لاگ ها/BalanceSheet/BourseView_Indeustry_Company_Log.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:
    from numpy.compat import os_PathLike
    import sys
    import os
    path = os.path.abspath("E:\\PY\\Tools")
    sys.path.append(path)
    import ResolveExceptions as exHandling
    import BourseViewHelper as helper
    #test2df = pandas.read_excel (r'E:\adib\boors\analysis2.xlsm', sheet_name='filterdFirms')
    ### Parameters ###
    environmentId = '82809'
    api_login = '09133135250'
    api_password = 'Adib4626'
    base_url = "https://api.bourseview.com"



    authToken, cookies = helper.login(api_login, api_password)
    helper.logoutall("https://api.bourseview.com/v2/logout-all", authToken, cookies)
    authToken, cookies = helper.login(api_login, api_password)

    helper.get_oragh(authToken)
    helper.get_industries(authToken)
    helper.get_tickers_and_write_to_db(authToken)


except Exception as err:
    print("errorrrrr")
    print(err)
    logger.log(level=logging.ERROR, msg=err)
    logger.error(err)
    logger.error(exHandling.ResolveExceptions(err))


