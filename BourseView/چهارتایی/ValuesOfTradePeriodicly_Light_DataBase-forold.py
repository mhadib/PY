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


import ValuesOfTradePeriodicly_Light_DataBase_RealLegal
import ValuesOfTradePeriodicly_Light_DataBase_MarketCap
import ValuesOfTradePeriodicly_Light_DataBase_FinalPriceVolume
import ValuesOfTradePeriodicly_Light_DataBase_Gen

#
logging.basicConfig(filename='E:/گزارشات لاگ ها/Chartayi/ChartayiforOld.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s %(lineno)d ')
logger=logging.getLogger(__name__)

try:
    import sys
    import os
    path = os.path.abspath("E:\\PY\\Tools")
    sys.path.append(path)
    import ResolveExceptions as exHandling

    BeforeDayCount = 12
    if BeforeDayCount > 0:
        for i in range(BeforeDayCount):
            try:
                finalResultMarketCap = pandas.DataFrame()
                finalResult = pandas.DataFrame()
                legalRealFinalResult = pandas.DataFrame()
                finalResultMarketCap = ValuesOfTradePeriodicly_Light_DataBase_MarketCap.doValuesOfTradePeriodicly(BeforeDayCount-i)
                finalResult = ValuesOfTradePeriodicly_Light_DataBase_FinalPriceVolume.doValuesOfTradePeriodicly(BeforeDayCount - i)
                legalRealFinalResult = ValuesOfTradePeriodicly_Light_DataBase_RealLegal.doValuesOfTradePeriodicly(BeforeDayCount-i)
                if(len(finalResultMarketCap.index) > 0  and len(finalResult.index) > 0 and len(legalRealFinalResult.index) > 0 ):
                    finalResultMarketCap['InstrumentId'] = finalResultMarketCap['instrumentId']
                    finalResultMarketCap['DayMarketCap'] = finalResultMarketCap['dayMarketCap']
                    finalResult['InstrumentId'] = finalResult['instrumentId']
                    finalResult['SymbolFA'] = finalResult['symbolFA']
                    finalResult['DAY'] = finalResult['day']
                    legalRealFinalResult['InstrumentId'] = legalRealFinalResult['instrumentId']
                    del finalResultMarketCap['instrumentId']
                    del finalResultMarketCap['dayMarketCap']
                    del finalResult['instrumentId']
                    del finalResult['symbolFA']
                    del finalResult['day']
                    del legalRealFinalResult['instrumentId']

                    ValuesOfTradePeriodicly_Light_DataBase_Gen.doValuesOfTradePeriodicly(finalResult, finalResultMarketCap, legalRealFinalResult)
                print(BeforeDayCount)
            except Exception as err:
                logger.error(err, exc_info=True)
                print(err)
            print(str(i) + ': done!')
            time.sleep(20)
except Exception as err:
    logger.error(err, exc_info=True)
    print(err)
except Exception as err14:
    print(err14)
    logger.error(err14, exc_info=True)
    logger.error(exHandling.ResolveExceptions(err))
