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

import pyodbc

logging.basicConfig(filename='E:/گزارشات لاگ ها/BalanceSheet/BalanSheetGheyreTalfighi_CashFlow_Bourse_Log.txt', level=logging.DEBUG,
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
    time.sleep(10)
    authToken, cookies = helper.login(api_login, api_password)
    choice = None
    StatementHelp = pandas.DataFrame()

    StatementHelp = helper.get_sessions('https://api.bourseview.com/v2/statementsItems', authToken, cookies)

    df1 = pandas.DataFrame()
    StatementHelp = pandas.DataFrame.from_dict(StatementHelp, orient="columns")
    for key, item in StatementHelp['categories'].items():
        dataframe = pandas.DataFrame.from_dict(item, orient="columns")
        for key2, item2 in dataframe['items'].items():
            dataframe2 = pandas.DataFrame.from_dict(item2, orient="columns")
            df1 = pandas.concat([df1, pandas.DataFrame({"item": dataframe2['item'], "helpFA": dataframe2['helpFA']})])


    df1 = df1.reset_index(drop=True)
    result = pandas.DataFrame()
    pandas.DataFrame.from_dict(StatementHelp['categories'][0])
    #           category                                              items
    #0     Balance Sheet  [{'helpFA': 'موجودی نقد', 'item': 'cash', 'sta...
    #1  Income Statement  [{'helpFA': 'درامد سود سهام', 'item': 'dividen...
    #2         Cash Flow  [{'helpFA': 'جریان خالص ورود (خروج) وجه نقد نا...

    #itemsURLs = ",".join(pandas.DataFrame.from_dict(pandas.DataFrame.from_dict(StatementHelp['categories'][0]).query('category == "Balance Sheet"')['items'][0])['item'])
    #df = pandas.read_json(r'StatementDev.json')


    itemsURLs = ",".join(pandas.DataFrame.from_dict(pandas.DataFrame.from_dict(StatementHelp['categories'][0]).query('category == "Balance Sheet"')['items'][0]).query('item not in ("CommonStock","totalNonCurrentLiabilities","otherLiabilities","deferredRevenueNonCurrent","nonControllingInterests","totalEquityAttributableToOwnersOfParent","TreasurySharesIncludingParentCompanyStockOwnedBySubsidiaries","capitalReserve","parentCompanyStockOwnedByTheSubsidiary","insurancePremiumNotRealized","otherTechnicalReserves","nonMaturedRisksReserves","deferredDamageReserves","insurancePremiumReserves","debtToAffiliatedCompanies","payablesToInsurerAndReinsurer","payablesToPolicyholdersAgentsAndBrokers","goodwill","landHeldForPropertyDevelopment","InvestmentsInSubsidiaries","reinsurersShareFromTechnicalReserves","creditFacilitiesToSubsidiary","receivableFromAffiliatedCompanies","receivableFromInsurersAndReinsurers","receivableFromPolicyholdersAgentsAndBrokers","listedCapital","otherAccountsPayableAndProvisions","otherReserves","investmentInSecurities","liabilitiesRelatedToAssetsForSale","expansionReserve","revaluationSurplus","totalStockHoldersEquity","TreasurySharesSurplus","treasuryShares","revaluationSurplusNonCurrentAssetsSale","intangibleAssets","allowanceForPostRetirement","CapitalSurplus","receivesForCapitalAdvance","legalReserve","expansionReservetotalStockHoldersEquity","totalLiabilitiesAndStockHoldersEquity","exchangeDifferencesOnTranslation","exchangeReserveGovernmentalCorporations")')['item'])

    #df = pandas.DataFrame.from_dict(get_sessions('http://api.bourseview.com/v1/stocks/statements?statementTypes=balanceSheet&tickers=IRO1IKCO0001,IRO3PZGZ0001', authToken, cookies), orient="columns")
    #http://api.bourseview.com/v1/stocks/statements?statementTypes=balanceSheet&tickers=IRO1IKCO0001

    #pandas.DataFrame.from_dict(pandas.DataFrame.from_dict(pandas.DataFrame.from_dict(pandas.DataFrame.from_dict(df['tickers'][0])['items'][0])['days'][0])['items'][0])['item']
    #0                                   goodwill
    #1     parentCompanyStockOwnedByTheSubsidiary
    #2    totalEquityAttributableToOwnersOfParent

    #stockDataFrame = pandas.DataFrame.from_dict(get_sessions('http://api.bourseview.com/v1/stocks', authToken, cookies), orient="columns")
    stockDataFrame = pandas.DataFrame.from_dict(helper.get_sessions('https://api.bourseview.com/v2/tickers?typeCodes=1000,4000,2000,3000,5000', authToken, cookies), orient="columns")
    newStock = pandas.DataFrame.from_dict(stockDataFrame['tickers'][0], orient='columns')
    for key11, item12 in newStock.iterrows():
        if not (item12['company'] is None):
            pandas.json_normalize(item12['company'])['key']
            try:
                newStock.at[key11, 'symbol'] = pandas.json_normalize(item12['company'])['key'][0]
            except Exception as e:
                print(e)
    newStock = newStock.rename(columns={'symbol': 'companyID'})

    t = pandas.DataFrame.from_dict(pandas.DataFrame.from_dict(stockDataFrame['tickers'][0])['company'], orient="columns")
    companyURLs1 = ""
    companyURLs2 = ""
    companyURLs3 = ""
    companyURLs4 = ""
    companyURLs5 = ""
    companyURLs6 = ""
    companyURLs7 = ""
    companyCounter = 0
    for key10, item10 in t.iterrows():
        try:
            #if pandas.DataFrame.from_dict(stockDataFrame['tickers'][0])['symbolFA'][key10] in test2df.query('type == 2')['signal'].values: # test2df['signal'].values: #
                if companyCounter <= 200:
                    # print(pandas.DataFrame.from_dict(stockDataFrame['tickers'][0])['symbolFA'][key10]+","+str(dict(item10['company'])['key']))
                    companyURLs1 = companyURLs1 + str(dict(item10['company'])['key'])+","
                if (companyCounter > 200 and companyCounter <= 400):
                    # print(pandas.DataFrame.from_dict(stockDataFrame['tickers'][0])['symbolFA'][key10] + "," + str(                        dict(item10['company'])['key']))
                    companyURLs2 = companyURLs2 + str(dict(item10['company'])['key'])+","
                if (companyCounter > 400 and companyCounter <= 600):
                    # print(pandas.DataFrame.from_dict(stockDataFrame['tickers'][0])['symbolFA'][key10] + "," + str(                        dict(item10['company'])['key']))
                    companyURLs3 = companyURLs3 + str(dict(item10['company'])['key']) + ","
                if (companyCounter > 600 and companyCounter <= 800):
                    companyURLs4 = companyURLs4 + str(dict(item10['company'])['key'])+","
                if (companyCounter > 800 and companyCounter <= 1000):
                    companyURLs5 = companyURLs5 + str(dict(item10['company'])['key'])+","
                if (companyCounter > 1000 and companyCounter <= 1200):
                    companyURLs6 = companyURLs6 + str(dict(item10['company'])['key'])+","
                if companyCounter > 1200 :
                    companyURLs7 = companyURLs7 + str(dict(item10['company'])['key'])+","
                companyCounter = companyCounter + 1
        except:
            print('unsupport type')

    test2df = pandas.read_excel(r'E:\PY\BourseView\چند روز قبل را برای بالانس شیت ها از بورس ویو بگیر؟.xlsx', sheet_name='Sheet1')
    BeforeDayCount = test2df['Value'][0]
    dateToGetDate = datetime.today() - timedelta(days=np.float64(BeforeDayCount))

    stockURLs = ",".join(pandas.DataFrame.from_dict(stockDataFrame['tickers'][0])['ticker'])
    df = pandas.DataFrame()
    for i in range(0,20):
        tickersStringIds = helper.get_tickers_bourse(authToken, i)
        if tickersStringIds == '':
            continue
        main_url = 'https://api.bourseview.com/v2/stocks/statements'
        day_param = '[20221005,20221007]'  # تاریخ مد نظر
        items = 'price'  # آیتم های مورد نظر
        get_parameters = {
            'recent': 'true',
            # 'items': 'investmentInSecurities,sales,costOfSales,netIncomeFromContinuingOperationsBeforeTax,cash,tangibleFixedAssets,shortTermInvestments,investmentProperty,dueFromCBI,tradeReceivables,dueFromBanksAndCreditInstitutions,tradeAndOtherReceivables,dueFromGovernment,nonTradeReceivables,creditFacilitiesToGovernmentalEntities,creditFacilitiesToGovernmentalEntities,inventories,facilitiesGrantedToIndividuals,otherAssets,creditFacilities,creditFacilities,prepayments,assetsForSale,assetsForSale,assetsForSale,totalCurrentAssets,longTermNotesAndAccountsReceivable,longTermNotesAndAccountsReceivable,longTermInvestments,totalAssets,capitalAdvances,totalNonCurrentAssets,totalNonCurrentAssets,allowanceForIncomeTax,dividendsPayable,tradePayables,longTermDebt,longTermDebt,nonTradePayables,tradeAndOtherPayables,liabilitiesAssociatedWithNonCurrentAssets,retainedEarnings,deferredRevenue,dueToCBI,dueToBanksAndCreditInstitutions,currentPortionOfLoanPayable,demandDeposits,provisions,totalCurrentLiabilities,savingsDeposits,longTermNotesAndAccountsPayable,termDeposits,otherDeposits,totalLiabilities,totalLiabilities',
            'statementTypes': 'cashFlow',
            'date': '['+dateToGetDate.strftime("%Y%m%d")+',null]',

            'tickers':  tickersStringIds
        }
        headers = {
            'Cookie': 'Authorization=' + authToken
        }
        result = requests.get(url=main_url, params=get_parameters, headers=headers)
        json_data = result.json()
        result = pandas.DataFrame.from_dict(result.json(), orient="columns")
        try:
            df = pandas.concat([df, result])
            result = pandas.DataFrame()
        except:
            print(5)
            result = pandas.DataFrame()

    stockDataFrameHelper = pandas.DataFrame.from_dict(stockDataFrame['tickers'], orient="columns")


    finalResult = pandas.DataFrame(columns=["instrumentId","symbolFA","day","سال مالی","ماه مالی","روز آخر دوره","روز آخر سال مالی","تاریخ انتشار","'فروش'","'بهای تمام شده کالای فروش رفته'","'سود (زیان) خالص عملیات در حال تداوم قبل از مالیات'","'موجودی نقد'","'سرمایه گذاری کوتاه مدت'","'سایر حسابها و اسناد دریافتنی'","'موجودی مواد و کالا'","'پیش پرداخت ها'","'دارایی های نگهداری شده برای فروش'","'جمع داراییهای جاری'","'سرمایه گذاریهای بلند مدت'","'داراییهای ثابت مشهود'","'حسابها و اسناد دریافتنی تجاری بلند مدت'","'سایر دارایی ها'","'جمع داراییهای غیرجاری'","'جمع داراییها'","'پیش دریافتها'","'ذخیره مالیات بر درامد'","'سود سهام پیشنهادی و پرداختنی'","'حصه جاری تسهیلات مالی دریافتی'","'جمع بدهیهای جاری'","'حسابها و اسناد پرداختنی بلند مدت'","'تسهیلات مالی دریافتی بلند مدت'","'سود (زیان) انباشته'","'دریافتنی‌های تجاری و سایر دریافتنی‌ها'","'پرداختنی‌های تجاری و سایر پرداختنی‌ها'","'سرمایه گذاری در املاک'","'ذخایر'","'مطالبات از سایر بانکها و موسسات اعتباری'","'مطالبات از بانک مرکزی'","'سپرده های دیداری'","'سپرده های قرض الحسنه و پس اندار و مشابه'","'بدهی به بانک مرکزی'","'سایر سپرده ها'","'بدهی به بانکها و موسسات اعتباری'","'مطالبات از دولت'","'تسهیلات اعطایی و مطالبات از اشخاص دولتی'","'سپرده های سرمایه گذاری مدت دار'","'تسهیلات اعطایی به سایر اشخاص'","'حسابها و اسناد دریافتنی تجاری'","'حسابها و اسناد پرداختنی تجاری'","'سایر حسابها و اسناد پرداختنی'","'سرمایه گذاری در اوراق بهادار'"])
    try:
        for key, item in pandas.DataFrame.from_dict(df['tickers'], orient="columns").iterrows():
            dataframe8 = pandas.DataFrame.from_dict(item[0], orient="columns")
            result = pandas.DataFrame()
            #loop throw tickers
            for index7, row7 in dataframe8.iterrows():
                # print(row7)
                try:
                    #add symboleFa and InstrumentId
                    try:
                        result = pandas.DataFrame({'instrumentId': [newStock.loc[newStock['ticker'] == str(row7['ticker'])]['ticker'].values[0]],'symbolFA':[newStock.loc[newStock['ticker'] == str(row7['ticker'])]['symbolFA'].values[0]]})
                    except:
                        continue
                    t = 0
                    for key1, item1 in pandas.DataFrame.from_dict(row7['items']).iterrows():
                        try:
                            try:
                                if (result['day'][0][0] < item1['days']['day']):
                                    result['day'][0][0] = item1['days']['day']
                                if (result['day'][0][0] > item1['days']['day']):
                                    continue
                            except:
                                result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                        pandas.DataFrame.from_dict([{'day': [item1['days']['day']]}],
                                                                                   orient="columns")], axis=1, join='inner')

                            try:
                                if (result['سال مالی'][0] != item1['days']['fiscalYear']):
                                    result['سال مالی'][0] = item1['days']['fiscalYear']
                            except:
                                result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                        pandas.DataFrame.from_dict([{'سال مالی': item1['days']['fiscalYear']}],
                                                                                   orient="columns")], axis=1, join='inner')
                            try:
                                if (result['ماه مالی'][0] != item1['days']['fiscalMonth']):
                                    result['ماه مالی'][0] = item1['days']['fiscalMonth']
                            except:
                                result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                        pandas.DataFrame.from_dict([{'ماه مالی': item1['days']['fiscalMonth']}],
                                                                                   orient="columns")], axis=1, join='inner')
                            try:
                                if (result['روز آخر دوره'][0] != item1['days']['periodEndDay']):
                                    result['روز آخر دوره'][0] = item1['days']['periodEndDay']
                            except:
                                result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                        pandas.DataFrame.from_dict([{'روز آخر دوره': item1['days']['periodEndDay']}],
                                                                                   orient="columns")], axis=1, join='inner')
                            try:
                                if (result['روز آخر سال مالی'][0] != item1['days']['yearEndDay']):
                                    result['روز آخر سال مالی'][0] = item1['days']['yearEndDay']
                            except:
                                result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                        pandas.DataFrame.from_dict(
                                                            [{'روز آخر سال مالی': item1['days']['yearEndDay']}],
                                                            orient="columns")], axis=1, join='inner')
                            try:
                                if (result['تاریخ انتشار'][0] != item1['days']['publishTime']):
                                    result['تاریخ انتشار'][0] = item1['days']['publishTime']
                            except:
                                result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),
                                                        pandas.DataFrame.from_dict(
                                                            [{'تاریخ انتشار': item1['days']['publishTime']}],
                                                            orient="columns")], axis=1, join='inner')
                            try:
                                for key3, item3 in pandas.DataFrame.from_dict(row7['items']['days'][0]['items'], orient="columns").iterrows():
                                    i = 0
                                    for item6 in df1['item']:
                                        if item6 == item3[0]:
                                            try:
                                                if (result["'" +str(df1.query('item == "' + item6 + '"')['helpFA'][i]) + "'"][0] != item3[1]['cumulative']):
                                                    result["'" + str(df1.query('item == "' + item6 + '"')['helpFA'][i]) + "'"][0] = item3[1]['cumulative']
                                            except:
                                                result = pandas.concat([pandas.DataFrame.from_dict(result, orient="columns"),pandas.DataFrame.from_dict([{"'" +str(df1.query('item == "' + item6 + '"')['helpFA'][i]) + "'": item3[1]['cumulative']}], orient="columns")], axis=1,join='inner')
                                        i = i + 1
                                t = t + 1
                            except:
                                print(1)
                        except:
                            print(2)
                    try:
                        finalResult = pandas.concat([finalResult, result])
                        result = pandas.DataFrame()
                    except:
                        print(5)
                        result = pandas.DataFrame()
                except Exception as err:
                    logger.error(err)

    except:
        print(4)
    print("end of final result ")
    finalResult.index = range(1,len(finalResult)+1)


    finalResult = finalResult.fillna(0)
    finalResult['day'] = finalResult['day'].str[0]
    #finalResult['day'] = finalResult['day'].apply(lambda x: "'" + str(x) + "'")

    print("start to write to sql ")

    # cnxn = pyodbc.connect(Trusted_Connection='Yes', DRIVER='{SQL Server}', SERVER='localhost', DATABASE='adib',
    #                       UID='sa', PWD='Adib4626', Charset='utf-8')


    # engin = sal.create_engine('mssql+pyodbc://sa:Adib4626@localhost/adib?driver=SQL+Server+Native+Client+11.0', echo=True)
    # conn = engin.connect()
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc://sa:Adib4626@localhost/adib?driver=ODBC+Driver+17+for+SQL+Server",
        echo=False, fast_executemany=True)
    finalResult.to_sql("BalanceSheetGheyreTalfighiCategurCashflow_Bourse", con=engine, index=False, if_exists='append',
                       schema='dbo')
    print("end of write to sql ")

    print("start to write to files ")
    finalResult.to_excel('E:\\گزارشات اکسل\\BourseView\\BalanceSheet\\BalanceSheetGheyreTalfighiCategurCashflow_Bourse.xlsx', index=False)
    logger.log(level=logging.DEBUG, msg="end of write to excel")
    print("end to write to files ")
except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
    logger.error(exHandling.ResolveExceptions(err))
