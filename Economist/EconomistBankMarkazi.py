import jdatetime
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/EconomistBankMarkazi.txt',
    }

    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/country-list/central-bank-balance-sheet'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/EconomistBankMarkazi.txt', level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling

            print('respons:')
            print(response)
            table = response.xpath('/html/body/form').extract()
            print('table:')
            print(table)
            print(2)

            dd = pd.DataFrame.from_dict(table, orient="columns")
            print(dd)
            dd.to_csv("E:\\PY\Economist\\CSVs\\BankMarkazi.csv", encoding='utf-8-sig')
            main()
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))
def main():
    result = pd.DataFrame()
    finalResult = pd.DataFrame()
    dd = pd.read_csv("E:\\PY\Economist\\CSVs\\BankMarkazi.csv")
    for key in pd.read_html(dd['0'][0]):
        #data1 = pd.read_html(key[1][1], skiprows=0);
        data1 = pd.DataFrame(key)
        #data1 = data1[0].replace(r'_x000D_', '', regex=True)
        # data1 = data1.replace(r' ', '', regex=True)
        data1.rename(columns={data1.columns[0]: "Name"}, inplace=True)
        if len(data1.query('Name == "ژاپن" ')) > 0:
            result = data1.query('Name == "ژاپن" ')
            result['2018'] = 552000
            result['آبان 1399'] = 689000
            finalResult = pd.concat([finalResult, result])
        if len(data1.query('Name == "سوئیس" ')) > 0:
            result = data1.query('Name == "سوئیس" ')
            result['2018'] = 817000
            result['آبان 1399'] = 975000
            finalResult = pd.concat([finalResult, result])
        if len(data1.query('Name == "انگلستان" ')) > 0:
            result = data1.query('Name == "انگلستان" ')
            result['2018'] = 591000
            result['آبان 1399'] = 876000
            finalResult = pd.concat([finalResult, result])
        if len(data1.query('Name == "منطقه یورو" ')) > 0:
            result = data1.query('Name == "منطقه یورو" ')
            result['2018'] = 4695000
            result['آبان 1399'] = 6781000
            finalResult = pd.concat([finalResult, result])
        if len(data1.query('Name == "ایالات متحده" ')) > 0:
            result = data1.query('Name == "ایالات متحده" ')
            result['2018'] = 4000
            result['آبان 1399'] = 7110
            finalResult = pd.concat([finalResult, result])

        if len(data1.query('Name == "کانادا" ')) > 0:
            result = data1.query('Name == "کانادا" ')
            result['2018'] = 116000
            result['آبان 1399'] = 533000
            finalResult = pd.concat([finalResult, result])
        if len(data1.query('Name == "کره جنوبی" ')) > 0:
            result = data1.query('Name == "کره جنوبی" ')
            result['2018'] = 495000
            result['آبان 1399'] = 537000
            finalResult = pd.concat([finalResult, result])
        if len(data1.query('Name == "چین" ')) > 0:
            result = data1.query('Name == "چین" ')
            result['2018'] = 372000
            result['آبان 1399'] = 374000
            finalResult = pd.concat([finalResult, result])

        finalResult = pd.concat([finalResult, data1])
        finalResult = finalResult.fillna(0)
        finalResult = finalResult[['Name','گذشته','2018','آبان 1399','واحد']]

        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LOCALHOST;'
                              'Database=Adib;'
                              'UID:sa;'
                              'PWD:Adib4626')
        cursor = conn.cursor()
        print('insert into sql')
        for row in finalResult.itertuples():
            cursor.execute('''INSERT INTO Adib.dbo.BankMarkazi([Keshvar] ,[Akharin] ,[2018] ,[آبان 1399], [Vahed] )
                                                   VALUES (?,?,?,?,?)
                                                     ''',
                           row[1],
                           row[2],
                           row[3],
                           row[4],
                           row[5]
                           )
        conn.commit()
        cursor.execute('''delete BankMarkazi where Keshvar in (select distinct Keshvar from BankMarkazi b where [2018] > 0) and [2018] = 0''')
        conn.commit()


    sqlHistory = pd.read_sql_query('select * from vBankMarkazi						', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\vBankMarkazi.xlsx', index=False)
    print("main done")
# main()