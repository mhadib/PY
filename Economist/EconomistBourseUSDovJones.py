import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/EconomistBourseDovJones.txt',
    }
    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/united-states/stock-market'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'download_timeout': 60})

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/EconomistBourseDovJones.txt', level=logging.DEBUG,
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
            dd.to_csv("E:\\PY\Economist\\CSVs\\bourseUSDovJones.csv", encoding='utf-8-sig')
            main()
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))
def main():
    result = pd.DataFrame()
    finalResult = pd.DataFrame()
    dd = pd.read_csv("E:\\PY\Economist\\CSVs\\bourseUSDovJones.csv")
    first = True
    for key in pd.read_html(dd['0'][0]):
        if first != True:
            continue
        #data1 = pd.read_html(key[1][1], skiprows=0);
        data1 = pd.DataFrame(key)
        #data1 = data1[0].replace(r'_x000D_', '', regex=True)
        #data1 = data1.replace(r' ', '', regex=True)
        data1.rename(columns={data1.columns[0]: "Name"}, inplace=True)
        data1.rename(columns={data1.columns[1]: "FullName"}, inplace=True)
        data1.rename(columns={data1.columns[4]: "IndexChange"}, inplace=True)
        finalResult = pd.concat([finalResult, data1])
        first = False

    finalResult = finalResult[['Name','FullName','قیمت','IndexChange','روز','سال','تاریخ']]
    finalResult = finalResult.replace(r'%', '', regex=True)
    finalResult = finalResult.fillna(0)
    conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LOCALHOST;'
                              'Database=Adib;'
                              'UID:sa;'
                              'PWD:Adib4626')
    cursor = conn.cursor()
    print('insert into sql')
    for row in finalResult.itertuples():
        print(row)
        cursor.execute('''INSERT INTO Adib.dbo.BoursUSDovJones(Name,FullName,Gheymat,IndexChange,DayDarsad,YearDarsad,RDate)
                                                   VALUES (?,?,?,?,?,?,?)
                                                     ''',
                           row[1],
                           row[2],
                           row[3],
                           row[4],
                       row[5],
                       row[6],
                       row[7],)
    conn.commit()
    #
    sqlHistory = pandas.read_sql_query('select * from [dbo].[VBoursUSDovJones]	order by [درصد تغییر نسبت به سال 2005] desc ', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\VBoursUSDovJones.xlsx', index=False)


    print("main done")
# main()