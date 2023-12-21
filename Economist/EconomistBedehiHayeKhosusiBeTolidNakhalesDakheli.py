import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/EconomistBedehiHayeKhosusiBeTolidNakhalesDakheli.txt',
    }

    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/country-list/private-debt-to-gdp'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/EconomistBedehiHayeKhosusiBeTolidNakhalesDakheli.txt', level=logging.DEBUG,
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
            dd.to_csv("E:\\PY\Economist\\CSVs\\BedehiHayeKhosusiBeTolidNakhalesDakheli.csv", encoding='utf-8-sig')
            main()
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))

def main():
    result = pd.DataFrame()
    finalResult = pd.DataFrame()
    dd = pd.read_csv("E:\\PY\Economist\\CSVs\\BedehiHayeKhosusiBeTolidNakhalesDakheli.csv")
    for key in pd.read_html(dd['0'][0]):
        #data1 = pd.read_html(key[1][1], skiprows=0);
        data1 = pd.DataFrame(key)
        #data1 = data1[0].replace(r'_x000D_', '', regex=True)
        #data1 = data1.replace(r' ', '', regex=True)
        data1.rename(columns={data1.columns[0]: "Name"}, inplace=True)
        finalResult = pd.concat([finalResult, data1])



    finalResult = finalResult[['Name','گذشته','مرجع']]
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
        cursor.execute('''INSERT INTO Adib.dbo.BedehiHayeKhosusiBeTolidNakhalesDakheli([Keshvar] ,[Gozashte],  [Marja])
                                                   VALUES (?,?,?)
                                                     ''',
                           row[1],
                           row[2],
                           row[3])
    conn.commit()

    sqlHistory = pandas.read_sql_query('select * from vBedehiSherkatHaBeTolidNakhalesDakheli order by [نسبت بدهی شرکت ها به تولید ناخالص ملی] desc', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\vBedehiHayeKhosusiBeTolidNakhalesDakheli.xlsx', index=False)
    print("main done")
#main()