import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes",
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/BourseGourdian.txt',
    }

    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/stocks'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/BourseGourdian.txt', level=logging.INFO,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling

            print(4)

            table = response.xpath('/html/body/form').extract()
            print('table:')
            print(table)
            print(2)
            dd = pd.DataFrame.from_dict(table, orient="columns")
            print(dd)
            dd.to_csv("E:\\PY\Economist\\CSVs\\t.csv", encoding='utf-8-sig')
            main()

        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))

def main():
    finalResult = pandas.DataFrame()
    dd = pd.read_csv("E:\\PY\Economist\\CSVs\\t.csv")

    for key in dd.iterrows():
        for key2 in pd.read_html(key[1][1], skiprows=0):
            data1 = key2
            # data1 = data1[0].replace(r'_x000D_', '', regex=True)
            # data1 = data1.replace(r' ', '', regex=True)
            data1.rename(columns={data1.columns[1]: "Keshvar"}, inplace=True)

            finalResult = pd.concat([finalResult, data1])
            # join.to_excel(r"E:\\گزارشات اکسل\\Economist\\HesabeJari.xlsx", index=False)

            print("1")
    print("2")
    # finalResult.to_excel(
    #     r"E:\\گزارشات اکسل\\Economist\\HesabeJariGaurdian" + jdatetime.datetime.now().strftime('%Y_%m_%d') + ".xlsx",
    #     index=False)
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=Adib;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()

    for row in finalResult.itertuples():
        cursor.execute('''INSERT INTO Adib.dbo.bourse([Name],[Index])
                                                 VALUES (?,?)
                                                   ''',
                       row[2],
                       row[3],
                       )
    conn.commit()
    sqlHistory = pandas.read_sql_query(
        'select * from [dbo].[VBourse] order by [رشد نسبت به 2005] desc	', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\VBourse.xlsx', index=False)
    print("main done!")
# main()