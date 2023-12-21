import jdatetime
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/EconomistZakhayerArzi.txt',
    }
    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/country-list/foreign-exchange-reserves'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        ##self.log(f'title: {response.css("title")}"')
        finalResult = pd.DataFrame();
        print('respons:')
        print(response)
        table = response.xpath('/html/body/form').extract()
        print('table:')
        print(table)
        print(2)
        dd = pd.DataFrame.from_dict(table, orient="columns")
        print(dd)
        dd.to_csv("E:\\PY\Economist\\CSVs\\ZakhayerArzi.csv", encoding='utf-8-sig')
        main()
def main():
    try:
        import logging
        logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/EconomistZakhayerArzi.txt', level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger = logging.getLogger(__name__)
        import sys
        import os
        path = os.path.abspath("E:\\PY\\Tools")
        sys.path.append(path)
        import ResolveExceptions as exHandling

        result = pd.DataFrame();
        finalResult = pd.DataFrame();
        dd = pd.read_csv("E:\\PY\Economist\\CSVs\\ZakhayerArzi.csv")
        for key in pd.read_html(dd['0'][0]):
            #data1 = pd.read_html(key[1][1], skiprows=0);
            data1 = pd.DataFrame(key)
            #data1 = data1[0].replace(r'_x000D_', '', regex=True)
            #data1 = data1.replace(r' ', '', regex=True)
            data1.rename(columns={data1.columns[0]: "Name"}, inplace=True)
            finalResult = pd.concat([finalResult, data1])

        finalResult = finalResult[['Name','گذشته','واحد']]
        conn = pyodbc.connect('Driver={SQL Server};'
                                  'Server=LOCALHOST;'
                                  'Database=Adib;'
                                  'UID:sa;'
                                  'PWD:Adib4626')
        cursor = conn.cursor()
        print('insert into sql')
        for row in finalResult.itertuples():
            cursor.execute('''INSERT INTO Adib.dbo.ZakhayerArzi([Keshvar] ,[Gozashte],[Vahed])
                                                       VALUES (?,?,?)
                                                         ''',
                               row[1],
                               row[2],
                               row[3])
        conn.commit()

        print("main done")
    except Exception as err:
        print("errorrrrr")
        print(err)
        logger.error(err)
        logger.error(exHandling.ResolveExceptions(err))
#main()