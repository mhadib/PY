import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'DOWNLOAD_DELAY': 10000
    }
    allowed_domains = ['tgju.org']


    def start_requests(self):
        urls = ['https://www.tgju.org/diff']
        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "DNT": "1",
            "Host": "www.tgju.org",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cookie": "",
            "Sec-GPC": "1",
            "TE": "trailers",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36",
            "X-Airbnb-API-Key": "API_KEY",
            "X-Airbnb-GraphQL-Platform": "web",
            "X-Airbnb-GraphQL-Platform-Client": "minimalist-niobe",
            "X-Airbnb-Supports-Airlock-V2": "true",
            "X-CSRF-Token": "null",
            "X-CSRF-Without-Token": "1",
            "X-KL-Ajax-Request": "Ajax_Request",
            "X-Niobe-Short-Circuited": "true"
            }
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=headers, meta={'download_timeout': 400})

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/tgju.txt', level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling

            # table = response.xpath('//*[@id="main"]/div[3]/div/div').extract()
            print(response)
            table = response.xpath('/html').extract()


            result = pd.DataFrame()
            finalResult = pd.DataFrame()

            dd = pd.DataFrame.from_dict(table, orient="columns")
            dd.to_csv("E:\\PY\Economist\\CSVs\\diff-table.csv", encoding='utf-8-sig')

            dd = pd.read_csv("E:\\PY\Economist\\CSVs\\diff-table.csv")
            for key in dd.iterrows():
                data1 = pd.read_html(key[1][1], skiprows=0)
                data1 = data1[0].replace(r'_x000D_', '', regex=True)
                # data1 = data1.replace(r' ', '', regex=True)
                data1 = data1[['برابری ارزها', 'هر دلار']]
                conn = pyodbc.connect('Driver={SQL Server};'
                                      'Server=LOCALHOST;'
                                      'Database=Adib;'
                                      'UID:sa;'
                                      'PWD:Adib4626')
                cursor = conn.cursor()
                print('insert into sql')
                for row in data1.itertuples():
                    cursor.execute('''INSERT INTO Adib.dbo.tgjuCurrency([Name] ,[Rate])
                                                               VALUES (?,?)
                                                                 ''',
                                   row[1],
                                   row[2]
                                   )
                conn.commit()
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))

def main():

    dd = pd.read_csv("E:\\PY\Economist\\CSVs\\diff-table.csv")
    for key in dd.iterrows():
        data1 = pd.read_html(key[1][1], skiprows=0)
        data1 = data1[0].replace(r'_x000D_', '', regex=True)
        # data1 = data1.replace(r' ', '', regex=True)
        data1 = data1[['برابری ارزها', 'هر دلار']]
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LOCALHOST;'
                              'Database=Adib;'
                              'UID:sa;'
                              'PWD:Adib4626')
        cursor = conn.cursor()
        print('insert into sql')
        for row in data1.itertuples():
            cursor.execute('''INSERT INTO Adib.dbo.tgjuCurrency([Name] ,[Rate])
                                                       VALUES (?,?)
                                                         ''',
                           row[1],
                           row[2]
                           )
        conn.commit()

        # join.to_excel(r"E:\\PY\Economist\\HesabeJari.xlsx", index=False)

        print("1")
    print("2")
# main()