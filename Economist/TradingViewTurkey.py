import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/BourseViewTurkey.txt',
    }

    def start_requests(self):
        urls = [
            'https://www.tradingview.com/markets/stocks-turkey/market-movers-highest-net-income/'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'download_timeout': 60})

    def parse(self, response):
        ##self.log(f'title: {response.css("title")}"')
        finalResult = pd.DataFrame()
        print('respons:')
        print(response)
        table = response.xpath('//*[@id="js-category-content"]/div[2]/div/div[4]/div[2]').extract()
        print('table:')
        print(table)
        print(2)
        dd = pd.DataFrame.from_dict(table, orient="columns")
        print(dd)
        dd.to_csv("E:\\PY\Economist\\CSVs\\BourseViewTurkey.csv", encoding='utf-8-sig')
        main()
def main():
    try:
        import logging
        logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/BourseViewTurkey.txt', level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger = logging.getLogger(__name__)
        import sys
        import os
        path = os.path.abspath("E:\\PY\\Tools")
        sys.path.append(path)
        import ResolveExceptions as exHandling

        result = pd.DataFrame()
        finalResult = pd.DataFrame()
        dd = pd.read_csv("E:\\PY\Economist\\CSVs\\BourseViewTurkey.csv")
        first = True
        for key in pd.read_html(dd['0'][0].replace("<a", "<td").replace("</a>", "</td>")):
            if first != True:
                continue
            #data1 = pd.read_html(key[1][1], skiprows=0);
            data1 = pd.DataFrame(key)
            #data1 = data1[0].replace(r'_x000D_', '', regex=True)
            #data1 = data1.replace(r' ', '', regex=True)
            # data1.rename(columns={data1.columns[0]: "Name"}, inplace=True)
            # data1.rename(columns={data1.columns[1]: "FullName"}, inplace=True)
            # data1.rename(columns={data1.columns[4]: "IndexChange"}, inplace=True)
            # finalResult = pd.concat([finalResult, data1])
            # first = False
        data1.columns = data1.columns.str.replace(' ', '')
        data1.columns = data1.columns.str.replace('%', '')
        cols = data1.columns[0:13]
        data1 = data1.drop('Symbol', axis=1)
        data1 = data1.drop('AnalystRating', axis=1)
        # data1 = data1.drop('Relative Volume 1D', 1)

        # cols = ['Symbol', 'Net income(FY)', 'Price', 'Change % 1D', 'Volume 1D',
        #         'Market cap', 'P/E', 'EPS diluted(TTM)',
        #         'EPS diluted growth %(TTM YoY)', 'Dividend yield %(TTM)', 'Sector',
        #         'Analyst Rating']
        data1.columns = cols
        # finalResult = data1[['Name','FullName','قیمت','IndexChange','روز','سال','تاریخ']]
        # finalResult = finalResult.replace(r'%', '', regex=True)
        data1 = data1.fillna(0)
        conn = pyodbc.connect('Driver={SQL Server};'
                                  'Server=LOCALHOST;'
                                  'Database=Adib;'
                                  'UID:sa;'
                                  'PWD:Adib4626')
        # data1.to_sql("BoursViewTurkey", conn, if_exists='append' )
        cursor = conn.cursor()
        print('insert into sql')
        for row in data1.itertuples():
            print(row)
            cursor.execute('''INSERT INTO Adib.dbo.BoursViewTurkey(Symbol , [Net income(FY)] , [Price] , [Change % 1D] ,
     [Volume 1D], [Market cap], [P/E], [EPS(TTM)], [Sector] )
                                                       VALUES (?,?,?,?,?,?,?, ?, ?)
                                                         ''',

                           row.Symbol,
                           row.NetincomeFY,
                           row.Price,
                           row.Change,
                           row.Volume,
                           row.Marketcap,
                           row[8],
                           row[9],
                           row.Sector,
                           )
        conn.commit()

        sqlHistory = pandas.read_sql_query('select * from [dbo].[VBoursViewTurkey]', conn)
        sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\BourseViewTurkey.xlsx', index=False)


        print("main done")
    except Exception as err:
        print("errorrrrr")
        print(err)
        logger.error(err)
        logger.error(exHandling.ResolveExceptions(err))

# main()