import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/EconomistHesabeJari.txt',
    }
    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/country-list/current-account'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/EconomistHesabeJari.txt', level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling

            table = response.xpath('/html/body').extract()
            result = pd.DataFrame()
            finalResult = pd.DataFrame()

            dd = pd.DataFrame.from_dict(table, orient="columns")
            dd.to_csv("E:\\PY\Economist\\CSVs\\HesabeJari.csv", encoding='utf-8-sig')

            main()
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))



def main():
    # process = CrawlerProcess()
    # process.crawl(QuotesSpider)
    # process.start()
    dd = pd.read_csv("E:\\PY\Economist\\CSVs\\HesabeJari.csv", encoding='utf-8-sig')
    # dd.to_csv("E:\\PY\Economist\\CSVs\\HesabeJari.csv", encoding='utf-8-sig')
    HesabeJariHistory = pd.read_excel("E:\\PY\Economist\\HesabeJariHistory.xlsx")
    print(3)
    for key in dd.iterrows():
        print(5)
        # print(key[1][1])
        data1 = pd.read_html(key[1][1], skiprows=0)
        print(6)
        data1 = data1[0].replace(r'_x000D_', '', regex=True)
        # data1 = data1.replace(r' ', '', regex=True)
        data1.rename(columns={data1.columns[0]: "Keshvar"}, inplace=True)
        join = pd.merge(HesabeJariHistory, data1, on='Keshvar', how='left')
        join = join.drop(
            columns=['آخرین ', 'سه ماهه اول 2020', 'سه ماهه دوم2020', 'سه ماهه سوم 2020', 'سه ماهه چهارم 2020 ',
                     'قبلی'], axis=1)
        print(7)
        join = join.fillna(0)
        join = join.sort_values([2020], ascending=False)
        join.to_excel(r"E:\\گزارشات اکسل\\Economist\\HesabeJari.xlsx", index=False)

        print("1");
    print("2")

    # join.to_excel(r"E:\\گزارشات اکسل\\Economist\\HesabeJariGaurdian"+jdatetime.datetime.now().strftime('%Y_%m_%d')+".xlsx", index=False)
    ##for sel in response.xpath('//div/div/html]'):
    ##    print(sel);

    ##t = pd.read_html(table, skiprows=0)[0];
    ##print(t);
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=Adib;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()

    currencyRate = pandas.read_sql_query('SELECT * FROM vtgjuCurrency', conn)
    join['Name2'] = join['واحد'].str[:3].str.lower()
    currencyRate['Name2'] = currencyRate['Name'].str[:3]
    currencyRate = currencyRate.query("Name2 != 'usd'")
    join = pd.merge(join, currencyRate, on='Name2', how='left')
    join = join.fillna(1)
    join['گذشته به ملیون دلار'] = join['گذشته'] * join['Rate']
    join = join.drop(columns=['ردیف', 'Id', 'Name2'], axis=1)
    for row in join.itertuples():
        print(row[1])
        cursor.execute('''INSERT INTO Adib.dbo.KasriHesabeJari(['Keshvar'],[2005],[2010],[2011],[2012],[2013],[2014],[2015],[2016],[2017],[2018],[2019],[2020],['دوره'],['گذشته'],['مرجع'],['واحد'],['Name'],['Rate'],['CurrentDateTsetms'],['گذشته به ملیون دلار'])
                                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                                      ''',
                       row[1],
                       row[2],
                       row[3],
                       row[4],
                       row[5],
                       row[6],
                       row[7],
                       row[8],
                       row[9],
                       row[10],
                       row[11],
                       row[12],
                       row[13],
                       row[14],
                       row[15],
                       row[16],
                       row[17],
                       row[18],
                       row[19],
                       row[20],
                       row[21],
                       )
    conn.commit()
    sqlHistory = pd.read_sql_query('select * from vKasriHesabeJari					', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\vKasriHesabeJari.xlsx', index=False)
    print("End of write to excel")
    # page = response.url.split("/")[-2]
    # filename = f'm2-{page}.txt'
    # with open(filename, 'wb') as f:
    #     f.write(table)
    #     ##f.write(response.body)
    # self.log(f'Saved file {filename}')
# main()