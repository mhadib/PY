import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/Economist/Economist-m2.txt',
    }

    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/country-list/money-supply-m2'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/Economist-m2.txt', level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling
            ##self.log(f'title: {response.css("title")}"')
            table = response.xpath('/html/body').extract()
            result = pd.DataFrame()
            finalResult = pd.DataFrame();
            dd = pd.DataFrame.from_dict(table, orient="columns")
            #dd.to_csv("C:\\Users\\MLM\\PycharmProjects\\pythonProject\\t.csv", encoding='utf-8-sig')
            for key in dd.iterrows():
                data1 = pd.read_html(key[1][0], skiprows=0);
                data1 = data1[0].replace(r'_x000D_', '', regex=True)
                #data1 = data1.replace(r' ', '', regex=True)
                data1.rename(columns={data1.columns[0]: "Keshvar"}, inplace=True)
                if len(data1.query('Keshvar == "ژاپن" ')) > 0:
                    result = data1.query('Keshvar == "ژاپن" ')
                    result['2005'] = 709000
                    result['2019'] = 1020000
                    finalResult = pandas.concat([finalResult, result])

                if len(data1.query('Keshvar == "سوئیس" ')) > 0:
                    result = data1.query('Keshvar == "سوئیس" ')
                    result['2005'] = 500000
                    result['2019'] = 1016000
                    finalResult = pandas.concat([finalResult, result])

                if len(data1.query('Keshvar == "انگلستان" ')) > 0:
                    result = data1.query('Keshvar == "انگلستان" ')
                    result['2005'] = 1220000
                    result['2019'] = 2465000
                    finalResult = pandas.concat([finalResult, result])

                if len(data1.query('Keshvar == "منطقه یورو" ')) > 0:
                    result = data1.query('Keshvar == "منطقه یورو" ')
                    result['2005'] = 6000000
                    result['2019'] = 12380000
                    finalResult = pandas.concat([finalResult, result])

                if len(data1.query('Keshvar == "ایالات متحده" ')) > 0:
                    result = data1.query('Keshvar == "ایالات متحده" ')
                    result['2005'] = 6650
                    result['2019'] = 15300
                    finalResult = pandas.concat([finalResult, result])

                if len(data1.query('Keshvar == "کانادا" ')) > 0:
                    result = data1.query('Keshvar == "کانادا" ')
                    result['2005'] = 673000
                    result['2019'] = 1797000
                    finalResult = pandas.concat([finalResult, result])

                if len(data1.query('Keshvar == "کره جنوبی" ')) > 0:
                    result = data1.query('Keshvar == "کره جنوبی" ')
                    result['2005'] = 1020000
                    result['2019'] = 2925000
                    finalResult = pandas.concat([finalResult, result])

                if len(data1.query('Keshvar == "چین" ')) > 0:
                    result = data1.query('Keshvar == "چین" ')
                    result['2005'] = 29800
                    result['2019'] = 198500
                    finalResult = pandas.concat([finalResult, result])

            finalResult['رشد نقدینگی نسبت به 2005'] = (finalResult['گذشته']-finalResult['2005'])/finalResult['2005']
            finalResult['رشد نقدینگی نسبت به 2019'] = (finalResult['گذشته'] - finalResult['2019']) / finalResult['2019']

            finalResult = pandas.concat([finalResult, data1])
            finalResult = finalResult.fillna(0)

            conn = pyodbc.connect('Driver={SQL Server};'
                                  'Server=LOCALHOST;'
                                  'Database=Adib;'
                                  'UID:sa;'
                                  'PWD:Adib4626')
            cursor = conn.cursor()
            print('insert into sql')
            for row in finalResult.itertuples():
                cursor.execute('''INSERT INTO Adib.dbo.Naghdinegi([Keshvar] ,['گذشته'] ,['قبلی'],['مرجع'] ,['واحد'] ,['2005'] ,['2019'] ,['رشد نقدینگی نسبت به 2005'] ,['رشد نقدینگی نسبت به 2019'])
                                      VALUES (?,?,?,?,?,?,?,?,?)
                                        ''',
                               row[1],
                               row[2],
                               row[3],
                               row[4],
                               row[5],
                               row[6],
                               row[7],
                               row[8],
                               row[9]
                               )
            conn.commit()
            cursor.execute('''delete Naghdinegi where Keshvar in (select distinct Keshvar from Naghdinegi b where ['2019'] > 0) and ['2019'] = 0''')
            conn.commit()
            print('end of insert into sql')
            sqlHistory = pandas.read_sql_query('''select * from [dbo].VNaghdinegi with (nolock) order by ['2005'] desc''', conn)
            sqlHistory.to_excel('E:\\گزارشات اکسل\\Economist\\VNaghdinegi.xlsx', index=False)

            print('end of write to excel')
            ##for sel in response.xpath('//div/div/html]'):
            ##    print(sel);

            ##t = pd.read_html(table, skiprows=0)[0];
            ##print(t);
            page = response.url.split("/")[-2]
            filename = f'm2-{page}.txt'
            with open(filename, 'wb') as f:
                f.write(table)
                ##f.write(response.body)
            self.log(f'Saved file {filename}')
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))

def main():
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()

# main()