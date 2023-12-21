import time
import jdatetime
# import logging
import pandas
import pyodbc
import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    # configure_logging(install_root_handler=True)
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/StackHolders/StackHoldersChanges.txt',
    }


    def start_requests(self):
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LOCALHOST;'
                              'Database=Adib;'
                              'UID:sa;'
                              'PWD:Adib4626')


        self.logger.debug('this is test')
        urls = []
        # for key in result.iterrows():
        urls.append('http://old.tsetmc.com/Loader.aspx?ParTree=15131I&t=1')

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'download_timeout': 1000})

        self.logger.info('this is test')


    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/StackHolders/StackHoldersChanges.txt', level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling
            table = response.xpath('/html/body').extract()
            dd = pd.DataFrame.from_dict(table, orient="columns")
            dd.to_csv("E:\\PY\\TSETMC\\CSVs\\TSETMCStackHolderChanges.csv", encoding='utf-8-sig')
            print(dd)
            finalResult = pd.DataFrame()
            dd = pd.read_csv("E:\\PY\\TSETMC\\CSVs\\TSETMCStackHolderChanges.csv")
            dd['0'][0] = dd['0'][0].replace("li", "td")
            dd['0'][0] = dd['0'][0].replace("div", "td")

            for v in pd.read_html(dd['0'][0]):
                d = pd.DataFrame(v)
                print(d)
            for key in pd.read_html(dd['0'][0]):
                data1 = pd.DataFrame(key)

                data1.rename(columns={data1.columns[0]: "RowNum"}, inplace=True)
                finalResult = pandas.concat([finalResult, data1])
                finalResult = finalResult.fillna('')
            conn = pyodbc.connect('Driver={SQL Server};'
                                  'Server=LOCALHOST;'
                                  'Database=Adib;'
                                  'UID:sa;'
                                  'PWD:Adib4626')
            cursor = conn.cursor()
            for row in finalResult.itertuples():
                if(row[3].isnumeric()!=True):
                    StackHolderTitle = row[2]
                    CompanyName = row[3]
                    ShareCount = row[4]
                    ChangeCount = row[5]
                else:
                    CompanyName = row[2]
                    ShareCount = row[3]
                    ChangeCount = row[4]
                cursor.execute('''INSERT INTO Adib.dbo.StackHolders(CompanyTitle,StackHolderTitle,ShareCount,ChangeCount, ChangeDateFa)
                                                      VALUES (?,?,?,?,?)
                                                        ''',
                               CompanyName,
                               StackHolderTitle,
                               ShareCount,
                               ChangeCount,
                               finalResult.columns[1]
                               )

            conn.commit()
            cursor.execute('insert into StackHoldersTriger select getdate()')
            conn.commit()

            sqlHistory = pandas.read_sql_query('select * from StackHoldersForLastDay1Mounth6Mounth with(nolock)', conn)
            sqlHistory.to_excel('E:\\گزارشات اکسل\\TSETMC\\StackHoldersForLastDay1Mounth6Mounth.xlsx', index=False)


            print("end of write to excel ")
            conn.close()
            self.logger.info("That's Ok")
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))

def main():
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()

main()