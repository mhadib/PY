import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/FaraBourse/FarabourseYTM.txt',
    }

    def start_requests(self):
        urls = [
            'https://www.ifb.ir/ytm.aspx'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/گزارشات لاگ ها/FaraBourse/FarabourseYTM.txt', level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling

            table = response.xpath('/html/body').extract()
            dd = pd.DataFrame.from_dict(table, orient="columns")
            dd.to_csv("E:\\PY\\FaraBourse\\CSVs\\FaraBoureseytm.csv", encoding='utf-8-sig')
            dd = pd.read_csv("E:\\PY\\FaraBourse\\CSVs\\FaraBoureseytm.csv")
            print(00000)

            main()
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))
def main():
    result = pd.DataFrame()
    finalResult = pd.DataFrame()
    dd = pd.read_csv("E:\\PY\\FaraBourse\\CSVs\\FaraBoureseytm.csv")
    for key in pd.read_html(dd['0'][0]):
        data1 = pd.DataFrame(key)

        data1.rename(columns={data1.columns[0]: "RowNum"}, inplace=True)
        if data1.columns[1] == 'نماد':
            finalResult = pd.concat([finalResult, data1])

    finalResult = finalResult.fillna('')
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'UID:sa;'
                          'PWD:Adib4626')
    cursor = conn.cursor()
    print('insert into sql')
    for row in finalResult.itertuples():
        print(row[1])
        cursor.execute('''INSERT INTO Adib.dbo.YTMFaraBourse(SymboleFa, Price, LastDate, SarresidDate, YTM, BazdehSalane)
                                           VALUES (?,?,?,?,?,?)
                                             ''',
                       row[2],
                       row[3],
                       row[4],
                       row[5],
                       row[6],
                       row[7],
                       )
        conn.commit()

    sqlHistory = pandas.read_sql_query('select * from VYTMFaraBourse with(nolock)', conn)

    sqlHistory.to_excel('E:\\گزارشات اکسل\\FaraBourse\\VYTMFaraBourse.xlsx', index=False)
    print("end of write to excel ")
    conn.close()


    print("main done")
# main()