# import time
#
# import jdatetime
# import pandas
# import pyodbc
# import scrapy
# import pandas as pd
# from scrapy.crawler import CrawlerProcess
#
#
# class QuotesSpider(scrapy.Spider):
#     name = "quotes"
#
# 
#     def start_requests(self):
#         conn = pyodbc.connect('Driver={SQL Server};'
#                               'Server=LOCALHOST;'
#                               'Database=Adib;'
#                               'UID:sa;'
#                               'PWD:Adib4626')
#         result = pandas.read_sql_query('select distinct instrumentId from VChartayi', conn)
#         scrapy.Spider.custom_settings = {
#             'CONCURRENT_REQUESTS': 0,
#             'MAX_CONCURRENT_REQUESTS_PER_DOMAIN':1,
#             'RETRY_TIMES': '1',
#             'DOWNLOAD_DELAY': 10,
#             'COOKIES_ENABLED': False
#         }
#         urls = []
#         for key in result.iterrows():
#             urls.append('http://www.tsetmc.com/tsev2/data/ShareHolder.aspx?i=48260%2C' + key[1][0])
#         for url in urls:
#             time.sleep(3)
#             yield scrapy.Request(url=url, callback=self.parse)
#
#     def parse(self, response):
#         table = response.xpath('/html/body').extract()
#         dd = pd.DataFrame.from_dict(table, orient="columns")
#         dd.to_csv("E:\\Moini\\Gourdian\\TSETMCStackHolder" + response.url[-12:] + ".csv", encoding='utf-8-sig')
#         print(dd)
#         finalResult = pd.DataFrame()
#         dd = pd.read_csv("E:\\Moini\\Gourdian\\TSETMCStackHolder" + response.url[-12:] + ".csv")
#         # print('myprint : '+ dd['0'][0])
#
#         conn = pyodbc.connect('Driver={SQL Server};'
#                               'Server=LOCALHOST;'
#                               'Database=Adib;'
#                               'UID:sa;'
#                               'PWD:Adib4626')
#         cursor = conn.cursor()
#
#         cursor.execute('''INSERT INTO Adib.dbo.TseStackHolders(InstrumentId, TseRespons)
#                                                   VALUES (?,?)
#                                                     ''',
#                            response.url[-12:],
#                            dd['0'][0])
#
#         conn.commit()
#
#
# def main():
#     process = CrawlerProcess()
#     process.crawl(QuotesSpider)
#     process.start()
#
#     # conn = pyodbc.connect('Driver={SQL Server};'
#     #                       'Server=LOCALHOST;'
#     #                       'Database=Adib;'
#     #                       'UID:sa;'
#     #                       'PWD:Adib4626')
#     # result = pandas.read_sql_query('select top 10 instrumentId from VChartayi', conn)
#     # urls = [
#     #
#     # ]
#     # for key in result.iterrows():
#     #     urls.append('http://www.tsetmc.com/Loader.aspx?Partree=15131T&c='+key[1][0])
#     # result = pandas.read_sql_query('select distinct instrumentId from VChartayi'+ conn)
#     # finalResult = pd.DataFrame()
#     # dd = pd.read_csv("E:\\Moini\\Gourdian\\TSETMCStackHolder.csv")
#     # for key in pd.read_html(dd['0'][0]):
#     #     #data1 = pd.read_html(key[1][1], skiprows=0);
#     #     data1 = pd.DataFrame(key)
#     # #     #data1 = data1[0].replace(r'_x000D_', '', regex=True)
#     # #     # data1 = data1.replace(r' ', '', regex=True)
#     # #     # data1.rename(columns={data1.columns[1]: "Name"}, inplace=True)
#     #     data1.rename(columns={data1.columns[0]: "RowNum"}, inplace=True)
#     # #     # if len(data1.query('Name == "تاریخ انجام معامله" ')) > 0:
#     # #     #     finalResult = finalResult.append(data1)
#     # #     # if len(data1.columns)>2 and data1.columns[2] == 'پذیره نویسی دولتی':
#     # #     #     bazarBaazFinalResult = data1
#     # #     if data1.columns[1] == 'نماد':
#     #     finalResult = finalResult.append(data1)
#     # #
#     #     finalResult = finalResult.fillna('')
#     # # # finalResult = finalResult[['Name', 'قیمت']]
#     # # # finalResult.to_excel(r"E:\\Moini\\Gourdian\\Bourse" + jdatetime.datetime.now().strftime('%Y_%m_%d') + ".xlsx",
#     # # #                      index=False)
#     #
#     # cursor = conn.cursor()
#     #
#     # for row in finalResult.itertuples():
#     #     print(row[1])
#     #     cursor.execute('''INSERT INTO Adib.dbo.StackHolders(InstrumentId,StackHolderTitle,ShareCount,SharePercentage)
#     #                                           VALUES (?,?,?,?)
#     #                                             ''',
#     #                        'IRO3SDFZ0007',
#     #                        row[1],
#     #                        row[2],
#     #                        row[3]
#     #                        )
#     #
#     # conn.commit()
#     #
#     #
#     # print("main done")
# main()