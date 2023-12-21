import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/گزارشات لاگ ها/FaraBourse/FarabourseAmareOragh.txt',
    }

    def start_requests(self):
        urls = [
            'https://www.ifb.ir/datareporter/DailySukukTrades.aspx'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            import logging
            logging.basicConfig(filename='E:/Economist/Logs/FarabourseAmareOragh.txt', level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s %(message)s')
            logger = logging.getLogger(__name__)
            import sys
            import os
            path = os.path.abspath("E:\\PY\\Tools")
            sys.path.append(path)
            import ResolveExceptions as exHandling

            ##self.log(f'title: {response.css("title")}"')
            finalResult = pd.DataFrame()
            table = response.xpath('/html/body').extract()
            dd = pd.DataFrame.from_dict(table, orient="columns")
            dd.to_csv("E:\\PY\\FaraBourse\\CSVs\\FaraBoureseDailySukukTrades.csv", encoding='utf-8-sig')
            dd = pd.read_csv("E:\\PY\\FaraBourse\\CSVs\\FaraBoureseDailySukukTrades.csv")
            print(00000)
            for key in pd.read_html(dd['0'][0]):
                # data1 = pd.read_html(key[1][1], skiprows=0);
                data1 = pd.DataFrame(key)
                # data1 = data1[0].replace(r'_x000D_', '', regex=True)
                data1 = data1.replace(r' ', '', regex=True)
                data1.rename(columns={data1.columns[1]: "Name"}, inplace=True)
                print(11111111)
                print(data1)

                finalResult = pd.concat([finalResult, data1])


            main()
        except Exception as err:
            print("errorrrrr")
            print(err)
            logger.error(err)
            logger.error(exHandling.ResolveExceptions(err))
def main():
    result = pd.DataFrame()
    finalResult = pd.DataFrame()
    bazarBaazFinalResult = pd.DataFrame()
    dd = pd.read_csv("E:\\PY\\FaraBourse\\CSVs\\FaraBoureseDailySukukTrades.csv")
    for key in pd.read_html(dd['0'][0]):
        #data1 = pd.read_html(key[1][1], skiprows=0);
        data1 = pd.DataFrame(key)
        #data1 = data1[0].replace(r'_x000D_', '', regex=True)
        # data1 = data1.replace(r' ', '', regex=True)
        data1.rename(columns={data1.columns[1]: "Name"}, inplace=True)
        data1.rename(columns={data1.columns[0]: "RowNum"}, inplace=True)
        if len(data1.query('Name == "تاریخ انجام معامله" ')) > 0:
            finalResult = pd.concat([finalResult, data1])
        if len(data1.columns)>2 and data1.columns[2] == 'پذیره نویسی دولتی':
            bazarBaazFinalResult = data1


    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'UID:sa;'
                          'PWD:Adib4626')
    cursor = conn.cursor()
    # print('insert into sql')
    sId = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    for row in finalResult.itertuples():
        if row[1] in sId:
            print(row[1])
            cursor.execute('''INSERT INTO Adib.dbo.AmareOraghFaraBourse(TradeDate , KharidDolat , KharidBankMarkazi , KharidSandoghHa , KharidBankHa , KharidSayerAshkhas ,ForushDolat , ForushBankMarkazi , ForushSandoghHa , ForushBankHa , ForushSayerAshkhas )
                                           VALUES (?,?,?,?,?,?,?,?,?,?,?)
                                             ''',
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
                       )
        conn.commit()

    for row in bazarBaazFinalResult.itertuples():
        if row[1] in sId:
            print(row[1])
            cursor.execute('''INSERT INTO Adib.dbo.AmareBazareBaazFaraBourse(TradeDate, PazireNevisiDolati, AmaliatBazarBaaz, Saayer)
                                           VALUES (?,?,?,?)
                                             ''',
                           row[2],
                           row[3],
                           row[4],
                           row[5]
                           )
        conn.commit()
    sqlHistory = pandas.read_sql_query('select * from VAmareBazareBaazFaraBourse with(nolock)', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\FaraBourse\\VAmareBazareBaazFaraBourse.xlsx', index=False)

    sqlHistory = pandas.read_sql_query('select * from VAmaliatBazarBazDorei with(nolock)', conn)
    sqlHistory.to_excel('E:\\گزارشات اکسل\\FaraBourse\\عملیات بازار باز دوره ای.xlsx', index=False)


    cursor.execute('exec PAmareOraghFaraBoursePreProcess')
    print("end of write to excel ")
    conn.close()

    print("main done")
# main()