import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    custom_settings = {
        'LOG_FILE': 'E:/Moini/Log/EconomistBourse.txt',
    }

    def start_requests(self):
        urls = [
            'https://fa.tradingeconomics.com/stocks'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        ##self.log(f'title: {response.css("title")}"')
        finalResult = pd.DataFrame();
        table = response.xpath('/html/body/form').extract()
        dd = pd.DataFrame.from_dict(table, orient="columns")
        dd.to_csv("E:\\Moini\\Gourdian\\stock.csv", encoding='utf-8-sig')
        dd = pd.read_csv("E:\\Moini\\Gourdian\\stock.csv")
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

        finalResult = finalResult[['Name', 'قیمت']]
        finalResult.to_excel(r"E:\\Moini\\Gourdian\\Bourse" + jdatetime.datetime.now().strftime('%Y_%m_%d') + ".xlsx",
                             index=False)
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=LOCALHOST;'
                              'Database=Adib;'
                              'UID:sa;'
                              'PWD:Adib4626')
        cursor = conn.cursor()
        print('insert into sql')
        for row in finalResult.itertuples():
            cursor.execute('''INSERT INTO Adib.dbo.Bourse([Name] ,[Index])
                                               VALUES (?,?)
                                                 ''',
                           row[1],
                           row[2],
                           )
        conn.commit()
        sqlHistory = pandas.read_sql_query('select * from [dbo].[VBOURSE] with (nolock)', conn)
        sqlHistory.to_excel('E:\\Moini\\Moini\\VBOURSE.xlsx', index=False)

def main():
    result = pd.DataFrame();
    finalResult = pd.DataFrame();
    dd = pd.read_csv("E:\\Moini\\Gourdian\\stock.csv")
    for key in pd.read_html(dd['0'][0]):
        #data1 = pd.read_html(key[1][1], skiprows=0);
        data1 = pd.DataFrame(key)
        #data1 = data1[0].replace(r'_x000D_', '', regex=True)
        data1 = data1.replace(r' ', '', regex=True)
        data1.rename(columns={data1.columns[1]: "Name"}, inplace=True)
        finalResult = pd.concat([finalResult, data1])



    finalResult = finalResult[['Name', 'قیمت']]
    finalResult.to_excel(r"E:\\Moini\\Gourdian\\Bourse" + jdatetime.datetime.now().strftime('%Y_%m_%d') + ".xlsx",
                         index=False)
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=LOCALHOST;'
                          'Database=Adib;'
                          'UID:sa;'
                          'PWD:Adib4626')
    cursor = conn.cursor()
    print('insert into sql')
    for row in finalResult.itertuples():
        cursor.execute('''INSERT INTO Adib.dbo.Bourse([Name] ,[Index])
                                           VALUES (?,?)
                                             ''',
                       row[1],
                       row[2],
                       )
    conn.commit()
    print("main done")
#main()