import jdatetime
import pandas
import pyodbc
import scrapy
import pandas as pd


def main():
    try:
        import logging
        logging.basicConfig(filename='E:/گزارشات لاگ ها/Economist/StartTrigger.txt', level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger = logging.getLogger(__name__)
        import sys
        import os
        path = os.path.abspath("E:\\PY\\Tools")
        sys.path.append(path)
        import ResolveExceptions as exHandling
        print("Start Trigger")
        conn = pyodbc.connect('Driver={SQL Server};'
                                  'Server=LOCALHOST;'
                                  'Database=Adib;'
                                  'UID:sa;'
                                  'PWD:Adib4626')
        # data1.to_sql("BoursViewUSA", conn, if_exists='append' )
        cursor = conn.cursor()
        cursor.execute('insert into StartTriger_Economist select getdate()')
        cursor.close()
        conn.commit()
        conn.close()


        print("Trigger Done")
    except Exception as err:
        print("errorrrrr")
        print(err)
        logger.error(err)
        logger.error(exHandling.ResolveExceptions(err))

# main()

