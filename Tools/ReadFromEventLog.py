from datetime import datetime, timedelta
from weakref import finalize
import pandas
import logging
import pyodbc
import wmi


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/ReadFromEventLog.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:

    rval = 0  # Default: Check passes.
    dateToGetDate = datetime.today() - timedelta(days=int(1))
    dateToGetDate.strftime("%Y%m%d")
    # Initialize WMI objects and query.
    wmi_o = wmi.WMI('.')

    wql = ("SELECT * FROM Win32_NTLogEvent WHERE Logfile="
           "'Security' AND EventCode='4624' "
           "AND NOT (Message LIKE '%Account Name:		SYSTEM%') "
           "AND TimeWritten>='" + dateToGetDate.strftime(
        "%Y%m%d000000.000000-000") + "'")
    # TargetUserName = 'SYSTEM' AND
    wql_r = wmi_o.query(wql)
    pandas.DataFrame(wql_r).to_excel('E:\\گزارش ارور ها\\VorudBeServer\\Movafagh'+dateToGetDate.strftime("%Y%m%d")+'.xlsx', index=False)

    wql = ("SELECT * FROM Win32_NTLogEvent WHERE Logfile= 'Security'"
           "AND NOT (Message LIKE '%Account Name:		SYSTEM%') "
           "AND EventCode='4625'  AND TimeWritten>='"+dateToGetDate.strftime("%Y%m%d000000.000000-000")+"'")
    wql_r = wmi_o.query(wql)
    pandas.DataFrame(wql_r).to_excel('E:\\گزارش ارور ها\\VorudBeServer\\NaMovafagh'+dateToGetDate.strftime("%Y%m%d")+'.xlsx', index=False)







except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
