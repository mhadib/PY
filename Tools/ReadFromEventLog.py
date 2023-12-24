
from weakref import finalize
import pandas
import logging
import pyodbc
import wmi


logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/SQLChangeHistory.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:

    rval = 0  # Default: Check passes.

    # Initialize WMI objects and query.
    wmi_o = wmi.WMI('.')
    wql = ("SELECT * FROM Win32_NTLogEvent WHERE Logfile="
           "'Security' AND EventCode='4625' AND TimeWritten>='20231224013749.000000-000' ")

    # Query WMI object.
    wql_r = wmi_o.query(wql)

    pandas.DataFrame(wql_r).to_excel('E:\\گزارش ارور ها\\EventLogs.xlsx', index=False)


except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
