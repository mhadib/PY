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
    t = pandas.DataFrame(wql_r)
    t['SourceIP'] = ''
    t['SourceWorkStation'] = ''
    t['Date'] = ''
    for key, item in t.iterrows():
        print(key, item)
        t['SourceIP'][key] = t[0][key].wmi_property('Message').value[t[0][key].wmi_property('Message').value.find('Source Network Address')+24:t[0][key].wmi_property('Message').value.find('Source Network Address')+40]
        t['SourceWorkStation'][key] = t[0][key].wmi_property('Message').value[
        t[0][key].wmi_property('Message').value.find('Workstation Name:') + 18:t[0][key].wmi_property(
            'Message').value.find('Workstation Name:') + 35]
        t['Date'][key] = t[0][key].wmi_property('TimeGenerated').value

    t.to_excel('E:\\گزارش ارور ها\\VorudBeServer\\Movafagh'+dateToGetDate.strftime("%Y%m%d")+'.xlsx', index=False)

    wql = ("SELECT * FROM Win32_NTLogEvent WHERE Logfile= 'Security'"
           "AND NOT (Message LIKE '%Account Name:		SYSTEM%') "
           "AND EventCode='4625'  AND TimeWritten>='"+dateToGetDate.strftime("%Y%m%d000000.000000-000")+"'")
    wql_r = wmi_o.query(wql)
    t = pandas.DataFrame(wql_r)
    t['SourceIP'] = ''
    t['SourceWorkStation'] = ''
    t['Date'] = ''

    for key, item in t.iterrows():
        print(key, item)
        t['SourceIP'][key] = t[0][key].wmi_property('Message').value[t[0][key].wmi_property('Message').value.find('Source Network Address')+24:t[0][key].wmi_property('Message').value.find('Source Network Address')+40]
        t['SourceWorkStation'][key] = t[0][key].wmi_property('Message').value[
        t[0][key].wmi_property('Message').value.find('Workstation Name:') + 18:t[0][key].wmi_property(
            'Message').value.find('Workstation Name:') + 35]
        t['Date'][key] = t[0][key].wmi_property('TimeGenerated').value

    pandas.DataFrame(t).to_excel('E:\\گزارش ارور ها\\VorudBeServer\\NaMovafagh'+dateToGetDate.strftime("%Y%m%d")+'.xlsx', index=False)







except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
