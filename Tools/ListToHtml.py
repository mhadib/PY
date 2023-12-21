from os import listdir
from os.path import isfile, join
from typing import re

import jdatetime
import requests
import logging
import pandas
import requests

import json, ast
import numpy as np
import shutil, os
from datetime import datetime
import time

from datetime import datetime, timedelta

logging.basicConfig(filename='C:/Users/Administrator/PycharmProjects/pythonProject/Log/ListToHtml.txt',
                    level=logging.ERROR,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

try:
    i = 0
    while (i < 100):
        i = i + 1
        gregorian_date = (jdatetime.datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        # jalili_date =  jdatetime.date.fromgregorian(day=19,month=5,year=2017)
        time.strftime("%Y%m%d")
        try:
            onlyfiles = [f for f in listdir("E:\\adib\\report\\" + gregorian_date + "\\") if
                         isfile(join("E:\\adib\\report\\" + gregorian_date + "\\", f))]
        except:
            continue
        history = pandas.DataFrame.from_dict(onlyfiles)

        for key, item in history.iterrows():
            print(item[0])
            newpath = r'E:\\adib\\report\\html\\' + gregorian_date + '\\'
            if not os.path.exists(newpath):
                os.makedirs(newpath)
            sheet_to_df_map = {}
            xls = pandas.ExcelFile(r'E:\\adib\\report\\' + gregorian_date + '\\' + item[0])
            for sheet_name in xls.sheet_names:
                sheet_to_df_map[sheet_name] = xls.parse(sheet_name).replace(np.nan, '', regex=True)
                # sheet_to_df_map[sheet_name].to_html(r'E:\\adib\\html\\' + gregorian_date + '\\' + sheet_name + item[0].replace("xlsx", "") + "html")
                html = sheet_to_df_map[sheet_name].to_html()
                html = "<!DOCTYPE html><head><meta charset=\"UTF-8\"><style>table {" \
                       "table.fixed {table-layout:fixed; width:1900px;}/*Setting the table width is important!*/" \
                       "table.fixed td {overflow:hidden;}/*Hide text outside the cell.*/" \
                       "thead tr th:first-child," \
                       "tbody tr td:first-child {  width: 3em;  min-width: 3em;  max-width: 3em;  word-break: " \
                       "break-all;}" \
                       "thead tr th:last-child,tbody tr td:last-child {  width: 19em;  min-width: 19em;  max-width: " \
                       "19em;  word-break: break-all;}" \
                       "border-collapse: collapse;  width: 100%; min-width: 2000px;}th, td {  padding: " \
                       "8px;}tr:nth-child(even) {background-color: #f2f2f2;}</style><script>"\
                       " function searchSname() {\n" \
                       "    var input, filter, found, table, tr, td, i, j; \n" \
                       "    input = document.getElementById(\"search\"); \n" \
                       "    filter = input.value.toUpperCase(); \n" \
                       "    table = document.getElementsByClassName(\"dataframe\"); \n" \
                       "    tr = document.getElementsByTagName(\"tr\");\n" \
                       "    for (i = 1; i < tr.length; i++) {\n" \
                       "        td = tr[i].getElementsByTagName(\"td\");\n" \
                       "        for (j = 0; j < td.length; j++) {\n" \
                       "            if (td[j].innerHTML.toUpperCase().indexOf(filter) > -1) {\n" \
                       "                found = true;\n" \
                       "            }\n" \
                       "        }\n" \
                       "        if (found) {\n" \
                       "            tr[i].style.display = \"\";\n" \
                       "            found = false;\n" \
                       "        } else {\n" \
                       "            tr[i].style.display = \"none\";\n" \
                       "        }    }}\n"\
                       "</script>"\
                       "</head><html " \
                       "dir=\"rtl\"> " \
                       "<input id='search' onkeyup='searchSname()' type='text' placeholder='جستجو...' >" \
                       + html + "</html> "
                Html_file = open(
                    r'E:\\adib\\report\\html\\' + gregorian_date + '\\' + sheet_name + item[0].replace("xlsx",
                                                                                                       "") + "html",
                    "w", encoding="utf-8")
                Html_file.write(html)
                Html_file.close()

            ##excelHistory = pandas.read_excel(r'E:\\adib\\report\\'+gregorian_date+'\\' + item[0])





except Exception as err14:
    print(err14)
    logger.error(err14)
