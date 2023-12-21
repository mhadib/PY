import logging
import subprocess, sys



logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/TaskScheduleHistory.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:

    p = subprocess.Popen(["powershell.exe",
                          "C:\\Users\\USER\\Desktop\\helloworld.ps1"],
                         stdout=sys.stdout)
    p.communicate()

except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
