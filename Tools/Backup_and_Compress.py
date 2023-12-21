import zipfile
import glob
import os, shutil
from weakref import finalize
import os
import logging
import jdatetime
from datetime import datetime, timedelta





logging.basicConfig(filename='E:/گزارشات لاگ ها/Tools/Backup_and_Compress.txt', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

try:



    todir = 'E://PY//Tools//DataBase_Backup'

    logger.log(level=logging.DEBUG, msg="start to write read sql")
    print("start to backup and compress ")

    for filename in os.listdir(todir):
        file_path = os.path.join(todir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

    backupDir = 'E://Backups'
    for filename in os.listdir(backupDir):
        file_path = os.path.join(backupDir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

    list_of_files = glob.glob('E://DataBase_BackUp//*')  # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    shutil.copy(latest_file, todir)

    # extension = input('Input file extension: ')
    extension = ""


    Zippy = zipfile.ZipFile("E://Backups//Backup"+jdatetime.datetime.now().strftime('%Y_%m_%d')+".zip", 'w')

    for folder, subfolders, file in os.walk("E:\\PY\\Economist"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)

    for folder, subfolders, file in os.walk("E:\\PY\\BourseView"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)

    for folder, subfolders, file in os.walk("E:\\PY\\Tools"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc") and not x.endswith("trc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)

    for folder, subfolders, file in os.walk("E:\\BatFiles"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)

    for folder, subfolders, file in os.walk("E:\\PY\\FaraBourse"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)

    for folder, subfolders, file in os.walk("E:\\PY\\TJGU"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)

    for folder, subfolders, file in os.walk("E:\\PY\\TSETMC"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)
    for folder, subfolders, file in os.walk("E:\\چه گزارشاتی ساخته نشده به علاوه کد های فراخوانی و توضیحات مازاد بر کد"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)
    for folder, subfolders, file in os.walk("E:\\گزارش ارور ها"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)
    for folder, subfolders, file in os.walk("E:\گزارشات اکسل"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)
    for folder, subfolders, file in os.walk("E:\گزارشات لاگ ها"):
        for subfolders in subfolders:
            path = folder + subfolders
        for x in file:
            if not x.endswith("csv") and not x.endswith("pyc"):
                filepath = folder + "\\" + x
                print(filepath)
                Zippy.write(filepath, compress_type=zipfile.ZIP_DEFLATED)
    Zippy.close()


except Exception as err:
    print("errorrrrr")
    print(err)
    logger.error(err)
