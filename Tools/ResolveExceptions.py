
import sys, os

def ResolveExceptions(exp: Exception):
    if exp.args[0].__contains__('Invalid column name'):
        return'عدم تطابق ستون ها در اس کیو ال سرور'
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, 'LineNumber : '+str(exc_tb.tb_lineno))
