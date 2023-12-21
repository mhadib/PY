import requests
import pandas as pd
import jdatetime
from datetime import datetime, timedelta


def post(url,data, city):
    print("Checking session...")
    r1 = requests.post('http://hmi.mrud.ir/Default/Index?ID=1&TreatyType=1')
    r = requests.post(url, data, cookies=r1.cookies)
    data96 = {"strDate": "1396/01/01",
              "strDate1": "1397/01/01", "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": city}
    r96 = requests.post(url, data96, cookies=r1.cookies)

    data97 = {"strDate": "1397/01/01",
              "strDate1": "1398/01/01", "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": city}
    r97 = requests.post(url, data97, cookies=r1.cookies)

    data98 = {"strDate": "1398/01/01",
              "strDate1": "1399/01/01", "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": city}
    r98 = requests.post(url, data98, cookies=r1.cookies)

    data99 = {"strDate": "1399/01/01",
              "strDate1": "1400/01/01", "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": city}
    r99 = requests.post(url, data99, cookies=r1.cookies)

    if r.ok:
        urlData96 = r96.content.decode('utf-8')
        dataFrame96 = pd.read_html(urlData96, skiprows=0)[0]
        d96 = dataFrame96.groupby('محله').count()['منطقه'].to_frame()
        dataFrame96['ZirBana96'] = dataFrame96['مساحت'].str.replace('متر مربع', '')
        dataFrame96['ZirBana96'] = pd.to_numeric(dataFrame96['ZirBana96'])
        dataFrame96['PricePerMeter96'] = dataFrame96['قیمت هر متر مربع'].str.replace(',', '').str.replace('تومان', '')
        dataFrame96['PricePerMeter96'] = pd.to_numeric(dataFrame96['PricePerMeter96'])
        d96['PricePerMeter96'] = dataFrame96.groupby('محله')['PricePerMeter96'].mean().to_frame();
        d96['ZirBana96'] = dataFrame96.groupby('محله')['ZirBana96'].mean().to_frame();
        d96['AveragePriceLastMounth96'] = d96['PricePerMeter96'] * d96['ZirBana96']
        d96['96'] = d96['منطقه']/12
        d96 = d96.drop(columns="منطقه")


        urlData97 = r97.content.decode('utf-8')
        dataFrame97 = pd.read_html(urlData97, skiprows=0)[0]
        d97 = dataFrame97.groupby('محله').count()['منطقه'].to_frame()
        dataFrame97['ZirBana97'] = dataFrame97['مساحت'].str.replace('متر مربع', '')
        dataFrame97['ZirBana97'] = pd.to_numeric(dataFrame97['ZirBana97'])
        dataFrame97['PricePerMeter97'] = dataFrame97['قیمت هر متر مربع'].str.replace(',', '').str.replace('تومان', '')
        dataFrame97['PricePerMeter97'] = pd.to_numeric(dataFrame97['PricePerMeter97'])
        d97['PricePerMeter97'] = dataFrame97.groupby('محله')['PricePerMeter97'].mean().to_frame()
        d97['ZirBana97'] = dataFrame97.groupby('محله')['ZirBana97'].mean().to_frame()
        d97['AveragePriceLastMounth97'] = d97['PricePerMeter97'] * d97['ZirBana97']
        d97['97'] = d97['منطقه']/12
        d97 = d97.drop(columns="منطقه")

        urlData98 = r98.content.decode('utf-8')
        dataFrame98 = pd.read_html(urlData98, skiprows=0)[0]
        d98 = dataFrame98.groupby('محله').count()['منطقه'].to_frame()
        dataFrame98['ZirBana98'] = dataFrame98['مساحت'].str.replace('متر مربع', '')
        dataFrame98['ZirBana98'] = pd.to_numeric(dataFrame98['ZirBana98'])
        dataFrame98['PricePerMeter98'] = dataFrame98['قیمت هر متر مربع'].str.replace(',', '').str.replace('تومان', '')
        dataFrame98['PricePerMeter98'] = pd.to_numeric(dataFrame98['PricePerMeter98'])
        d98['PricePerMeter98'] = dataFrame98.groupby('محله')['PricePerMeter98'].mean().to_frame()
        d98['ZirBana98'] = dataFrame98.groupby('محله')['ZirBana98'].mean().to_frame()
        d98['AveragePriceLastMounth98'] = d98['PricePerMeter98'] * d98['ZirBana98']

        d98['98'] = d98['منطقه']/12
        d98 = d98.drop(columns="منطقه")

        urlData99 = r99.content.decode('utf-8')
        dataFrame99 = pd.read_html(urlData99, skiprows=0)[0]
        d99 = dataFrame99.groupby('محله').count()['منطقه'].to_frame()

        dataFrame99['ZirBana99'] = dataFrame99['مساحت'].str.replace('متر مربع', '')
        dataFrame99['ZirBana99'] = pd.to_numeric(dataFrame99['ZirBana99'])
        dataFrame99['PricePerMeter99'] = dataFrame99['قیمت هر متر مربع'].str.replace(',', '').str.replace('تومان', '')
        dataFrame99['PricePerMeter99'] = pd.to_numeric(dataFrame99['PricePerMeter99'])
        d99['PricePerMeter99'] = dataFrame99.groupby('محله')['PricePerMeter99'].mean().to_frame()
        d99['ZirBana99'] = dataFrame99.groupby('محله')['ZirBana99'].mean().to_frame()
        d99['AveragePriceLastMounth99'] = d99['PricePerMeter99'] * d99['ZirBana99']
        d99['99'] = d99['منطقه']/12
        d99 = d99.drop(columns="منطقه")

        urlData = r.content.decode('utf-8')
        data1 = pd.read_html(urlData, skiprows=0)[0]

        data1['ZeroToOne'] = data1['سن ساختمان'].between(0, 1)
        data1['OneToTwo'] = data1['سن ساختمان'].between(1, 2)
        data1['TwoToThree'] = data1['سن ساختمان'].between(2, 3)
        data1['ThreeToFour'] = data1['سن ساختمان'].between(3, 4)
        data1['FourToFive'] = data1['سن ساختمان'].between(4, 5)
        data1['FiveToTen'] = data1['سن ساختمان'].between(5, 10)
        data1['BiggerThanTen'] = data1['سن ساختمان'].between(10, 10000000)
        data1['PricePerMeter'] = data1['قیمت هر متر مربع'].str.replace(',', '').str.replace('تومان', '')
        data1['ZirBana'] = data1['مساحت'].str.replace('متر مربع', '')
        data1['PricePerMeter'] = pd.to_numeric(data1['PricePerMeter'])
        data1['ZirBana'] = pd.to_numeric(data1['ZirBana'])
        ZeroToOne = data1.query('ZeroToOne == True').groupby('محله').count()['ZeroToOne'].to_frame();
        OneToTwo = data1.query('OneToTwo == True').groupby('محله').count()['OneToTwo'].to_frame();
        TwoToThree = data1.query('TwoToThree == True').groupby('محله').count()['TwoToThree'].to_frame();
        ThreeToFour = data1.query('ThreeToFour == True').groupby('محله').count()['ThreeToFour'].to_frame();
        FourToFive = data1.query('FourToFive == True').groupby('محله').count()['FourToFive'].to_frame();
        FiveToTen = data1.query('FiveToTen == True').groupby('محله').count()['FiveToTen'].to_frame();
        BiggerThanTen = data1.query('BiggerThanTen == True').groupby('محله').count()['BiggerThanTen'].to_frame();
        PricePerMeter = data1.groupby('محله')['PricePerMeter'].mean().to_frame();
        ZirBana =  data1.groupby('محله')['ZirBana'].mean().to_frame();

        result = pd.concat([ZeroToOne, OneToTwo, TwoToThree, ThreeToFour, FourToFive, FiveToTen, BiggerThanTen, PricePerMeter, ZirBana, d96, d97, d98, d99], axis=1)
        result['AveragePriceLastMounth'] = result['PricePerMeter']*result['ZirBana']
        result = result.fillna(0)
        result["sum"] = result["96"] + result["97"] + result["98"] + result["99"] + result["ZeroToOne"]+ result["OneToTwo"]+ result["TwoToThree"]+ result["ThreeToFour"]+ result["FourToFive"]+ result["FiveToTen"]+ result["BiggerThanTen"]
        result["SumLastMounth"] = result["ZeroToOne"] + result[
            "OneToTwo"] + result["TwoToThree"] + result["ThreeToFour"] + result["FourToFive"] + result["FiveToTen"] + \
                        result["BiggerThanTen"]

        result["97Increase"] = (result["PricePerMeter97"]-result["PricePerMeter96"])/result["PricePerMeter96"]
        result["98Increase"] = (result["PricePerMeter98"] - result["PricePerMeter97"]) / result["PricePerMeter97"]
        result["99Increase"] = (result["PricePerMeter99"] - result["PricePerMeter98"]) / result["PricePerMeter98"]
        result["400Increase"] = (result["PricePerMeter"] - result["PricePerMeter99"]) / result["PricePerMeter99"]
        result["LastMounthIncreaseFrom96"] = (result["PricePerMeter"] - result["PricePerMeter96"]) / result["PricePerMeter96"]

        result = result.sort_values(by=['SumLastMounth'], ascending=False)
        result.columns = ['طول عمر بنا کمتر از یکسال','طول عمر بنا بین یک تا دو سال','طول عمر بنا بین دو تا سه سال',
                          'طول عمر بنا بین سه تا چهار سال','طول عمر بنا بین چهار تا پنج سال','طول عمر بنا بین پنچ تا ده سال','طول عمر بنا بیشتر از ده سال',
                          'میانگین قیمت هر متر خانه برای یک ماه اخیر',
                          'متوسط مساحت در یک ماه اخیر',

                          'میانگین قیمت هر متر خانه برای سال 96',
                          'متوسط مساحت در سال 96',
                          'متوسط قیمت مسکن در سال 96',
                          'متوسط معاملات مسکن در یک ماه سال 96',

                          'میانگین قیمت هر متر خانه برای سال 97',
                          'متوسط مساحت در سال 97',
                          'متوسط قیمت مسکن در سال 97',
            'متوسط معاملات مسکن در یک ماه سال 97',

                          'میانگین قیمت هر متر خانه برای سال 98',
                          'متوسط مساحت در سال 98',
                          'متوسط قیمت مسکن در سال 98',
            'متوسط معاملات مسکن در یک ماه سال 98',

                          'میانگین قیمت هر متر خانه برای سال 99',
                          'متوسط مساحت در سال 99',
                          'متوسط قیمت مسکن در سال 99',
            'متوسط معاملات مسکن در یک ماه سال 99','متوسط قیمت مسکن در یک ماه اخیر','جمع کل','جمع یک ماه اخیر',
                          'رشد قیمت میانگین متر مربع مسکن در سال 97 نسبت به میانگین سال 96',
                          'رشد قیمت میانگین متر مربع مسکن در سال 98 نسبت به میانگین سال 97',
                          'رشد قیمت میانگین متر مربع مسکن در سال 99 نسبت به میانگین سال 98',
                          'رشد قیمت میانگین یک ماه اخیر متر مربع مسکن نسبت به میانگین سال 99',
                          'رشد قیمت میانگین یک ماه اخیر متر مربع مسکن نسبت به میانگین سال 96',
                          ]
        if city==1:
            result.to_excel('E:\\Moini\\Moini\\maskan_Tehran'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Tehran' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                            index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Tehran.txt',
                          index=True)
        if city==2:
            result.to_excel('E:\\Moini\\Moini\\maskan_Mashhad'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.txt', index=True)
            result.to_csv(
                'E:\\Moini\\Moini\\maskan_Mashhad' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                index=True)
            result.to_csv(
                'E:\\Moini\\Moini\\maskan_Mashhad.txt',
                index=True)
        if city==3:
            result.to_excel('E:\\Moini\\Moini\\maskan_Zanjan'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Zanjan' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                            index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Zanjan.txt',
                          index=True)
        if city==4:
            result.to_excel('E:\\Moini\\Moini\\maskan_Rasht'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Rasht' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                            index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Rasht.txt',
                          index=True)
        if city==5:
            result.to_excel('E:\\Moini\\Moini\\maskan_Yazd'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Yazd' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                            index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Yazd.txt',
                          index=True)
        if city==6:
            result.to_excel('E:\\Moini\\Moini\\maskan_Shiraz'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Shiraz' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                            index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Shiraz.txt',
                          index=True)
        if city==7:
            result.to_excel('E:\\Moini\\Moini\\maskan_Isfahan'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.txt', index=True)
            result.to_csv(
                'E:\\Moini\\Moini\\maskan_Isfahan' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.xlsx',
                index=True)
            result.to_csv(
                'E:\\Moini\\Moini\\maskan_Isfahan.xlsx',
                index=True)
        if city==8:
            result.to_excel('E:\\Moini\\Moini\\maskan_Qom'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Qom' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                            index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Qom.txt',
                          index=True)
        if city==9:
            result.to_excel('E:\\Moini\\Moini\\maskan_Qazvin'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Qazvin' + jdatetime.datetime.now().strftime('%Y_%m_%d') + '.txt',
                            index=True)
            result.to_csv('E:\\Moini\\Moini\\maskan_Qazvin.txt',
                          index=True)

        #t = dd['سن ساختمان'].count()
        print("Authenticated...")
        print(r)
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


def main():

   #post('http://hmi.mrud.ir/Default/SearchDate', data={'strDate': '1399%2F09%2F18', 'strDate1': '1399%2F10%2F18', 'FromArea': '', 'ToArea': '', 'CostPerMeter': '', 'FromYearBuild':0, 'EndYearBuild': 1,'c':1, 'strZoneName':'null', 'iTrust':'', 'ToTrust':'' ,'Frame':'null', 'VID':1, 'Street':'', 'munzone':-1, 'FRentPerMeter':'', 'ERentPerMeter':''})
   post('http://hmi.mrud.ir/Default/listSearchDate', data={	"strDate": (jdatetime.datetime.now()- timedelta(days=30)).strftime('%Y/%m/%d'),	"strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "",	"ToArea": "",	"CostPerMeter": "",	"toCostPerMeter": "",	"FromYearBuild": "",	"EndYearBuild": "",	"c": "1",	"strZoneName": "null",	"iTrust": "",	"ToTrust": "",	"Frame": "null",	"VID": "1"},city=1)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "2"}, city=2)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "3"}, city=3)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "4"}, city=4)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "5"}, city=5)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "6"}, city=6)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "7"}, city=7)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "8"}, city=8)
   post('http://hmi.mrud.ir/Default/listSearchDate',
        data={"strDate": (jdatetime.datetime.now() - timedelta(days=30)).strftime('%Y/%m/%d'),
              "strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "", "ToArea": "",
              "CostPerMeter": "", "toCostPerMeter": "", "FromYearBuild": "", "EndYearBuild": "", "c": "1",
              "strZoneName": "null", "iTrust": "", "ToTrust": "", "Frame": "null", "VID": "9"}, city=9)
##strDate=1399%2F09%2F18&strDate1=1399%2F10%2F18&FromArea=&ToArea=&CostPerMeter=&toCostPerMeter=&FromYearBuild=0&EndYearBuild=1&c=1&strZoneName=null&iTrust=&ToTrust=&Frame=null&VID=1&Street=&munzone=-1&FRentPerMeter=&ERentPerMeter=
### Main program

main();