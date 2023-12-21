import requests
import pandas as pd
import jdatetime
from datetime import datetime, timedelta


def post(url,data):
    print("Checking session...")
    r1 = requests.post('http://hmi.mrud.ir/Default/Index?ID=1&TreatyType=1')
    r = requests.post(url, data, cookies=r1.cookies)
    if r.ok:
        urlData = r.content.decode('utf-8')
        data1 = pd.read_html(urlData, skiprows=0)[0];

        data1['ZeroToOne'] = data1['سن ساختمان'].between(0, 1)
        data1['OneToTwo'] = data1['سن ساختمان'].between(1, 2)
        data1['TwoToThree'] = data1['سن ساختمان'].between(2, 3)
        data1['ThreeToFour'] = data1['سن ساختمان'].between(3, 4)
        data1['FourToFive'] = data1['سن ساختمان'].between(4, 5)
        data1['FiveToTen'] = data1['سن ساختمان'].between(5, 10)
        data1['BiggerThanTen'] = data1['سن ساختمان'].between(10, 10000000)
        ZeroToOne = data1.query('ZeroToOne == True').groupby('محله').count()['ZeroToOne'].to_frame();
        OneToTwo = data1.query('OneToTwo == True').groupby('محله').count()['OneToTwo'].to_frame();
        TwoToThree = data1.query('TwoToThree == True').groupby('محله').count()['TwoToThree'].to_frame();
        ThreeToFour = data1.query('ThreeToFour == True').groupby('محله').count()['ThreeToFour'].to_frame();
        FourToFive = data1.query('FourToFive == True').groupby('محله').count()['FourToFive'].to_frame();
        FiveToTen = data1.query('FiveToTen == True').groupby('محله').count()['FiveToTen'].to_frame();
        BiggerThanTen = data1.query('BiggerThanTen == True').groupby('محله').count()['BiggerThanTen'].to_frame();
        result = pd.concat([ZeroToOne, OneToTwo, TwoToThree, ThreeToFour, FourToFive, FiveToTen, BiggerThanTen], axis=1)
        result = result.fillna(0);
        result["sum"] = result["ZeroToOne"]+ result["OneToTwo"]+ result["TwoToThree"]+ result["ThreeToFour"]+ result["FourToFive"]+ result["FiveToTen"]+ result["BiggerThanTen"]
        result = result.sort_values(by=['sum'], ascending=False)
        result.to_excel('C:\\Users\\Administrator\\PycharmProjects\\pythonProject\\Moini\\maskan'+jdatetime.datetime.now().strftime('%Y_%m_%d')+'.xlsx', index=True)
        #t = dd['سن ساختمان'].count()
        print("Authenticated...")
        print(r)
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))


def main():

   #post('http://hmi.mrud.ir/Default/SearchDate', data={'strDate': '1399%2F09%2F18', 'strDate1': '1399%2F10%2F18', 'FromArea': '', 'ToArea': '', 'CostPerMeter': '', 'FromYearBuild':0, 'EndYearBuild': 1,'c':1, 'strZoneName':'null', 'iTrust':'', 'ToTrust':'' ,'Frame':'null', 'VID':1, 'Street':'', 'munzone':-1, 'FRentPerMeter':'', 'ERentPerMeter':''})
   post('http://hmi.mrud.ir/Default/listSearchDate', data={	"strDate": (jdatetime.datetime.now()- timedelta(days=30)).strftime('%Y/%m/%d'),	"strDate1": jdatetime.datetime.now().strftime('%Y/%m/%d'), "FromArea": "",	"ToArea": "",	"CostPerMeter": "",	"toCostPerMeter": "",	"FromYearBuild": "",	"EndYearBuild": "",	"c": "1",	"strZoneName": "null",	"iTrust": "",	"ToTrust": "",	"Frame": "null",	"VID": "1"})
##strDate=1399%2F09%2F18&strDate1=1399%2F10%2F18&FromArea=&ToArea=&CostPerMeter=&toCostPerMeter=&FromYearBuild=0&EndYearBuild=1&c=1&strZoneName=null&iTrust=&ToTrust=&Frame=null&VID=1&Street=&munzone=-1&FRentPerMeter=&ERentPerMeter=
### Main program

main();