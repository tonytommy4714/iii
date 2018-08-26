import requests
import html5lib
from bs4 import BeautifulSoup
import time
import pandas as pd
import numpy as np
import re
import json
import time



head=[]
price=[]
days=[]
date=[]
member=[]
trip_id=[]
sale_yet=[]
page=input("頁數")
page=int(page)+1
for i in range(1,int(page)):
    
    url = "https://www.gloriatour.com.tw/EW/Services/SearchListData.asp"

    information = "{"+\
    r'''"displayType":"G",
    "subCd":"",
    "orderCd":"1",
    "pageALL": {p},
    "pageGO":"1",
    "pagePGO":"1",
    "waitData":"false",
    "waitPage":"false",
    "mGrupCd":"",
    "SrcCls":"",
    "tabList":"",
    "regmCd":"",
    "regsCd":"",
    "beginDt":"2018/08/25",
    "endDt":"2019/02/25",
    "portCd":"",
    "tdays":"",
    "bjt":"",
    "carr":"",
    "allowJoin":"1",
    "allowWait":"1",
    "ikeyword":"" '''.format(p=i)+"}"
    
    data = json.loads(information)
    headers = json.loads(r'''{
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Content-Length": "235",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "_ga=GA1.3.1722798424.1534767998; __utmz=256869725.1534767998.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); ASPSESSIONIDSUBTSASS=JLJKAJDANLEJLFJNCIHNDCNM; _gid=GA1.3.1419019726.1535200114; __utma=256869725.1722798424.1534767998.1535036422.1535200114.4; __utmc=256869725; __utmt=1; __utmb=256869725.2.10.1535200114; _gat=1; _gat_UA-45094953-1=1; _gat_UA-121442952-1=1; _gat_UA-47251058-21=1; _gat_UA-69062987-1=1; _gat_UA-105819071-1=1",
        "Host": "www.gloriatour.com.tw",
        "Origin": "https://www.gloriatour.com.tw",
        "Referer": "https://www.gloriatour.com.tw/EW/GO/GroupList.asp",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }''')

    resp = requests.post(url, data=data, headers=headers)
    resp.encoding = 'big5'
    data=resp.json()['All']
    for js in data:
        date.append(js['SaleAm'])
        trip_id.append(js['GrupCd'])
        head.append(js['GrupSnm'])
        price.append(js['LeavDt'])
        days.append(js['GrupLn'])
        member.append(js['EstmYqt'])
        sale_yet.append(js['SaleYqt'])
    time.sleep(3)

saleyet_df=pd.DataFrame(sale_yet)
head_df=pd.DataFrame(head)
price_df=pd.DataFrame(price)
days_df=pd.DataFrame(days)
date_df=pd.DataFrame(date)
member_df=pd.DataFrame(member)
trip_id_df=pd.DataFrame(trip_id)

all_data=[]
all_data.append(trip_id_df)
all_data.append(head_df)
all_data.append(price_df)
all_data.append(days_df)
all_data.append(date_df)
all_data.append(member_df)
all_data.append(saleyet_df)

table=pd.concat(all_data,axis=1,ignore_index=True)

table.columns=['ID','團名','出發日期','旅遊天數','價格','總團位','可售位']