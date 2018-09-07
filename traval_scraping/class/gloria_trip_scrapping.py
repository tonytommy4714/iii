import requests
import html5lib
from bs4 import BeautifulSoup
import time
import pandas as pd
import numpy as np
import re
import json
from selenium import webdriver
import time

class gloria():
    def test(self):
        home_p_source=[]

        for i in range(1,8):
            page_link="https://www.tripresso.com/agency/GLORIA#pageNum={p}".format(p=i)
            browser = webdriver.Chrome()
            browser.get(page_link)
            time.sleep(7)
            #soup=BeautifulSoup(browser.page_source,'html.parser')
            home_p_source.append(browser.page_source)
            browser.close()

        clr_all_link=[]
        for i in home_p_source:
            all_link=[]
            a_type_link=re.findall('''\"\/detail\?tour_key=GLORIA-\w*\&amp;group_code=\w*\"''',i)
            b_type_link=re.findall('''\"\/detail\?tour_key=GLORIA-\w*-\w*\&amp;group_code=\w*\"''',i)
            a_type_link.extend(b_type_link)
            all_link=set(a_type_link)
            for link in all_link:
                clr_all_link.append(link)

        all_p_source=[]
        a=0
        for i in clr_all_link:

            complete_link="https://www.tripresso.com"+i.replace("\"","").replace("amp;","")
            print(complete_link,a)
            browser = webdriver.Chrome()
            browser.get(complete_link)
            time.sleep(10)
            all_p_source.append(browser.page_source)
            a+=1
            browser.close()

        head=[]
        price=[]
        days=[]
        date=[]
        member=[]
        trip_id=[]
        error_page=0
        try:
            for i in  all_p_source:
                error_page+=1
                soup=BeautifulSoup(i,'html.parser')
                txt_head=soup.find('h2',{"itemprop":"name"})
                txt_price=soup.find('p',{"class":"newPrice"})
                txt_days=soup.find('div',{"class":"day"})
                txt_date=soup.findAll('div',{"class":"day"})
                txt_member=soup.findAll('div',{"class":"groupBox"})
                txt_trip_id=soup.find("div",{"class":"strokeNo"})
                head.append(txt_head.text)
                price.append(txt_price.text[7:13].replace(',',""))
                days.append(txt_days.text.split()[0])
                date.append(txt_date[1].text.split()[0].replace("月","-").replace("日",""))
                trip_id.append(txt_trip_id.text.split()[0].split("號")[1])
                if len(txt_member[1].text.split()[2])<=5:
                    member.append(txt_member[1].text.split()[2][1])
                else:
                    member.append(txt_member[1].text.split()[2][1:3])
        except:
            print(error_page)


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

        table=pd.concat(all_data,axis=1,ignore_index=True)

        table.columns=['ID','團名','價格','旅遊天數','出發日期','團位']
        
        return table
