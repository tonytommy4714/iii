import requests
import html5lib
from bs4 import BeautifulSoup
from openpyxl import Workbook
import time
import pandas as pd
import numpy as np




def reptile(total,sectionid,kind,year):
        i=0
        table=[]
        while i<= int(total):
                head={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
                line='https://www.591.com.tw/webService-market-1.html?firstRow='
                page=str(i)
                last='&totalRows=&type=1&sectionid={sectionid}&kind={kind}&year={year}rderType=desc&orderField=refreshtime'.format(sectionid=sectionid,kind=kind,year=year)
                real_link=line+page+last
                res = requests.get( real_link , headers = head)
                print( real_link)
                soup = BeautifulSoup(res.text,"html.parser")
                df = pd.read_html(res.text)
                table.append(df[0].drop([0,1]))

                i+=20
                time.sleep(1) 
        
              
        return table
    
    
    
    
    
def combine(a1,b1,c1,d1,e1,f1):
    
    a1.extend(b1)
    a1.extend(c1)
    a1.extend(d1)
    a1.extend(e1)
    a1.extend(f1)
    
    return a1


def cleartable(jointable,block):
        alljointable=pd.concat(jointable,ignore_index=True)#合併table重新給索引值
        alljointable.columns=['成交時間','用途','形態','街道','坪數','租金(車位)','格局','樓層']#給需要的欄位
        alljointable['樓']=np.nan #增加一個樓的新欄位
        alljointable['總樓層']=np.nan#增加一個總樓層的新欄位
       
        i=0
        while i<len(alljointable):
            alljointable.ix[i,['街道']]=block +alljointable.ix[i,['街道']]#把街道的每個值加上區域

            size=str(alljointable.ix[i,['坪數']]).split()[1].replace('坪','')#選取坪數的值轉為字串後做split選取第一個元素替評為空白
            alljointable.ix[i,['坪數']]=size #把更新後的值重新指定到原位置 

            clear_price=str(alljointable.ix[i,['租金(車位)']]).split()[1].replace('元','')
            alljointable.ix[i,['租金(車位)']]=clear_price

            if str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0]=='+1':
                alljointable.ix[i,['樓']]=str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0][1]
            elif str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0]=='-2':
                alljointable.ix[i,['樓']]=str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0][1]
            elif str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0]=='-1':
                alljointable.ix[i,['樓']]=str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0][1]    
            elif str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0]== '整棟' :
                 alljointable.ix[i,['樓']]=str(alljointable.ix[i,['樓層']]).split()[1].split('/')[1]
            elif str(alljointable.ix[i,['樓層']]).split()[1]=='--':
                 alljointable.ix[i,['樓']]=''
            else:
                alljointable.ix[i,['樓']]=str(alljointable.ix[i,['樓層']]).split()[1].split('/')[0]
                
                
            if str(alljointable.ix[i,['形態']]).split()[1]=='--':
                alljointable.ix[i,['形態']]=''

            if str(alljointable.ix[i,['樓層']]).split()[1]=='--':
                alljointable.ix[i,['總樓層']]=''
            else:
                alljointable.ix[i,['總樓層']]=str(alljointable.ix[i,['樓層']]).split()[1].split('/')[1]
            i+=1
        alljointable=alljointable.drop(['樓層'],axis=1)
        alljointable.columns=['dealTime','Usage','Type','Address','Area','rentPrice','Pattern','Floor','floorSum']
        return alljointable

def savefile(savetable,position):
        writer='text.xlsx'
        file_path = position
        writer = pd.ExcelWriter(file_path, engine='openpyxl')
        savetable.to_excel( writer,index=False,encoding='utf-8')
        writer.save()


#中山區
table1=reptile(440,3,1,2018)
table2=reptile(700,3,2,2018)
table3=reptile(160,3,3,2018)
table4=reptile(80,3,4,2018)
table5=reptile(100,3,5,2018)
table6=reptile(20,3,12,2018)
preparetable=combine(table1,table2,table3,table4,table5,table6)
finshtable=cleartable(preparetable,'中山區')
savefile(finshtable,'C:/Users/Big data/Desktop/租屋資料/中山區2018.xlsx')

