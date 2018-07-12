from pyspark import SparkConf ,SparkContext
from math import cos


def latAndLon_to_int(x):
    x[3] = float(x[3])
    x[4] = float(x[4])
    return x

def stat_code_compute(x):
    scale_km = 0.5
    lat = x[3]
    lon = x[4]
    lat_to_km = lat*110.574
    lon_to_km = lon*(111.320*cos(lat))

    state_code_x = int(lat_to_km//scale_km)
    state_code_y = int(lon_to_km//scale_km)
    state_code = str(state_code_x)+str(state_code_y)
    return (x,state_code)

def status_code_explore(x):
    import statistics
    price=[]
    floorsum=[]
    for i in x[1]:                          #groupby 後回傳[('553726935', <pyspark.resultiterable.ResultIterable at 0x7fed51a7a048>)]
        if str(i[0][7])!= '':               #<pyspark.resultiterable.ResultIterable at 0x7fed51a7a048>) 對這個物件做迭代可取額裡面內容
            price.append(float(i[0][7]))
        if str(i[0][10]) != ''and "-":        #資料內有遺漏值if 過濾掉這些值 不然會有錯誤
            floorsum.append(float(i[0][10]))
        
      
    
    meanprice=str(statistics.mean(price))
    median_price=str(statistics.median(price))
    std_price=str(statistics.stdev(price))
    q3_price=str(statistics.median_high(price))
    q1_price=str(statistics.median_low( price))
    
    meanfloorsum=str(statistics.mean(floorsum))
    median_floorsum=str(statistics.median(floorsum))
    std_floorsum=str(statistics.stdev(floorsum))
    q3_floorsum=str(statistics.median_high(floorsum))
    q1_floorsum=str(statistics.median_low(floorsum))
    return (x[0],meanprice,median_price,std_price,q3_price,q1_price,meanfloorsum,median_floorsum,std_floorsum,q3_floorsum,q1_floorsum)



rdd=sc.textFile("hdfs://localhost/user/cloudera/user/data/lease_all_2018.csv") 

rdd.take(2)[1].split(",")[3]

Header = rdd.first()

noHeaderrdd=rdd.filter(lambda x:x != Header)#去掉欄位

noHeaderrdd.take(1)[0]

splitnoHeaderrdd=noHeaderrdd.map(lambda x:x.split(","))#對裡面的元素做切割產生list

splitnoHeaderrdd.take(1)

change_rdd=splitnoHeaderrdd.map(latAndLon_to_int)#對lai lon 做轉換為float

result=change_rdd.map(stat_code_compute)#運算 state code

group_result=result.groupBy(lambda x:x[1]) #運算出來的state code 做groupby

explore_result=group_result.map(status_code_explore) 


for i in group_result.collect():
    print(i)
    
result.saveAsTextFile("hdfs://localhost/user/cloudera/user/output")


try:
    explore_result.saveAsTextFile("hdfs://localhost/user/cloudera/user/lease_all_2008_mean")
except:
    print("no")

