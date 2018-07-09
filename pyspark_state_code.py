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

for i in group_result.collect():
    print(i)
    
result.saveAsTextFile("hdfs://localhost/user/cloudera/user/output")