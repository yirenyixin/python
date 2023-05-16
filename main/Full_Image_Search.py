import happybase
import numpy
import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image
from pyspark import SparkContext,SparkConf
from pyspark.sql.session import SparkSession
import redis
import time
from threading import Thread

T1 = time.time()
name="样例一.bmp"
fname = 'full_image_log.txt'
f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')
f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'r', encoding='utf-8')
lines = f.readlines()
if(str(lines)!="[]"):
    log=lines[-1]
    print(log)
else:
    log=""
    print("log为空")

if(log!=""):
    list=log.split(" ")
    x=list[2]
    y=list[3]
    if(x=="全图搜索"):
        print("上次任务没有执行完成")
        name=y.split()[0]
        print("搜索图片名改为",name)
    else:
        print("上次任务已全部执行完成")


f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')


pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
index=1
flag="1"
l=r.llen("redis:img")
for i in range(l):
    a=r.lindex("redis:img",i)
    a=a.split("'")
    flag1=a[1]
    name1=a[3]
    context=a[-2]
    if(flag==flag1):
        if(name==name1):
            print("最近20次记录搜索过，结果为：")
            print("图片名",context)
            index=0
            t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            operation='成功'
            f.write(str(t)+" "+operation+" "+name+" "+str(context)+" "+'\n')
            redis_context=r.lindex("redis:img",i)
            r.lrem("redis:img",0,redis_context)
            r.rpush("redis:img",redis_context)
            break
if(index==1):
    print("最近20次记录没有搜索过，开始搜索：")

    t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    operation='全图搜索'
    f.write(str(t)+" "+operation+" "+name+'\n')
    f.close()
    # 1.初始化 SparkContext，该对象是 Spark 程序的入口
    conf = SparkConf().setMaster("local[*]").setAppName("My App").set("spark.driver.memory", "5g")
    sc = SparkContext(conf = conf)


    connection = happybase.Connection('localhost',port=9090,autoconnect=False)
    connection.open()
    table = connection.table('img')

    url="C:\\Users\yirenyixin\Desktop\database\Full_map_search\\"+name
    image=Image.open(url)
    array = np.asarray(image)
    data_pd=np.array(array).flatten()#转换一维数组
    # data_pd = list(np.array(array_count).flatten())

    unique,counts=np.unique(data_pd,return_counts=True)
    plt.bar(unique, counts)
    url="C:\\Users\yirenyixin\Desktop\database\\save\\直方图.jpg"
    plt.savefig(url)
    # plt.show()
    image=Image.open(url)
    array = np.asarray(image)
    array=np.array(array,dtype=np.uint8)

    q=array.tobytes()

    b=[]
    for i in range(1,1001):
        row = table.row(str(i), columns=['cf2:imgname','cf5:hist'])
        if row.__len__() != 0:
            a=row[b'cf5:hist']
            c=row[b'cf2:imgname']
            c=str(c)
            b.append([a,c])

    rdd=sc.parallelize(b,32)
    # print(rdd.getNumPartitions())

    print("开始过滤")
    result=rdd.filter(lambda x:q in x)

    str0=result.collect()
    str1=str(str0)
    str1=str1.split(',')[-1]
    str1=str1.split("'")[1]
    print("查找结果")
    print(str1)
    # 6.停止 SparkContext
    connection.close()
    sc.stop()
    list=[flag,name,str1]
    r.rpush("redis:img",str(list))
    if(int(l)>20):
        r.lpop("redis:img")

    f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')
    t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    operation='成功'
    f.write(str(t)+" "+operation+" "+name+" 搜索结果图片名 "+str(list)+" "+'\n')

    T2 = time.time()
    print('程序运行时间:%s毫秒' % ((T2 - T1)*1000))
    print("结束")