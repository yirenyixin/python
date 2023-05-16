import math
import happybase
import numpy
import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image
from pyspark import SparkContext,SparkConf
from pyspark.sql.session import SparkSession
import time
import redis
import psutil
import time
from threading import Thread
T1 = time.time()
name="样例二.bmp"
fname = 'tempering_check_log.txt'
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
    if(x=="图像篡改检查"):
        print("上次任务没有执行完成")
        name=y.split()[0]
        print("搜索图片名改为",name)
    else:
        print("上次任务已全部执行完成")


f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
index=1
flag="3"

l=r.llen("redis:img")
for i in range(l):
    a=r.lindex("redis:img",i)
    a=a.split("'")
    # print(a)
    flag1=a[1]
    name1=a[3]
    context=a[-2]
    if(flag==flag1):
        if(name==name1):
            print("最近20次记录搜索过，结果为：")
            print("篡改图片名",name1)
            print("篡改点坐标",context)
            index=0
            t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            operation='成功'
            f.write(str(t)+" "+operation+" "+name+" 图片名 "+name1+" 篡改点坐标 "+context+" "+'\n')
            redis_context=r.lindex("redis:img",i)
            r.lrem("redis:img",0,redis_context)
            r.rpush("redis:img",redis_context)
            break
if(index==1):
    print("最近20次记录没有搜索过，开始搜索：")

    t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    operation='图像篡改检查'
    f.write(str(t)+" "+operation+" "+name+" "+'\n')
    f.close()
    conf = SparkConf().setMaster("local[*]").setAppName("My App").set("spark.driver.memory", "10g")
    sc = SparkContext(conf = conf)


    connection = happybase.Connection('localhost',port=9090,autoconnect=False)
    connection.open()

    table = connection.table('img')

    url="C:\\Users\yirenyixin\Desktop\database\Image_tampering_check\\"+name
    image=Image.open(url)
    array = np.asarray(image)

    array=np.array(array)

    a1=array[:256,:256]
    a2=array[256:,:256]
    a3=array[:256,256:]
    a4=array[256:,256:]
    def check1(x):
        str1=str(x)
        array1=str1.split("(")[1]
        array1=array1.split(")")[0]
        imgname=str1.split("'")[-2]
        a=str(array1)
        array1=eval(a)
        array1=np.array(array1)

        array_x=a1-np.sum(a1)/np.size(a1)
        array_y=array1-np.sum(array1)/np.size(array1)
        x=(array_x*array_y).sum()/math.sqrt((array_x*array_x).sum()*(array_y*array_y).sum())
        # print(x)
        index_list=[]
        if(x>0.95):
            for i in range(256):
                for j in range(256):
                    if(a1[i][j]!=array1[i][j]):
                        index_list.append([i,j])#检查篡改的像素点坐标
        index_list=str(index_list)

        return [x,imgname,index_list]
    def check2(x):
        str1=str(x)
        array1=str1.split("(")[1]
        array1=array1.split(")")[0]
        imgname=str1.split("'")[-2]
        a=str(array1)
        array1=eval(a)
        array1=np.array(array1)
        array_x=a2-np.sum(a2)/np.size(a2)
        array_y=array1-np.sum(array1)/np.size(array1)
        x=(array_x*array_y).sum()/math.sqrt((array_x*array_x).sum()*(array_y*array_y).sum())
        # print(x)
        index_list=[]
        if(x>0.95):
            for i in range(256):
                for j in range(256):
                    if(a2[i][j]!=array1[i][j]):
                        index_list.append([i,j+256])
        index_list=str(index_list)
        return [x,imgname,index_list]
    def check3(x):
        str1=str(x)
        array1=str1.split("(")[1]
        array1=array1.split(")")[0]
        imgname=str1.split("'")[-2]
        a=str(array1)
        array1=eval(a)
        array1=np.array(array1)
        array_x=a3-np.sum(a3)/np.size(a3)
        array_y=array1-np.sum(array1)/np.size(array1)
        x=(array_x*array_y).sum()/math.sqrt((array_x*array_x).sum()*(array_y*array_y).sum())
        # print(x)
        index_list=[]
        if(x>0.95):
            for i in range(256):
                for j in range(256):
                    if(a3[i][j]!=array1[i][j]):
                        index_list.append([i+256,j])
        index_list=str(index_list)
        return [x,imgname,index_list]
    def check4(x):
        str1=str(x)
        array1=str1.split("(")[1]
        array1=array1.split(")")[0]
        imgname=str1.split("'")[-2]
        a=str(array1)
        array1=eval(a)
        array1=np.array(array1)
        array_x=a4-np.sum(a4)/np.size(a4)
        array_y=array1-np.sum(array1)/np.size(array1)
        x=(array_x*array_y).sum()/math.sqrt((array_x*array_x).sum()*(array_y*array_y).sum())
        # print(x)
        index_list=[]
        if(x>0.75):
            for i in range(256):
                for j in range(256):
                    if(a4[i][j]!=array1[i][j]):
                        index_list.append([i+256,j+256])
        index_list=str(index_list)
        return [x,imgname,index_list]

    def search1(rdd):
        return rdd.map(check1).collect()

    class MyThread1(Thread):

        def __init__(self, rdd):
            Thread.__init__(self)
            self.rdd=rdd

        def run(self):
            self.result = search1(self.rdd)

        def get_result(self):
            return self.result
    def search2(rdd):
        return rdd.map(check2).collect()

    class MyThread2(Thread):

        def __init__(self, rdd):
            Thread.__init__(self)
            self.rdd=rdd

        def run(self):
            self.result = search2(self.rdd)

        def get_result(self):
            return self.result
    def search3(rdd):
        return rdd.map(check3).collect()

    class MyThread3(Thread):

        def __init__(self, rdd):
            Thread.__init__(self)
            self.rdd=rdd

        def run(self):
            self.result = search3(self.rdd)

        def get_result(self):
            return self.result
    def search4(rdd):
        return rdd.map(check4).collect()

    class MyThread4(Thread):

        def __init__(self, rdd):
            Thread.__init__(self)
            self.rdd=rdd

        def run(self):
            self.result = search4(self.rdd)

        def get_result(self):
            return self.result
    d1=[]
    d2=[]
    d3=[]
    d4=[]
    for i in range(1,1001):
        row = table.row(str(i), columns=['cf1:img','cf2:imgname'])
        if row.__len__() != 0:
            a=row[b'cf1:img']
            c=row[b'cf2:imgname']
            a=np.frombuffer(a,dtype=np.uint8).reshape(512,512)
            my_list1 = a.tolist()
            s = "np.array({})".format(my_list1)
            c=str(c)
            b1=a[:256,:256]
            b2=a[256:,:256]
            b3=a[:256,256:]
            b4=a[256:,256:]
            my_list1 = b1.tolist()
            s = "np.array({})".format(my_list1)
            d1.append([s,c])
            my_list1 = b2.tolist()
            s = "np.array({})".format(my_list1)
            d2.append([s,c])
            my_list1 = b3.tolist()
            s = "np.array({})".format(my_list1)
            d3.append([s,c])
            my_list1 = b4.tolist()
            s = "np.array({})".format(my_list1)
            d4.append([s,c])
    rdd1=sc.parallelize(d1,32)
    rdd2=sc.parallelize(d2,32)
    rdd3=sc.parallelize(d3,32)
    rdd4=sc.parallelize(d4,32)
    # l1=rdd1.map(check1).collect()
    # l2=rdd2.map(check2).collect()
    # l3=rdd3.map(check3).collect()
    # l4=rdd4.map(check4).collect()
    t1=MyThread1(rdd1)
    t2=MyThread2(rdd2)
    t3=MyThread3(rdd3)
    t4=MyThread4(rdd4)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    l1=t1.get_result()
    l2=t2.get_result()
    l3=t3.get_result()
    l4=t4.get_result()
    e=[]
    e.append(flag)
    e.append(name)
    max=0.0
    index_i=0
    for i in range(len(l1)):
        if((l1[i][0]+l2[i][0]+l3[i][0]+l4[i][0])/4>max):
            max=(l1[i][0]+l2[i][0]+l3[i][0]+l4[i][0])/4
            index_i=i
    e.append(l1[index_i][1])
    if(l1[index_i][2]!="[]"):
        e.append(l1[index_i][2])
    if(l2[index_i][2]!="[]"):
        e.append(l2[index_i][2])
    if(l3[index_i][2]!="[]"):
        e.append(l3[index_i][2])
    if(l4[index_i][2]!="[]"):
        e.append(l4[index_i][2])
    # x=rdd2.first()
    print("结果：",e)

    connection.close()
    sc.stop()
    r.rpush("redis:img",str(e))
    if(int(l)>20):
        r.lpop("redis:img")

    f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')
    t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    operation='成功'
    f.write(str(t)+" "+operation+" "+name+" 搜索结果 "+str(e)+" "+'\n')

    print("结束")
    T2 = time.time()
    print('程序运行时间:%s毫秒' % ((T2 - T1)*1000))