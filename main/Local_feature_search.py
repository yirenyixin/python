import happybase
import numpy
import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt
import cv2
from PIL import Image
from pyspark import SparkContext,SparkConf
from pyspark.sql.session import SparkSession
import redis
import time
from threading import Thread
T1 = time.time()
name="特征二.bmp"
fname = 'local_feature_log.txt'
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
    if(x=="局部特征搜索"):
        print("上次任务没有执行完成")
        name=y.split()[0]
        print("搜索图片名改为",name)
    else:
        print("上次任务已全部执行完成")


f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')


pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
index=1
flag="2"
l=r.llen("redis:img")
for i in range(l):
    a=r.lindex("redis:img",i)
    a1=a
    a=a.split("'")
    flag1=a[1]
    name1=a[3]
    context=a[-2]
    if(flag==flag1):
        if(name==name1):
            print("最近20次记录搜索过，结果为：")
            a=a1.split('"')[-2]
            print(a)
            index=0
            t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            operation='成功'
            f.write(str(t)+" "+operation+" "+name+" "+str(a)+" "+'\n')
            redis_context=r.lindex("redis:img",i)
            r.lrem("redis:img",0,redis_context)
            r.rpush("redis:img",redis_context)
            T2 = time.time()
            print('程序运行时间:%s毫秒' % ((T2 - T1)*1000))
            break
if(index==1):
    print("最近20次记录没有搜索过，开始搜索：")

    t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    operation='局部特征搜索'
    f.write(str(t)+" "+operation+" "+name+'\n')
    f.close()
    conf = SparkConf().setMaster("local[*]").setAppName("My App").set("spark.driver.memory", "5g")
    sc = SparkContext(conf = conf)

    connection = happybase.Connection('localhost',port=9090,autoconnect=False)
    connection.open()

    table = connection.table('img')

    url="C:\\Users\yirenyixin\Desktop\database\Local_feature_search\\"+name
    image=Image.open(url)
    array = np.asarray(image)

    # array=np.array(array,dtype=np.uint8)
    # str1=str(array)
    # print(array)
    # print("------")
    # print(array.shape)
    # q=array.tobytes()
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

    def check(x):
        str1=str(x)
        array1=str1.split("(")[1]
        array1=array1.split(")")[0]
        imgname=str1.split("'")[-2]
        a=str(array1)
        array1=eval(a)
        array1=np.array(array1)
        array1=np.asarray(array1)
        # print(array1.shape)
        # print(array)
        # print("---")

        # 数组转图片
        gray_cat = Image.fromarray(array1.astype('uint8'))
        # 保存图片
        gray_cat.save("C:\\Users\yirenyixin\Desktop\database\\test\\"+str(imgname)+".jpg")
        #  展示查看
        # gray_cat.show()
        url1="C:\\Users\yirenyixin\Desktop\database\\test\\"+str(imgname)+".jpg"
        image1=Image.open(url1)
        array1 = np.asarray(image1)

        result = cv2.matchTemplate(array1, array, cv2.TM_SQDIFF_NORMED)
        theight, twidth = array.shape[:2]

        minValue, maxValue, minLoc, maxLoc = cv2.minMaxLoc(result)
        if(minValue<0.01):
            print(imgname)
            # print(minValue)
            #对于cv2.TM_SQDIFF及cv2.TM_SQDIFF_NORMED方法min_val越趋近与0匹配度越好，匹配位置定点取min_loc,对于其他方法max_val越趋近于1匹配度越好，匹配位置取max_loc
            strmin_val = str(minValue)
            #绘制矩形边框，将匹配区域标注出来
            cv2.rectangle(array1,minLoc,(minLoc[0]+twidth,minLoc[1]+theight),(0,0,225),2)
            #显示结果,并将匹配值显示在标题栏上
            cv2.imshow("MatchResult----MatchingValue="+strmin_val,array1)
            cv2.waitKey()
            cv2.destroyAllWindows()
            return [minValue,imgname]
        else:
            return [1.,imgname]
    def search(rdd):
        return rdd.map(check).filter(lambda x: x[0] < 1.).collect()
    class MyThread(Thread):

        def __init__(self, rdd):
            Thread.__init__(self)
            self.rdd=rdd

        def run(self):
            self.result = search(self.rdd)

        def get_result(self):
            return self.result

    rdd1=sc.parallelize(d1,32)
    rdd2=sc.parallelize(d2,32)
    rdd3=sc.parallelize(d3,32)
    rdd4=sc.parallelize(d4,32)
    # l1=rdd1.map(check).filter(lambda x: x[0] < 1.).collect()
    # l2=rdd2.map(check).filter(lambda x: x[0] < 1.).collect()
    # l3=rdd3.map(check).filter(lambda x: x[0] < 1.).collect()
    # l4=rdd4.map(check).filter(lambda x: x[0] < 1.).collect()
    t1=MyThread(rdd1)
    t2=MyThread(rdd2)
    t3=MyThread(rdd3)
    t4=MyThread(rdd4)
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
    # print(l1)
    # print(l2)
    # print(l3)
    # print(l4)
    d=[]
    if(l1!="[]"):
        d.extend(l1)
    if(l2!="[]"):
        d.extend(l2)
    if(l3!="[]"):
        d.extend(l3)
    if(l4!="[]"):
        d.extend(l4)
    # result=[]
    # for i in range(0,len(d)):
    #     result_flag=1
    #     for j in range(i+1,len(d)):
    #         if(d[i][1]==d[j][1]):
    #             result_flag=0
    #     if(result_flag==1):
    #         result.append(d[i])
    # print(result)
    print(d)
    list=[]
    list.append([flag,name,str(d)])
    r.rpush("redis:img",str(list))
    if(int(l)>20):
        r.lpop("redis:img")


    f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')
    t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    operation='成功'
    f.write(str(t)+" "+operation+" "+name+" 搜索结果 "+str(list)+" "+'\n')
    T2 = time.time()
    print('程序运行时间:%s毫秒' % ((T2 - T1)*1000))
    print("结束")