import happybase
import numpy
import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image
from threading import Thread
import time


T1 = time.time()


connection = happybase.Connection('localhost',port=9090,autoconnect=False)
connection.open()

#表结构
# connection.create_table(
#     'img',
#     {
#         'cf1': dict(),#图片
#         'cf2': dict(),#图片名
#         'cf3': dict(),#统计结果
#         'cf4': dict(),#图像尺寸
#         'cf5': dict(),#图像直方图
#     }
# )



table = connection.table('img')
flag=0

fname = 'hbase_log.txt'
f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')
f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'r', encoding='utf-8')
lines = f.readlines()
if(str(lines)!="[]"):
    log=lines[-1]
    print(log)
else:
    log=""
    flag=0
    print("log为空")

if(log!=""):
    list=log.split(" ")
    x=list[2]
    if(x=="开始插入"):
        flag=1
        print("上次任务没有执行完成")
    else:
        print("上次任务已全部执行完成")


f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')
if(flag==0):
    t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    operation='开始插入'
    f.write(str(t)+" "+operation+" "+'\n')
    f.close()
    print("正在开始新任务")
else:
    print("正在重新执行任务")

for x in range(1,1001):
    url="C:\\Users\yirenyixin\Desktop\database\\bossbase\\"+str(x)+".bmp"
    image=Image.open(url)
    array = np.asarray(image)
    # array=np.array(array,dtype=np.uint8)

    array_count=np.array(array).flatten()#转换一维数组
    imgname=str(x)+".bmp"

    data_pd = array_count
    unique,counts=np.unique(data_pd,return_counts=True)

    array_count=np.array([unique,counts])
    plt.clf()
    plt.bar(unique, counts)
    url="C:\\Users\yirenyixin\Desktop\database\\save\\"+str(x)+".jpg"
    plt.savefig(url)
    # plt.show()

    image1=Image.open(url)
    array_hist=np.asarray(image1)
    array_hist=np.asarray(array_hist,dtype=np.uint8)

    table.put(str(x), {'cf1:img':array.tobytes(),'cf2:imgname':imgname,
                       'cf3:result':array_count.tobytes(),'cf4:shape':str(array.shape),'cf5:hist':array_hist.tobytes()})
    print(x)





connection.close()

f = open('C:\\Users\yirenyixin\Desktop\database\log\\'+fname, 'a+', encoding='utf-8')
t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
operation='插入成功'
f.write(str(t)+" "+operation+" "+'\n')

T2 = time.time()
print('程序运行时间:%s毫秒' % ((T2 - T1)*1000))