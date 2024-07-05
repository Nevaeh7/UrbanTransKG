# -*- coding: utf-8 -*-
# @Time    : 2022/1/18 14:38
# @Author  :  Nevaeh
# @File    : status_schedule.py
# @Software: PyCharm

import asyncio
import itertools
import json
import logging
import math
import multiprocessing
import random
import sys
import time
import os
from threading import Thread
from urllib.parse import unquote,quote
import aiohttp
import pandas as pd
import pymongo
import requests
from lxml import etree
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import hashlib
import csv
import uuid
import schedule
from datetime import datetime as dd

client = pymongo.MongoClient(host='localhost', port=27017)
db = client['traffic']
traffic_status = db['traffic_status']
traffic_list = db['traffic_road_status_li']

# 初始API的URL
start_time_m = '2022-01-18 06:30:00'
end_time_m = '2022-01-18 10:30:00'
start_time_a = '2022-01-18 16:00:00'
end_time_a = '2022-01-18 20:30:00'

m_times = pd.date_range(start=pd.Timestamp(start_time_m).round('5T'), end=pd.Timestamp(end_time_m).round('5T'),
                        freq="5T")
a_times = pd.date_range(start=pd.Timestamp(start_time_a).round('5T'), end=pd.Timestamp(end_time_a).round('5T'),
                        freq="5T")

morning = m_times.strftime('%H:%M').to_list()
afternoon = a_times.strftime('%H:%M').to_list()
time_list = morning + afternoon


def get_status(d_time):
    today = dd.today().strftime('%Y-%m-%d')
    week = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    weekday = week[dd.weekday(dd.today())]
    url = "https://restapi.amap.com/v3/traffic/status/rectangle?key=Your Gaode API Key&extensions=all&level=6&rectangle="
    # 设定整个网格左下角坐标的经纬度值
    baselng = 116.107403
    baselat = 39.738143
    # 设定每个网格单元的经纬度宽
    widthlng = 0.07
    # 同一维度，lng=0.01≈1000米
    widthlat = 0.063
    # 同一经度，lat=0.01≈1113米
    # 用于储存数据
    x = []
    # 用于标识交通态势线段
    num = 0
    roadid_li = []

    # 爬取过程可能会出错中断，因此增加异常处理
    try:
        # 循环每个网格进行数据爬取，在这里构建了3X3网格
        for i in range(0, 6):
            # 设定网格单元的左下与右上坐标的纬度值
            # 在这里对数据进行处理，使之保留6位小数（不保留可能会莫名其妙出错）
            startlat = round(baselat + i * widthlat, 6)
            endlat = round(startlat + widthlat, 6)
            for j in range(0, 6):
                # 设定网格单元的左下与右上坐标的经度值
                startlng = round(baselng + j * widthlng, 6)
                endlng = round(startlng + widthlng, 6)
                # 设置API的URL并进行输出测试
                locStr = str(startlng) + "," + str(startlat) + ";" + str(endlng) + "," + str(endlat)
                thisUrl = url + locStr
                print(thisUrl)
                # 爬取数据
                data = requests.get(thisUrl)
                s = data.json()
                a = s["trafficinfo"]["roads"]
                r_li = s['trafficinfo']
                r_li['_id'] = uuid.uuid1().hex
                r_li['time'] = d_time
                r_li['weekday'] = weekday
                r_li['date'] = today
                r_li['grid_loc'] = locStr
                r_li['grid_id'] = hashlib.md5(locStr.encode('utf-8')).hexdigest()
                traffic_list.insert_one(r_li)
                # 注意，提取数值需要使用XXX.get()的方式来实现，如a[k].get('speed')
                # 若使用a[k]['speed']来提取，或会导致KeyError错误
                for k in range(0, len(a)):
                    temp = a[k]
                    temp['_id'] = uuid.uuid1().hex
                    temp['time'] = d_time
                    temp['weekday'] = weekday
                    temp['date'] = today
                    md5 = hashlib.md5((temp['name'] + temp['direction']).encode('utf-8')).hexdigest()
                    temp['road_id'] = md5
                    temp['r_li_id'] = r_li['_id']
                    roadid_li.append(md5)
                    traffic_status.insert_one(temp)

    #                 for l in range(0,len(s3)):
    #                     s4=s3[l].split(",")
    #                 time.sleep(0.1)
    # 若爬取网格较多，可使用time.sleep(秒数)来避免高德的单秒API调用次数的限制
    except Exception as e:
        print(e)
        print(s)
    print(len(set(roadid_li)), len(roadid_li))

for i in range(len(time_list)):
    schedule.every().day.at(time_list[i]).do(get_status, time_list[i])
while True:
    schedule.run_pending()
    time.sleep(5)