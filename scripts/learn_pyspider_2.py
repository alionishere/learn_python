import json
import requests
import urllib.request as urllibreq
from datetime import datetime, timedelta
import datetime
import time
from pymongo import MongoClient

myclient = MongoClient("mongodb://127.0.0.1/", 27017)
db = myclient['mytestdb']
# collection = myclient["mytestdb"]["rzrq_detail"]
data_time = datetime.datetime.now()
delta_time = datetime.timedelta(days=1)
datetime = data_time - delta_time
data_date = datetime.strftime('%Y-%m-%d')


class RzrqSpider(object):
    def __init__(self):

        self.url = "http://datacenter.eastmoney.com/api/data/get?callback=datatable9860586&" \
                    "type=RPTA_WEB_RZRQ_GGMX&sty=ALL&" \
                    "source=WEB&p={}&ps=50&st=RZJME&sr=-1&filter=(date%3D%27" + data_date + "%27)&" \
                    "pageNo=1&_=1608023503446"
        time.sleep(3)

    def get_url_list(self):
        url_list = []
        for i in range(1, 41):
            url = self.url.format(i)
            url_list.append(url)
        return url_list

    def send_request(response, url):
        data = urllibreq.urlopen(url)
        return data

    def parse_save_data(self, data):
        data = data.read().decode("utf-8")
        try:
            start_pos = data.index('[{')
            end_pos = data.index('}]')
            json_data = data[start_pos:end_pos + 2]
            list_data = json.loads(json_data)
            for item in list_data:
                items = {}
                # 日期
                items["DATE"] = item["DATE"][0:10].replace('-', '').strip()
                # 证券市场
                items["MARKET"] = item["MARKET"][5:7]
                # 证券代码
                items["SCODE"] = item["SCODE"]
                # 证券简称
                items["SECNAME"] = item["SECNAME"]
                # 收盘价
                items["SPJ"] = item["SPJ"]
                # 涨跌幅
                items["ZDF"] = item["ZDF"]
                # 融资余额
                items["RZYE"] = item["RZYE"]
                # 融资余额占流通市值比
                items["RZYEZB"] = item["RZYEZB"]
                # 融资买入额(元)
                items["RZMRE"] = item["RZMRE"]
                # 融资偿还额(元)
                items["RZCHE"] = item["RZCHE"]
                # 融资净买入(元)
                items["RZJME"] = item["RZJME"]
                # 融券余额(元)
                items["RQYE"] = item["RQYE"]
                # 融券余量(股)
                items["RQYL"] = item["RQYL"]
                # 融券卖出量(股)
                items["RQMCL"] = item["RQMCL"]
                # 融券偿还量(股)
                items["RQCHL"] = item["RQCHL"]
                # 融券净卖出(股)
                items["RQJMG"] = item["RQJMG"]
                # 融资融券余额(元)
                items["RZRQYE"] = item["RZRQYE"]
                # 融资融券余额差值(元)
                items["RZRQYECZ"] = item["RZRQYE"]
                # 第一种方法
                # 删除当天数据
                # collection.delete_many(items)
                # 重新插入数据
                # collection.insert(items)

                # 第二种方法
                # 重复数据不插入
                if db['rzrq_detail'].update({'DATE': items['DATE'], 'SCODE': items['SCODE']}, {'$set': items}, True):
                    print('SUCCESS')
                else:
                    print('"FAIL"')
        except:
            print("超出最大页数,返回没有数据")

    def start(self):
        url_list = self.get_url_list()
        # 循环遍历发送请求
        for url in url_list:
            data = self.send_request(url)
            self.parse_save_data(data)


RzrqSpider().start()
