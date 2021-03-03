#!/usr/bin/env python3
import requests
import cx_Oracle
from datetime import date, datetime, timedelta
import logging
from sqlalchemy import desc, create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

# 配置日志
t_today = datetime.now().strftime("%Y%m%d")
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='D:/whk/log/toutiao_fi_%s.log' % t_today, level=logging.INFO, format=LOG_FORMAT)


class OEClient:
    # APP_ID = '1685569372679180'
    # APP_SECRET = '5a759bd4055e12d8b5867c5886317a354b1ee26e'
    APP_ID = '1679163061779460'
    APP_SECRET = 'd85f9f6be5bd6c9250d6e268dce99ce9b6c73cc3'
    APP_OAUTH_CALLBACK_URL = 'http://localhost:8000/oauth/oceanengine'
    AUTHORIZED_URL = '''https://ad.oceanengine.com/openapi/audit/oauth.html?app_id=1676714476618766&state=your_custom_params&scope=%5B800%2C100%2C5%2C200%2C210%2C42%2C43%2C44%2C45%2C47%2C40%2C242%2C243%2C250%2C220%2C30%5D&material_auth=1&redirect_uri=http%3A%2F%2Flocalhost%3A8000%2Foauth%2Foceanengine&rid=jcr7ae1pq7n'''

    def __init__(self):
        super().__init__()
        self.access_token = None

    def fetch_access_token(self, auth_code):
        url = 'https://ad.oceanengine.com/open_api/oauth2/access_token/'
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            "app_id": self.APP_ID,
            "secret": self.APP_SECRET,
            "grant_type": "auth_code",
            "auth_code": auth_code,
        }
        return requests.post(url, headers=headers, json=data)

    def refresh_access_token(self, refresh_token):
        open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
        uri = "oauth2/refresh_token/"
        refresh_token_url = open_api_url_prefix + uri
        data = {
            "appid": self.APP_ID,
            "secret": self.APP_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        rsp = requests.post(refresh_token_url, json=data)
        rsp_data = rsp.json()
        return rsp_data

    def get_advertiser_daily_stat(self, advertiser_id, start_date, end_date, page):
        open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
        uri = "2/advertiser/fund/daily_stat/"
        url = open_api_url_prefix + uri
        params = {
            "advertiser_id": advertiser_id,
            "start_date": start_date,
            "end_date": end_date,
            "page": page,
            "page_size": 100
        }
        headers = {"Access-Token": self.access_token}
        rsp = requests.get(url, json=params, headers=headers)
        rsp_data = rsp.json()
        return rsp_data


client = OEClient()

###################################
# refresh token
###################################
with open('D:/whk/log/refresh_token_fi_4.dat', 'r') as fr:
    refresh_token = fr.read()
# rps_data = client.fetch_access_token(auth_code='c78cdb035e6dd60a930c6dc08ae3f6ec1858601f')
# print(rps_data.text)
# import sys
# sys.exit(0)
# refresh_token = '1e1aa4717445bd5466b9b879a8d4f6a97e45147b'
rsp_refresh_token = client.refresh_access_token(refresh_token)
refresh_token = rsp_refresh_token['data']['refresh_token']
# print('refresh_token: %s' % refresh_token)

logging.info('refresh_token: %s' % refresh_token)
with open('D:/whk/log/refresh_token_fi_4.dat', 'w') as fw:
    fw.write(refresh_token)

client.access_token = rsp_refresh_token['data']['access_token']
logging.info('access_token: %s' % client.access_token)
# print('access_token: %s' % client.access_token)


def get_db_conn():
    return cx_Oracle.connect('kingstar', 'kingstar', '10.29.7.211:1521/siddc01')


def close_db(cursor, conn):
    cursor.close()
    conn.close()


def del_data(cursor, tb_name, advertise_id, t_date):
    sql = 'delete sc61.%s where advertise_id = %s and date_f = %s' % (tb_name, advertise_id, t_date)
    cursor.execute(sql)


def write2db(tb_name, rsp_dic, conn, cur):
    keys = ', '.join(key + '_F' for key in rsp_dic.keys())
    # values = ', '.join(['%s'] * len(rsp_dic))
    values = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12'
    sql = 'INSERT INTO {tb_name}({keys}) VALUES ({values})'.format(tb_name=tb_name, keys=keys, values=values)
    # print(sql)
    print(list(rsp_dic.values()))
    # conn = get_db_conn()
    # cur = conn.cursor()
    cur.execute(sql, list(rsp_dic.values()))
    conn.commit()
    close_db(cur, conn)


def fetch_advertiser_data(tb_name, advertiser_ids, start_date, end_date):
    for advertiser_id in advertiser_ids:
        try:
            for page in range(1, 2):
                response = client.get_advertiser_daily_stat(advertiser_id, start_date, end_date, page)
                rsp_dic = response['data']['list'][0]
                print(rsp_dic)
                conn = get_db_conn()
                cur = conn.cursor()
                # del_data(cur, tb_name, advertiser_id, start_date)
                # write2db(tb_name, rsp_dic)
        except Exception as e:
            print('Error 1: %s' % e)
            pass
        continue


# advertiser_ids = [1692445980939271, 1690834797520903, 1690635545155608, 1678971206080525, 1678971205601288,
#                   1678971205260301, 1678971204908045, 1678971179505677, 1692446581753869, 1692446503475214,
#                   1692446580487182, 1692446580941837, 1692446581382152, 1668529402661896, 1690550581541902,
#                   1690550581083149, 1690550580623373, 1668459097343000, 1688580006747278, 1689568285194248,
#                   1689568285708301, 1689568289877005, 1689568290292750, 1689568290787335, 1689475569015880,
#                   1688759945497678]
advertiser_ids = [1692445980939271, 1690834797520903, 1690635545155608, 1678971206080525, 1678971205601288,
                  1678971205260301, 1678971204908045, 1678971179505677]
advertiser_ids2 = [1692446581753869, 1692446503475214, 1692446580487182, 1692446580941837, 1692446581382152,
                   1668529402661896, 1690550581541902, 1690550581083149, 1690550580623373, 1668459097343000]
advertiser_ids3 = [1688580006747278, 1689568285194248, 1689568285708301, 1689568289877005, 1689568290292750,
                   1689568290787335]
advertiser_ids4 = [1689475569015880, 1688759945497678]
# start_date = '2021-02-23'
start_date = str(date.today() + timedelta(days=0))
end_date = str(date.today())

tb_name = 'SC61.T_TOUTIAO_FI'

for i in range(1, 5):
    start_date = str(date.today() + timedelta(days=-i))
    # print(start_date)
    tb_name = 'SC61.T_TOUTIAO_FI'
    fetch_advertiser_data(tb_name, advertiser_ids4, start_date, start_date)
