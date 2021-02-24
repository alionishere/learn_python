#!/usr/bin/env python3
import requests
import cx_Oracle
from datetime import date, datetime
import logging
from sqlalchemy import desc, create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

# 配置日志
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='D:/whk/log/toutiao.log', level=logging.INFO, format=LOG_FORMAT)


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

    def fetch_clue_list(self, advertiser_ids, start_time, end_time, page=1, size=100):
        url = 'https://ad.oceanengine.com/open_api/2/tools/clue/get/'
        headers = {
            'Access-Token': self.access_token,
        }
        data = {
            'advertiser_ids': advertiser_ids,
            'start_time': start_time,
            'end_time': end_time,
            'page': page,
            'page_size': size,
        }
        return requests.get(url, headers=headers, json=data)

    def get_advertiser_daily_stat(self, advertiser_id, start_date, end_date, page):
        open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
        uri = "2/advertiser/fund/daily_stat/"
        url = open_api_url_prefix + uri
        params = {
            "advertiser_id": advertiser_id,
            "start_date": start_date,
            "end_date": end_date,
            "page": page,
            "page_size": 10
        }
        headers = {"Access-Token": self.access_token}
        rsp = requests.get(url, json=params, headers=headers)
        rsp_data = rsp.json()
        return rsp_data


client = OEClient()

###################################
# refresh token
###################################
with open('D:/whk/log/refresh_token2.dat', 'r') as fr:
    refresh_token = fr.read()
# rps_data = client.fetch_access_token(auth_code='44486644382e5ee400609dfe0fc54ff9f5596c05')
# print(rps_data.text)

# refresh_token = '9522ab5f46fab3d99e3486354fff8620ebc3f208'
rsp_refresh_token = client.refresh_access_token(refresh_token)
refresh_token = rsp_refresh_token['data']['refresh_token']
print('refresh_token: %s' % refresh_token)

logging.info('refresh_token: %s' % refresh_token)
with open('D:/whk/log/refresh_token2.dat', 'w') as fw:
    fw.write(refresh_token)

client.access_token = rsp_refresh_token['data']['access_token']
logging.info('access_token: %s' % client.access_token)
print('access_token: %s' % client.access_token)


def get_db_conn():
    return cx_Oracle.connect('kingstar', 'kingstar', '10.29.7.211:1521/siddc01')


advertiser_ids = [1692445980939271, 1690834797520903, 1690635545155608, 1678971206080525, 1678971205601288,
                  1678971205260301, 1678971204908045, 1678971179505677]
start_date = '2021-02-23'
end_date = '2021-02-23'
advertiser_id = 1692445980939271
for page in range(1, 2):
    response = client.get_advertiser_daily_stat(advertiser_id, start_date, end_date, page)
    print(response)
    # rsp_lst = response.json()['data']['list']

    # conn = get_db_conn()
    # cur = conn.cursor()
