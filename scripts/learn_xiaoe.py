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


def fetch_access_token():
    url = 'https://ad.oceanengine.com/open_api/oauth2/access_token/'
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "app_id": 'appuwfgd0ga8531',
        "client_id": 'xopqWENYl6r5831',
        "secret_key": "zcsR7GHMpfCi4Y0ynW40l9ihED6CC4OU",
        "grant_type": "client_credential",
    }
    return requests.post(url, headers=headers, json=data)


print(fetch_access_token())
