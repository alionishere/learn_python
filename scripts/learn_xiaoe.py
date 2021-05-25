#!/usr/bin/env python3
import math
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


def get_db_conn():
    return cx_Oracle.connect('kingstar', 'kingstar', '10.29.7.211:1521/siddc01')


def close_db(cursor, conn):
    cursor.close()
    conn.close()


def del_data(cursor, tb_name, advertiser_id, if_trun, t_date):
    if if_trun:
        sql = "truncate table sc61.%s" % tb_name
    else:
        sql = "delete from sc61.%s where advertiser_id_f = %s and date_f = '%s'" % (tb_name, advertiser_id, t_date)
    print(sql)
    cursor.execute(sql)


def fetch_access_token():
    url = 'https://api.xiaoe-tech.com/token'
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "app_id": "appuwfgd0ga8531",
        "client_id": "xopqWENYl6r5831",
        "secret_key": "zcsR7GHMpfCi4Y0ynW40l9ihED6CC4OU",
        "grant_type": "client_credential"
    }
    return requests.get(url, headers=headers, json=data)


def write2db(conn, cur, tb_name, rsp_dic):
    keys = ', '.join(key for key in rsp_dic.keys())
    # values = ', '.join(['%s'] * len(rsp_dic))
    values = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11'
    sql = 'INSERT INTO {tb_name}({keys}) VALUES ({values})'.format(tb_name=tb_name, keys=keys, values=values)
    # print(sql)
    # print(list(rsp_dic.values()))
    cur.execute(sql, list(rsp_dic.values()))
    conn.commit()
    close_db(cur, conn)


def get_user_info_batch(access_token, page=1):
    url = 'http://api.xiaoe-tech.com/xe.user.batch.get/1.0.0'
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "access_token": access_token,
        "page": page,
        "page_size": 50
    }
    return requests.post(url, headers=headers, json=data)


def get_goods_list(access_token, page=1, resource_type=1):
    url = 'http://api.xiaoe-tech.com/api/xe.goods.list.get/3.0.0'
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "access_token": access_token,
        "resource_type": 1,
        "page_index": page,
        "page_size": 50
    }
    return requests.post(url, headers=headers, json=data)


def get_learn_record_by_resource_id(access_token, page=1, resource_type=1):
    url = 'http://api.xiaoe-tech.com/api/xe.goods.list.get/3.0.0'
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "access_token": access_token,
        "shop_id": "appuwfgd0ga8531",
        "resource_id": page,
        "data": {
            "list": {
                "0": "u_api_60aca190a6e36_i6BoF8mmR0",
            }
        }
    }
    return requests.post(url, headers=headers, json=data)


def get_user_info_all(access_token, total_page):
    for p in range(1, total_page + 1):
        # user_info_dic = get_user_info_batch(access_token, p).json()['data']['list'][0]
        print(get_user_info_batch(access_token, p).json()['data']['list'])
        # print(user_info_dic.values())


def get_goods_list_all(access_token):
    resource_type_lst = [1, 2, 3, 4, 6, 20]  # 商品类型 1.图文 2.音频 3.视频 4.直播 6.专栏 20.电子书
    # resource_type_lst = [1]  # 商品类型 1.图文 2.音频 3.视频 4.直播 6.专栏 20.电子书
    for resource_type in resource_type_lst:
        # total_page = math.ceil(get_goods_list(access_token, 1, resource_type).json()['data']['total'] / 50)
        total_page = 2
        print('resource_type: %s' % resource_type)
        for p in range(1, total_page + 1):
            # user_info_dic = get_user_info_batch(access_token, p).json()['data']['list'][0]
            print('page: %s' % p)
            print(get_goods_list(access_token, p, resource_type).json()['data']['list'])
            # print(user_info_dic.values())


# access_token = 'xe_60ac88358f1b6_o7m41XRzd6zUnNLXMvJS5AmhVvk3Why0PQXQgXfuDVxamfbBPNgNSnpIkSWlEqFoqp7HbMU7YQsR0Q0yu2ezRrUqcySuIlLiKj4ZDxGIW88AZFC2dGScgVJxTaWS3uT1NeyTkPR'
# user_info_cnt = get_user_info_batch(access_token).json()['data']['total']
# total_page = math.ceil(user_info_cnt / 50)
# get_user_info_all(access_token, total_page)
access_token = fetch_access_token().json()['data']['access_token']
print(get_goods_list_all(access_token))
