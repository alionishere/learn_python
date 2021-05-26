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


def del_data(cursor, tb_name, if_trun, t_date=None):
    if if_trun:
        sql = "truncate table sc61.%s" % tb_name
    else:
        sql = "delete from sc61.%s where date_f = '%s'" % (tb_name, t_date)
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


def write2db(conn, cur, tb_name, rsp_dic, values):
    keys = ', '.join(key + '_c' for key in rsp_dic.keys())
    # values = ', '.join(['%s'] * len(rsp_dic))
    sql = 'INSERT INTO sc61.{tb_name}({keys}) VALUES ({values})'.format(tb_name=tb_name, keys=keys, values=values)
    print(sql)
    print(list(rsp_dic.values()))
    cur.execute(sql, list(rsp_dic.values()))
    # conn.commit()
    # close_db(cur, conn)


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


def get_user_info_all(access_token, tb_name):
    total_page = math.ceil(get_user_info_batch(access_token).json()['data']['total'] / 50)
    values = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11'
    conn = get_db_conn()
    cur = conn.cursor()
    del_data(cur, tb_name, True)
    for p in range(1, total_page + 1):
        user_info_lst = get_user_info_batch(access_token, p).json()['data']['list']
        for user_info_dic in user_info_lst:
            write2db(conn, cur, tb_name, user_info_dic, values)
        print(get_user_info_batch(access_token, p).json()['data']['list'])
    conn.commit()
    close_db(cur, conn)


def get_goods_list_all(access_token, tb_name):
    resource_type_lst = [1, 2, 3, 4, 6, 20]  # 商品类型 1.图文 2.音频 3.视频 4.直播 6.专栏 20.电子书
    conn = get_db_conn()
    cur = conn.cursor()
    values = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10'
    del_data(cur, tb_name, True)
    for resource_type in resource_type_lst:
        # total_page = math.ceil(get_goods_list(access_token, 1, resource_type).json()['data']['total'] / 50)
        total_page = 2
        print('resource_type: %s' % resource_type)
        for p in range(1, total_page + 1):
            # print('page: %s' % p)
            goods_lst = get_goods_list(access_token, p, resource_type).json()['data']['list']
            for goods_dic in goods_lst:
                write2db(conn, cur, tb_name, goods_dic, values)
            print(goods_lst)


# get_user_info_all(access_token, total_page)
# print(fetch_access_token().text)
# import sys
# sys.exit(0)
access_token = fetch_access_token().json()['data']['access_token']
# get_user_info_all(access_token, 't_xiaoe_user_info')
get_goods_list_all(access_token, 't_xiaoe_goods_list')
