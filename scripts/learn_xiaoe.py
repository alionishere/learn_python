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
logging.basicConfig(filename='D:/whk/log/xiaoe.log', level=logging.INFO, format=LOG_FORMAT)


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


def get_data_from_db(cursor, sql):
    try:
        # 解析sql语句
        cursor.parse(sql)
        cursor.execute(sql)
        # 捕获SQL异常
    except cx_Oracle.DatabaseError as e:
        print(e)
        logging.info(e)
    return cursor.fetchall()


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
        "resource_type": resource_type,
        "page_index": page,
        "page_size": 50
    }
    return requests.post(url, headers=headers, json=data)


def get_learn_record_by_rsc_id(access_token, user_id_dic=None, resource_id=None):
    url = 'https://api.xiaoe-tech.com/xe.user.leaning_record_by_resource.get/1.0.0'
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "access_token": access_token,
        "shop_id": "appuwfgd0ga8531",
        "resource_id": resource_id,
        "data": {
            "list": user_id_dic
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
    conn.commit()
    close_db(cur, conn)


def get_learn_record_by_rsc_id_all(access_token, user_id_dic_lst, rsc_id_lst, tb_name):
    values = ':1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17'
    conn = get_db_conn()
    cur = conn.cursor()
    del_data(cur, tb_name, True)
    for rsc_id in rsc_id_lst:
        for user_id_dic in user_id_dic_lst:
            res_data = get_learn_record_by_rsc_id(access_token, user_id_dic=user_id_dic,
                                                              resource_id=rsc_id).json()['data']
            if len(res_data) == 0:
                continue
            else:
                learn_record_lst = res_data['list']

            for lear_rcd_dic in learn_record_lst:
                write2db(conn, cur, tb_name, lear_rcd_dic, values)
            print(learn_record_lst)
    conn.commit()
    close_db(cur, conn)


def get_user_id_dic(user_id_lst, user_id_cnt):
    user_id_dic = {}
    if len(user_id_lst) <= user_id_cnt:
        for inx, user_id in enumerate(user_id_lst):
            user_id_dic[inx] = user_id
        # print(get_learn_record_by_rsc_id(access_token, user_id_dic=user_id_dic,
        #                                  resource_id='i_6094eddee4b071a81eb81c88').text)
        user_id_dic_lst.append(user_id_dic)
        return user_id_dic
    else:
        for inx, user_id in enumerate(user_id_lst):
            if inx >= user_id_cnt:
                break
            user_id_dic[inx % user_id_cnt] = user_id
        del user_id_lst[:user_id_cnt]
        # print(get_learn_record_by_rsc_id(access_token, user_id_dic=user_id_dic,
        #                                  resource_id='i_6094eddee4b071a81eb81c88').text)
        user_id_dic_lst.append(user_id_dic)
        return get_user_id_dic(user_id_lst, user_id_cnt)


def get_learn_record_main(user_id_cnt=100):
    sql_user = 'SELECT USER_ID_C FROM SC61.T_XIAOE_USER_INFO'
    sql_rsc = 'SELECT ID_C FROM SC61.T_XIAOE_GOODS_LIST'
    conn = get_db_conn()
    cur = conn.cursor()
    # 获取user_id列表
    user_ids = get_data_from_db(cur, sql_user)
    user_id_lst = [user_id[0] for user_id in user_ids]
    get_user_id_dic(user_id_lst, user_id_cnt)
    # 获取resource_id列表
    rsc_ids = get_data_from_db(cur, sql_rsc)
    rsc_id_lst = [rsc_id[0] for rsc_id in rsc_ids]
    close_db(cur, conn)
    get_learn_record_by_rsc_id_all(access_token, user_id_dic_lst, rsc_id_lst, 't_xiaoe_learn_record_by_rsc_id')


# main
access_token = fetch_access_token().json()['data']['access_token']
logging.info('-' * 50)
logging.info('Start to fetch user info.')
get_user_info_all(access_token, 't_xiaoe_user_info')
logging.info('Complete the data of user info crawl. And Start to fetch goods list.')
get_goods_list_all(access_token, 't_xiaoe_goods_list')
logging.info('Complete the data of goods list crawl. And Start to fetch learn record')
user_id_dic_lst = []
get_learn_record_main()
logging.info('complete the data of learn record crawl..')
