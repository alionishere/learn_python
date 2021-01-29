# -*- coding: utf-8 -*-
import xlrd
import cx_Oracle
import time
from datetime import datetime


# 访问数据库，插入数据
def insert_sql(cur, v_sql, data):
    cur.execute(v_sql, data)


def get_db_conn():
    return cx_Oracle.connect('kingstar', 'kingstar', '10.29.7.211:1521/siddc01')


def close_db(cursor, conn):
    cursor.close()
    conn.close()


def trunc_tb(cur, tb_name):
    trunc_sql = 'TRUNCATE TABLE SC61.%s' % tb_name
    cur.execute(trunc_sql)


def write2db(file_name, t_date, user_name, sql, tb_name):
    readbook = xlrd.open_workbook(file_name)
    sheet = readbook.sheet_by_index(1)  # 索引的方式，从0开始
    nrows = sheet.nrows  # 行
    ncols = sheet.ncols  # 列
    conn = get_db_conn()
    cur = conn.cursor()
    trunc_tb(cur, tb_name)
    for r in range(0, nrows):
        data = [sheet.cell(r, c).value for c in range(0, ncols)]
        data.append(user_name)
        data.append(t_date)
        # print(data)
        insert_sql(cur, sql, data)
    conn.commit()
    close_db(cur, conn)


# main
file_name = r'D:\whk\doc\附件1、证券公司风险控制指标并表监管报表.xls'
# readbook = xlrd.open_workbook(r'D:\doc\标准模板 附件2、并表风控指标报表明细.xls')
# print(len(readbook.sheets()))
t_date = datetime.now().strftime('%Y%m%d')
user_name = 'dwhk'
tb_name = 'TMP_DWHK_RICS3601_IMP'
sql1 = 'INSERT INTO SC61.%s(A,B,C,D,E,F,G,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9)' % tb_name
# sql2 = 'INSERT INTO SC61.TMP_DWHK_RICS3601_IMP(A,B,C,D,E,F,G,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9)'

write2db(file_name, t_date, user_name, sql1, tb_name)
