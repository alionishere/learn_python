# -*- coding: utf-8 -*-
import xlrd
import cx_Oracle
import openpyxl
from datetime import datetime


# 访问数据库，插入数据
def insert_sql(v_sql, data):
    conn = cx_Oracle.connect('kingstar/kingstar@10.111.30.11:1521/orcl')
    c = conn.cursor()
    try:
        c.parse(v_sql)
    except cx_Oracle.DatabaseError as e:
        print(e)
    c.execute(v_sql, data)
    conn.commit()
    c.close()
    conn.close()


readbook = xlrd.open_workbook(r'E:\tmp\附件1、证券公司风险控制指标并表监管报表.xls')
# readbook = xlrd.open_workbook(r'E:\tmp\标准模板 附件2、并表风控指标报表明细.xls')
t_date = datetime.now().strftime('%Y%m%d')
user_name = 'dwhk'
sql = 'INSERT INTO SC61.TMP_3601(A,B,C,D,E,F,G,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9)'
sheet = readbook.sheet_by_index(1)  # 索引的方式，从0开始
nrows = sheet.nrows  # 行
ncols = sheet.ncols  # 列
for r in range(0, nrows):
    data = [sheet.cell(r, c).value for c in range(0, ncols)]
    # for c in range(0, ncols):
    #     print(sheet.cell(r, c).value, end=',')
    print(data)
    insert_sql(sql, data)
