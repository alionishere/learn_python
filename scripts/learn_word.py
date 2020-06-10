# -*- coding:utf-8 -*

from docx import Document
import numpy as np
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
import os
import time


def get_db_conn():
    db_name = 'postgres'
    db_user = 'postgres'
    # pwd = 'postgres'
    pwd = 'postgres'
    # host = '127.0.0.1'
    host = '192.250.107.198'
    port = '5432'
    return pg.connect(database=db_name, user=db_user, password=pwd, host=host, port=port)


# filename = r'E:\tmp\2020-04-25-601555.SH-601555东吴证券2019年年度报告.doc'
filename = r"E:\tmp\test1.docx"

# print(os.path.abspath(filename))
# time.sleep(3)
doc = Document(filename)
tables = doc.tables

print(len(tables))
data = []
table = tables[142]
table2 = tables[143]

# 获取表格字段名
varname = []
for i in range(len(table.columns)):
    varname.append(table.cell(0, i).text)


# print(varname)


def get_df(table):
    # 获取表格数据
    for i in range(1, len(table.rows)):
        for j in range(len(table.columns)):
            data.append(table.cell(i, j).text)
    # print(data)

    # list to 1D array
    arr1 = np.array(data)
    # 2D array
    arr2 = arr1.reshape(len(table.rows) - 1, len(table.columns))
    # 2D array to 2D dataset
    df = pd.DataFrame(arr2)
    # 给数据集赋予变量名
    # df.columns = ['category','nominal_amount_end_hedging','fair_value_fund_end_hedging','fair_value_debt_end_hedging','nominal_amount_debt_end_unhedging','fair_value_fund_end_unhedging','fair_value_debt_end_unhedging','nominal_amount_start_hedging','fair_value_fund_start_hedging','fair_value_debt_start_hedging','nominal_amount_debt_start_unhedging','fair_value_fund_start_unhedging','fair_value_debt_start_unhedging']
    df.columns = ['item', 'ending_book_balance_foreign_currency_amount', 'ending_book_balance_conversion_rate',
                  'ending_book_balance_rmb', 'opening_book_balance_foreign_currency_amount',
                  'opening_book_balance_conversion_rate', 'opening_book_balance_rmb']
    return df


df = get_df(table)
df = df[1:]
print(df)
print('-*' * 30)
df2 = get_df(table2)
print(df2)
# 导出数据
# df.to_excel(r'E:/tmp/tst_excel.xlsx', index=False)

# 写入数据库
# db_conn = get_db_conn()
engine = create_engine('postgresql+psycopg2://postgres:postgres@192.250.107.198/postgres')
# df.to_sql(name='financial_derivative2', con=engine)
# df.to_sql(name='t_guaranteed_deposits_paid', con=engine)
