# -*- coding:utf-8 -*

from docx import Document
import numpy as np
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
import os
import time


def get_df(table, tb_column):
    data = []
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
    df.columns = tb_column
    return df


# 写入数据库
def write2db(df, tb_name):
    engine = create_engine('postgresql+psycopg2://postgres:postgres@192.250.107.198/postgres')
    # df.to_sql(name='financial_derivative2', con=engine)
    df.to_sql(name=tb_name, con=engine, if_exists='replace', index=None)


# 获取表格字段名
def get_tb_varname(table):
    varname = []
    for i in range(len(table.columns)):
        varname.append(table.cell(0, i).text)
    return varname


if __name__ == '__main__':
    # filename = r'E:\tmp\2020-04-25-601555.SH-601555东吴证券2019年年度报告.doc'
    filename = r"E:\tmp\test1.docx"

    doc = Document(filename)
    tables = doc.tables
    print(len(tables))

    ###########################
    # 存出保证金              #
    ###########################
    table = tables[142]
    table2 = tables[143]
    tb_column = ['item', 'ending_book_balance_foreign_currency_amount', 'ending_book_balance_conversion_rate',
                  'ending_book_balance_rmb', 'opening_book_balance_foreign_currency_amount',
                  'opening_book_balance_conversion_rate', 'opening_book_balance_rmb']
    df = get_df(table, tb_column)
    df = df[1:]
    df2 = get_df(table2, tb_column)
    df_concat = pd.concat([df, df2])
    # print(df_concat.iloc[1:])
    # 写入数据库
    write2db(df_concat, 't_guaranteed_deposits_paid')

    ###########################
    # 资本公积 P: 193         #
    ###########################
    table = tables[207]
    tb_column = ['item', 'initial_balance', 'current_increase', 'current_decrease', 'closing_balance']
    df = get_df(table, tb_column)
    print(df)
    write2db(df, 't_capital_reserve')

    ###########################
    # 库存股   P: 193         #
    ###########################
    table = tables[208]
    tb_column = ['item', 'initial_balance', 'current_increase', 'current_decrease', 'closing_balance']
    df = get_df(table, tb_column)
    print(df)
    write2db(df, 't_treasure_stock')
