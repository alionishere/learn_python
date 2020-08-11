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


# 对比两个表字段差异，用于判断给定表结构对应的表
def retrieve_tgt_tb(src_tb_header, tgt_tb_header):
    if len(set(src_tb_header).difference(set(tgt_tb_header))) == 0:
        return True
    else:
        return False


def get_tgt_tb_header(tgt_tb):
    tgt_tb_header = []
    for j in range(len(tgt_tb.columns)):
        tgt_tb_header.append(tgt_tb.cell(0, j).text.strip())
    return tgt_tb_header


def get_tgt_tb(src_tbs, src_tb_header):
    for i in range(len(tables)):
        if retrieve_tgt_tb(src_tb_header, get_tgt_tb_header(tables[i])):
            print('The index of target table is %s' %i)
            return tables[i]
    return None


def get_tgt_tb_content(tgt_tb):
    tgt_tb_content = []
    for i in range(1, len(tgt_tb.rows)):
        for j in range(len(tgt_tb.columns)):
            tgt_tb_content.append(tgt_tb.cell(i, j).text.strip())
    return tgt_tb_content


if __name__ == '__main__':
    # filename = r'E:\tmp\2020-04-25-601555.SH-601555东吴证券2019年年度报告.doc'
    filename = r"E:\tmp\test1.docx"

    doc = Document(filename)
    tables = doc.tables
    print(len(tables))

    # ###########################
    # # 存出保证金              #
    # ###########################
    # table = tables[142]
    # table2 = tables[143]
    # tb_column = ['item', 'ending_book_balance_foreign_currency_amount', 'ending_book_balance_conversion_rate',
    #               'ending_book_balance_rmb', 'opening_book_balance_foreign_currency_amount',
    #               'opening_book_balance_conversion_rate', 'opening_book_balance_rmb']
    # df = get_df(table, tb_column)
    # df = df[1:]
    # df2 = get_df(table2, tb_column)
    # df_concat = pd.concat([df, df2])
    # # print(df_concat.iloc[1:])
    # # 写入数据库
    # write2db(df_concat, 't_guaranteed_deposits_paid')
    #
    # ###########################
    # # 资本公积 P: 193         #
    # ###########################
    # table = tables[207]
    # tb_column = ['item', 'initial_balance', 'current_increase', 'current_decrease', 'closing_balance']
    # df = get_df(table, tb_column)
    # print(df)
    # write2db(df, 't_capital_reserve')
    #
    # ###########################
    # # 库存股   P: 193         #
    # ###########################
    # table = tables[208]
    # tb_column = ['item', 'initial_balance', 'current_increase', 'current_decrease', 'closing_balance']
    # df = get_df(table, tb_column)
    # print(df)
    # write2db(df, 't_treasure_stock')

    # ###########################
    # # 库存股 P: 193           #
    # ###########################
    table = tables[208]
    tb_column = ['item', 'initial_balance', 'current_increase', 'current_decrease', 'closing_balance']
    tb_header = ['项目', '期末余额', '本期增加', '本期减少', '期初余额']
    # df = get_df(table, tb_column)
    # print(df)
    print(get_tgt_tb_content(get_tgt_tb(tables, tb_header)))



