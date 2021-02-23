# -*- coding: utf-8 -*-
import xlrd
import cx_Oracle
from datetime import datetime
import logging

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='D:/whk/log/dwhk_%s.log' % datetime.now().strftime('%Y%m%d'), level=logging.INFO,
                    format=LOG_FORMAT)


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


def write2db(file_name, t_date, user_name, sql, tb_name, sheet_idx, row_limit, if_row_limit=True):
    readbook = xlrd.open_workbook(file_name)
    sheet = readbook.sheet_by_index(sheet_idx)  # 索引的方式，从0开始
    if if_row_limit:
        nrows = row_limit
    else:
        nrows = sheet.nrows  # 行
    ncols = sheet.ncols  # 列
    conn = get_db_conn()
    cur = conn.cursor()
    trunc_tb(cur, tb_name)
    for r in range(0, nrows):
        data = [sheet.cell(r, c).value for c in range(0, ncols)]
        data.append(user_name)
        data.append(t_date)
        insert_sql(cur, sql, data)
    conn.commit()
    close_db(cur, conn)


def run(file_name, tb_name_lst, sql_lst, sheet_interval=1, if_row_limit=True):
    t_date = datetime.now().strftime('%Y%m%d')
    user_name = 'dwhk'
    for i in range(len(tb_name_lst)):
        tb_name = tb_name_lst[i].split('&')[0]
        row_limit = int(tb_name_lst[i].split('&')[1])
        sql = sql_lst[i] % tb_name
        sheet_inx = i + sheet_interval
        write2db(file_name, t_date, user_name, sql, tb_name, sheet_inx, row_limit, if_row_limit)


# main
file_name = r'D:\whk\doc\附件1、证券公司风险控制指标并表监管报表.xls'
file_name2 = r'D:\whk\doc\附件2、并表风控指标报表明细.xlsx'
tb_name_lst = ['TMP_DWHK_RICS3601_IMP&31', 'TMP_DWHK_RICS3602_IMP&114', 'TMP_DWHK_RICS3603_IMP&29',
               'TMP_DWHK_RICS3604_IMP&77', 'TMP_DWHK_RICS3605_IMP&86', 'TMP_DWHK_RICS3606_IMP&13']
tb_name_lst2 = ['TMP_DWHK_INV_STOCK_BALANCE_IMP&28', 'TMP_DWHK_DER_BALANCE_IMP&19', 'TMP_DWHK_HEDGE_STK_BALANCE_IMP&41',
                'TMP_DWHK_EQUITY_INVEST_IMP&7', 'TMP_DWHK_FINANCE_BUSINESS_IMP&10', 'TMP_DWHK_ASSET_MANAGEMENT_IMP&8',
                'TMP_DWHK_DEBTS_IMP&14', 'TMP_DWHK_COLLATERALS_IMP&15', 'TMP_DWHK_ACCOUNT_BALANCE_IMP&12']
sql_lst = ['INSERT INTO SC61.%s(A,B,C,D,E,F,G,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9)',
           'INSERT INTO SC61.%s(A,B,C,D,E,F,G,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9)',
           'INSERT INTO SC61.%s(A,B,C,D,E,F,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8)',
           'INSERT INTO SC61.%s(A,B,C,D,E,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7)',
           'INSERT INTO SC61.%s(A,B,C,D,E,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7)',
           'INSERT INTO SC61.%s(A,B,C,D,E,F,G,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9)']
sql_lst2 = [
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,L,M,N,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)',
    'INSERT INTO SC61.%s(A,B,C,D,E,F,G,H,I,J,K,USER_NAME,DS_DATE) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)']

try:
    run(file_name, tb_name_lst, sql_lst)
except Exception as e:
    logging.error('read file 1 error!')
    logging.error(e)
else:
    logging.info('read file 1 successful!')

try:
    run(file_name2, tb_name_lst2, sql_lst2, sheet_interval=0, if_row_limit=False)
except:
    logging.error('read file 2 error!')
    logging.error(e)
else:
    logging.info('read file 2 successful!')

