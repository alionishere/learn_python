# -*- coding: utf-8 -*-
import base64
import pymysql as pm
# import psycopg2 as pg
import cx_Oracle
import sys
from datetime import date, timedelta


def get_group_flag(group_no, if_ck_date=True, tx_date=(date.today() + timedelta(days=-1)).strftime('%Y%m%d')):
    sql = '''
    select db_type
           ,db_ip
           ,db_user
           ,db_pwd
           ,db_name
           ,sql_stmt
      from base_data.tmg_task_group_flag
     where task_group_no = '%s'
    ''' % group_no
    user = 'base_data'
    pwd = 'YmFzZV9kYXRh'.encode(encoding='utf-8')
    pwd = base64.b64decode(pwd).decode('utf-8')
    ip = '172.22.131.28'
    origin_db_name = 'orcl'

    # check tx_date
    if if_ck_date:
        is_trading_date(tx_date, get_oracle_conn(ip, user, pwd, origin_db_name))
    else:
        print('Skip trading day checking!')

    db_conn = get_oracle_conn(ip, user, pwd, origin_db_name)
    res = query(db_conn, sql)
    if res is None:
        print('Group no %s does not exist!' % group_no)
        sys.exit(0)

    db_ip = res[1]
    db_user = res[2]
    db_pwd = base64.b64decode(res[3]).decode('utf-8')
    db_name = res[4]
    db_sql = res[5].read().replace('{$tx_date}', tx_date)
    flag = ''
    if res[0].lower() == 'oracle':
        flag = query(get_oracle_conn(db_ip, db_user, db_pwd, db_name), db_sql)[0]
    elif res[0].lower() == 'mysql':
        flag = query(get_mysql_conn(db_ip, db_user, db_pwd, db_name), db_sql)[0]
    elif res[0].lower() == 'postgresql':
        flag = query(get_pg_conn(db_ip, db_user, db_pwd, db_name), db_sql)[0]
    else:
        print("Database type %s dos not exist!" % res[0])

    db_conn.close()
    return flag


def is_trading_date(tx_date, conn):
    sql = 'SELECT JYRBS FROM KETTLE.JYR WHERE RQ = \'{}\''.format(tx_date)
    cur = conn.cursor()
    cur.execute(sql)
    re = cur.fetchone()[0]
    cur.close()
    conn.close()
    if re == '3':
        print('Check over! %s is a trading day!' % tx_date)
        return True
    else:
        print('Not a trading day!')
        sys.exit(0)


def get_mysql_conn(ip, user, pwd, db_name):
    return pm.connect(ip, user, pwd, db_name)


def get_pg_conn(ip, user, pwd, db_name):
    return pg.connect(database=db_name, user=user, password=pwd, host=ip, port='5432')


def get_oracle_conn(ip, user, pwd, db_name):
    return cx_Oracle.connect(user, pwd, ip + ':' + '1521' + '/' + db_name)


def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    res = cur.fetchone()
    return res


if __name__ == '__main__':
    print(get_group_flag("01"))
    print(get_group_flag("02"))
    print(get_group_flag("03"))
