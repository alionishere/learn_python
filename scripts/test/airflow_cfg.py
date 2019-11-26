# -*- coding: utf-8 -*-
import base64
import pymysql as pm
import psycopg2 as pg
import cx_Oracle
import sys
from datetime import date, timedelta


def get_group_flag(group_no, if_ck_date=True, tx_date=(date.today() + timedelta(days=-1)).strftime('%Y%m%d')):
    if if_ck_date:
        is_trading_date(tx_date)

    sql = '''
    select db_type
           ,db_ip
           ,db_user
           ,db_pwd
           ,db_name
           ,sql_stmt
      from public.t_task_group_flag
     where group_no = '%s'
    ''' % group_no
    user = 'root'
    pwd = 'QWRtaW5AMTIz'.encode(encoding='utf-8')
    pwd = base64.b64decode(pwd).decode('utf-8')
    ip = '192.250.107.198'
    origin_db_name = 'public'
    res = query(get_mysql_conn(ip, user, pwd, origin_db_name), sql)
    if res is None:
        print('Group no %s does not exist!' % group_no)
        sys.exit(0)

    db_ip = res[1]
    db_user = res[2]
    db_pwd = base64.b64decode(res[3]).decode('utf-8')
    db_name = res[4]
    db_sql = res[5]
    flag = ''
    if res[0].lower() == 'oracle':
        flag = query(get_oracle_conn(db_ip, db_user, db_pwd, db_name), db_sql)[0]
    elif res[0].lower() == 'mysql':
        flag = query(get_mysql_conn(db_ip, db_user, db_pwd, db_name), db_sql)[0]
    elif res[0].lower() == 'postgresql':
        flag = query(get_pg_conn(db_ip, db_user, db_pwd, db_name), db_sql)[0]
    else:
        print("Database type %s dos not exist!" % res[0])
    return flag


def is_trading_date(tx_date):
    print('Check over')
    return True


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
    cur.close()
    conn.close()
    return res


if __name__ == '__main__':
    print(get_group_flag("01"))
    print(get_group_flag("02"))
    print(get_group_flag("03"))
    # print(get_group_flag('04'))
    flag = get_group_flag("03")
    if flag == '0':
        print('this is postgres')
