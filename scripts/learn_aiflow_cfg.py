# -*- coding: utf-8 -*-
import base64
import pymysql as pm
import psycopg2 as pg
import cx_Oracle


def get_group_flag(group_no):
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
    pwd = base64.b64decode(pwd)
    ip = '192.250.107.198'
    origin_db_name = 'public'
    res = query_mysql(ip, user, pwd, origin_db_name, sql)
    db_ip = res[1]
    db_user = res[2]
    db_pwd = base64.b64decode(res[3]).decode('utf-8')
    db_name = res[4]
    db_sql = res[5]
    flag = ''
    if res[0].lower() == 'oracle':
        flag = query_oracle(db_ip, db_user, db_pwd, db_name, db_sql)[0]
    elif res[0].lower() == 'mysql':
        flag = query_mysql(db_ip, db_user, db_pwd, db_name, db_sql)[0]
    elif res[0].lower() == 'postgresql':
        flag = query_pg(db_ip, db_user, db_pwd, db_name, db_sql)[0]
    else:
        print("Database type %s dos not exist!" % res[0])
    return flag


def query_mysql(ip, user, pwd, db_name, sql):
    db_con = pm.connect(ip, user, pwd, db_name)
    cur = db_con.cursor()
    cur.execute(sql)
    res = cur.fetchone()
    return res


def query_pg(ip, user, pwd, db_name, sql):
    db_con = pg.connect(database=db_name, user=user, password=pwd, host=ip, port='5432')
    cur = db_con.cursor()
    cur.execute(sql)
    res = cur.fetchone()
    return res


def query_oracle(ip, user, pwd, db_name, sql):
    db_con = cx_Oracle.connect(user, pwd, ip + ':' + '1521' + '/' + db_name)
    cur = db_con.cursor()
    cur.execute(sql)
    res = cur.fetchone()
    return res


if __name__ == '__main__':
    print(get_group_flag("01"))
    print(get_group_flag("02"))
    print(get_group_flag("03"))
