# -*- coding: utf-8 -*-
import pymysql as pm
import sys
from datetime import datetime, date, timedelta
sys.path.append('/root/base_data')
import check_schedule as cs


def get_mysql_conn():
    db = pm.connect('127.0.0.1', 'root', 'Dwzq!951753', 'airflow')
    return db.cursor()


def close_conn(cursor):
    cursor.close


def get_dag_info(query_date):
    query_dag_sql = '''
    select dag_id
           ,date_format(start_date, '%%Y/%%m/%%d %%H:%%i:%%S') as start_date
           ,date_format(end_date, '%%Y/%%m/%%d %%H:%%i:%%S') as end_date
           ,state
           ,round(end_date - start_date) as duration
      from airflow.dag_run
     where date_format(start_date, '%%Y%%m%%d') = %s
    ''' %query_date
    cur = get_mysql_conn()
    cur.execute(query_dag_sql)
    dag_res = cur.fetchall()
    for dag_id, start_date, end_date, state, duration in dag_res:
        print('%s  %s  %s  %s  %s' %(dag_id,start_date, end_date, duration, state))

    close_conn(cur)


def get_task_info(query_date):
    query_task_sql = '''
    select dag_id
           ,task_id
           ,date_format(start_date, '%%Y%%m%%d %%H%%i%%S') as start_date
           ,date_format(end_date, '%%Y%%m%%d %%H%%i%%S') as end_date
           ,duration
           ,state
      from airflow.task_instance
     where dag_id = 'task_daily_1'
       and date_format(start_date, '%%Y%%m%%d') = %s
    ''' %query_date
    cur = get_mysql_conn()
    cur.execute(query_task_sql)
    task_res = cur.fetchall()
    for dag_id, task_id, start_date, end_date, duration, state in task_res:
        print('%s  %s  %s  %s  %s' %(task_id,start_date, end_date, duration, state))

    close_conn(cur)


def get_dag_state_flag(exe_date=(date.today() + timedelta(days=-1)).strftime('%Y%m%d'), dag_id='task_base_data'):
    cs.is_trade_day(exe_date)
    sql = '''
    select state 
      from airflow.dag_run 
     where date_format(execution_date, '%%Y%%m%%d') = '%s'
       and dag_id = '%s'
    ''' %(exe_date, dag_id)
    cur = get_mysql_conn()
    cur.execute(sql)
    task_res = cur.fetchone()
    if task_res is not None and task_res[0] == 'success':
        return '1'
    else:
        return '0'


def helper():
    msg = '''
    Parameter error!
    Example: python get_exe_info.py / python get_exe_info.py 20190101
         or: python get_exe_info.py 20190101 task_base_data_daily_new
    '''
    print(msg)


if __name__ == '__main__':
    query_date = (date.today() + timedelta(days=-1)).strftime('%Y%m%d')
    dag_id = 'task_base_data_daily_new'
    if len(sys.argv) == 1:
        print get_dag_state_flag()
    elif len(sys.argv) == 2:
        print get_dag_state_flag(sys.argv[1])
    elif len(sys.argv) == 3:
        get_dag_state_flag(sys.argv[1], sys.argv[2])
    else: 
        helper()
