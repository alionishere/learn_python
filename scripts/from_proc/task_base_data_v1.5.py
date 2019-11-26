import cx_Oracle
import pandas as pd
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from pyhive import hive
from datetime import datetime, date, timedelta
import time
import sys

sys.path.append('/root/base_data')
import check_schedule as cs

sys.path.append('/root/base_data/scripts/python')
import get_exe_info as gei

default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('task_base_data',
          default_args=default_args,
          schedule_interval='30 2 * * *',
          )


def start2check(**kwargs):
    while True:
        task_group_no = kwargs['task_group_no']
        tx_date = kwargs['tx_date']
        start_date = (date.today() + timedelta(days=-10)).strftime('%Y%m%d')
        # tx_date = '20190828'
        print('task group no :%s' % task_group_no)
        print('tx_date :%s' % tx_date)
        flag = cs.get_check_flag(task_group_no, tx_date)
        # 1: upstream data is ready; 0: upstream data is not ready
        if flag == '0':
            print('-*' * 50)
            print('The upstream data is not ready!')
            time.sleep(60)
        elif flag == '1':
            print('The task of %s is currently running!' % tx_date)
            return tx_date
        else:
            print('Check schedule error!')
            sys.exit(0)


def get_task_ids(sql):
    o_user = 'base_data'
    o_pwd = 'base_data'
    o_ip = '172.22.131.28'
    o_port = '1521'
    o_instance = 'orcl'
    db = cx_Oracle.connect(o_user, o_pwd, o_ip + ':' + o_port + '/' + o_instance)
    cur = db.cursor()
    cur.execute(sql)
    re = cur.fetchall()
    cur.close()
    db.close()
    return re


def generate_task(task_id, cmd):
    task_id = BashOperator(
        task_id=task_id,
        bash_command=cmd,
        dag=dag,
    )
    return task_id


t1 = PythonOperator(
    task_id='start2check',
    python_callable=start2check,
    op_kwargs={'task_group_no': '01', 'tx_date': (date.today() + timedelta(days=-1)).strftime('%Y%m%d')},
    provide_context=True,
    dag=dag)

sql = '''
select task_id
       ,task_topic
       ,pre_task_id
       ,task_cmd
  from base_data.tmg_task_cfg
 where task_group_no = 01
 order by task_id
'''

res = get_task_ids(sql)
res_pd = pd.DataFrame(res, columns=["task_id", "task_topic", "pre_task_id", "task_cmd"])

task_id_dic = {}
for index, row in res_pd[['task_id', 'task_cmd']].iterrows():
    task_id_dic.setdefault(row['task_id'], generate_task(row['task_id'], row['task_cmd']))

topics = set()
for data in res:
    topics.add(data[1])

for topic in topics:
    task_id_df = res_pd.iloc[:, [0, 2, 3]][res_pd.task_topic == topic]
    task_ids = []
    for index, row in task_id_df.iterrows():
        if row['pre_task_id'] != '' and row['pre_task_id'] is not None:
            pre_task_ids = []
            for pre_task in row['pre_task_id'].split(','):
                pre_task_ids.append(task_id_dic[pre_task])
            pre_task_ids >> task_id_dic[row['task_id']]
        task_ids.append(task_id_dic[row['task_id']])
    for t in range(len(task_ids)):
        if t == 0:
            t1 >> task_ids[t]
        else:
            task_ids[t - 1] >> task_ids[t]
