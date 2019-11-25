import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime,timedelta
from pyhive import hive
from datetime import datetime, date, timedelta
import time
import configparser
import sys
sys.path.append('/root/base_data')
import check_schedule as cs
sys.path.append('/root/base_data/scripts/python')
import get_exe_info as gei

    
def start2check(**kwargs):
    while True:
        flag = gei.get_dag_state_flag() # or param1: execution_date; param2: dag_id
        # flag = 0
        # 1: upstream dag is ready; 0: upstream dag is not ready
        pre_dag_id = 'task_base_name'
        if flag == '0':
            print('-*' * 50)
            print('The upstream dag is not ready!')
            time.sleep(120)
        elif flag == '1':
            print('The upstream dag %s is ready. Now launch the task of monitor!' %pre_dag_id)
            # return (date.today() + timedelta(days=-1)).strftime('%Y%m%d')
            return date.today().strftime('%Y%m%d')
        else:
            print('Get dag state error!')
            sys.exit(0)


default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    #'end_date':datetime(2100,12,31),
}


dag = DAG('task_monitor',
default_args=default_args,
schedule_interval='30 3 * * *',
)


t1 = PythonOperator(
    task_id='start2check',
    python_callable=start2check,
    # op_kwargs={'task_group_no':'01', 'tx_date':(date.today() + timedelta(days=-1)).strftime('%Y%m%d')},
    provide_context=True,
    dag=dag)

cfg_file = '/root/base_data/conf/task_monitor_cfg.conf'
cfp = configparser.ConfigParser()
cfp.read(cfg_file)

for task_theme in cfp.sections():
    itms = cfp.items(task_theme)
    tasks = []
    for i in range(len(itms)):
        task = BashOperator(
            task_id=itms[i][0].upper(),
            bash_command=itms[i][1],
            dag=dag)
        tasks.append(task)
    for t in range(len(tasks)):
        if t == 0:
            t1 >> tasks[t]
        else:
           tasks[t-1] >> tasks[t]