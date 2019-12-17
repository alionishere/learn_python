from builtins import range
import psycopg2 as pg
import pandas as pd
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
import airflow_cfg as ac
import time
import configparser
from airflow.operators.dummy_operator import DummyOperator
# import sys

# user_date = airflow.utils.dates.days_ago(1)
# user_date = airflow.utils.dates.hours_ago(1)
# user_date = datetime(2019, 12, 1, 10, 10, 10)
# with open('/opt/logs/time.dat', 'a') as f:
#     f.write('\n' + str(user_date))

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    # 'start_date': datetime(2019, 12, 5, 13, 10, 10),
}


def gen_args(**kwargs):
    return {
        'owner': 'root',
        'start_date': kwargs['start_date']
    }


def gen_dag(dag_id, sh_interval):
    dag = DAG(
        dag_id=dag_id,
        default_args=args,
        schedule_interval=sh_interval,
        dagrun_timeout=timedelta(minutes=60),
    )
    return dag


def gen_ck_task(fst_task, task_group_no, start_interval, if_ck_date, dag):
    fst_task = PythonOperator(
        task_id='start2check',
        python_callable=start2check,
        op_kwargs={'task_group_no': task_group_no,
                   'tx_date': (date.today() + timedelta(days=start_interval)).strftime('%Y%m%d'),
                   'start_interval': start_interval,
                   'if_ck_date': if_ck_date},
        provide_context=True,
        dag=dag)
    return fst_task


def start2check(**kwargs):
    while True:
        task_group_no = kwargs['task_group_no']
        tx_date = kwargs['tx_date']
        if_ck_date = kwargs['if_ck_date']
        start_interval = kwargs['start_interval']
        # or param1: execution_date; param2: dag_id
        flag = ac.get_group_flag(task_group_no, if_ck_date, tx_date)
        # flag = 0
        # 1: upstream dag is ready; 0: upstream dag is not ready
        execution_date = (kwargs['execution_date'] + timedelta(days=start_interval)).strftime('%Y%m%d')
        tx_date = execution_date
        for i in range(10):
            with open('/opt/logs/time.dat', 'a') as f:
                f.write('\nexecution_date: ' + execution_date)
            print('-*' * 50)
            print('Test dag problem......!')
            time.sleep(3)

        if flag == '0':
            print('-*' * 50)
            print('The upstream dag is not ready!')
            time.sleep(120)
        elif flag == '1':
            print('The upstream dag is ready. Now launch the task of monitor!')
            # return (date.today() + timedelta(days=-1)).strftime('%Y%m%d')
            return tx_date
        else:
            print('Get dag state error!')
            assert False
            # sys.exit(0)


def get_db_conn():
    db_name = 'postgres'
    db_user = 'postgres'
    pwd = 'postgres'
    host = '192.250.107.198'
    port = '5432'
    return pg.connect(database=db_name, user=db_user, password=pwd, host=host, port=port)


def generate_task(task_id, cmd, dag):
    task_id = BashOperator(
        task_id=task_id,
        bash_command=cmd,
        dag=dag,
    )
    return task_id


def get_query_res(task_group_no):
    conn = get_db_conn()
    cur = conn.cursor()
    sql = '''
    select task_id
           ,task_topic
           ,pre_task_id
           ,task_cmd
      from public.t_task_cfg
     where task_group_no = '%s' and is_validity = 0
     order by task_oder
    ''' % task_group_no
    cur.execute(sql)
    res = cur.fetchall()
    return res


def gen_depending(dag, fst_task, task_group_no):
    res = get_query_res(task_group_no)
    res_pd = pd.DataFrame(res, columns=["task_id", "task_topic", "pre_task_id", "task_cmd"])

    task_id_dic = {}
    for index, row in res_pd[['task_id', 'task_cmd']].iterrows():
        task_id_dic.setdefault(row['task_id'], generate_task(row['task_id'], row['task_cmd'], dag))

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
                fst_task >> task_ids[t]
            else:
                task_ids[t - 1] >> task_ids[t]



def gen_task(task_id, dag):
    task = DummyOperator(
        task_id=task_id,
        dag=dag,
    )
    return task


####################
# def start(cfg_path='../conf/task_conf.cfg'):
cfg_path='../conf/task_conf.cfg'
cfp = configparser.RawConfigParser()
cfp.read(cfg_path)
groups = cfp.sections()
# print(groups)
lst = []
for group in groups:
    lst.append(group)
# lst = ['01']
# print(lst)

# for group in lst:
#     dag_id = cfp.get(group, 'dag_id')
#     task_group_no = cfp.get(group, 'task_group_no')
#     start_interval = int(cfp.get(group, 'start_interval'))
#     if_ck_date = cfp.get(group, 'if_ck_date')
#     sh_interval = cfp.get(group, 'sh_interval')
#     print(dag_id, sh_interval)
#     # dag = gen_dag(dag_id, sh_interval)
###############################################
#  dag: test_501                              #
###############################################
dag_id = 'test_501'
sh_interval = '0 1 * * *'
task_group_no = '03'
start_interval = 0
if_ck_date = True
dag = gen_dag(dag_id, sh_interval)
fst_task = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag)
gen_depending(dag, fst_task, task_group_no)

###############################################
#  dag: test_502                              #
###############################################
dag_id = 'test_502'
sh_interval = '0 1 * * *'
task_group_no = '02'
start_interval = -1
if_ck_date = False
dag1 = gen_dag(dag_id, sh_interval)
fst_task1 = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag1)
gen_depending(dag1, fst_task1, task_group_no)


###############################################
#  dag: test_503                              #
###############################################
dag_id = 'test_503'
sh_interval = '0 1 * * *'
task_group_no = '04'
start_interval = 0
if_ck_date = False
dag_3 = gen_dag(dag_id, sh_interval)
fst_task_3 = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag_3)
gen_depending(dag_3, fst_task_3, task_group_no)


###############################################
#  dag: test_504                              #
###############################################
dag_id = 'test_504'
sh_interval = '0 1 * * *'
task_group_no = '03'
start_interval = 0
if_ck_date = False
dag_4 = gen_dag(dag_id, sh_interval)
fst_task_4 = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag_4)
gen_depending(dag_4, fst_task_4, task_group_no)


###############################################
#  dag: test_505                              #
###############################################
dag_id = 'test_505'
sh_interval = '0 1 * * *'
task_group_no = '04'
start_interval = 0
if_ck_date = False
dag_5 = gen_dag(dag_id, sh_interval)
fst_task_5 = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag_5)
gen_depending(dag_5, fst_task_5, task_group_no)


###############################################
#  dag: test_506                              #
###############################################
dag_id = 'test_506'
sh_interval = '0 * * * *'
task_group_no = '02'
start_interval = 0
if_ck_date = False
dag_6 = gen_dag(dag_id, sh_interval)
fst_task_6 = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag_6)
gen_depending(dag_6, fst_task_6, task_group_no)


###############################################
#  dag: test_507                              #
###############################################
dag_id = 'test_507'
sh_interval = '0 * * * *'
task_group_no = '02'
start_interval = 0
if_ck_date = False
dag_7 = gen_dag(dag_id, sh_interval)
fst_task_7 = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag_7)
gen_depending(dag_7, fst_task_7, task_group_no)


###############################################
#  dag: test_508                              #
###############################################
dag_id = 'test_508'
sh_interval = '0 * * * *'
task_group_no = '02'
start_interval = 0
if_ck_date = False
dag_8 = gen_dag(dag_id, sh_interval)
fst_task_8 = gen_ck_task(dag_id, task_group_no, start_interval, if_ck_date, dag_8)
gen_depending(dag_8, fst_task_8, task_group_no)
