from builtins import range
from datetime import timedelta
import psycopg2 as pg
import pandas as pd
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='test_0003',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

t1 = BashOperator(
    task_id='t1',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)


def get_db_conn():
    db_name = 'postgres'
    db_user = 'postgres'
    pwd = 'postgres'
    host = '192.250.107.198'
    port = '5432'
    return pg.connect(database=db_name, user=db_user, password=pwd, host=host, port=port)


def generate_task(task_id, cmd):
    task_id = BashOperator(
        task_id=task_id,
        bash_command=cmd,
        dag=dag,
    )
    return task_id

conn = get_db_conn()
cur = conn.cursor()

sql = '''
select task_id
       ,task_topic
       ,pre_task_id
       ,task_cmd
  from public.t_task_cfg
 where task_group_no = 0 and is_validity = 0
 order by task_oder
'''
cur.execute(sql)
res = cur.fetchall()
res_pd = pd.DataFrame(res, columns=["task_id", "task_topic", "pre_task_id", "task_cmd"])
cur.close()
conn.close()

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
                pre_task_ids.append(task_id_dic[pre_task.strip()])
            pre_task_ids >> task_id_dic[row['task_id']]
        task_ids.append(task_id_dic[row['task_id']])
    for t in range(len(task_ids)):
        if t == 0:
            t1 >> task_ids[t]
        else:
            task_ids[t-1] >> task_ids[t]