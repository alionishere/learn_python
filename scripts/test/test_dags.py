from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='test_00021',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

dag2 = DAG(
    dag_id='test_00020',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

run_this_last2 = DummyOperator(
    task_id='run_this_last2',
    dag=dag2,
)


# [START howto_operator_bash_template]
t1 = BashOperator(
    task_id='t1',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)

t2 = BashOperator(
    task_id='t2',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)

t3 = BashOperator(
    task_id='t3',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)

t4 = BashOperator(
    task_id='t4',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)
# [END howto_operator_bash_template]
t1 >> [t2,t3,t4] >> run_this_last

tt1 = BashOperator(
    task_id='tt1',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag2,
)

tt2 = BashOperator(
    task_id='tt2',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag2,
)

tt3 = BashOperator(
    task_id='tt3',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag2,
)

tt4 = BashOperator(
    task_id='tt4',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag2,
)
tt1 >> [tt2,tt3,tt4] >> run_this_last2