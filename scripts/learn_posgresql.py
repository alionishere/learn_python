import psycopg2 as pg
import pandas as pd


def get_db_conn():
    db_name = 'postgres'
    db_user = 'postgres'
    # pwd = 'postgres'
    pwd = 'postgres'
    # host = '127.0.0.1'
    host = '192.250.107.198'
    port = '5432'
    return pg.connect(database=db_name, user=db_user, password=pwd, host=host, port=port)


def generate_task(task_id, cmd):
    task_id = cmd
    return task_id


def generate_sql():
    sql = "insert into public.t_task_cfg values ('PT005', 'PT', '', 'echo ''this is pt004''', '03', '005', 0)"
    return sql


conn = get_db_conn()
cur = conn.cursor()
sql = '''
select task_id
       ,task_topic
       ,pre_task_id
       ,task_cmd
       -- ,task_group_no
  from public.t_task_cfg
 where task_group_no = 0
 order by task_oder
'''
# sql = generate_sql()
cur.execute(sql)
res = cur.fetchall()
res_pd = pd.DataFrame(res, columns=["task_id", 'task_topic', "pre_task_id", "task_cmd"])
cur.close()
conn.close()

task_id_dic = {}
for index, row in res_pd[['task_id', 'task_cmd']].iterrows():
    task_id_dic.setdefault(row['task_id'], generate_task(row['task_id'], row['task_cmd']))

print(task_id_dic)
print(res_pd)
print('--*' * 15)

topics = set()
for data in res:
    topics.add(data[1])

for topic in topics:
    task_id_df = res_pd[['task_id', 'pre_task_id', 'task_cmd']][res_pd.task_topic == topic]
    # task_id_df = res_pd.iloc[:, [0, 2, 3]][res_pd.task_topic == topic]
    task_ids = []
    for index, row in task_id_df.iterrows():
        if row['pre_task_id'] != '' and row['pre_task_id'] is not None:
            pre_task_ids = []
            for pre_task in row['pre_task_id'].split(','):
                pre_task_ids.append(task_id_dic[pre_task.strip()])
            print('%s: %s' % (row['task_id'], pre_task_ids))
        task_ids.append(task_id_dic[row['task_id']])
    print(task_ids)
    print('-*' * 15)
    print(task_id_df)
    print('-**--' * 25)
