import psycopg2 as pg


def get_db_conn():
    db_name = 'postgres'
    db_user = 'postgres'
    # pwd = 'postgres'
    pwd = 'postgres'
    # host = '127.0.0.1'
    host = '192.250.107.198'
    port = '5432'
    return pg.connect(database=db_name, user=db_user, password=pwd, host=host, port=port)


def generate_sql(topic_name, topic_no, group_no):
    sql = "insert into public.t_task_cfg values ('%s%s', '%s', '', 'echo ''this is %s%s''', '%s', '%s', 0)" \
          % (topic_name, topic_no, topic_name, topic_name, topic_no, group_no, topic_no)
    return sql


conn = get_db_conn()
cur = conn.cursor()

for i in range(1, 101):
    i = str(i)
    if len(i) == 1:
        i = '00%s' % i
    elif len(i) == 2:
        i = '0%s' % i

    print(generate_sql('AC', i, '04'))
    cur.execute(generate_sql('AC', i, '04'))

conn.commit()
cur.close()
conn.close()
