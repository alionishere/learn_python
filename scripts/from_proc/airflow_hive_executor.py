#-*- coding:utf8 -*-
import cx_Oracle
import os
import datetime, time
# from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf
import sys
import get_task_sql as gs
import commands


def help():
    msg = '''
    Usage: python /root/base_data/airflow_hive_executor.py TX_DATE=... task_id=...
    '''
    print(msg)

# 第一个参数：tx_date
# 第二个参数：task_no
if len(sys.argv) < 3:
    print('Parameter error!')
    help()
    sys.exit(1)

sql_path = '/root/base_data/scripts/sql/monitor/%s'
start_date_key='{$%s}' % str(sys.argv[1].split('=')[0])
start_date_value=str(sys.argv[1].split('=')[1])
task_no_k = str(sys.argv[2].split('=')[0])
task_no_v = str(sys.argv[2].split('=')[1])
# end_date=str(sys.argv[1])

sqllist = gs.get_task_sql(task_no_v)
# order_key = 1
# separator = '{;}'
print('\n')
for line in sqllist:
    sql = line[0].read().replace(start_date_key, start_date_value)
    for i in range(len(sys.argv)):
        if i > 2:
            k = '{$%s}' % sys.argv[i].split('=')[0]
            v = '%s' % sys.argv[i].split('=')[1]
            print('Key:%s Value:%s' %(k,v))
            sql = sql.replace(k, v)
    # sql_sep = sql.split(separator)
    print('Task sql : %s' %sql)
    # write sql to file
    sql_full_path = sql_path % task_no_v
    with open(sql_full_path, 'w') as f:
        f.write(sql)
    # launch hive
    os.system('hive -f %s' %sql_full_path)
    # for one_sql in sql_sep:
    #     if len(one_sql.strip()) != 0 and one_sql.strip():
    #         print('Task sql %s : %s' %(order_key,one_sql))
    #         spark.sql(one_sql)
    # order_key = order_key + 1
print "********end***********"