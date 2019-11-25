#-*- coding:utf-8 -*-
import cx_Oracle
import os
import datetime, time
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys
import get_task_sql as gs


def get_date(delta):
    today = datetime.date.today()
    get_date = today + datetime.timedelta(days=a)
    get_date1 = int(get_date.strftime("%Y%m%d"))
    return get_date1


spark=SparkSession.builder.master("yarn")\
    .appName("test_base_data")\
    .enableHiveSupport()\
    .config("spark.sql.parquet.compression.codec","snappy")\
    .config("spark.sql.parquet.writeLegacyFormat","true")\
    .getOrCreate()


def set_hive_partition():
    # spark.sql('set hive.exec.dynamic.partition=true')
    # spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
    spark.sql('set spark.sql.shuffle.partitions=10')
    print 'set hive dynamic.partition success'


def get_recent_trading_day(a, b, c):
    b = int(b)
    c = int(c)
    recent_trading_day_sql = 'select trim(rq) as rq from comm.jyr where jyrbs=3 and rq<= %s order by rq desc limit 20' % a
    recent_trading_day = spark.sql(recent_trading_day_sql)
    near_trading_day = int(recent_trading_day.head(b)[c][0])
    # return recent_trading_day_sql
    return near_trading_day


set_hive_partition()
start_date_key='{$%s}' % str(sys.argv[1].split('=')[0])
start_date_value=str(sys.argv[1].split('=')[1])
task_no_k = str(sys.argv[2].split('=')[0])
task_no_v = str(sys.argv[2].split('=')[1])
# end_date=str(sys.argv[1])

sqllist = gs.get_task_sql(task_no_v)
order_key = 1
separator = '{;}'
print('\n')
print('%s:%s' %(start_date_key,start_date_value))
print('%s:%s' %(task_no_k,task_no_v))
for line in sqllist:
    sql = line[0].read().replace(start_date_key, start_date_value)
    for i in range(len(sys.argv)):
        if i > 2:
            k = '{$%s}' % sys.argv[i].split('=')[0]
            v = '%s' % sys.argv[i].split('=')[1]
            print('Key:%s Value:%s' %(k,v))
            sql = sql.replace(k, v)
    sql_sep = sql.split(separator)
    for one_sql in sql_sep:
        if len(one_sql.strip()) != 0 and one_sql.strip():
            print('Task sql %s : %s' %(order_key,one_sql))
            spark.sql(one_sql)
    order_key = order_key + 1
print "********end***********"