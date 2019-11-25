import cx_Oracle
# import pymysql as pm
# import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys


def gen_tb_sql(tb_name, tb_type,):
    sql = '''
select '%s' as tb_name,
       '%s' as tb_type,
       count(1) as cnt,
       '{$run_date}' as run_date
  from base_data.%s
 where from_unixtime(unix_timestamp(biz_time, 'yyyy-mm-dd'), 'yyyymmdd') = '{$run_date}'
    ''' %(tb_name, tb_type, tb_name)
    return sql


def get_tb_info(sql):
    o_user = 'base_data'
    o_pwd = 'base_data'
    o_ip = '172.22.131.28'
    o_port = '1521'
    o_instance = 'orcl'
    db = cx_Oracle.connect(o_user, o_pwd, o_ip + ':' + o_port + '/' + o_instance)
    # o_user = 'root'
    # o_pwd = '123456'
    # o_ip = '172.22.131.24'
    # o_port = '3306'
    # o_instance = 'hive'
    # db = pm.connect(o_ip, o_user, o_pwd, o_instance)
    cur = db.cursor()
    cur.execute(sql)
    re = cur.fetchall()
    cur.close()
    db.close()
    return re


def set_hive_cfg(spark):
    spark.sql('set hive.exec.dynamic.partition=true')
    spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')


def gen_exe_sql():
    insert_sql = 'insert overwrite table base_data.tmg_cnt_monitor partition(run_date) '
    query_sql = '''
    SELECT TABLE_ENGLISH_NAME, TABLE_TYPE
      FROM BASE_DATA.TMG_TABLES_NAME
     WHERE TABLE_DATABASE = 'BASE_DATA'
       AND TABLE_ENGLISH_NAME NOT LIKE 'TMG_%'
    '''
    # query_sql = '''
    # select `TBL_NAME`, if(part_ids is null, '00', '10') as tb_type
    #   from view_tb_part_detail
    #  where `DB_NAME` = 'base_data'
    #    and `TBL_NAME` not like 'tmg_%'
    # '''

    sql_union_list = []
    res = get_tb_info(query_sql)
    for tb_name, tb_type in res:
        # print('%s:%s' %(tb_name, tb_type))
        tb_name = tb_name.lower()
        if tb_type == '00':
            sql_union_list.append(gen_tb_sql(tb_name, 'full scale table'))
        elif tb_type == '10':
            sql_union_list.append(gen_tb_sql(tb_name, 'partitioned table'))
        else:
            print('Table %s is not configured correctly' %tb_name)
    sql_union_str = insert_sql + '\nunion all\n'.join(sql_union_list)
    return sql_union_str


def get_spark(task_name):
    spark=SparkSession.builder.master("yarn") \
        .appName(task_name) \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec","snappy") \
        .config("spark.sql.parquet.writeLegacyFormat","true") \
        .getOrCreate()
    return spark


def run(task_name):
    start_date_key='{$%s}' % str(sys.argv[1].split('=')[0])
    start_date_value=str(sys.argv[1].split('=')[1])
    task_sql = gen_exe_sql().replace(start_date_key, start_date_value)
    print('Task sql: %s\n' %task_sql)
    spark = get_spark(task_name)
    set_hive_cfg(spark)
    spark.sql(task_sql)


if __name__ == '__main__':
    run('Task_Cnt_Monitor')
    # task_sql = gen_exe_sql().replace('{$run_date}', '20191107')
    # print(task_sql)
