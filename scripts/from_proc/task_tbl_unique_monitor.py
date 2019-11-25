import cx_Oracle
from datetime import datetime, timedelta
# import pymysql as pm
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys


def gen_full_tb_sql(db_name,tb_name, primary_key_cols, run_date, ck_item):
    sql = '''
select '%s' as tb_name,
       '%s' as ck_cols,
       count(1) exp_cnt,
       'full scale table' as part_date,
       '%s' as run_date,
       '%s' as ck_item
  from %s.%s
 group by %s
having exp_cnt > 1
    ''' %(tb_name, primary_key_cols, run_date, ck_item, db_name, tb_name, primary_key_cols)
    return sql


def gen_part_tb_sql(db_name, tb_name, primary_key_cols, part_date, part_col, run_date, ck_item):
    sql = '''
select '%s' as tb_name,
       '%s' as ck_cols,
       count(1) exp_cnt,
       '%s' as part_date,
       '%s' as run_date,
       '%s' as ck_item
  from %s.%s
 where %s = '%s'
 group by %s
having exp_cnt > 1
    ''' %(tb_name, primary_key_cols, part_date, run_date, ck_item, db_name, tb_name, part_col, part_date, primary_key_cols)
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
    spark.sql('set spark.sql.shuffle.partitions=10')


def gen_exe_sql(part_date, run_date, ck_item):
    insert_sql = 'insert overwrite table base_data.tmg_ck_monitor partition(run_date, ck_item) '
    query_sql = '''
    SELECT TABLE_DATABASE
           ,TABLE_ENGLISH_NAME
           ,TABLE_TYPE
           ,PRIMARY_KEY_COLS
           ,PARTITION_COL
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
    for db_name, tb_name, tb_type, primary_key_cols, part_col in res:
        # print('%s:%s' %(tb_name, tb_type))
        db_name = db_name.lower()
        tb_name = tb_name.lower()
        primary_key_cols = primary_key_cols.lower()
        if tb_type == '00':
            sql_union_list.append(gen_full_tb_sql(db_name, tb_name, primary_key_cols, run_date, ck_item))
        elif tb_type == '10':
            part_col = part_col.lower()
            sql_union_list.append(gen_part_tb_sql(db_name, tb_name, primary_key_cols, part_date, part_col, run_date, ck_item))
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


def run(task_name, run_date, ck_item):
    start_date_value = (datetime.strptime(run_date, '%Y%m%d') + timedelta(days=-1)).strftime('%Y%m%d')
    # task_sql = gen_exe_sql().replace(start_date_key, start_date_value)
    task_sql = gen_exe_sql(start_date_value, run_date, ck_item)
    print('Task sql: %s\n' %task_sql)
    spark = get_spark(task_name)
    set_hive_cfg(spark)
    spark.sql(task_sql)


if __name__ == '__main__':
    start_date_key='{$%s}' % str(sys.argv[1].split('=')[0])
    run_date=str(sys.argv[1].split('=')[1])
    run('Task_Unique_Monitor', run_date, 'Primary_Key_Check')
