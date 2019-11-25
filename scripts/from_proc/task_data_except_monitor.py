# import cx_Oracle
from datetime import datetime, timedelta
import pymysql as pm
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys
import warnings

warnings.filterwarnings('ignore')


def gen_full_tb_sql(db_name,tb_name, ck_cols, ck_data, run_date, ck_item):
    sql = '''
select '%s' as tb_name,
       '%s' as ck_cols,
       sum(case when %s like '%%%s%%' then 1 else 0 end) exp_cnt,
       'full scale table' as part_date,
       '%s' as run_date,
       '%s' as ck_item
  from %s.%s
 where %s like '%%%s%%'
    ''' %(tb_name, ck_cols, ck_cols, ck_data, run_date, ck_item, db_name, tb_name, ck_cols, ck_data)
    return sql


def gen_part_tb_sql(db_name, tb_name, ck_cols, ck_data, part_date, part_col, run_date, ck_item):
    sql = '''
select '%s' as tb_name,
       '%s' as ck_cols,
       sum(case when %s like '%%%s%%' then 1 else 0 end) exp_cnt,
       '%s' as part_date,
       '%s' as run_date,
       '%s' as ck_item
  from %s.%s
 where %s like '%%%s%%'
   and %s = '%s'
    ''' %(tb_name, ck_cols, ck_cols, ck_data, part_date, run_date, ck_item, db_name, tb_name, ck_cols, ck_data, part_col, part_date)
    return sql


def get_oracle_tb_info(sql):
    o_user = 'base_data'
    o_pwd = 'base_data'
    o_ip = '172.22.131.28'
    o_port = '1521'
    o_instance = 'orcl'
    db = cx_Oracle.connect(o_user, o_pwd, o_ip + ':' + o_port + '/' + o_instance)

    cur = db.cursor()
    cur.execute(sql)
    re = cur.fetchall()
    cur.close()
    db.close()
    return re


def get_mysql_tb_info(sql):
    user = 'root'
    pwd = '123456'
    ip = '172.22.131.24'
    port = '3306'
    instance = 'hive'
    db = pm.connect(ip, user, pwd, instance)

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


def gen_exe_sql(spark, part_date, run_date, ck_item, ck_data):
    insert_sql = 'insert into base_data.tmg_ck_monitor partition(run_date, ck_item) '
    query_sql = '''
    select v1.`DB_NAME`,
           v1.`TBL_NAME`,
           v1.`COLUMN_NAME`,
           v2.part_ids
      from view_tb_col_detail v1
      left join view_tb_part_detail v2 
        on (v1.`DB_ID` = v2.`DB_ID` and v1.`TBL_NAME` = v2.`TBL_NAME`)
     where v1.`DB_NAME` = 'base_data'
       and v1.`COLUMN_NAME` like '%%code%%'
       and v1.`TBL_NAME` not like '%%tmg%%'
       and v1.`TBL_NAME` not like '%%tmp'
    '''

    res = get_mysql_tb_info(query_sql)
    res_df = pd.DataFrame(list(res),columns=["db_name", "tb_name", "column_name", "part_id"])
    tbs_df = res_df[['tb_name', 'part_id']].drop_duplicates()
    total = len(tbs_df)
    current_num = 1
    for index, row in tbs_df.iterrows():
        tb_name = row['tb_name']
        part_id = row['part_id']
        sql_union_list = []
        cols_df = res_df[res_df.tb_name == tb_name]
        if part_id is None:
            for index, row in cols_df.iterrows():
                sql_union_list.append(gen_full_tb_sql(row['db_name'],tb_name, row['column_name'], ck_data, run_date, ck_item))
            sql_union_str = insert_sql + '\nunion all\n'.join(sql_union_list)
            print('Now start to check %s. Rate of process is %d/%d. Sql statement is:\n%s' %(tb_name, current_num, total, sql_union_str))
            current_num = current_num + 1
            spark.sql(sql_union_str)
        else:
            for index, row in cols_df.iterrows():
                sql_union_list.append(gen_part_tb_sql(row['db_name'], tb_name, row['column_name'], ck_data, part_date, 'biz_date', run_date, ck_item))
            sql_union_str = insert_sql + '\nunion all\n'.join(sql_union_list)
            print('Now start to check %s. Rate of process is %d/%d. Sql statement is:\n%s' %(tb_name, current_num, total, sql_union_str))
            current_num = current_num + 1
            spark.sql(sql_union_str)
        print('Table %s has been checked over.\n%s' %(tb_name, '=='*50))


def get_spark(task_name):
    spark=SparkSession.builder.master("yarn") \
        .appName(task_name) \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec","snappy") \
        .config("spark.sql.parquet.writeLegacyFormat","true") \
        .getOrCreate()
    return spark


def clear_null_data(spark, run_date, ck_item):
    clear_sql = '''
    insert overwrite table base_data.tmg_ck_monitor partition(run_date, ck_item)
    select * from base_data.tmg_ck_monitor
     where run_date = '%s' and ck_item = '%s'
       and exp_cnt is not null
    ''' %(run_date, ck_item)
    spark.sql(clear_sql)
    print('\nClear null data over!')


def drop_today_partition(spark, run_date, ck_item):
    drop_sql = '''
    alter table base_data.tmg_ck_monitor drop if exists partition (run_date='%s',ck_item='%s')
    ''' %(run_date, ck_item)
    spark.sql(drop_sql)
    print('Drop the partition of (%s, %s) success!' %(run_date, ck_item))


def run(task_name, run_date, ck_item, ck_data):
    start_date_value = (datetime.strptime(run_date, '%Y%m%d') + timedelta(days=-1)).strftime('%Y%m%d')
    # task_sql = gen_exe_sql().replace(start_date_key, start_date_value)
    spark = get_spark(task_name)
    set_hive_cfg(spark)
    drop_today_partition(spark, run_date, ck_item)
    task_sql = gen_exe_sql(spark, start_date_value, run_date, ck_item, ck_data)
    clear_null_data(spark, run_date, ck_item)
    # print('Task sql: %s\n' %task_sql)
    # spark.sql(task_sql)


if __name__ == '__main__':
    start_date_key='{$%s}' % str(sys.argv[1].split('=')[0])
    run_date=str(sys.argv[1].split('=')[1])
    run('Task_Except_Monitor', run_date, 'Except_Data_Check', '?')
