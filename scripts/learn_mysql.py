"""
Usage:
    1. 传参规范： k=v，注意v两边不要引号，且"="两边不能有空格
    2. SQL参数设置规范： k = $k, 注意，这里也不要引号
    example： python learn_mysql.py task_id=AC001 trade_date=20190807
"""
import pymysql as pm
import sys
import cx_Oracle

# from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf

# 获取参数
# start_date = str(sys.argv[1])
# task_id = str(sys.argv[2])
task_id = 'task001'

# 初始化SparkSession
# spark = SparkSession.builder.master("yarn") \
#     .appName("kcb") \
#     .enableHiveSupport() \
#     .config("spark.sql.parquet.compression.codec", "uncompressed") \
#     .getOrCreate()

# 打开数据库连接
db = pm.connect("192.250.107.198", 'root', 'Admin@123', 'mysql')

# 创建游标
cursor = db.cursor()

# 执行SQL
query_sql = 'select * from testdb.t_para_conf where task_id = \'%s\' order by 2' % task_id
print(query_sql)

cursor.execute(query_sql)

# 获取数据
# fetchone: 获取一条
# fetchall: 获取所有
results = cursor.fetchall()


# 参数配置
# if len(sys.argv) > 1:
#     for i in range(sys.argv):
#         if i > 0:

# 定義task sql
# t_sql = ''
for row in results:
    t_order = row[1]
    # t_sql = row[2].replace('recent_1_trading_day', '20190807').replace('recent_20_trading_day', '20190707')
    t_sql = row[2]
    if len(sys.argv) > 1:
        for i in range(len(sys.argv)):
            if i > 0:
                k = '$%s' % sys.argv[i].split('=')[0]
                v = '\'%s\'' % sys.argv[i].split('=')[1]
                t_sql = t_sql.replace(k, v)
                print('k: %s, v: %s' %(k, v))
    # print('\nid: %s\nsql: %s\npara: %s\n' %(row[0], row[1], row[2]))
    print('-' * 50)
    print('task order: %s' % t_order)
    print(t_sql)
    if t_order == 6:
        cursor.execute(t_sql)
        res = cursor.fetchall()
        # print(res)
    # 此种模式是串行执行
    # spark.sql(t_sql)

# 关闭数据库连接
cursor.close()
db.close()
