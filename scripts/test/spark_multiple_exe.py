import threading
import time
from pyspark.sql import SparkSession


def get_spark(app_name):
    return SparkSession.builder.master("yarn") \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .getOrCreate()


def run_sql(spark, sql):
    spark.sql(sql)


def gen_test_sql(num):
    return "insert into cif.t_test_01 values('%s')" % num


spark = get_spark('test_701')
run_sql(spark, gen_test_sql('02'))
# t1 = threading.Thread(target=run_sql, name='1', args=(spark, gen_test_sql('02')))
# t1.start()
#
# t2 = threading.Thread(target=run_sql, name='2', args=(spark, gen_test_sql('02')))
# t2.start()
#
# t3 = threading.Thread(target=run_sql, name='3', args=(spark, gen_test_sql('02')))
# t3.start()


# def add(x, y):
#     print('{}+{}={}'.format(x, y, x + y))
#
#
# t1 = threading.Thread(target=add, name='1', args=(4, 5))
# t1.start()
# time.sleep(2)
#
# t2 = threading.Thread(target=add, name='2', args=(4,), kwargs={'y': 6})
# t2.start()
# time.sleep(2)
# t3 = threading.Thread(target=add, name='3', kwargs={'x': 4, 'y': 7})
# t3.start()
