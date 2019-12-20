#!/usr/bin/python
# -*- coding: utf-8 -*-
# server.py
import socket
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


def tcplink(sock, addr, spark):
    print('Accept new connection from %s:%s...' % addr)
    sock.send(b'Welcome!')
    while True:
        data = sock.recv(4096)
        # time.sleep(1)
        if not data or data.decode('utf-8') == 'exit':
            break
        run_sql(spark, data)
        # sock.send(('Hello, %s!' % data.decode('utf-8')).encode('utf-8'))
        sock.send('Hello, success')
    sock.close()
    print('Connection from %s:%s closed.' % addr)


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 监听端口:
s.bind(('127.0.0.1', 9998))
s.listen(5)
spark = get_spark('test_001')

while True:
    # 接受一个新连接:
    global spark
    sock, addr = s.accept()
    # 此处应该判断spark是否有效，去官网查询对应的API
    try:
        spark.sql('select 1').show()
    except Exception as e:
        print('-**=' * 20)
        print('Try to rebuild spark.......')
        spark = None
        spark = get_spark('test_002')
        print('Rebuild spark successfully.......')
        print('-**=' * 30)
        # time.sleep(3)

    # 创建新线程来处理TCP连接:
    t = threading.Thread(target=tcplink, args=(sock, addr, spark))
    t.start()
