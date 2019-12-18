# -*- coding: utf-8 -*-
import socket
import time
import sys


def gen_test_sql(num):
    return "insert into cif.t_test_01 values('%s')" % num


start_num = int(sys.argv[1])
end_num = int(sys.argv[2])

sql_lst = [gen_test_sql(n) for n in range(start_num, end_num)]
# sql_lst = [b"insert into cif.t_test_01 values('03')"]

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 建立连接:
s.connect(('127.0.0.1', 9998))
# 接收欢迎消息:
print(s.recv(4096).decode('utf-8'))
for data in sql_lst:
    # 发送数据:
    print('send: %s' % data)
    s.send(data)
    print('accepted: %s' % s.recv(1024).decode('utf-8'))
    # time.sleep(5)
s.send(b'exit')
s.close()
