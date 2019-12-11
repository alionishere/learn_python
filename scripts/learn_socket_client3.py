import socket
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 建立连接:
s.connect(('127.0.0.1', 9999))
# 接收欢迎消息:
print(s.recv(1024).decode('utf-8'))
for data in [b'Michael3', b'Tracy3', b'Sarah3']:
    # 发送数据:
    print('send: %s' % data)
    s.send(data)
    print('accepted: %s' % s.recv(1024).decode('utf-8'))
    time.sleep(5)
s.send(b'exit')
s.close()
