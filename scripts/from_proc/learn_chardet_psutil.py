#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
psutil还可以获取用户信息、Windows服务等很多有用的系统信息，
具体请参考psutil的官网：https://github.com/giampaolo/psutil
"""
import chardet
import psutil

data = '离离原上草，一岁一枯荣'.encode('gbk')
data2 = '离离原上草，一岁一枯荣'.encode('utf-8')
data3 = '最新の主要ニュース'.encode('euc-jp')
print(chardet.detect(data))
print(chardet.detect(data2))
print(chardet.detect(data3))

print(psutil.cpu_count())  # CPU逻辑数量
print(psutil.cpu_count(logical=False))  # CPU物理核心
print(psutil.cpu_times())  # CPU的用户、系统、空闲时间

# for x in range(10):
#     print(psutil.cpu_percent(interval=1, percpu=True))
print('内存：')
print(psutil.virtual_memory())  # 获取内存信息
print(psutil.swap_memory())
print('磁盘：')
print(psutil.disk_partitions())  # 磁盘分区信息
print(psutil.disk_usage('/'))  # 磁盘使用情况
print(psutil.disk_io_counters())  # 磁盘ID
print('网络：')
print(psutil.net_io_counters())  # 获取网络读写字节／包的个数
print(psutil.net_if_addrs())  # 网络接口信息
print(psutil.net_if_stats())  # 网络接口状态
print(psutil.net_connections())  # 获取当前网络连接信息
print('进程：')
print(psutil.pids())  # 获取所哟进程
p = psutil.Process(2128)
print(p.name())  # 获取指定进程
# print(p.exe())  # 进程的exe路径，需要对应权限
# p.cwd()  # 进程工作目录
# p.cmdline()  # 进程启动的命令行
# p.ppid()  # 父进程ID
# p.parent()  # 父进程
# p.children()  # 子进程列表
# p.status()  # 进程状态
# p.username()  # 进程用户名
# p.create_time()  # 进程创建时间
# p.terminal()  # 进程终端
# p.cpu_times()  # 进程使用的CPU时间
# p.memory_info()  # 进程使用的内存
# p.open_files()  # 进程打开的文件
# p.connections()  # 进程相关网络连接
# p.num_threads()  # 进程的线程数量
# p.threads()  # 所有线程信息
# p.environ()  # 进程环境变量
# p.terminate()  # 结束进程
print(psutil.test())
