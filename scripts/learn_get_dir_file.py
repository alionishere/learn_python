# -*- coding: utf-8 -*-
import os


def file_name(file_dir):
    for root, dirs, files in os.walk(file_dir):
        print(root)  # 当前目录路径
        # print(dirs)  # 当前路径下所有子目录
        # print(files)  # 当前路径下所有非目录子文件


# 只获取当前路径下文件名,不获取文件夹中文件名
def file_name_(file_dir):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.jpeg':  # 想要保存的文件格式
                L.append(os.path.join(root, file))
    return L


file_path = r'\\10.3.1.138\movie'
file_name(file_path)
