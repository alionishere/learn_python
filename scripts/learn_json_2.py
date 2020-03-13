#!/usr/bin/python
# -*- coding:utf-8 -*-

import json

cfg_lst = []
with open("doc/task01.json", "r", encoding='utf-8') as f:
    line = f.readline()
    cfg_lst.append(line)
    while line:
        line = f.readline()
        cfg_lst.append(line)

with open("doc/task01.json", "r", encoding='utf-8') as f:
    dic = json.load(f)

print(dic)

# print('**--'* 30)
# print(cfg_lst)
# get all keys
# for k in d:
#     print(k)
#
# print('*-' * 15)
# # get all values
# for v in d.values():
#     print(v)
#
# print('*-' * 15)
# # get all keys and values
# for k in d:
#     print(k, ':', d[k])
# print('-' * 20)
# for k, v in d.items():
#     print(k, ':', v)

