#!/usr/bin/python
# -*- coding:utf-8 -*-

import json

# d = {'谦谦': {'sex': '男', 'addr': '北京', 'age': 34}, '千千': {'sex': '女', 'addr': '北京', 'age': 34}, }
# print(json.dumps(d, ensure_ascii=False, indent=4))

# fw = open(r'E:\pycharm_code\learn\scripts\doc\user_info.json', 'w', encoding='utf-8')
# json.dump(d, fw, ensure_ascii=False, indent=4)

# fr = open(r'E:\pycharm_code\learn\scripts\doc\product.json', encoding='utf-8')
# print(json.load(fr))
# res = fr.read()
# print(json.loads(res))
# print(json.dumps(json.loads(res), ensure_ascii=False, indent=4))
d = {
    "k1": 18,
    "k2": True,
    "k3": ['Su', {
        'kk1': 'vv1',
        'kk2': 'vv2',
        'kk3': (11, 22),
    }
           ],
    "k4": (11, 22, 33, 44)
}

# get all keys
for k in d:
    print(k)

print('*-' * 15)
# get all values
for v in d.values():
    print(v)

print('*-' * 15)
# get all keys and values
for k in d:
    print(k, ':', d[k])
print('-' * 20)
for k, v in d.items():
    print(k, ':', v)

print('-' * 20)
d.setdefault('k4', 'notset')
d.setdefault('k5', 'set')
print(d.get('k4'))
print(d.get('k5'))
print(d.get('k5', 'default'))
