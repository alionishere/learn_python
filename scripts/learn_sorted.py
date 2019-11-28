# -*- coding: utf-8 -*-

# key指定的函数将作用于list的每一个元素上，并根据key函数返回的结果进行排序
print(sorted([36, 5, -12, 9, -21]))
print(sorted([36, 5, -12, 9, -21], key=abs))
print(sorted(['bob', 'about', 'Zoo', 'Credit']))
print(sorted(['bob', 'about', 'Zoo', 'Credit'], key=str.lower))
