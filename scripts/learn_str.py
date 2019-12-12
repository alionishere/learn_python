#!/usr/bin/python
# -*- coding = utf-8 -*-
import time
import sys

str = 'abcd$ab'
str.replace('\$ab', '1233')
print(str)


def test():
    i = 1
    while True:
        p(i)
        i = i + 1
        if i == 10:
            return
        time.sleep(1)


def p(i):
    print(i)
    print('-' * 10)


# test()
def test_args(*args, **kwargs):
    print(args)
    print(type(args))
    print('-' * 50)
    print(kwargs)
    print(type(kwargs))


def rm_semicolon(sql_stmt):
    return sql_stmt.strip().strip(';')


sql = '''
select * from ; dual ; 
'''
print(rm_semicolon(sql))
# test_args(12, 3, 4, a='asd', b='asdsd')
# print('-*' * 50)
# test_args()
