#!/usr/bin/python
# -*- coding = utf-8 -*-
import time
import sys


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
select * from ; dual ; ||
abc;
abcd
'''


def replace_char(string, char, index):
    string = list(string)
    string[index] = char
    return ''.join(string)


# format的用法
def learn_format():
    sex = 'Girl'
    name = 'Tom'
    # 以f开头表示在字符串中支持大括号内的python表达式
    s = f"{name} is a {sex}"
    s = "My name is {}, i am {age} year old, She name is {}".format('Liming', 'Lily', age=10)
    s = "The word is {s}, {s[0]} is initials".format(s='world')
    s = 'π is {:.2f}'.format(3.1415926)
    s = "{:.1}".format('Hello')
    s = "{:,}".format(1000000)
    s = "{:b}".format(8)
    s = "{:o}".format(8)
    s = "{:X}".format(12)
    # 通过：+数字指定转换后的字符串长度，不足的部分用空格补充
    s = "{:2}b".format('a')
    # 字符的填充，默认填充到字符串的两端
    # 如果数字小于字符串的长度，则不进行填充操作。
    s = "{:*^10}".format('Hello')
    s = "{:-^20}".format('123456')
    # list、tuple的拆分
    foods = ['fish', 'beef', 'fruit']
    s = 'i like eat {} and {} and {}'.format(*foods)
    s = 'i like eat {2} and {0} and {1}'.format(*foods)
    dict_temp = {'name': 'Lily', 'age': 18}
    # 字典需要用 ** 进行拆分
    s = 'My name is {name}, i am {age} years old'.format(**dict_temp)
    print(s)
    print(type(s))


if __name__ == '__main__':
    # idx = sql.rfind(';')
    # print(idx)
    # print(replace_char(sql, '&&', idx))
    # test format
    learn_format()

# test_args(12, 3, 4, a='asd', b='asdsd')
# print('-*' * 50)
# test_args()
