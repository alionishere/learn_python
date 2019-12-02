# -*- coding: utf-8 -*-
from enum import Enum, unique


@unique
class Weekday(Enum):
    Sun = 0  # Sun的value被设定为0
    Mon = 1
    Tue = 2
    Wed = 3
    Thu = 4
    Fri = 5
    Sat = 6


def test_01(par):
    try:
        print('try...')
        r = 10 / par
        print('result:', r)
    except ZeroDivisionError as e:
        print('except:', e)
    finally:
        print('finally...')
    print('END')


def test_02(a):
    try:
        print('try...')
        r = 10 / int(a)
        print('result:', r)
    except ValueError as e:
        print('ValueError:', e)
    except ZeroDivisionError as e:
        print('ZeroDivisionError:', e)
    else:
        print('no error!')
    finally:
        print('finally...')
    print('END')
    test_01(3)


test_02('1')
