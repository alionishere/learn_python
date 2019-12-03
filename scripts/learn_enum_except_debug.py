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


class FooError(ValueError):
    pass


def foo(s):
    n = int(s)
    if n == 0:
        raise FooError('invalid value: %s' % s)
    return 10 / n


def foo2(s):
    n = int(s)
    if n == 0:
        raise ValueError('invalid value: %s' % s)
    return 10 / n


def bar():
    try:
        foo('0')
    except ValueError as e:
        print('ValueError!')
        raise


# debug
def foo3(s):
    n = int(s)
    assert n != 0, 'n is zero!'
    return 10 / n


# logging
import logging
logging.basicConfig(level=logging.DEBUG)


def foo4(s):
    n = int(s)
    logging.info('n = %d' % n)
    logging.debug('this is debug')
    print(10 / n)


foo4('3')
