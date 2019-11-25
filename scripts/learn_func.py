# -*- coding: utf-8 -*-


# global m
m = 0


def test():
    global m
    m += 1
    return m


def recurse():
    re = test()
    while True:
        if re == 5:
            print('re: %s' % re)
            print('Now is ok!')
            return
        else:
            re = test()
            print('Continue: %s' % re)


recurse()
