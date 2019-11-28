# -*- coding: utf-8 -*-

lst = range(10)


def is_odd(x):
    return x % 2 == 1


print(list(filter(is_odd, lst)))


# 把一个序列中的空字符串删掉
def not_empty(s):
    return s and s.strip()


print(list(filter(not_empty, ['A', '', 'B', None, 'C', '  '])))


def _odd_iter():
    n = 1
    while True:
        n = n + 2
        yield n


def _not_divisible(n):
    return lambda x: x % n > 0


def primes():
    yield 2
    it = _odd_iter()
    while True:
        n = next(it)
        yield n
        it = filter(_not_divisible(n), it)


def test():
    yield 2
    it = _odd_iter()
    while True:
        n = next(it)
        yield n


# for i in test():
#     if i < 15:
#         print(i)
#     else:
#         break

lst = []
for n in primes():
    if n < 1000:
        lst.append(n)
    else:
        break

print(lst)
