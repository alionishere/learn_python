# -*- coding: utf-8 -*-
from datetime import datetime, date, timedelta
import sys


# 动态参数
def test(*args, **kwargs):
    print(args)
    print(kwargs)
    print(kwargs['name'])


# test(1, 2, 3, 4, 5, name='Kylin', age='27')
# 日期的获取合加减
print('today is is %s' %(date.today()))
end_date = (date.today() + timedelta(days=-1))   # strftime('%Y%m%d')
# 日期格式和字符串格式的互转
start_date = end_date + timedelta(days=-10)

# print(type(start_date))
#
# print(start_date - end_date)

start_date = datetime.strptime(sys.argv[1], '%Y%m%d')
end_date = datetime.strptime(sys.argv[2], '%Y%m%d')
# print(start_date)


def run(tx_date):
    print(tx_date)
    print('-' * 50)


if __name__ == '__main__':
    while start_date <= end_date:
        print(start_date)
        run(start_date.strftime('%Y%m%d'))
        start_date = start_date + timedelta(days=1)
