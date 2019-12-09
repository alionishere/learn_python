#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 文件名：server.py
from datetime import datetime, timedelta


def run_in_loop(start_date, end_date):
    run_date = start_date
    start_interval = 1
    while True:
        if run_date > end_date:
            print('Game over...')
            break
        print(run_date)
        run_date = (datetime.strptime(start_date, '%Y%m%d') + timedelta(days=start_interval)).strftime('%Y%m%d')
        start_interval = start_interval + 1


if __name__ == '__main__':
    start_date = '20191201'
    end_date = '20191210'
    run_in_loop(start_date, end_date)
