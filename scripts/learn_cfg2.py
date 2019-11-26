# -*- coding: utf-8 -*-
import configparser
from datetime import date, timedelta

cfg_file = 'conf/task_conf.cfg'
# cfp = configparser.ConfigParser()
cfp = configparser.RawConfigParser()
cfp.read(cfg_file)

# secs = cfp.sections()
groups = cfp.sections()
# print(secs)


def get_date(interval):
    return (date.today() + timedelta(days=interval)).strftime('%Y%m%d')


# for sec in secs:
#     itms = cfp.items(sec)
#     # print(itms)
#     # print(itms[2][0], itms[2][1])
#     interval = itms[2][1]
#     print(get_date(int(interval)))
#     # print(itms[1])
#     print('--**' * 30)

for group in groups:
    print(cfp.get(group, 'dag_id'))
    print(cfp.get(group, 'task_group_no'))
    print(cfp.get(group, 'start_interval'))
    print(cfp.get(group, 'sh_interval'))
    print(cfp.get(group, 'if_ck_date'))
    print('---***' * 20)
