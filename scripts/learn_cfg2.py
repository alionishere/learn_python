# -*- coding: utf-8 -*-
import configparser

cfg_file = 'conf/task_conf.cfg'
# cfp = configparser.ConfigParser()
cfp = configparser.RawConfigParser()
cfp.read(cfg_file)

secs = cfp.sections()
print(secs)

for sec in secs:
    itms = cfp.items(sec)
    # print(itms)
    print(itms[0][0], itms[0][1])
    print(itms[1])
    print('--**' * 30)
