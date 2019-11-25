# -*- coding: utf-8 -*-
import configparser

cfg_file = 'conf/default.cfg'
cfp = configparser.ConfigParser()
cfp.read(cfg_file)

sec = cfp.sections()
print(sec)

ops = cfp.options('TASK_COMMAND')
print('options: %s : %s' %(ops, type(ops)))

kvs = cfp.items('TASK_COMMAND')
print('kvs: %s' % kvs)

bash_command = 'spark-submit --master yarn --num-executors 10 --executor-memory 5g --executor-cores 5 run.py %s'
bash_command = bash_command % cfp.get('TASK_COMMAND', 'tk_001')
print('bash_command : %s' % bash_command)

