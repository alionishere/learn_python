#-*- coding:utf-8 -*-
import cx_Oracle,sys
import os

# global task_id
# task_id = sys.argv[1].upper()
# os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
def get_task_sql(task_id):
    o_user = 'base_data'
    o_pwd = 'base_data'
    o_ip = '172.22.131.28'
    o_port = '1521'
    o_instance = 'orcl'
    db = cx_Oracle.connect(o_user, o_pwd, o_ip + ':' + o_port + '/' + o_instance)
    sql = 'SELECT SQL_STMT FROM BASE_DATA.TMG_TASK_SQL WHERE TASK_NO = \'{}\' ORDER BY SQL_ORDER'.format(task_id)
    cur = db.cursor()
    cur.execute(sql)
    re = cur.fetchall()
    cur.close()
    return re


if __name__ == '__main__':
    print(get_task_sql('MG001'))
    sqllist = get_task_sql('MG001')
    for line in sqllist:
        print(line[0].read())