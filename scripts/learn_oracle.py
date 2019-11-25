import cx_Oracle

# ----------------Oracle demo-------------------#
o_user = 'dcp'
o_pwd = 'dcp'
o_ip = '192.250.107.199'
o_port = '1521'
o_instance = 'sjgk'

sql = 'select count(1) from all_all_tables'
sql1 = 'SELECT * FROM dept '

o_db = cx_Oracle.connect(o_user, o_pwd, o_ip + ':' + o_port + '/' + o_instance)
o_cur = o_db.cursor()
o_cur.execute(sql)

titles = [i[0] for i in o_cur.description]
sql_set = o_cur.fetchall()

for title in titles:
    print(title, end='\t\t')

print('')

for sql_line in sql_set:
    for line in sql_line:
        print(line, end='\t\t')
    print('')

o_cur.close()
o_db.close()
