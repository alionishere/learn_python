import cx_Oracle

# ----------------Oracle demo-------------------#
o_user = 'fxgl_data'
o_pwd = 'fxgl_data'
# o_ip = '192.250.107.199'
o_ip = '192.250.107.142'
o_port = '1521'
o_instance = 'fkgl'

sql = 'insert into fxgl.t_test values(:1, :2, :3, :4)'
data_lst = ['1', 'jack', '001', '002']

o_conn = cx_Oracle.connect(o_user, o_pwd, o_ip + ':' + o_port + '/' + o_instance)
o_cur = o_conn.cursor()
o_cur.execute(sql, data_lst)
o_conn.commit()
# titles = [i[0] for i in o_cur.description]
# sql_set = o_cur.fetchall()
#
# for title in titles:
#     print(title, end='\t\t')
#
# print('')
#
# for sql_line in sql_set:
#     for line in sql_line:
#         print(line, end='\t\t')
#     print('')

o_cur.close()
o_conn.close()
