# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 16:10:25 2017

@author: dwzq
"""





import cx_Oracle
username="kingstar"
userpwd="kingstar"
host="10.29.7.211"
port=1521
dbname="siddc01"
dsn=cx_Oracle.makedsn(host, port, dbname)
connection=cx_Oracle.connect(username, userpwd, dsn) 
cursor = connection.cursor() 
#sql = "select count(*) from SC61.Tmp_DBF_SCCJ" 
sql = "select * from SC61.tb_dbf_sccj"
cursor.execute(sql) 
result = cursor.fetchall() 
count = cursor.rowcount 
print ("=====================" )
print ("Total:", count)
print ("=====================")
for row in result: 
        print (row)
cursor.close() 
connection.close()