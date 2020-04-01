# -*- coding: utf-8 -*-  
import urllib.request,sys
from bs4 import BeautifulSoup
import cx_Oracle
import json,ast

def conndb():
    username="kingstar"
    userpwd="kingstar"
    host="10.29.7.211"
    port=1521
    dbname="siddc01"    
    dsn=cx_Oracle.makedsn(host, port, dbname)
    db=cx_Oracle.connect(username, userpwd, dsn) 
    return db

def ExecDB(sql):
    db=conndb()
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()
    return result

def getHtml(url):  
    page=urllib.request.urlopen(url)
    html=page.read().decode(encoding='utf-8',errors='strict')
    return html

def check(value):
    if value.find('e') >= 0 or value =='-':
      value = '0'
    else :
      value = value
    return value

page = 1

url_sh = 'http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get?token=70f12f2f4f091e459a279469fe49eca5&st=tdate&sr=-1\
&p='+str(page)+'&ps=50&js=var%20TYhRIYCc={pages:(tp),data:%20(x)}&type=RZRQ_HSTOTAL_NJ&filter=(market=%27SH%27)&mk_time=1&rt=51584747'

url_sz = 'http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get?token=70f12f2f4f091e459a279469fe49eca5&st=tdate&sr=-1\
&p='+str(page)+'&ps=50&js=var%20oziRpEXk={pages:(tp),data:%20(x)}&type=RZRQ_HSTOTAL_NJ&filter=(market=%27SZ%27)&mk_time=1&rt=51584553'

url_hs = 'http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get?token=70f12f2f4f091e459a279469fe49eca5&st=tdate&sr=-1\
&p='+str(page)+'&ps=50&js=var%20GcoYDxkD={pages:(tp),data:%20(x)}&type=RZRQ_LSTOTAL_NJ&mk_time=1&rt=51584732'

url_list = [url_sh,url_sz,url_hs]

for url in url_list:
  html = getHtml(url)
  start = html.find('[')
  end = html.rfind(']')+1
  lists = ast.literal_eval(html[start:end])
  result = lists[0]
  #result = lists[2]
  print(result)
  busidate = result['tdate'][0:10].replace('-','')
  try:
    market = result['market']
  except:
    market = 'HS'
  rzye = check(str(result['rzye']))
  rzyezb = check(str(result['rzyezb']))
  rzmre = check(str(result['rzmre']))
  rzche = check(str(result['rzche']))
  rzjmre = check(str(result['rzjmre']))
  rqye = check(str(result['rqye']))
  rqyl = check(str(result['rqyl']))
  rqmcl = check(str(result['rqmcl']))
  rqchl = check(str(result['rqchl']))
  rqjmcl = check(str(result['rqjmcl']))
  rzrqye = check(str(result['rzrqye']))
  rzrqyecz = check(str(result['rzrqyecz']))
  sql = "MERGE INTO SC61.TRZRQYE A USING(SELECT '"+busidate+"' busidate,'"+market+"' market,'"+rzye+"' rzye,'"+rzyezb+"' rzyezb,'"+rzmre+"' rzmre,'"\
+rzche+"' rzche,'"+rzjmre+"' rzjmre,'"+rqye+"' rqye,'"+rqyl+"' rqyl,'"+rqmcl+"' rqmcl,'"+rqchl+"' rqchl,'"+rqjmcl+"' rqjmcl,'"+rzrqye+"' rzrqye,'"\
+rzrqyecz+"' rzrqyecz FROM DUAL) B ON (A.busidate = B.busidate AND A.market = B.market)\
  WHEN MATCHED THEN UPDATE SET A.rzye=B.rzye,A.rzyezb=B.rzyezb,A.rzmre=B.rzmre,A.rzche=B.rzche,A.rzjmre=B.rzjmre,A.rqye=B.rqye,\
A.rqyl=B.rqyl,A.rqmcl=B.rqmcl,A.rqchl=B.rqchl,A.rqjmcl=B.rqjmcl,A.rzrqye=B.rzrqye,A.rzrqyecz=B.rzrqyecz\
  WHEN NOT MATCHED THEN INSERT(A.busidate,A.market,A.rzye,A.rzyezb,A.rzmre,A.rzche,A.rzjmre,A.rqye,A.rqyl,A.rqmcl,A.rqchl,A.rqjmcl,A.rzrqye,A.rzrqyecz)\
VALUES(B.busidate,B.market,B.rzye,B.rzyezb,B.rzmre,B.rzche,B.rzjmre,B.rqye,B.rqyl,B.rqmcl,B.rqchl,B.rqjmcl,B.rzrqye,B.rzrqyecz)"
  try:
    ExecDB(sql)
  except:
    print(sql)









