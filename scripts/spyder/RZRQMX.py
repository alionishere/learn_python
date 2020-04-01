# -*- coding: utf-8 -*-  
import urllib.request,sys
from bs4 import BeautifulSoup
import cx_Oracle
import json,ast,datetime

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

def UP(url):
  html = getHtml(url)
  page_start = html.find(':')+1
  page_end = html.find(',')
  page_num = html[page_start:page_end]
  start = html.find('[')
  end = html.rfind(']')+1
  lists = ast.literal_eval(html[start:end])
  for i in range(len(lists)):
    result = lists[i]
    busidate = result['tdate'][0:10].replace('-','')
    market = result['market']
    scode = str(result['scode'])
    sname = str(result['sname'])
    close = check(str(result['close']))
    zdf = check(str(result['zdf']))
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

    sql = "MERGE INTO SC61.TRZRQMX A USING(SELECT '"+busidate+"' busidate,'"+market+"' market,'"+scode+"' scode,'"\
    +sname+"' sname,'"+close+"' close,'"+zdf+"' zdf,'"+rzye+"' rzye,'"+rzyezb+"' rzyezb,'"+rzmre+"' rzmre,'"+rzche+"' rzche,'"\
    +rzjmre+"' rzjmre,'"+rqye+"' rqye,'"+rqyl+"' rqyl,'"+rqmcl+"' rqmcl,'"+rqchl+"' rqchl,'"+rqjmcl+"' rqjmcl,'"+rzrqye+"' rzrqye,'"\
    +rzrqyecz+"' rzrqyecz FROM DUAL) B ON (A.busidate = B.busidate AND A.market = B.market AND A.scode=B.scode)\
    WHEN NOT MATCHED THEN\
    INSERT(A.busidate,A.market,A.scode,A.sname,A.close,A.zdf,A.rzye,A.rzyezb,A.rzmre,A.rzche,A.rzjmre,A.rqye,A.rqyl,A.rqmcl,A.rqchl,A.rqjmcl,A.rzrqye,A.rzrqyecz)\
    VALUES(B.busidate,B.market,B.scode,B.sname,B.close,B.zdf,B.rzye,B.rzyezb,B.rzmre,B.rzche,B.rzjmre,B.rqye,B.rqyl,B.rqmcl,B.rqchl,B.rqjmcl,B.rzrqye,B.rzrqyecz)"
    try:
        ExecDB(sql)
    except:
        print(sql)

page = 1

for page in range(1,20):
  now = datetime.datetime.now()
  delta = datetime.timedelta(days=-1)
  date = (now + delta).strftime('%Y-%m-%d')
  url_sh = "http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=RZRQ_DETAIL_NJ&token=70f12f2f4f091e459a279469fe49eca5\
&filter=(market=%27SH%27%20and%20tdate=%27"+date+"%27)&st=rzjmre&sr=-1&p="+str(page)+"&ps=50&js=var%20fGjPpnWK={pages:(tp),data:(x)}&type=RZRQ_DETAIL_NJ&filter=(market=%27SH%27)&time=1&rt=51589654"
  url_sz = "http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=RZRQ_DETAIL_NJ&token=70f12f2f4f091e459a279469fe49eca5\
&filter=(market=%27SZ%27%20and%20tdate=%27"+date+"%27)&st=rzjmre&sr=-1&p="+str(page)+"&ps=50&js=var%20fGjPpnWK={pages:(tp),data:(x)}&type=RZRQ_DETAIL_NJ&filter=(market=%27SZ%27)&time=1&rt=51589654"
  UP(url_sh)
  UP(url_sz)









