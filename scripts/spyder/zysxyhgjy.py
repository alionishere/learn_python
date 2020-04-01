# -*- coding: utf-8 -*-  

import requests,io,sys,time
import json,urllib,chardet
import cx_Oracle,datetime


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
    #return result

def getHtml(url):  
    page=urllib.request.urlopen(url)  
    html=page.read().decode(encoding='utf-8',errors='strict')
    page.close()
    return html


def main():
    enddate = datetime.datetime.now().strftime('%Y-%m-%d')
    delta = datetime.timedelta(days=5)
    begindate = (datetime.datetime.now() - delta).strftime('%Y-%m-%d')
    url = 'http://bond.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=GS_XYHGJYGK&TABKEY=tab1&txtBeginDate='+begindate+'&txtEndDate='+enddate+'&random=0.9428884000399672'
    content = getHtml(url)
    text = json.loads(content)
    data = text[0]['data']
    lenth = len(data)
    if lenth > 0:
        for i in range(lenth):
            jyrq = data[i]['jyrq'].replace('-','')
            zqjc = data[i]['zqjc'].replace('-','0')
            zqdm = data[i]['zqdm'].replace('-','0')
            zgll = data[i]['zgll'].replace('-','0')
            zdll = data[i]['zdll'].replace('-','0')
            jqpjll = data[i]['jqpjll'].replace('-','0')
            jqzdfd = data[i]['jqzdfd'].replace('-','0')
            cjje = data[i]['cjje'].replace('-','0')
            sql = "MERGE INTO SC61.TXYHGJY A USING(SELECT '"+jyrq+"' JYRQ,'"+zqjc+"' ZQJC,'"+zqdm+"' ZQDM,'"+zgll+"' ZGLL,'"+zdll+"' ZDLL,'"\
                  +jqpjll+"' JQPJLL,'"+jqzdfd+"' JQZDFD,'"+cjje+"' CJJE FROM DUAL) B ON (A.JYRQ = B.JYRQ AND A.ZQDM = B.ZQDM)\
                  WHEN MATCHED THEN UPDATE SET A.ZQJC=B.ZQJC,A.ZGLL=B.ZGLL,A.ZDLL=B.ZDLL,A.JQPJLL=B.JQPJLL,A.JQZDFD=B.JQZDFD,A.CJJE=B.CJJE \
                  WHEN NOT MATCHED THEN INSERT(A.JYRQ,A.ZQJC,A.ZQDM,A.ZGLL,A.ZDLL,A.JQPJLL,A.JQZDFD,A.CJJE)\
                  VALUES(B.JYRQ,B.ZQJC,B.ZQDM,B.ZGLL,B.ZDLL,B.JQPJLL,B.JQZDFD,B.CJJE)"
            sys = str(datetime.datetime.now())
            sql_log = "MERGE INTO SC61.TXYHGJY_LOG A USING(SELECT '"+jyrq+"' JYRQ,'"+sys+"' SYS FROM DUAL) B\
                  ON(A.JYRQ=B.JYRQ) WHEN NOT MATCHED THEN INSERT(A.JYRQ,A.SYS) VALUES(B.JYRQ,B.SYS)"
            #print(sql) 
            ExecDB(sql)
            ExecDB(sql_log)
            
if __name__ == '__main__':
    main()









