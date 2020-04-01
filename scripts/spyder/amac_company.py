# -*- coding: utf-8 -*-  

import requests,io,sys,time
from bs4 import BeautifulSoup
import json,urllib
import cx_Oracle
from lxml import etree
import threading
from queue import Queuea


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

def GetJson(url):
    payload = {}
    headers = {'content-type': 'application/json'}
    ret = requests.post(url, data=json.dumps(payload), headers=headers)
    ret.encoding = 'utf-8'
    ret = ret.text
    text = json.loads(ret)
    return text

def getHtml(url):
    b = False
    while not b:
      try:
         page=urllib.request.urlopen(url)  
         html=page.read().decode(encoding='utf-8',errors='strict')
         page.close()
         b = True
      except :
         pass
    return html

def ListTemp(lists):
    if lists:
        member = lists[0].xpath('string(.)').replace('\n','').replace(' ','').replace(' ','')
    else :
        member = 'None'
    return member

def Getcontent(url):
    text2 = GetJson(url)      
    content = text2['content']
    return content

def Gettxt(text):
    txt=' '.join(text.split())
    return txt

def Trysql(sql):
    try:
        sql = ExecDB(sql.encode("GB18030"))
        #print(sql)
    except:
        #print("sql:",sql)
        pass

def Truncate():
    truncate_1 = "truncate table amac_smjj_company"
    truncate_1 = ExecDB(truncate_1)

def GetQ():
    url = 'http://gs.amac.org.cn/amac-infodisc/api/pof/manager?rand=0.3904312621164139&page=0&size=20'
    global q
    q = Queue()
    text = GetJson(url)
    n = text['totalPages']
    for i in range(n):
        url2 = 'http://gs.amac.org.cn/amac-infodisc/api/pof/manager?rand=0.3904312621164139&page='+str(i)+'&size=20'
        q.put(url2)
    return q

def Thread():
    threads=[]
    for code in range(20):
       thread=threading.Thread(target=Work)
       threads.append(thread)
    for t in threads:
       t.start()  #启动一个线程
    for t in threads:
       t.join()  #等待每个线程

def Work():
    while not q.empty():
        print(q.qsize())
        url3 = q.get()
        try :
          txt = GetJson(url3)
          print(txt)
          m = txt['numberOfElements']
          content = Getcontent(url3)
          for x in range(m):
            dicts = content[x]
            managerName =str(dicts['managerName'])
            registerNo =str(dicts['registerNo'])
            registerAddress =str(dicts['registerAddress'])
            officeAddress =str(dicts['officeAddress'])
            sql = "insert into amac_smjj_company(managerName,registerNo,registerAddress,officeAddress)"\
                  "values('"+managerName+"','"+registerNo+"','"+registerAddress+"','"+officeAddress+"')"
#            #print(sql)
            sql = Trysql(sql)           
#            html_doc1 = getHtml(fundurl)
#            html = etree.HTML(html_doc1)
#            jjmc = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[1]/td[2]'))).strip().replace('\'','\'\'')
#            jjbm = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[2]/td[2]'))).strip()
#            clsj = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[3]/td[2]'))).strip().replace('-','')
#            basj = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[4]/td[2]'))).strip().replace('-','')
#            jjbajd = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[5]/td[2]'))).strip().replace('-','')
#            jjlx = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[6]/td[2]'))).strip()
#            bz = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[7]/td[2]'))).strip()
#            jjglr = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[8]/td[2]'))).strip()
#            gllx = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[9]/td[2]'))).strip()
#            tgr = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[10]/td[2]'))).strip()
#            yzzt = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[11]/td[2]'))).strip()
#            jjxxzhgxsj = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[12]/td[2]'))).strip().replace('-','')
#            jjxhtbts = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[13]/td[2]'))).strip()
#            dyyb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[15]/td[2]'))).strip()
#            bnb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[16]/td[2]'))).strip()
#            nb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[17]/td[2]'))).strip()
#            jb = str(ListTemp(html.xpath('/html/body/div/div[2]/div/table/tbody/tr[18]/td[2]'))).strip()
#            sql2 = "insert into amac_smjj_fund(jjmc,jjbm,clsj,basj,jjbajd,jjlx,bz,jjglr,gllx,tgr,"\
#                   "yzzt,jjxxzhgxsj,jjxhtbts,dyyb,bnb,nb,jb) "\
#                   "values('"+jjmc+"','"+jjbm+"','"+clsj+"','"+basj+"','"+jjbajd+"','"+jjlx+"','"+bz\
#                   +"','"+jjglr+"','"+gllx+"','"+tgr+"','"+yzzt+"','"+jjxxzhgxsj+"','"+jjxhtbts+"','"\
#                   +dyyb+"','"+bnb+"','"+nb+"','"+jb+"')"
#            #print(sql2)
#            sql2 = Trysql(sql2)
#            html_doc2 = getHtml(managerurl)
#            html2 = etree.HTML(html_doc2)
#            jgcxxx = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[1]/td[2]')))).strip().replace('\'','\'\'')
#            jjglrch = str(ListTemp(html2.xpath('//*[@id="complaint2"]'))).replace('&nbsp','').strip().replace('\'','\'\'')
#            jjglrzh = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[4]/td[2]'))).strip().replace('\'','\'\'')
#            djbh = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[5]/td[2]'))).strip()
#            zzjgdm = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[6]/td[2]'))).strip()
#            djsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[7]/td[2]'))).strip().replace('-','')
#            clsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[7]/td[4]'))).strip().replace('-','')
#            zcdz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[8]/td[2]'))).strip()
#            bgdz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[9]/td[2]'))).strip()
#            zczb = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[10]/td[2]'))).strip()
#            sjzb = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[10]/td[4]'))).strip()
#            qyxz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[11]/td[2]'))).strip()
#            zczbsjbl = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[11]/td[4]'))).strip()
#            gljjzylb = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[12]/td[2]'))).strip()
#            sqqtywlx = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[12]/td[4]'))).strip()
#            ygrs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[13]/td[2]'))).strip()
#            jgwz = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[13]/td[4]'))).strip()
#            sfwhy = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[15]/td[2]'))).strip()
#            if sfwhy == '是' :           
#                dqhylx = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[16]/td[2]'))).strip()
#                rhsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[16]/td[4]'))).strip().replace('-','')
#                flyjszt = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[18]/td[2]'))).strip()
#                if flyjszt == '办结':
#                    lsswsmc = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[19]/td[2]'))).strip().replace('\'','\'\'')
#                    lsxm = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[2]'))).strip()
#                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]'))).strip()
#                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[2]'))).strip()
#                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[4]'))).strip()
#                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[24]/td[2]')))).strip().replace('\'','\'\'')
#                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[25]/td[2]')))).strip().replace('\'','\'\'')
#                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[30]/td[2]'))).strip().replace('-','')
#                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip()
#                else :
#                    lsswsmc = ''
#                    lsxm = ''
#                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[2]'))).strip()
#                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[2]'))).strip()
#                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[4]'))).strip()
#                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]')))).strip().replace('\'','\'\'')
#                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[2]')))).strip().replace('\'','\'\'')
#                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[28]/td[2]'))).strip().replace('-','')
#                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip()
#            else:
#                dqhylx = ''
#                rhsj = ''
#                flyjszt = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[17]/td[2]'))).strip()
#                if flyjszt == '办结' :
#                    lsswsmc = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[18]/td[2]'))).strip().replace('\'','\'\'')
#                    lsxm = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[19]/td[2]'))).strip()
#                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[2]'))).strip()
#                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]'))).strip()
#                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[4]'))).strip()
#                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[23]/td[2]')))).strip().replace('\'','\'\'')
#                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[24]/td[2]')))).strip().replace('\'','\'\'')
#                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[29]/td[2]'))).strip().replace('-','')
#                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip()
#                else :
#                    lsswsmc = ''
#                    lsxm = ''
#                    fddbr = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[19]/td[2]'))).strip()
#                    sfycyzg = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[2]'))).strip()
#                    zgqdfs = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[20]/td[4]'))).strip()
#                    gzll = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[21]/td[2]')))).strip().replace('\'','\'\'')
#                    ggqk = str(Gettxt(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[22]/td[2]')))).strip().replace('\'','\'\'')
#                    jgxxzhgxsj = str(ListTemp(html2.xpath('/html/body/div/div[2]/div/table/tbody/tr[27]/td[2]'))).strip().replace('-','')
#                    tbtsxx = str(ListTemp(html2.xpath('//*[@id="specialInfos"]'))).strip()                   
#            sql3 = "declare \n"\
#                   "   gzll_v clob; \n"\
#                   "   ggqk_v clob; \n"\
#                   "begin \n"\
#                   "   gzll_v := '"+gzll+"';\n"\
#                   "   ggqk_v := '"+ggqk+"';\n"\
#                   "   insert into amac_smjj_manager (jgcxxx,jjglrch,jjglrzh,djbh,zzjgdm,djsj,clsj,zcdz,bgdz,zczb,sjzb,qyxz,zczbsjbl,"\
#                   "gljjzylb,sqqtywlx,ygrs,jgwz,sfwhy,dqhylx,rhsj,flyjszt,lsswsmc,lsxm,fddbr,sfycyzg,zgqdfs,gzll,ggqk,jgxxzhgxsj,tbtsxx) "\
#                   "values('"+jgcxxx+"','"+jjglrch+"','"+jjglrzh+"','"+djbh+"','"+zzjgdm+"','"+djsj+"','"+clsj+"','"+zcdz+"','"\
#                   +bgdz+"','"+zczb+"','"+sjzb+"','"+qyxz+"','"+zczbsjbl+"','"+gljjzylb+"','"+sqqtywlx+"','"+ygrs+"','"+jgwz+"','"\
#                   +sfwhy+"','"+dqhylx+"','"+rhsj+"','"+flyjszt+"','"+lsswsmc+"','"+lsxm+"','"+fddbr+"','"+sfycyzg+"','"+zgqdfs\
#                   +"',gzll_v,ggqk_v,'"+jgxxzhgxsj+"','"+tbtsxx+"');\n"\
#                   "end;"
#            sql3 = Trysql(sql3)
        except :
          #print("ERR: "+url3+"\n")
          q.put(url3)

def main():
    Truncate()
    GetQ()
    #Thread()
    Work()

    
            
if __name__ == '__main__':
   start = time.time()
   print(start)
   main()
   end = time.time()
   print(end)
   m, s = divmod(end-start, 60)
   h, m = divmod(m, 60)
   print("运行时长：%02d:%02d:%02d" % (h, m, s))








