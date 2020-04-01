import requests,datetime
import pylog
import socket,json
import csv
import os
import sys
import time
import cx_Oracle
from bs4 import BeautifulSoup

success_cnt =0
row_sz = ['1900-01-01','SH_SZ',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
row_sh = ['1900-01-01','SH_SZ',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
# 利用datetime对输入的date值进行自增 n 天操作
# 返回值需要再次转换为str，方便对获取股票数据的接口调用
def dateIncrease(currentDate,n):
    currentDate = date2Datetime(currentDate)
    next_date = currentDate + datetime.timedelta(days=n)
    return str(next_date)[0:10]


def date2Datetime(date):
    return datetime.datetime.strptime(date, "%Y-%m-%d")

# 获得深交所html文件
def get_content(url):
    try:
        rep = requests.get(url)
        rep.encoding = 'GBK'
        return rep.text
    except socket.timeout as e:
        pylog.log.exception("请求出现异常：")

# 深交所数据解析
def save_sz_data(date):
    result = []
    cnt = 0
    global row_sz
    global success_cnt
    #row = ['1900-01-01','SH_SZ',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
    row_sz[0] = date.replace('-','')
    row_sz[1] = "\'SZ\'"
   # url = 'http://www.szse.cn/szseWeb/ShowReport.szse?CATALOGID=1804&SHOWTYPE=excel&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
    url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
    try:
        html_doc = get_content(url)
     #   print(html_doc,"oral data ")
        bs = BeautifulSoup(html_doc, "html.parser")  # 创建BeautifulSoup对象
        #print(date,"数据：")
        tr_content = bs.findAll('tr')
#        print("content:",tr_content)
        # 第一行是表头，去掉
       # print(date,"深圳市场数据:\n")
#        print(tr_content)        
#        print("len=",len(tr_content))
        for i in range(1,len(tr_content)):
            td_content = tr_content[i].findAll('td')
            cnt = cnt +1
           # print(td_content)
            if(td_content[0].text.strip() == '股票'):
                row_sz[2] = int(td_content[2].text.replace(',',''))
                #print("gu piao\n")
            if(td_content[0].text.strip() == '主板A股'):
                row_sz[3] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '主板B股'):
                row_sz[4] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '中小板'):
                row_sz[5] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '创业板'):
                row_sz[6] = int(td_content[2].text.replace(',',''))

            if(td_content[0].text.strip() == '基金'):
                row_sz[7] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == 'LOF'):
                row_sz[8] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == 'ETF'):
                row_sz[9] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '分级基金'):
                row_sz[10] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '封闭式基金'):
                row_sz[11] = int(td_content[2].text.replace(',',''))
                
            if(td_content[0].text.strip() == '债券'):
                row_sz[12] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '国债'):
                row_sz[13] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '公司债'):
                row_sz[16] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '企业债'):
                row_sz[14] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '债券回购'):
                row_sz[18] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '可转换债券'):
                row_sz[15] = int(td_content[2].text.replace(',',''))   
                
            print(td_content[0].text,":",td_content[2].text)
            
        if cnt == 0:           
            return
        result.append(row_sz)
       # write_data(result, 'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv")
        success_cnt = success_cnt + 1
    except Exception as e:
        pylog.log.exception("解析dom tree 时出现异常：")
        



# 上交所数据解析
# date 为想要获得的数据的日期，target为gp,jj,zq(股票，基金，债券)
#def save_sh_data(date, target):
def save_sh_data(date):
    result = []
    #row_sh = ['1900-01-01','SH_SZ',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    global row_sh
    global success_cnt
    webresponse_gp = []
    webresponse_jj = []
    webresponse_zq = []
    result_gp = []
    result_jj = []
    result_zq = []
    web_headers = {
        'Referer': 'http://www.sse.com.cn/market/stockdata/overview/day/',
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.4',
    }

   # web_url = "http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?\
    #           jsonCallBack=jsonpCallback80603&searchDate=" + date + "&prodType=" + target + "&_=1505887257745"
    
    web_url_gp = "http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?\
               jsonCallBack=jsonpCallback80603&searchDate=" + date + "&prodType=gp&_=1505887257745"              
              

    web_url_jj = "http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?\
               jsonCallBack=jsonpCallback80603&searchDate=" + date + "&prodType=jj&_=1505887257745"
               
     # oraginal info        
#    web_url_zq = "http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?\
#               jsonCallBack=jsonpCallback80603&searchDate=" + date + "&prodType=zq&_=1505887257745"                    
#               
    # modify info
    web_url_zq = "http://query.sse.com.cn/marketdata/tradedata/queryBondTradingOverAll.do?\
               jsonCallBack=jsonpCallback39586&searchDate=" + date + "&_=1506566229880"
    
    #print("zq web url:" + web_url_zq )
    webresponse_gp = requests.get(url=web_url_gp, headers=web_headers).content.decode()
    webresponse_jj = requests.get(url=web_url_jj, headers=web_headers).content.decode()
    webresponse_zq = requests.get(url=web_url_zq, headers=web_headers).content.decode()
    
   # print(webresponse_gp)
  #  print(webresponse_jj)
   # print(webresponse_zq)
    
    #print("zq web response context:" + webresponse_zq)
    webdata_gp = webresponse_gp.lstrip("jsonpCallback80603(")
    webdata_jj = webresponse_jj.lstrip("jsonpCallback80603(")
    webdata_zq = webresponse_zq.lstrip("jsonpCallback39586(")
    
    result_gp = json.loads(webdata_gp.rstrip(")"))["result"]
    result_jj = json.loads(webdata_jj.rstrip(")"))["result"]
    result_zq = json.loads(webdata_zq.rstrip(")"))["result"]
    result_zq = result_zq[0]
    if result_zq:
        pass
    else:
        return
    
  #  print("上海市场 -------------------------------------------------------------“)
    #if target == "gp":
    #for gp handle
    #print(result_gp)
    print(date, "上海市场股票成交额：",result_gp[2]['trdAmt1'])
    print(date, "A股成交额：", result_gp[0]['trdAmt1'])
    print(date, "B股成交额：", result_gp[1]['trdAmt1'])
   # if target == "jj":
      #for jj handle      
   # print(result_jj)
    print(date, "基金总体成交额：", result_jj[1]['trdAmt'])
    print(date, "封闭式基金：", result_jj[0]['trdAmt'])
    print(date, "ETF成交额：", result_jj[2]['trdAmt'])
    print(date, "LOF", result_jj[3]['trdAmt'])
    print(date, "分级LOF", result_jj[4]['trdAmt'])
    #if target == "zq":
    # for zq handle
   # print("zq result set" , result_zq)
    print(date, "国债现货成交额：", result_zq[3]['txAmout'])
    print(date, "企业债：", result_zq[2]['txAmout'])
    print(date, "可转债：", result_zq[1]['txAmout'])
    print(date, "质押回购：", result_zq[6]['txAmout'])
    print(date, "公司债", result_zq[5]['txAmout'])    
    print(date, "地方政府债", result_zq[4]['txAmout'])
    print(date, "债券", result_zq[7]['txAmout'], '\n')
    
 
    row_sh[0] = date.replace('-','')
    row_sh[1] = "\'SH\'"
    row_sh[2] = result_gp[2]['trdAmt1']
    row_sh[3] = result_gp[0]['trdAmt1']
    row_sh[4] = result_gp[1]['trdAmt1']
    
    row_sh[7] = result_jj[1]['trdAmt']
    row_sh[8] = result_jj[3]['trdAmt']
    row_sh[9] = result_jj[2]['trdAmt']
    row_sh[10] = result_jj[4]['trdAmt']
    row_sh[11] = result_jj[0]['trdAmt']
    
    row_sh[12] = result_zq[7]['txAmout']
    row_sh[13] = result_zq[3]['txAmout']
    row_sh[14] = result_zq[2]['txAmout']
    row_sh[15] = result_zq[1]['txAmout']
    row_sh[16] = result_zq[5]['txAmout']
    row_sh[17] = result_zq[4]['txAmout']
    row_sh[18] = result_zq[6]['txAmout']
    
    result.append(row_sh)
    #write_data(result, 'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv")
    success_cnt = success_cnt + 1 

def conndb():
    username="kingstar"
    userpwd="kingstar"
    host="10.29.7.211"
    port=1521
    dbname="siddc01"    
    dsn=cx_Oracle.makedsn(host, port, dbname)
    db=cx_Oracle.connect(username, userpwd, dsn) 
    return db

def SelectDB(db,sql):
##select 查询
    cursor = db.cursor() 
    cursor.execute(sql)
    result=cursor.fetchall()
    cursor.close()
    return result
    
def DMLDB_N(db,sql):
##插入，更新，删除
   cursor = db.cursor()
   cursor.execute(sql)
   cursor.close()
   db.commit()
   
def save_sh_sz_data(date):
 #   print("handle date :" + data +"\n")
    print("###########################################################")
    print(date,"上海市场数据")
    save_sh_data(date)
    print("###########################################################")    
    print(date,"深圳市场数据")    
    save_sz_data(date)
    print("###########################################################")

def get_execute_sql(row):    
    index = 0 
    sql_str = "insert into sc61.tb_dbf_sccj(RQ,JYS,CJJEGP,CJJEA,CJJEB,CJJEZXB,CJJECYB,CJJEJJ,CJJELOF,CJJEETF,CJJEFJ,CJJEFBJJ,CJJEZQ,CJJEGZ,CJJEQZ,CJJEZZ,CJJEGSZ,CJJEDFZ,CJJEHG) values("
    
    while index < len(row):        
        if index == len(row) -1:
            sql_str = sql_str + str(row[index])
        else:
            sql_str = sql_str + str(row[index])
            sql_str = sql_str + ","
        index = index +1
    sql_str = sql_str +")"        
    return sql_str

def sync_sh_sz_data_2_oracle():
    global row_sh
    global row_sz
    #print("start to saving data")
    pylog.log.exception("start to saving data")
    try:
        db=conndb()
    except Exception as e:
        pylog.log.exception("db connect exception")   
        return 0
    # insert sh data
    try:
        sh_sql = get_execute_sql(row_sh)
        DMLDB_N(db,sh_sql)
    except Exception as e:
        pylog.log.exception("sh sql exec exception")
    # insert sz data
    try:
        sz_sql = get_execute_sql(row_sz)
        DMLDB_N(db,sz_sql)
    except Exception as e:
        pylog.log.exception("sz sql exec exception")
    db.close()
    pylog.log.exception("sync data done")
    
def write_data(data, name):
    file_name = name
    with open(file_name, 'a+', errors='ignore', newline='') as f:
        f_csv = csv.writer(f)
        f_csv.writerows(data)
    
def write_info():
    result = []
    filename =  'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv"
    if os.path.exists(filename):
        os.remove(filename)
    #os.remove(filename)
    row = ['rq','jys','cjjegp','cjjea','cjjeb','cjjezxb','cjjecyb','cjjejj','cjjelof','cjjeetf','cjjefj','cjjefbjj','cjjezq','cjjegz','cjjeqz','cjjezz','cjjegsz','cjjedfz','cjjehg']    
    result.append(row)
    write_data(result, filename)

if __name__ == "__main__":
    begin_time=162000
    #end_time = 240000
    
    date_today = str(datetime.date.today())  # 今天的日期
    date = date_today
    #date = '2018-11-21'

    try:
        while success_cnt < 2:
           cur_time = int(time.strftime("%H%M%S"))
           if  cur_time < begin_time:
               print("time hit ")
               sys.exit(0)

           success_cnt = 0
           save_sh_sz_data(date)
           if success_cnt <= 1:
              #time.sleep(60)
              break
              
           pylog.log.exception("succent_cnt=" + str(success_cnt))
        sync_sh_sz_data_2_oracle()
    except SystemExit:
        pylog.log.exception("system exception")
        pass
    