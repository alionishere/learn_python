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
    #url = 'http://www.szse.cn/szseWeb/ShowReport.szse?CATALOGID=1804&SHOWTYPE=excel&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
    #url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
    url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1803_sczm&TABKEY=tab1&txtQueryDate='+date+'&random=0.9305893718552449'
    url2 = 'http://bond.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1804_gspt&TABKEY=tab1&txtDate='+date+'&random=0.5843141572927899'
    try:
        html_doc = json.loads(get_content(url))[0]['data']
        html_doc2 = json.loads(get_content(url2))[0]['data']
        row_sz[2] = float(html_doc[0]['cjje'].replace(',',''))*100000000  #股票
        row_sz[3] = float(html_doc[1]['cjje'].replace(',',''))*100000000  #主板A股
        row_sz[4] = float(html_doc[2]['cjje'].replace(',',''))*100000000  #主板B股
        row_sz[5] = float(html_doc[3]['cjje'].replace(',',''))*100000000  #中小板
        row_sz[6] = float(html_doc[4]['cjje'].replace(',',''))*100000000  #创业板
        row_sz[7] = float(html_doc[5]['cjje'].replace(',',''))*100000000  #基金
        row_sz[8] = float(html_doc[7]['cjje'].replace(',',''))*100000000  #LOF
        row_sz[9] = float(html_doc[6]['cjje'].replace(',',''))*100000000  #ETF
        row_sz[10] = float(html_doc[9]['cjje'].replace(',',''))*100000000  #分级基金
        row_sz[11] = float(html_doc[8]['cjje'].replace(',',''))*100000000  #封闭式基金
        row_sz[12] = float(html_doc[10]['cjje'].replace(',',''))*100000000  #债券
        row_sz[13] = float(html_doc2[1]['cjje'].replace(',',''))*10000  #国债
        row_sz[14] = float(html_doc2[5]['cjje'].replace(',',''))*10000  #企业债
        row_sz[15] = float(html_doc2[8]['cjje'].replace(',',''))*10000  #可转换债券
        row_sz[16] = float(html_doc2[6]['cjje'].replace(',',''))*10000  #公司债
        row_sz[17] = float(html_doc2[2]['cjje'].replace(',',''))*10000  #地方债
        row_sz[18] = float(html_doc2[14]['cjje'].replace(',',''))*10000  #债券回购
        result.append(row_sz)
       # write_data(result, 'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv")
        success_cnt = success_cnt + 1
    except Exception as e:
        pass
        #pylog.log.exception("解析dom tree 时出现异常：")
        



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
    #print(date, "上海市场股票成交额：",result_gp[2]['trdAmt1'])
    #print(date, "A股成交额：", result_gp[0]['trdAmt1'])
    #print(date, "B股成交额：", result_gp[1]['trdAmt1'])
   # if target == "jj":
      #for jj handle      
   # print(result_jj)
    #print(date, "基金总体成交额：", result_jj[1]['trdAmt'])
    #print(date, "封闭式基金：", result_jj[0]['trdAmt'])
    #print(date, "ETF成交额：", result_jj[2]['trdAmt'])
    #print(date, "LOF", result_jj[3]['trdAmt'])
    #print(date, "分级LOF", result_jj[4]['trdAmt'])
    #if target == "zq":
    # for zq handle
   # print("zq result set" , result_zq)
    #print(date, "国债现货成交额：", result_zq[3]['txAmout'])
    #print(date, "企业债：", result_zq[2]['txAmout'])
    #print(date, "可转债：", result_zq[1]['txAmout'])
    #print(date, "质押回购：", result_zq[6]['txAmout'])
    #print(date, "公司债", result_zq[5]['txAmout'])    
    #print(date, "地方政府债", result_zq[4]['txAmout'])
    #print(date, "债券", result_zq[7]['txAmout'], '\n')
    
 
    row_sh[0] = date.replace('-','')
    row_sh[1] = "\'SH\'"
    row_sh[2] = result_gp[2]['trdAmt1']
    row_sh[3] = result_gp[0]['trdAmt1']
    row_sh[4] = result_gp[1]['trdAmt1']
    row_sh[6] = result_gp[3]['trdAmt1']
    
    row_sh[7] = result_jj[1]['trdAmt']
    row_sh[8] = result_jj[3]['trdAmt']
    row_sh[9] = result_jj[2]['trdAmt']
    row_sh[10] = result_jj[4]['trdAmt']
    if len(result_jj[0]['trdAmt']) == 0:
       row_sh[11] = 0
    else:
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
    #print("###########################################################")
    #print(date,"上海市场数据")
    save_sh_data(date)
    #print("###########################################################")    
    #print(date,"深圳市场数据")    
    save_sz_data(date)
    #print("###########################################################")

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
    end_time = 240000
    
    date_today = str(datetime.date.today())  # 今天的日期
    date = date_today
    #date = '2018-11-27'

    try:
        while success_cnt < 2:
           cur_time = int(time.strftime("%H%M%S"))
           if  cur_time < begin_time:
               print("time hit ")
               sys.exit(0)

           success_cnt = 0
           save_sh_sz_data(date)
           if success_cnt <= 1:
              time.sleep(60)
              
           pylog.log.exception("succent_cnt=" + str(success_cnt))
        sync_sh_sz_data_2_oracle()
    except SystemExit:
        pylog.log.exception("system exception")
        pass
    