import requests,datetime
import pylog
import socket,json
import csv
import os
import time
from bs4 import BeautifulSoup

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
    row = ['1900-01-01','SH_SZ',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
    row[0] = date.replace('-','')
    row[1] = 'SZ'
   # url = 'http://www.szse.cn/szseWeb/ShowReport.szse?CATALOGID=1804&SHOWTYPE=excel&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
    url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
    try:
        html_doc = get_content(url)
     #   print(html_doc,"oral data ")
        bs = BeautifulSoup(html_doc, "html.parser")  # 创建BeautifulSoup对象
        print(date,"数据：")
        tr_content = bs.findAll('tr')
#        print("content:",tr_content)
        # 第一行是表头，去掉
        for i in range(1,len(tr_content)):
            td_content = tr_content[i].findAll('td')
            
           # print(td_content)
            if(td_content[0].text.strip() == '股票'):
                row[2] = int(td_content[2].text.replace(',',''))
                #print("gu piao\n")
            if(td_content[0].text.strip() == '主板A股'):
                row[3] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '主板B股'):
                row[4] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '中小板'):
                row[5] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '创业板'):
                row[6] = int(td_content[2].text.replace(',',''))

            if(td_content[0].text.strip() == '基金'):
                row[7] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == 'LOF'):
                row[8] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == 'ETF'):
                row[9] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '分级基金'):
                row[10] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '封闭式基金'):
                row[11] = int(td_content[2].text.replace(',',''))
                
            if(td_content[0].text.strip() == '债券'):
                row[12] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '国债'):
                row[13] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '公司债'):
                row[16] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '企业债'):
                row[14] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '债券回购'):
                row[18] = int(td_content[2].text.replace(',',''))
            if(td_content[0].text.strip() == '可转换债券'):
                row[15] = int(td_content[2].text.replace(',',''))   
                
            print(td_content[0].text,":",td_content[2].text)
            
        
        result.append(row)
        write_data(result, 'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv")
    except Exception as e:
        pylog.log.exception("解析dom tree 时出现异常：")



# 上交所数据解析
# date 为想要获得的数据的日期，target为gp,jj,zq(股票，基金，债券)
#def save_sh_data(date, target):
def save_sh_data(date):
    result = []
    row = ['1900-01-01','SH_SZ',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
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
               
    web_url_zq = "http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?\
               jsonCallBack=jsonpCallback80603&searchDate=" + date + "&prodType=zq&_=1505887257745"                    
               
    print("web url:" + web_url_gp)
    webresponse_gp = requests.get(url=web_url_gp, headers=web_headers).content.decode()
    webresponse_jj = requests.get(url=web_url_jj, headers=web_headers).content.decode()
    webresponse_zq = requests.get(url=web_url_zq, headers=web_headers).content.decode()
    
   # print(webresponse_gp)
  #  print(webresponse_jj)
   # print(webresponse_zq)
    webdata_gp = webresponse_gp.lstrip("jsonpCallback80603(")
    webdata_jj = webresponse_jj.lstrip("jsonpCallback80603(")
    webdata_zq = webresponse_zq.lstrip("jsonpCallback80603(")
    
    result_gp = json.loads(webdata_gp.rstrip(")"))["result"]
    result_jj = json.loads(webdata_jj.rstrip(")"))["result"]
    result_zq = json.loads(webdata_zq.rstrip(")"))["result"]
    
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
    print(result_zq)
    print(date, "国债现货成交额：", result_zq[0]['trdAmt'])
    print(date, "企业债：", result_zq[1]['trdAmt'])
    print(date, "可转债：", result_zq[2]['trdAmt'])
    print(date, "质押回购：", result_zq[4]['trdAmt'])
    print(date, "公司债", result_zq[5]['trdAmt'])    
    print(date, "地方政府债", result_zq[6]['trdAmt'], '\n')
    
 
    row[0] = date.replace('-','')
    row[1] = 'SH'
    row[2] = result_gp[2]['trdAmt1']
    row[3] = result_gp[0]['trdAmt1']
    row[4] = result_gp[1]['trdAmt1']
    
    row[7] = result_jj[1]['trdAmt']
    row[8] = result_jj[3]['trdAmt']
    row[9] = result_jj[2]['trdAmt']
    row[10] = result_jj[4]['trdAmt']
    row[11] = result_jj[0]['trdAmt']
    
    row[13] = result_zq[0]['trdAmt']
    row[14] = result_zq[1]['trdAmt']
    row[15] = result_zq[2]['trdAmt']
    row[16] = result_zq[5]['trdAmt']
    row[17] = result_zq[6]['trdAmt']
    row[18] = result_zq[4]['trdAmt']
    
    result.append(row)
    write_data(result, 'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv")
    

def save_sh_sz_data(data):
 #   print("handle date :" + data +"\n")
    save_sh_data(date)
    save_sz_data(date)
    

def write_data(data, name):
    file_name = name
    with open(file_name, 'a+', errors='ignore', newline='') as f:
        f_csv = csv.writer(f)
        f_csv.writerows(data)
    
def write_info():
    result = []
    filename =  'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv"
#    os.remove(filename)
    row = ['rq','jys','cjjegp','cjjea','cjjeb','cjjezxb','cjjecyb','cjjejj','cjjelof','cjjeetf','cjjefj','cjjefbjj','cjjezq','cjjegz','cjjeqz','cjjezz','cjjegsz','cjjedfz','cjjehg']    
    result.append(row)
    write_data(result, filename)

if __name__ == "__main__":

    #default 20 days data
    
#    print("test starting........................\n")
#    money = "282,652,178,197"
#    print(money)
#    new_money = money.replace(',','')
#    print(new_money)
#    
#    in_money = int(new_money)
#    print(in_money)
#    print("-----------------------------------------\n")
#    
#    
    
    cur_index = 0
    default = 5
    date_today = str(datetime.date.today())  # 今天的日期
  
   # 
    ############ 以下部分用于落库当天的市场数据 ###################
   # save_sh_data(date_today,"gp")
   # save_sh_data(date_today, "jj")
    #save_sh_data(date_today, "zq")

   # sz_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+' + date_today + '&ENCODE=1&TABKEY=tab1'
    #save_sz_data(sz_url,date_today)

    ############以下部分用于回补历史数据（从当天开始往回回溯）############
    print("###########################################################################################")
    date = date_today
    date = dateIncrease(date,-1)
    write_info()
    while date:
        # 深交所数据
        #sz_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
       # sz_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?CATALOGID=1804&SHOWTYPE=excel&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
       #  sz_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
      #  sz_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?CATALOGID=1804&ENCODE=1&SHOWTYPE=excel&txtDate=20170920&TABKEY=tab1'
      #  save_sz_data(sz_url,date)
        # 上交所数据
       # save_sh_data(date, "gp")
       # save_sh_data(date, "jj")
       # save_sh_data(date, "zq")
       cur_index = cur_index + 1
       if cur_index >= default:
           break;
       save_sh_sz_data(date)
 #       save_sh(data)
 #       save_sh_data(date)
        # 往前移动一天
       date = dateIncrease(date,-1)        
       print("###########################################################################################")
    
       # 延时 1秒
       time.sleep(1)
    
#    write_data(result, 'SHCJ_' + datetime.datetime.now().strftime('%Y-%m-%d') + ".csv")