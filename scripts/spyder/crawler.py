import requests,datetime
import pylog
import socket,json
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
def save_sz_data(url,date):

    try:
        html_doc = get_content(url)
        bs = BeautifulSoup(html_doc, "html.parser")  # 创建BeautifulSoup对象
        print(date,"数据：")
        tr_content = bs.findAll('tr')
        # 第一行是表头，去掉
        for i in range(1,len(tr_content)):
            td_content = tr_content[i].findAll('td')
            print(td_content[0].text,":",td_content[2].text)

    except Exception as e:
        pylog.log.exception("解析dom tree 时出现异常：")



# 上交所数据解析
# date 为想要获得的数据的日期，target为gp,jj,zq(股票，基金，债券)
def save_sh_data(date, target):
    web_headers = {
        'Referer': 'http://www.sse.com.cn/market/stockdata/overview/day/',
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.4',
    }

    web_url = "http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?\
               jsonCallBack=jsonpCallback80603&searchDate=" + date + "&prodType=" + target + "&_=1505887257745"
    webresponse = requests.get(url=web_url, headers=web_headers).content.decode()

    webdata = webresponse.lstrip("jsonpCallback80603(")
    result = json.loads(webdata.rstrip(")"))["result"]
    if target == "gp":
        print(date, "上海市场股票成交额：", result[2]['trdAmt1'])
        print(date, "A股成交额：", result[0]['trdAmt1'])
        print(date, "B股成交额：", result[1]['trdAmt1'])
    if target == "jj":
        print(date, "基金总体成交额：", result[1]['trdAmt'])
        print(date, "ETF成交额：", result[2]['trdAmt'])
    if target == "zq":
        print(date, "国债现货成交额：", result[0]['trdAmt'])
        print(date, "可转债成交额：", result[2]['trdAmt'], '\n')



if __name__ == "__main__":

    date_today = str(datetime.date.today())  # 今天的日期

    ############ 以下部分用于落库当天的市场数据 ###################
    save_sh_data(date_today,"gp")
    save_sh_data(date_today, "jj")
    save_sh_data(date_today, "zq")

    sz_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+' + date_today + '&ENCODE=1&TABKEY=tab1'
    save_sz_data(sz_url,date_today)

    ############以下部分用于回补历史数据（从当天开始往回回溯）############
    date = date_today
    while date:
        # 深交所数据
        sz_url = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=excel&CATALOGID=1804&txtDate=+'+date+'&ENCODE=1&TABKEY=tab1'
        save_sz_data(sz_url,date)
        # 上交所数据
        save_sh_data(date, "gp")
        save_sh_data(date, "jj")
        save_sh_data(date, "zq")
        # 往前移动一天
        date = dateIncrease(date,-1)