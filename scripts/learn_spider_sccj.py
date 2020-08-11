# import pylog
import json
import requests


def get_content(url):
    rep = requests.get(url)
    rep.encoding = 'GBK'
    return rep.text


row_sz = ['1900-01-01','SH_SZ',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
result = []
cnt = 0
# global row_sz
global success_cnt

# row_sz[0] = date.replace('-' ,'')
row_sz[1] = "\'SZ\'"
url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1803_sczm&TABKEY=tab1&txtQueryDate=2020-07-31&random=0.07271119836211493'
url2 = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=scsj_zqrdgk&TABKEY=tab1&txtDate=2020-07-31&tjzqlb=D&random=0.5630625386842656'

html_doc = json.loads(get_content(url))[0]['data']
print(html_doc[0])
html_doc2 = json.loads(get_content(url2))[0]['data']
print((html_doc2[0]))
row_sz[2] = float(html_doc[0]['cjje'].replace(',' ,'') ) *100000000  # 股票
row_sz[3] = float(html_doc[1]['cjje'].replace(',' ,'') ) *100000000  # 主板A股
row_sz[4] = float(html_doc[2]['cjje'].replace(',' ,'') ) *100000000  # 主板B股
row_sz[5] = float(html_doc[3]['cjje'].replace(',' ,'') ) *100000000  # 中小板
row_sz[6] = float(html_doc[4]['cjje'].replace(',' ,'') ) *100000000  # 创业板
row_sz[7] = float(html_doc[5]['cjje'].replace(',' ,'') ) *100000000  # 基金
row_sz[8] = float(html_doc[7]['cjje'].replace(',' ,'') ) *100000000  # LOF
row_sz[9] = float(html_doc[6]['cjje'].replace(',' ,'') ) *100000000  # ETF
row_sz[10] = float(html_doc[9]['cjje'].replace(',' ,'') ) *100000000  # 分级基金
row_sz[11] = float(html_doc[8]['cjje'].replace(',' ,'') ) *100000000  # 封闭式基金
row_sz[12] = float(html_doc[10]['cjje'].replace(',' ,'') ) *100000000  # 债券
row_sz[13] = float(html_doc2[1]['cjje'].replace(',' ,'') ) *10000  # 国债
row_sz[14] = float(html_doc2[5]['cjje'].replace(',' ,'') ) *10000  # 企业债
row_sz[15] = float(html_doc2[8]['cjje'].replace(',' ,'') ) *10000  # 可转换债券
row_sz[16] = float(html_doc2[6]['cjje'].replace(',' ,'') ) *10000  # 公司债
row_sz[17] = float(html_doc2[2]['cjje'].replace(',' ,'') ) *10000  # 地方债
row_sz[18] = float(html_doc2[14]['cjje'].replace(',' ,'') ) *10000  # 债券回购
result.append(row_sz)
# write_data(result, 'd:\crwwlerfile\SH_SZ_DATA_' + datetime.datetime.now().strftime('%Y%m%d') + ".csv")
# success_cnt = success_cnt + 1
print(row_sz)
print(result)
    # pylog.log.exception("The data of sz is ok!")
# except Exception as e:
#     pass
#     # pylog.log.exception("解析dom tree 时出现异常：")


