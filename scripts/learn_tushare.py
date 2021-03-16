import tushare as ts

token = 'cd21671a8fdccfb075508780f3edb2627c36aa1be9d588a6e749ee53'
ts.set_token(token)

# 初始化pro接口
pro = ts.pro_api()
df = pro.trade_cal(exchange='', start_date='20180901', end_date='20181001',
                   fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
# df = pro.query('trade_cal', exchange='', start_date='20180901', end_date='20181001',
#                fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
print(df)
