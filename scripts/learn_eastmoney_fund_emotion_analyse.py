import akshare as ak
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
import jieba
import stylecloud
from IPython.display import Image
import paddlehub as hub

df = pd.read_csv("./005827.csv", names=['阅读', '评论', '标题', '作者', '时间'])
# print(df)
# 重复和缺失数据
df = df.drop_duplicates()
df = df.dropna()
# print(df)
# print(df['阅读'])
# 数据类型转换
df['阅读'] = df['阅读'].str.replace('万', '').str.replace('亿', '').astype('float')
# df['阅读'] = df['阅读'].astype('float')
# print(df['阅读'])
df['时间'] = pd.to_datetime(df['时间'], errors='ignore')


# sys.exit(0)

# 机械压缩去重
def yasuo(st):
    for i in range(1, int(len(st) / 2) + 1):
        for j in range(len(st)):
            if st[j:j + i] == st[j + i:j + 2 * i]:
                k = j + i
                while st[k:k + i] == st[k + i:k + 2 * i] and k < len(st):
                    k = k + i
                st = st[:j] + st[k:]
    return st


yasuo(st="J哥J哥J哥J哥J哥")
df["标题"] = df["标题"].apply(yasuo)

# 过滤表情
df['标题'] = df['标题'].str.extract(r"([\u4e00-\u9fa5]+)")
df = df.dropna()  # 纯表情直接删除

# 过滤短句
df = df[df["标题"].apply(len) >= 3]
df = df.dropna()
print(df["标题"])

# 绘制词云图
# text1 = get_cut_words(content_series=df['标题'])
# text1 = df['标题']
# stylecloud.gen_stylecloud(text=' '.join(text1), max_words=200,
#                           collocations=False,
#                           font_path='simhei.ttf',
#                           icon_name='fas fa-heart',
#                           size=653,
#                           # palette='matplotlib.Inferno_9',
#                           output_name='./005827.png')
# Image(filename='./005827.png')

# 用更为量化的方法，计算出每个评论的情感评分
senta = hub.Module(name="senta_bilstm")
texts = df['标题'].tolist()
input_data = {'text': texts}
res = senta.sentiment_classify(data=input_data)
df['投资者情绪'] = [x['positive_probs'] for x in res]
# 重采样至15分钟
df['时间'] = pd.to_datetime(df['时间'])
df.index = df['时间']
data = df.resample('15min').mean().reset_index()

# 通过AkShare这一开源API接口获取上证指数分时数据，AkShare是基于Python的财经数据接口库，
# 可以实现对股票、期货、期权、基金、外汇、债券、指数、
# 数字货币等金融产品的基本面数据、历史行情数据的快速采集和清洗。
sz_index = ak.stock_zh_a_minute(symbol='sh000001', period='15', adjust="qfq")
sz_index['日期'] = pd.to_datetime(sz_index['day'])
sz_index['收盘价'] = sz_index['close'].astype('float')
data = data.merge(sz_index, left_on='时间', right_on='日期', how='inner')
plt.use('Qt5Agg')
data.index = data['时间']
data[['投资者情绪', '收盘价']].plot(secondary_y=['close'])
plt.show()
