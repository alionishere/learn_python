import chardet

data = '离离原上草，一岁一枯荣'.encode('gbk')
data2 = '离离原上草，一岁一枯荣'.encode('utf-8')
data3 = '最新の主要ニュース'.encode('euc-jp')
print(chardet.detect(data))
print(chardet.detect(data2))
print(chardet.detect(data3))
