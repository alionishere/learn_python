# -*- coding: utf-8 -*-
import requests
import re
url = 'https://www.baidu.com/s?ie=UTF-8&wd=%E9%98%BF%E9%87%8C%E5%B7%B4%E5%B7%B4'
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}
res = requests.get(url, headers=headers).text
# print(res)

p_info = '<h.* class="t.*>(.*?)</h>'
info = re.findall(p_info, res, re.S)
print(info)

# res = '''<lsjdfajd>和
# 中<abcd>'''
#
# p_href = '<.*>(.*?)<.*>'
# p_title = '<>.*?<>'
# href = re.findall(p_href, res, re.S)
# print(href)

