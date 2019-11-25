# -*- coding: utf-8 -*-
import json
import requests

# username = 'sac023002'
# password = 'Sac023002*zg'

global cookie
cookie = input("请输入首页cookie：")


def mainurl(url, cookie):
    headers = {"Accept": "application/json, text/plain, */*",
               "Accept-Encoding": "gzip, deflate, br",
               "Accept-Language": "zh-CN,zh;q=0.9",
               "Connection": "keep-alive",
               "Content-Length": "2",
               "Content-Type": "application/json;charset=UTF-8",
               "Cookie": cookie,
               "Host": "ambers.amac.org.cn",
               "Origin": "https://ambers.amac.org.cn",
               "Referer": "https://ambers.amac.org.cn/web/app.html",
               "User-Agent": "User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36)",
               "X-CSRF-TOKEN": "Y"
               }
    payload = {"fundStatus": "FS01"}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.encoding = 'utf-8'
    re = response.text
    # print(re)
    return re


def sonurl(url, cookie):
    headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
               "Accept-Encoding": "gzip, deflate, br",
               "Accept-Language": "zh-CN,zh;q=0.9",
               "Cache-Control": "max-age=0",
               "Connection": "keep-alive",
               "Cookie": cookie,
               "Host": "ambers.amac.org.cn",
               "Upgrade-Insecure-Requests": "1",
               "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36",
               }
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'
    re = response.text
    # print(re)
    return re


def main():
    main_url = 'https://ambers.amac.org.cn/web/api/product/queryIM/search?pageSize=10&pageNo=1'
    # https: // ambers.amac.org.cn / web / api / product / queryIM / search?pageSize = 10 & pageNo = 1
    baseinfo_url = 'https://ambers.amac.org.cn/web/api/product/queryIM/baseInfo/1909021041103410'
    mainurl(main_url, cookie)
    # sonurl(baseinfo_url, cookie)


if __name__ == '__main__':
    main()
