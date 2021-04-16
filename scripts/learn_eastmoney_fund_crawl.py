import csv
import time
import random
import requests
import traceback
from time import sleep
from fake_useragent import UserAgent
from lxml import etree

# 尝试请求一页数据，尽量设置随机睡眠时间和使用随机生成的headers，
# 这是爬虫人最基本的道德修养，也是最简单的防反爬措施：
page = 1  # 设置爬取的页数


def get_fund(url):
    # sleep(random.uniform(1, 2))  # 随机出现1-2之间的数，包含小数
    headers = {"User-Agent": UserAgent(verify_ssl=False).random}
    # url = f'http://guba.eastmoney.com/list,of{fundcode}_{page}.html'
    response = requests.get(url, headers=headers, timeout=10)
    return response
    # print(response)


def parse_fund(response, fundcode):
    parse = etree.HTML(response.text)  # 解析网页
    items = parse.xpath('//*[@id="articlelistnew"]/div')[1:91]
    for item in items:
        item = {
            '阅读': ''.join(item.xpath('./span[1]/text()')).strip(),
            '评论': ''.join(item.xpath('./span[2]/text()')).strip(),
            '标题': ''.join(item.xpath('./span[3]/a/text()')).strip(),
            '作者': ''.join(item.xpath('./span[4]/a/font/text()')).strip(),
            '时间': ''.join(item.xpath('./span[5]/text()')).strip()
        }
        # print(item)

        with open(f'./{fundcode}.csv', 'a', encoding='utf_8_sig', newline='') as fp:
            fieldnames = ['阅读', '评论', '标题', '作者', '时间']
            writer = csv.DictWriter(fp, fieldnames)
            writer.writerow(item)


def main(page):
    fundcode = '320007'  # 可替换任意基金代码 005827 161725 320007
    url = f'http://guba.eastmoney.com/list,of{fundcode}_{page}.html'
    html = get_fund(url)
    parse_fund(html, fundcode)


if __name__ == '__main__':
    for page in range(1, 3376):  # 爬取多少页
        main(page)
        time.sleep(random.uniform(1, 2))
        print(f"第{page}页提取完成")
