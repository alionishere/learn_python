# -*- coding: utf-8 -*- #
import requests,io,sys,time
import json,urllib,math
import datetime
import hashlib
import random
def GetJson(url):
    headers =  {
    }
    ret = requests.post(url,headers=headers)
    ret.encoding = 'utf-8'
    ret = ret.text
    return ret

def getHtml(url):  
    page=urllib.request.urlopen(url)
    html=page.read().decode(encoding='utf-8',errors='strict')
    page.close()
    return html




    
def Work():
    for var in list(range(100)):
        suijishu=''.join(str(random.choice(range(10))) for _ in range(10))
        randomNumber =datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+str(suijishu)
        main_url = 'http://221.178.136.186:8090/pkt/service/getCode?random='+randomNumber
        #html_doc = GetJson(main_url)

        img = requests.get(main_url)
        if img.status_code==200:
           imgname = 'E:\\1-zhaxiaodong\\pickCar\\img\\'+suijishu+'.png'
           print('下载图片'+imgname)
           with open(imgname, 'wb') as fd:
                fd.write(img.content)

    
def main():
    Work()
    
            
if __name__ == '__main__':
   main()










