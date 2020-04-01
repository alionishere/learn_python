# -*- coding: utf-8 -*-

# !/bin/env python
# coding=utf-8

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from PIL import Image, ImageDraw, ImageEnhance
import base64
import time, datetime, random
import smtplib
import argparse
import verifyCodes
import pyautogui
from PIL import Image
from smtplib import SMTP
from email.mime.text import MIMEText
from email.header import Header
from twilio.rest import Client
from time import sleep
import os
import pytesseract


class PickCar:
    # PROXY = "61.168.162.32:80"
    # chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--proxy-server={0}'.format(PROXY))
    user_agent = "Mozilla/5.0 (iphone x Build/MXB48T; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.49 Mobile MQQBrowser/6.2 TBS/043632 Safari/537.36 MicroMessenger/6.6.1.1220(0x26060135) NetType/WIFI Language/zh_CN"

    driver_path = 'E:\chromedriver.exe'

    opt = webdriver.ChromeOptions()
    opt.add_argument('--user-agent=%s' % user_agent)
    driver = webdriver.Chrome(executable_path=driver_path, options=opt)

    url_loginPage = 'http://221.178.136.186:8090/pkt/login/loginPage'
    url_rentPage = 'http://221.178.136.186:8090/pkt/service/myRentPage'
    url_test = 'C:\\Users\\dwzq\\Desktop\\test.html'

    # 系统登录按钮Xpath
    loginXpath = '//*[@id="command"]/div[4]/div[1]/button'
    # 车位预约按钮Xpath
    bookXpath = '//*[@id="rent"]'
    # 非预约时间显示的剩余车位文字的Xpath，用于提前开始抢车位
    spareParkingInfo = '//*[@id="command"]/div[1]/div/div[3]/div[1]/p'
    # 预约到的车位号文字的Xpath，用于获取预约到的车位号发邮件通知
    bookedParkingInfo1 = '//*[@id="parking-info-container"]/div[1]/div[3]/p'

    bookedParkingInfo2 = '//*[@id="parking-info-container"]/div[1]/div[4]/p'
    # 我要预约
    successOrNot1 = '//*[@id="command"]/div[1]/div/div[4]/div[12]/div/a'
    # 没有车位可预约
    successOrNot2 = '//*[@id="command"]/div[1]/div/div[4]/div[9]/div/a'
    # 成功预约
    success = '//*[@id="command"]/div[1]/div/div[3]/div[9]/div/a'

    # 系统登陆的用户名和密码
    sysUser = 'zhaxiaodong'
    sysPasswd = '123456'

    # 发送者的登陆用户名和密码
    mailUser = '382312462@qq.com'
    mailPasswd = 'knryznpqzqdubiag'
    # 发送者邮箱的SMTP服务器地址
    smtpserver = 'smtp.qq.com'
    smtpPort = 465
    # 发送者邮箱
    senderMail = '382312462@qq.com'
    # 接收者邮箱列表
    receiverMails = '610271678@qq.com'
    t2val = {}

    @staticmethod
    def get_sys_time():
        sys_time = time.time()
        return sys_time

    @staticmethod
    def get_time(timePara):
        set_time = timePara
        # 将其转换为时间数组
        time_array = time.strptime(set_time, '%Y-%m-%d %H:%M:%S')
        # 转换为时间戳
        time_stamp = int(time.mktime(time_array))
        return time_stamp

    def send_message(self, address):
        # Your Account Sid and Auth Token from twilio.com/console
        # DANGER! This is insecure. See http://twil.io/secure
        account_sid = 'AC4dd8ed739ee8ec561b95e36305c1a19a'
        auth_token = '511e3085a361aa8f38fb074703e779d1'
        client = Client(account_sid, auth_token)
        message = client.messages.create(
            body=address,
            from_='+12028835589',
            to='+8618549981950'
        )
        print(message.sid)

    def login(self):
        # try:
        self.driver.get(self.url_loginPage)
        input_username = self.driver.find_element_by_id('inputUsername')
        input_username.clear()
        input_username.send_keys(self.sysUser)
        input_password = self.driver.find_element_by_id('inputPassword')
        input_password.clear()
        input_password.send_keys(self.sysPasswd)
        login_butten = self.driver.find_element_by_xpath(self.loginXpath)
        login_butten.click()

    # @staticmethod
    @property
    def downloadVerifyCode(self):
        # try:
        self.driver.get(self.url_test)
        wait = WebDriverWait(self.driver, 10)
        # 右键单击图片
        img = wait.until(EC.element_to_be_clickable((By.ID, 'validateCodeImg')))
        # 执行鼠标动作
        actions = ActionChains(self.driver)
        # 找到图片后右键单击图片
        actions.context_click(img)
        actions.perform()
        # 发送键盘按键，根据不同的网页，
        # 右键之后按对应次数向下键，
        # 找到图片另存为菜单
        pyautogui.press('V')
        # 单击图片另存之后等1s敲回车
        randomnum = random.randint(1, 1000000)
        os.system('kuang.exe "C:\Work\dwzq\pickCar\imgs\原始图片' + str(randomnum) + '.png"')
        pyautogui.typewrite(['enter'])

        return './imgs/原始图片' + str(randomnum) + '.png'

        # self.driver.save_screenshot('imgs/printscreen.png')
        # imgelement = self.driver.find_element_by_xpath('//*[@id="validateCodeImg"]')  # 定位验证码
        # # imgelement = self.driver.find_element_by_xpath('//*[@id="validateCodeImg"]').src
        # location = imgelement.location  # 获取验证码x,y轴坐标
        # size = imgelement.size  # 获取验证码的长宽
        # print(int(location['x']),)
        # rangle = (int(location['x']), int(location['y']), int(location['x'] + size['width']),
        #           int(location['y'] + size['height']))  # 写成我们需要截取的位置坐标
        # i = Image.open("imgs/printscreen.png")  # 打开截图
        # frame4 = i.crop(rangle)  # 使用Image的crop函数，从截图中再次截取我们需要的区域
        # frame4.save('imgs/原始图片.png')  # 保存我们接下来的验证码图片 进行打码

    def send_msg(self):

        mail_title = '东吴共享停车提醒'
        mail_body = '有车位释放！'
        message = MIMEText(mail_body, 'plain', 'utf-8')  # 邮件正文
        # (plain表示mail_body的内容直接显示，也可以用text，则mail_body的内容在正文中以文本的形式显示，需要下载）
        message['From'] = self.senderMail  # 邮件上显示的发件人
        message['To'] = self.receiverMails  # 邮件上显示的收件人
        message['Subject'] = Header(mail_title, 'utf-8')

        # 发送者的登陆用户名和密码
        user = self.mailUser
        password = self.mailPasswd
        # 发送者邮箱和发送者邮箱的SMTP服务器地址
        sender = self.senderMail
        smtpserver = self.smtpserver

        # 接收者的邮箱地址
        receiver = self.receiverMails  # receiver 可以是一个list

        # 登陆smtp服务器
        smtp = smtplib.SMTP()
        smtp.connect(smtpserver)
        smtp.login(user, password)
        # 发送邮件
        smtp.sendmail(sender, receiver, message.as_string())
        print(u'发送邮件提醒')
        smtp.quit()

    def downimage(self, i):
        randomNumber = 75587214

        image = self.driver.get('http://221.178.136.186:8090/pkt/service/getCode?random=' + randomNumber)
        # 保存到本地
        print(image)
        with open(str(i) + "image.jpg", "wb") as f:
            f.write(image)

    def get_car(self, stop_time, start_time):

        booked_parking_id = None
        while True:
            if self.get_sys_time() <= stop_time:
                if self.get_sys_time() <= start_time:
                    time.sleep(1)
                    # print("未到预约时间!")
                    continue
                else:
                    try:
                        self.driver.find_element_by_xpath('//*[@id="button_reserve"]/img').click()
                    except Exception as e:
                        return self.driver.find_element_by_xpath(
                            self.bookedParkingInfo1).text + self.driver.find_element_by_xpath(
                            self.bookedParkingInfo2).text
                        break
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    #                    self.driver.get('http://221.178.136.186:8090/pkt/service/reservePage?isSpare=2')
                    alertWin = EC.alert_is_present()(self.driver)
                    if alertWin:
                        print('点击下方预约按钮,弹框出现,点掉弹框!')
                        alertWin.accept()
                    else:
                        #                        print("点击下方预约按钮.弹窗未弹出!")
                        if self.driver.find_element_by_xpath(
                                self.spareParkingInfo).text == '/':  # not in grabbing window
                            time.sleep(1)
                            print("未到预约时间!")
                            continue
                        elif self.driver.find_element_by_id('ratio').text.split('/')[
                            0] != '0' and self.driver.find_element_by_xpath(
                            self.successOrNot1).text == '我要预约':  # spare parking exists
                            try:
                                #  输入验证码
                                # veriflyImg = self.downloadVerifyCode()
                                wait = WebDriverWait(self.driver, 10)
                                # 右键单击图片
                                img = wait.until(EC.element_to_be_clickable((By.ID, 'validateCodeImg')))
                                # 执行鼠标动作
                                actions = ActionChains(self.driver)
                                # 找到图片后右键单击图片
                                actions.context_click(img)
                                actions.perform()
                                # 发送键盘按键，根据不同的网页，
                                # 右键之后按对应次数向下键，V
                                # 找到图片另存为菜单
                                # 右键后等待菜单弹出
                                # sleep(0.5)
                                pyautogui.press('V')
                                # 单击图片另存之后等1s敲回车
                                # sleep(1)
                                randomnum = random.randint(1, 1000000)
                                os.system('autoit.exe "E:\\1-zhaxiaodong\\pickCar\\imgs\\原始图片' + str(randomnum) + '.png"')
                                pyautogui.typewrite(['enter'])
                                veriflyImg = 'imgs/原始图片' + str(randomnum) + '.png'
                                print(veriflyImg)
                                sleep(0.4)
                                verify = verifyCodes.dowork(veriflyImg)
                                print(verify)
                                verifyCodeInput = self.driver.find_element_by_id('verifyCode')
                                verifyCodeInput.clear()
                                verifyCodeInput.send_keys(verify)
                                # verifyCodeInput.send_keys("12121")
                                self.driver.find_element_by_xpath(self.bookXpath).click()
                                alert = self.driver.switch_to.alert
                                if alert:
                                    print(alert.text)
                                    alert.accept()
                                # print(alert.text)
                                # if alert:
                                #     alertText = alertWin.text()
                                #     print(alertText)
                                #     print('点击上方预约按钮,弹框出现,点掉弹框!')
                                #     alertWin.accept()
                                #     continue
                                # self.driver.get(self.url_rentPage)
                                # booked_parking_id = self.driver.find_element_by_xpath(self.bookedParkingInfo).text
                            except Exception as e:
                                print(e)
                            continue
                        elif self.driver.find_element_by_id('ratio').text.split('/')[0] == '0' and self.driver.find_element_by_xpath(self.successOrNot2).text == '没有车位可预约':
                            time.sleep(1)
                            #                            print("当前没有车位释放，继续捡漏!")
                            continue
                        else:
                            print('已经有车位了，停止!')
                            break
            else:  # determine the stop time
                print(u'到点了，不抢了!')
                break

    def parking_release(self, stop_time):
        booked_parking_id = None
        try:
            while True:
                if self.get_sys_time() <= stop_time:
                    self.driver.find_element_by_xpath('//*[@id="button_reserve"]/img').click()
                    alertWin = EC.alert_is_present()(self.driver)
                    if alertWin:
                        print('点击下方预约按钮,弹框出现,点掉弹框!')
                        alertWin.accept()
                    else:
                        #                        print("点击下方预约按钮.弹窗未弹出!")
                        if self.driver.find_element_by_xpath(
                                self.spareParkingInfo).text == '/':  # not in grabbing window
                            time.sleep(0.6)
                            print("未到预约时间!")
                            continue
                        elif self.driver.find_element_by_id('ratio').text.split('/')[
                            0] != '0' and self.driver.find_element_by_xpath(self.successOrNot).text != '没有车位可预约':
                            print('有车位释放！')
                            break
                        elif self.driver.find_element_by_id('ratio').text.split('/')[
                            0] == '0' and self.driver.find_element_by_xpath(self.successOrNot).text == '没有车位可预约':
                            time.sleep(10)
                            continue
                        else:
                            print('已经有车位了，停止!')
                            return None
                else:  # determine the stop time
                    print(u'到点了，不抢了!')
                    booked_parking_id = '到点了，不抢了!'
                    break
        except Exception as e:
            print(e)
            return booked_parking_id


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("stop_time", type=str, help="time when to stop grabbing, format: yyyy-MM-dd hh:mm:ss")
    args = parser.parse_args()

    start_time = '2019-08-23 17:58:30'
    #    stop_time = '2019-07-05 20:00:00'

    run = PickCar()
    # veriflyImg = run.downloadVerifyCode()
    # sleep(0.2)
    # verify = verifyCodes.dowork(veriflyImg)
    run.login()
    # run.downimage(1)
    booked_parking_id = run.get_car(run.get_time(args.stop_time), run.get_time(start_time))
    print(booked_parking_id)
    # 发送短信或邮件
    run.send_message(booked_parking_id)
    # 判断是否有车位释放

    # count = 0
    # time1 = '2019-07-09 00:01:00'
    # time2 = '2019-07-09 06:00:00'
    # while count < 10:
    #     run.parking_release(run.get_time(args.stop_time))
    #     # 发送邮件提醒
    #     print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    #     run.send_message()
    #     count = count + 1
    #     if(run.get_sys_time() > run.get_time(time1) and run.get_sys_time() < run.get_time(time2)):
    #         sleep(3600)
    #     else:
    #         sleep(600)
