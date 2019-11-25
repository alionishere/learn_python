from selenium import webdriver
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')  # 上面三行代码就是为了将Chrome不弹出界面，实现无界面爬取
browser = webdriver.Chrome(chrome_options=chrome_options)
driver = webdriver.PhantomJS()
# url = "https://et.xiamenair.com/xiamenair/book/findFlights.action?lang=zh&tripType=0&queryFlightInfo=XMN,PEK,2018-01-15"
target_url = 'https://ambers.amac.org.cn/cas/login?service=https://ambers.amac.org.cn/web/'

driver.get(target_url)
# 获取cookie列表
cookie_list = driver.get_cookies()
# 格式化打印cookie
cookie_dict = []
for cookie in cookie_list:
    cookie_dict[cookie['name']] = cookie['value']
print(cookie_dict)
