#!/usr/bin/env python3
import requests
import cx_Oracle
from datetime import date, datetime, timedelta
import logging
from sqlalchemy import desc, create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

# 配置日志
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='D:/whk/log/toutiao.log', level=logging.INFO, format=LOG_FORMAT)


class OEClient:
    # APP_ID = '1675629103409171'
    APP_ID = '1679163061779460'
    # APP_SECRET = 'bb7b48457ea3722742a7f34888bc21c66243c0fa'
    APP_SECRET = 'd85f9f6be5bd6c9250d6e268dce99ce9b6c73cc3'
    # APP_OAUTH_CALLBACK_URL = 'http://localhost:8000/oauth/oceanengine'
    APP_OAUTH_CALLBACK_URL = 'http://localhost:8000/oauth/oceanengine'
    # AUTHORIZED_URL = '''https://ad.oceanengine.com/openapi/audit/oauth.html?app_id=1675629103409171&state=your_custom_params&scope=%5B800%2C100%2C69%2C200%2C73%2C42%2C43%2C44%2C45%2C210%2C47%2C720%2C40%2C242%2C243%2C723%2C56%2C760%2C250%2C220%2C30%5D&material_auth=1&redirect_uri=http%3A%2F%2Flocalhost%3A8000%2Foauth%2Foceanengine&rid=07svavgox0yf'''
    AUTHORIZED_URL = '''https://ad.oceanengine.com/openapi/audit/oauth.html?app_id=1676714476618766&state=your_custom_params&scope=%5B800%2C100%2C5%2C200%2C210%2C42%2C43%2C44%2C45%2C47%2C40%2C242%2C243%2C250%2C220%2C30%5D&material_auth=1&redirect_uri=http%3A%2F%2Flocalhost%3A8000%2Foauth%2Foceanengine&rid=jcr7ae1pq7n'''

    def __init__(self):
        super().__init__()
        self.access_token = None

    def fetch_access_token(self, auth_code):
        url = 'https://ad.oceanengine.com/open_api/oauth2/access_token/'
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            "app_id": self.APP_ID,
            "secret": self.APP_SECRET,
            "grant_type": "auth_code",
            "auth_code": auth_code,
        }
        return requests.post(url, headers=headers, json=data)

    def refresh_access_token(self, refresh_token):
        open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
        uri = "oauth2/refresh_token/"
        refresh_token_url = open_api_url_prefix + uri
        data = {
            "appid": self.APP_ID,
            "secret": self.APP_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        rsp = requests.post(refresh_token_url, json=data)
        rsp_data = rsp.json()
        return rsp_data

    def fetch_clue_list(self, advertiser_ids, start_time, end_time, page=1, size=100):
        url = 'https://ad.oceanengine.com/open_api/2/tools/clue/get/'
        headers = {
            'Access-Token': self.access_token,
        }
        data = {
            'advertiser_ids': advertiser_ids,
            'start_time': start_time,
            'end_time': end_time,
            'page': page,
            'page_size': size,
        }
        return requests.get(url, headers=headers, json=data)


client = OEClient()

# 有效期自行查询文档，记得是一天一换
# http://localhost:8000/oauth/oceanengine?state=your_custom_params&auth_code=2b6d5c90d1eb9fee8b036dae96c2d2fccd7ea1d3
# auth_code = 'd58336650607ba3e1afa059606e10f2457e6b466'
# auth_code = '02a0620098da4d94edc82aecc768ff6490ff5ada'
# response = client.fetch_access_token(auth_code)
# print(response.text)

###################################
# refresh token
###################################
# hour = str(datetime.now().hour)
# refresh_token = "08a23e120dcd7871c4d96392024d4f52dac21461"
with open('D:/whk/log/refresh_token2.dat', 'r') as fr:
    refresh_token = fr.read()

rsp_refresh_token = client.refresh_access_token(refresh_token)
print(rsp_refresh_token)
# print(rsp_refresh_token['data']['refresh_token'])
refresh_token = rsp_refresh_token['data']['refresh_token']

logging.info('refresh_token: %s' % refresh_token)
with open('D:/whk/log/refresh_token2.dat', 'w') as fw:
    fw.write(refresh_token)

# client.access_token = '24fb05d857eb17f77199fa7f36ac267d3d342c29'
client.access_token = rsp_refresh_token['data']['access_token']
logging.info('access_token: %s' % client.access_token)


# logging.info('refresh_token: %s' % rsp_refresh_token)
# print(rsp_refresh_token)

# get db connection
def get_db_conn():
    return cx_Oracle.connect('kingstar', 'kingstar', '10.29.7.211:1521/siddc01')


# write to db
def write2db(cursor, data_lst):
    merge_sql = '''
MERGE INTO SC61.T_TOUTIAO T1 USING(SELECT '%s' AS CLUE_ID FROM DUAL) T2 ON (T1.CLUE_ID = T2.CLUE_ID)
WHEN MATCHED THEN UPDATE SET T1.CONVERT_STATUS = '%s'
WHEN NOT MATCHED THEN INSERT 
(ADVERTISER_NAME,APP_NAME,SITE_ID,TELEPHONE,CREATE_TIME_DETAIL,CITY_NAME,CREATE_TIME,CONVERT_STATUS,MODULE_ID,CLUE_ID,"date",FORM_REMARK,LOCATION,EMAIL,STORE_ID,STORE_PACK_REMARK,STORE_PACK_NAME,STORE_ADDRESS,STORE_LOCATION,STORE_NAME,STORE_REMARK,STORE_PACK_ID,PROVINCE_NAME,AD_ID,CLUE_SOURCE,WEIXIN,ADVERTISER_ID,REMARK_DICT,ADDRESS,AD_NAME,QQ,REMARK,NAME,GENDER,AGE,REQ_ID,CLUE_TYPE,MODULE_NAME,EXTERNAL_URL)
VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
  ''' % (
        data_lst[9], data_lst[7], data_lst[0], data_lst[1], data_lst[2], data_lst[3], data_lst[4], data_lst[5],
        data_lst[6], data_lst[7], data_lst[8],
        data_lst[9], data_lst[10], data_lst[11], data_lst[12], data_lst[13], data_lst[14], data_lst[15], data_lst[16],
        data_lst[17], data_lst[18], data_lst[19], data_lst[20], data_lst[21], data_lst[22], data_lst[23], data_lst[24],
        data_lst[25], data_lst[26], data_lst[27], data_lst[28], data_lst[29], data_lst[30], data_lst[31], data_lst[32],
        data_lst[33], data_lst[34], data_lst[35], data_lst[36], data_lst[37], data_lst[38])
    cursor.execute(merge_sql)


def close_db(cursor, conn):
    cursor.close()
    conn.close()


def fetch_data_to_db(advertiser_ids, start_time, end_time):
    for page in range(1, 7):
        response = client.fetch_clue_list(advertiser_ids, start_time, end_time, page)
        print(response.text)
        rsp_lst = response.json()['data']['list']

        conn = get_db_conn()
        cur = conn.cursor()

        for res in rsp_lst:
            result = []
            advertiser_name = res['advertiser_name']
            app_name = res['app_name']
            site_id = res['site_id']
            telephone = res['telephone']
            create_time_detail = res['create_time_detail']
            city_name = res['city_name']
            create_time = res['create_time']
            convert_status = res['convert_status']
            module_id = res['module_id']
            clue_id = res['clue_id']
            date = res['date']
            form_remark = res['form_remark']
            location = res['location']
            email = res['email']
            store_id = res['store']['store_id']
            store_pack_remark = res['store']['store_pack_remark']
            store_pack_name = res['store']['store_pack_name']
            store_address = res['store']['store_address']
            store_location = res['store']['store_location']
            store_name = res['store']['store_name']
            store_remark = res['store']['store_remark']
            store_pack_id = res['store']['store_pack_id']
            province_name = res['province_name']
            ad_id = res['ad_id']
            clue_source = res['clue_source']
            weixin = res['weixin']
            advertiser_id = res['advertiser_id']
            remark_dict = res['remark_dict']
            address = res['address']
            ad_name = res['ad_name']
            qq = res['qq']
            remark = res['remark']
            name = res['name']
            gender = res['gender']
            age = res['age']
            req_id = res['req_id']
            clue_type = res['clue_type']
            module_name = res['module_name']
            external_url = res['external_url']

            result.append(advertiser_name)
            result.append(app_name)
            result.append(site_id)
            result.append(telephone)
            result.append(create_time_detail)
            result.append(city_name)
            result.append(create_time)
            result.append(convert_status)
            result.append(module_id)
            result.append(clue_id)
            result.append(date)
            result.append(form_remark)
            result.append(location)
            result.append(email)
            result.append(store_id)
            result.append(store_pack_remark)
            result.append(store_pack_name)
            result.append(store_address)
            result.append(store_location)
            result.append(store_name)
            result.append(store_remark)
            result.append(store_pack_id)
            result.append(province_name)
            result.append(ad_id)
            result.append(clue_source)
            result.append(weixin)
            result.append(advertiser_id)
            result.append(remark_dict)
            result.append(address)
            result.append(ad_name)
            result.append(qq)
            result.append(remark)
            result.append(name)
            result.append(gender)
            result.append(age)
            result.append(req_id)
            result.append(clue_type)
            result.append(module_name)
            result.append(external_url)
            print(result)
            write2db(cur, result)
        conn.commit()
        close_db(cur, conn)


# 1662581690593287 这个ID得单独带上，可能还得看看有没有其他的广告主ID
# 头条的ID体系不是太明白，oAuth后获取到的ID是1668338098434056，实际应该写1662581690593287
advertiser_ids = [1678971206080525, 1678971204908045, 1678971179505677, 1678971205601288, 1678971205260301]
# start_time = str(date.today())
# end_time = str(date.today())
start_date = date.today() + timedelta(days=0)
for i in range(1, 3):
    print(start_date + timedelta(days=-i), end=',')
    fetch_data_to_db(advertiser_ids, str(start_date + timedelta(days=-i)), str(start_date + timedelta(days=-i)))
