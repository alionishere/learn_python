#!/usr/bin/env python3
import requests


class OEClient:
    APP_ID = '1675629103409171'
    APP_SECRET = 'bb7b48457ea3722742a7f34888bc21c66243c0fa'
    APP_OAUTH_CALLBACK_URL = 'http://localhost:8000/oauth/oceanengine'
    AUTHORIZED_URL = '''https://ad.oceanengine.com/openapi/audit/oauth.html?app_id=1675629103409171&state=your_custom_params&scope=%5B800%2C100%2C69%2C200%2C73%2C42%2C43%2C44%2C45%2C210%2C47%2C720%2C40%2C242%2C243%2C723%2C56%2C760%2C250%2C220%2C30%5D&material_auth=1&redirect_uri=http%3A%2F%2Flocalhost%3A8000%2Foauth%2Foceanengine&rid=07svavgox0yf'''

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

    def fetch_clue_list(self, advertiser_ids, start_time, end_time, page=1, size=10):
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
# auth_code = '4a8c3bbccc50f2af4d4c65c5d589a7bacf76703b'
# response = client.fetch_access_token(auth_code)
# print(response.text)
# 下面是上面几行的输出
'''
{
    "message": "OK",
    "code": 0,
    "data": {
        "advertiser_id": 1668338098434056,
        "advertiser_name": "东吴证券-西北街营业部",
        "access_token": "e7db5285d22efd25ab85aa0c0f968f5a200c0b4b",
        "refresh_token_expires_in": 2591999,
        "advertiser_ids": [
            1668338098434056
        ],
        "expires_in": 86399,
        "refresh_token": "9ed0c2f4934750a1a1941b3b8bf2390d78893ac2"
    },
    "request_id": "202008251522300100230302171B00F2CF"
}
'''


client.access_token = 'e7db5285d22efd25ab85aa0c0f968f5a200c0b4b'
# 1662581690593287 这个ID得单独带上，可能还得看看有没有其他的广告主ID
# 头条的ID体系不是太明白，oAuth后获取到的ID是1668338098434056，实际应该写1662581690593287
advertiser_ids = [1668338098434056, 1662581690593287]
start_time = '2020-01-01'
end_time = '2020-08-25'
response = client.fetch_clue_list(advertiser_ids, start_time, end_time)
print(response.text)


'''
{
    "message": "OK",
    "code": 0,
    "data": {
        "page_info": {
            "total_number": 7935,
            "page": 1,
            "page_size": 10,
            "total_page": 794
        },
        "list": [
            {
                "advertiser_name": "东吴证券-测试账户",
                "app_name": "",
                "site_id": "6859619537424662536",
                "telephone": "【脱敏处理】10086",
                "create_time_detail": "2020-08-25 13:44:35",
                "city_name": "",
                "create_time": "2020-08-25",
                "convert_status": "合法转化",
                "module_id": "【脱敏处理】10086",
                "clue_id": "【脱敏处理】10086",
                "date": null,
                "form_remark": "",
                "location": "",
                "email": "",
                "store": {
                    "store_id": 0,
                    "store_pack_remark": "",
                    "store_pack_name": "",
                    "store_address": "",
                    "store_location": "",
                    "store_name": "",
                    "store_remark": "",
                    "store_pack_id": 0
                },
                "province_name": "",
                "ad_id": "1675257494439975",
                "clue_source": 1,
                "weixin": "",
                "advertiser_id": "1662581690593287",
                "remark_dict": {},
                "address": "",
                "ad_name": "明星投顾-8.17-火山",
                "qq": "",
                "remark": "",
                "name": "【脱敏处理】NAME",
                "gender": 0,
                "age": 0,
                "req_id": "202008251342440100110270671A55C1F9",
                "clue_type": 0,
                "module_name": "限时惊喜佣金-下-6.5",
                "external_url": "https://www.chengzijianzhan.com/tetris/page/6859619537424662536/"
            }
        ]
    },
    "request_id": "2020082515331901002202004812014653"
}

'''
