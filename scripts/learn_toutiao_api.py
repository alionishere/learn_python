import requests


# 请求地址
# https://ad.oceanengine.com/open_api/2/tools/clue/get/
def get_clue_list():
    headers = {
        # ':method': 'GET',
        # ':scheme': 'https',
        # 'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 AppVersion/1.7.0BXMQTJ AppVersion/1.7.0BXMQTJ AppVersion/1.7.0BXMQTJ AppVersion/1.7.0BXMQTJ AppVersion/1.7.0BXMQTJ AppVersion/1.7.0BXMQTJ',
        # 'X-Debug-Mode': '1',
        # 'access_token': '9396dcfe63999568bbfca0f8324c6b8937509e4e'
        'access_token': '9396dcfe63999568bbfca0f8324c6b8937509e4e'
    }

    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "2/tools/clue/get/"
    url = open_api_url_prefix + uri
    data = {
        'advertiser_ids': ["1662581690593287"],
        # 'access_token': '9396dcfe63999568bbfca0f8324c6b8937509e4e',
        'start_time': '2019-06-17',
        'end_time': '2021-06-17'
    }
    rsp = requests.get(url, headers=headers, json=data)
    print(rsp.url)
    rsp_data = rsp.json()
    return rsp_data


if __name__ == '__main__':
    get_clue_list()
