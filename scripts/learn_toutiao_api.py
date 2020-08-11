import requests


# 请求地址
# https://ad.oceanengine.com/open_api/2/tools/clue/get/
def get_clue_list():
    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "2/tools/clue/get/"
    url = open_api_url_prefix + uri
    data = {
        'advertiser_ids': ["0"],
        'access_token': '9396dcfe63999568bbfca0f8324c6b8937509e4e',
        'start_time': '2019-06-17',
        'end_time': '2021-06-17'
    }
    rsp = requests.get(url, json=data)
    rsp_data = rsp.json()
    return rsp_data


if __name__ == '__main__':
    get_clue_list()
