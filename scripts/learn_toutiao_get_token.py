import requests


def get_access_token():
    # import requests
    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "oauth2/access_token/"
    url = open_api_url_prefix + uri
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        "app_id": 1662581690593287,
        "secret": "xxx",
        "grant_type": "auth_code",
        "auth_code": "xxx"
    }
    rsp = requests.post(url, headers=headers, json=data)
    rsp_data = rsp.json()
    return rsp_data


def refresh_access_token():
    # import requests
    open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    uri = "oauth2/refresh_token/"
    refresh_token_url = open_api_url_prefix + uri
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        "appid": 1662581690593287,
        "secret": "xxx",
        "grant_type": "refresh_token",
        "refresh_token": "xxx",
    }
    rsp = requests.post(refresh_token_url, headers=headers, json=data)
    rsp_data = rsp.json()
    return rsp_data


if __name__ == '__main__':
    print('Access_token: %s' % get_access_token())
    print('Refresh_access_token: %s' % refresh_access_token())
