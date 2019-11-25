import lxml.html
import requests


def parse_form(html):
    tree = lxml.html.fromstring(html)
    data = {}
    for e in tree.cssselect('form input'):
        if e.get('name'):
            data[e.get('name')] = e.get('value')
    return data


def get_cookie():
    s = requests.session()
    target_url = 'https://ambers.amac.org.cn/cas/login?service=https://ambers.amac.org.cn/web/'
    result = s.get(target_url)
    post_data = parse_form(result.text)
    print(s.cookies.get_dict())
    # login_url = 'http://example.webscraping.com/places/default/user/login?_next=/places/default/index'
    login_url = target_url
    # post_data['email'] = '872992572@qq.com'
    # post_data['password'] = 'love9918'
    post_data['username'] = 'sac023002'
    post_data['password'] = 'Sac023002*zg'
    s.post(login_url, post_data)
    rs = s.post(target_url)
    with open('login.html', 'w+') as f:
        f.write(rs.text)
    print(rs. text)


if __name__ == '__main__':
    get_cookie()
