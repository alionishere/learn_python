# -*- coding: utf-8 -*-
import time

lst = []
st = set()
with open('doc/test.txt', 'r') as f:
    # while
    s = f.readline()
    # s = s[:-1]
    while s:
        lst.append(s)
        st.add(s)
        s = f.readline()
        # s = s[:-1]
    # print(s)


file_no = 1
for i in st:
    file_lst = []
    ts = str(time.time()).replace('.','')
    # print(i, end='')
    for j in lst:
        if i == j:
            file_lst.append(j)
    print(file_lst)
    with open('doc/export_%s' % ts, 'a+') as fw:
        fw.writelines(file_lst)
    file_no = file_no + 1
    print('--' * 10)
