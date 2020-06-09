# -*- coding: utf-8 -*-

import PyPDF2
import pdfplumber
from openpyxl import  Workbook
from copy import copy

with pdfplumber.open(r'E:\tmp\2020-04-29-000783.SZ-长江证券：2019年年度报告.pdf') as p:
    page = p.pages[183]
    # print(len(p.pages))
    # print(page.extract_text())
    # 遍历整个PDF
    # for page in p.pages:
    #     # print(page.extract_text())
    #     with open(r'E:\tmp\pdf.txt', 'a', encoding='gb18030', errors='ignore') as f:
    #         f.write(page.extract_text())
    #         f.write('\n')
    # 操作Excel
    table = page.extract_table()
    for t in table:
        print(t)
    # print(table)
    # workbook = Workbook()
    # sheet = workbook.active
    # for row in table:
    #     if not "".join([str(i) for i in row]) == "":
    #         sheet.append(row)
    #     workbook.save(filename=r"E:\tmp\new.xlsx")
