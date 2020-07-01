# -*- coding: utf-8 -*-

import PyPDF2
import pdfplumber
from openpyxl import Workbook
from copy import copy
import camelot

with pdfplumber.open(r'E:\tmp\2020-04-29-000783.SZ-长江证券：2019年年度报告.pdf') as p:
    # page = p.pages[183]
    # print(len(p.pages))
    # print(page.extract_text())
    # 遍历整个PDF
    for i in range(len(p.pages)):
        page = p.pages[i]
        page_content = page.extract_text()
        # print(page_content)
        tb_name = '最大信用风险敞口金额'
        if page_content.find(tb_name) >= 0:
            print(i)
            table = page.extract_table()
            for row in range(len(table)):
                print(table[row])
            workbook = Workbook()
            sheet = workbook.active
            for row in table:
                if not "".join([str(i) for i in row]) == "":
                    sheet.append(row)
                workbook.save(filename=r"E:\tmp\new.xlsx")

    #     with open(r'E:\tmp\pdf.txt', 'a', encoding='gb18030', errors='ignore') as f:
    #         f.write(page.extract_text())
    #         f.write('\n')
    # 操作Excel
    # print(table)
