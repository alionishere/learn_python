# -*- coding: utf-8 -*-

import camelot
import pdfplumber
import pandas as pd

path = r'E:\tmp\东北证券：2019年年度报告.pdf'
# extract tb from pdf
tbs = camelot.read_pdf(path, pages='149-175', flavor='stream')
# tbs = camelot.read_pdf(path)
print(len(tbs))
print(tbs[0].data)
# with pdfplumber.open(path) as pdf:
#     # content = ''
#     # for i in range(len(pdf.pages)):
#     #     page = pdf.pages[i]
#     #     page_content = '\n'.join(page.extract_text().split('\n')[:-1])
#     #     content = content + page_content
#     # print(content)
#     page = pdf.pages[160]
#     for table in page.extract_table():
#         df = pd.DataFrame(table)
#     print(df)
