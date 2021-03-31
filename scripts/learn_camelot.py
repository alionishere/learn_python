# -*- coding: utf-8 -*-

import camelot
import time
import pdfplumber
import pandas as pd

path = r'E:\tmp\中信证券：中信证券股份有限公司2020年年度报告.pdf'
# extract tb from pdf
tb_page = 190
start_time = time.time()
tbs = camelot.read_pdf(path, edge_tol=500, pages=str(tb_page), flavor='stream')
# tbs = camelot.read_pdf(path)
print(len(tbs))
print(tbs[0].data)
end_time = time.time()
print(end_time - start_time)
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
