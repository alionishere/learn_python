# -*- coding: utf-8 -*-

import camelot
import pdfplumber
import pandas as pd

path = r'E:\tmp\temp_dw.pdf'
# extract tb from pdf
tbs = camelot.read_pdf(path, pages='169', flavor='lattice')
# tbs = camelot.read_pdf(path)

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

