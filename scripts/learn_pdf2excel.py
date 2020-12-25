# import pdfplumber
# import xlwt
#
# workbook = xlwt.Workbook()
# sheet = workbook.add_sheet('Sheet1')
# i = 0
#
# pdf = pdfplumber.open('C:/Users/13637/Desktop/dwzq.pdf')
#
# print('\n')
# print('开始读取数据')
# print('\n')
#
# for page in pdf.pages[107:108]:
#
#     for table in page.extract_tables():
#
#         for row in table:
#             if '利息净收入' in row:
#                 print(row)
#                 for j in range(len(row)):
#                     sheet.write(i, j, row[j])
#                     i += 1
#
# pdf.close()

# workbook.save('C:/Users/13637/Desktop/us2.xls')
# print('\n')
# print('写入excel成功')
# print('保存位置：')
# print('C:/Users/13637/Desktop/us2.xls')
# print('\n')
# input('PDF取读完毕，按任意键退出')
#


# import pdfplumber
# import pandas as pd
#
# df = pd.DataFrame()
# dic = ['资产负债率（%）', '资产总计', '负债合计', '其中：客户资金存款', '交易性金融资产', '货币资金', '融出资金', '买入返售金融资产', '其他债权投资',
#        '债权投资', '长期股权投资', '其他权益工具投资', '交易性金融负债', '自营权益类证券及证券衍生品/净资本(%)', '应付短期融资款', '衍生金融负债',
#        '应付债券', '利息净收入', '一、营业总收入', '手续费及佣金净收入', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）',
#        '二、营业总支出', '其中：对联营企业和合营企业的投资收益 ', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）',
#        '业务及管理费', '五、净利润（净亏损以“－”号填列）', '净资本', '风险覆盖率（%）', '资本杠杆率（%）', '流动性覆盖率（%）', '净稳定资金率（%）']
#
# pdf = pdfplumber.open('E:/tmp/dwzq.pdf')
#
# print('\n')
# print('开始读取数据')
# print('\n')
#
# for page in pdf.pages[17:114]:
#
#     for table in page.extract_tables():
#
#         for row in table:
#
#             for m in dic:
#                 if m in row:
#                     if row[0] is not None and row[1] is not None and len(row[2]) != 0 and len(row[1]) >= 5:
#                         print('%s : %s ---%s ' % (row[0], row[1], row[2]))
#                     elif len(row[1]) <= 5 and len(row) >= 4 and len(row[2]) != 0 and len(row[3]) != 0:
#                         print('%s : %s ---%s ' % (row[0], row[2], row[3]))
#                     # else:
#                     #     print('%s : %s ---%s ' % (row[0], row[1], row[2]))
#                     # print('%s : %s ---%s ' % (row[0], row[1], row[2]))
#                     # print(len(row[1]), len(row[2]))
#                     # df = df.append(row)
#
# pdf.close()
# print(df)
# df.to_excel('E:/tmp/dwzq.xls')

##################################################
# 02
##################################################
import pdfplumber
import re

# path2 = r'D:/年报/东吴证券2019年报.pdf'
path2 = r'E:/tmp/dwzq.pdf'
pdf = pdfplumber.open(path2)

pages = pdf.pages[1:300]
for page in pages:
    data = page.extract_text()
    # 判断界面中是否含有关键词，接下来才是表格
    if '母公司资产负债表' in data:
        # 将对象换成str
        print(page.page_number)
        page_str = str(page)[6:9]

        # 再将str转换成int
        page_num = int(page_str)
        print(page_num)
        for i in range(page_num - 1, page_num + 2):
            page_table = pdf.pages[i]
            for pdf_table in page_table.extract_tables():
                table = []
                # any(x)判断x对象是否为空对象，如果都为空、0、false，则返回false，如果不都为空、0、false，则返回true
                # all(x)如果all(x)参数x对象的所有元素不为0、''、False或者x为空对象，则返回True，否则返回False
                for row in pdf_table:
                    if any(row):
                        table.append(row)
                for row in table:
                    # print([re.sub('\s+', '', cell) if cell is not None else None for cell in row])
                    if '融出资金' in row \
                            or '买入返售金融资产' in row \
                            or '应收利息' in row \
                            or '交易性金融资产' in row \
                            or '其他债权投资' in row \
                            or '长期股权投资' in row \
                            or '应付债券'    in row \
                            or '应付短期融资款' in row \
                            or '交易性金融资产' in row \
                            or '衍生金融资产'  in row:
                        print("find{}".format(row))

pdf.close()

