import pdfplumber

table1 = []

dic = ['资产负债率（%）', '资产总计', '负债合计', '其中：客户资金存款', '交易性金融资产', '货币资金', '融出资金', '买入返售金融资产', '其他债权投资',
       '债权投资', '长期股权投资', '其他权益工具投资', '交易性金融负债', '自营权益类证券及证券衍生品/净资本(%)', '应付短期融资款', '衍生金融负债',
       '应付债券', '利息净收入', '一、营业总收入', '手续费及佣金净收入', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）',
       '二、营业总支出', '其中：对联营企业和合营企业的投资收益 ', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）',
       '业务及管理费', '五、净利润（净亏损以“－”号填列）', '净资本', '风险覆盖率（%）', '资本杠杆率（%）', '流动性覆盖率（%）', '净稳定资金率（%）']
dic1 = ['融出资金', '买入返售金融资产', '应收利息', '交易性金融资产', '其他债权投资', '长期股权投资', '应付债券', '应付短期融资款', '交易性金融资产', '衍生金融资产']
dic2 = ['手续费及佣金净收入', '一、营业总收入', '利息净收入', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）', '二、营业总支出',
        '归属于母公司股东的净利润', '五、净利润（净亏损以“－”号填列）']
dic3 = ['其中：对联营企业和合营企业的投资收益', '一、营业总收入', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）', '二、营业总支出 ']
dic4 = ['净资本', '风险覆盖率（%）', '资本杠杆率（%）', '流动性覆盖率（%）', '净稳定资金率（%）']

k = 0

# pdf = pdfplumber.open('C:/Users/13637/Desktop/dwzq.pdf')
pdf = pdfplumber.open('E:/tmp/dwzq.pdf')

print('\n')
print('开始读取数据')
print('\n')

for page in pdf.pages[1:300]:
    data = page.extract_text()
    if '母公司资产负债表' in data:
        k = k + 1
        page_number = page.page_number
        print(page_number)
        print('母公司资产负债表')
        for i in range(page_number - 1, page_number + 2):
            page_use = pdf.pages[i]
            for table in page_use.extract_tables():

                for row in table:
                    for m in dic1:
                        if m in row:
                            table1.append(row)
                            print(row)
    if k >= 1:
        k = 0
        break

for page in pdf.pages[1:300]:
    data = page.extract_text()
    if '合并利润表' in data:
        k = k + 1
        page_number = page.page_number
        print(page_number)
        print('合并利润表')
        for i in range(page_number - 1, page_number + 2):
            page_use = pdf.pages[i]
            for table in page_use.extract_tables():

                for row in table:
                    for m in dic2:
                        if m in row:
                            table1.append(row)
                            print(row)
    if k >= 1:
        k = 0
        break

for page in pdf.pages[1:300]:
    data = page.extract_text()
    if '母公司利润表' in data:
        k = k + 1
        page_number = page.page_number
        print(page_number)
        print('母公司利润表')
        for i in range(page_number - 1, page_number + 2):
            page_use = pdf.pages[i]
            for table in page_use.extract_tables():

                for row in table:
                    for m in dic3:
                        if m in row:
                            table1.append(row)
                            print(row)
    if k >= 1:
        k = 0
        break

for page in pdf.pages[1:300]:
    data = page.extract_text()
    if '母公司的净资本及风险控制指标' in data:
        k = k + 1
        page_number = page.page_number
        print(page_number)
        print('母公司的净资本及风险控制指标')
        for i in range(page_number - 1, page_number + 2):
            page_use = pdf.pages[i]
            for table in page_use.extract_tables():

                for row in table:
                    for m in dic4:
                        if m in row:
                            table1.append(row)
                            print(row)
    if k >= 1:
        k = 0
        break
pdf.close()
# print(df)
# df.to_excel('E:/tmp/dwzq.xls')
