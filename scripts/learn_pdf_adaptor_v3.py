import pdfplumber

dic1 = ['融出资金', '买入返售金融资产', '应收利息', '交易性金融资产', '其他债权投资', '长期股权投资', '应付债券', '应付短期融资款', '交易性金融资产', '衍生金融资产']
dic2 = ['手续费及佣金净收入', '一、营业总收入', '利息净收入', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）', '二、营业总支出',
        '归属于母公司股东的净利润', '五、净利润（净亏损以“－”号填列）']
dic3 = ['其中：对联营企业和合营企业的投资收益', '一、营业总收入', '投资收益（损失以“-”号填列）', '公允价值变动收益（损失以“-”号填列）', '二、营业总支出',
        '3.可供出售金融资产公允价值变动损\n益', '2.其他债权投资公允价值变动', '公允价值变动收益（损失以“－”号填列）', '二、营业支出', '一、营业收入',
        '投资收益（损失以“－”号填列）']
dic4 = ['净资本', '风险覆盖率（%）', '资本杠杆率（%）', '流动性覆盖率（%）', '净稳定资金率（%）',
        '风险覆盖率(%)', '资本杠杆率(%)', '流动性覆盖率(%)', '净稳定资金率(%)']
# dic5 = ['东吴证券','中信证券','招商证券','华西证券']
dic5 = ['华泰证券']

k = 0

table_data = []

for bond_company in dic5:
    print("证券公司名称:" + bond_company)
    pdf = pdfplumber.open(r'E:\tmp\光大证券2019年年度报告.pdf')
    # 母公司资产负债表
    for page in pdf.pages[1:400]:
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
                                table_data = [i for i in row if i is not None]
                                print(table_data)
        if not table_data:
            if k >= 2:
                k = 0
                table_data = []
                break
        else:
            k = 0
            table_data = []
            break

    # 合并利润表
    for page in pdf.pages[1:400]:
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
                                table_data = [i for i in row if i is not None]
                                print(table_data)
        if not table_data:
            if k >= 2:
                k = 0
                table_data = []
                break
        else:
            k = 0
            table_data = []
            break
    # 母公司利润表
    for page in pdf.pages[1:400]:
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
                                table_data = [i for i in row if i is not None]
                                print(table_data)
        if table_data == []:
            if k >= 2:
                k = 0
                table_data = []
                break
        else:
            k = 0
            table_data = []
            break

    # 母公司的净资本及风险控制指标
    for page in pdf.pages[1:400]:
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
                                table_data = [i for i in row if i is not None]
                                print(table_data)
        if table_data == []:
            if k >= 2:
                k = 0
                table_data = []
                break
        else:
            k = 0
            table_data = []
            break
pdf.close()
