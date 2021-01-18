import sys
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
# 方案1
# 通过表名+表的位置确定（在当前页第几个表为需求表）
# 此处需要考虑全面
# 1、某一指标项，包含两个数据表，且两个数据表都在同一页，均在同一页第几个表？
# 2、某一指标项，包含两个数据表，且两个数据表不在同一页，第二个数据表完全在第二页
# 3、某一指标项，包含两个数据表，且两个数据表不在同一页，第二个数据表部分在第一页，部分在第二页
# 4、某一指标项，包含两个数据表，且两个数据表不在同一页，第一个数据表部分在第一页，部分在第二页
# 5、某一指标项，只包含一个数据表，且完全在当前页，在当前页第几个表如何确定？
# 6、某一指标项，只包含一个数据表，且部分在当前页，部分在第二页
# 这里暂不考虑一张表跨三页的情况
# ebs = ['3、融出资金#129#3']

# 方案2
# 通过表名+页码定位
# 表名#表序号#是否跨页#页码&表定位
ebs = ['2、结算备付金#1#n#129&1', '3、融出资金#1#n#129&2', '3、融出资金#1#y#129']
k = 0
table_data = []

pdf = pdfplumber.open(r'E:\tmp\光大证券2019年年度报告.pdf')
# 母公司资产负债表
for page in pdf.pages[1:400]:
    # print(type(page.extract_tables()))
    # sys.exit()
    data = page.extract_text()
    for i in ebs:
        item = i.split('#')[0]
        tb_seq_str = i.split('#')[1]
        if item in data:   # 有些复制出的带空格，其实PDF原本不带
            print('item: %s' % item)
            page_no = page.page_number
            print('page_no: %s' % page_no)
            # for m in rang e(page_no - 1, page_no + 2):
            content_lst = []
            if tb_seq_str == '3':
                page_content_1 = pdf.pages[page_no - 1]
                tbls_1 = page_content_1.extract_tables()
                print('tb_cnt: %s' % len(tbls_1))
                for row in tbls_1[len(tbls_1) - 1]:
                    content_lst_temp = [n for n in row]
                    # content_lst.append(content_lst_temp)
                    print(content_lst_temp)
                page_content_2 = pdf.pages[page_no]
                print('--' * 20)
                tbls_2 = page_content_2.extract_tables()
                for row in tbls_2[0]:
                    content_lst = [n for n in row]
                    print(content_lst)
            # print(content_lst)

            # for tb_seq in tb_seq_lst:
            #     for row in tbls[int(tb_seq)]:
            #         # print(type(row))
            #         content_lst = [n for n in row]
            #         print(content_lst)
    # if '母公司资产负债表' in data:
    #     k = k + 1
    #     page_number = page.page_number
    #     print(page_number)
        # for i in range(page_number - 1, page_number + 2):
        #     page_use = pdf.pages[i]
        #     for table in page_use.extract_tables():
        #         for row in table:
        #             for m in dic1:
        #                 if m in row:
        #                     table_data = [i for i in row if i is not None]
        #                     print(table_data)
    # if not table_data:
    #     if k >= 2:
    #         k = 0
    #         table_data = []
    #         break
    # else:
    #     k = 0
    #     table_data = []
    #     break

# pdf.close()
