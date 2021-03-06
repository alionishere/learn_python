import sys
import pdfplumber

# 通过表名+页码定位
# 表名#表序号#是否跨页#页码&表定位
# 0：指标
# 1：指标项序号
# 2：是否跨页（y/n）
# 3：跨页次数
# 4：详细配置项（第几页，第几个表），若跨页，2&3的形式标识跨页表的位置
ebs = ['结算备付金#1#n#0#129&1', '融出资金#1#n#0#129&2', '融出资金#2#y#1#129&3&1', '衍生金融工具#1#y#1#130&2&2', '买入返售金融资产#1#y#1#133&2&1',
       '买入返售金融资产#2#n#0#134&2', '买入返售金融资产#3#n#0#134&3', '存出保证金#1#n#0#131&1', '交易性金融资产#1#n#0#136&1',
       '交易性金融资产#2#n#0#137&3', '交易性金融资产#3#n#0#137&2', '债权投资#1#y#1#137&1&2', '债权投资#2#n#0#138&3', '其他债权投资#1#n#0#138&1',
       '其他权益工具投资#1#n#0#139&1', '其他权益工具投资#1#n#0#140&2', '长期股权投资#1#y#2#140&1&1', '融券业务#1#n#0#152&2',
       '资产减值准备变动表#1#n#0#152&1', '金融工具及其他项目预期信用损失准备表#1#n#0#153&1', '应付短期融资款#1#n#0#154&1',
       '交易性金融负债#1#n#0#155&1', '卖出回购金融资产款#1#y#1#155&2&1', '卖出回购金融资产款#2#n#1#156&2', '卖出回购金融资产款#3#n#0#156&3',
       '卖出回购金融资产款#4#n#0#156&4', '应付债券#1#y#1#160&1&1', '投资收益#1#n#0#171&1', '投资收益#2#n#0#171&2', '公允价值变动收益#1#n#0#172&1',
       '信用减值损失#1#n#0#173&2']
haitong = ['结算备付金#1#n#0#216&2', '结算备付金#2#n#0#217&1', '融出资金#1#n#0#217&3', '融出资金#2#n#0#218&1', '融出资金#3#n#0#218&2',
           '融出资金#4#n#0#218&3', '衍生金融工具#1#n#1#219&1', '买入返售金融资产#1#n#0#221&1', '买入返售金融资产#2#n#0#222&1',
           '买入返售金融资产#3#n#0#222&2', '买入返售金融资产#4#n#0#222&3', '买入返售金融资产#5#n#0#223&1', '买入返售金融资产#6#n#0#223&2',
           '存出保证金#1#n#0#220&1', '存出保证金#2#n#0#220&2', '交易性金融资产#1#n#0#224&1', '交易性金融资产#2#n#0#224&2',
           '交易性金融资产#3#n#0#224&3', '交易性金融资产#4#n#0#224&4', '债权投资#1#n#0#225&2', '债权投资#2#n#0#225&3', '其他债权投资#1#n#0#226&3',
           '其他债权投资#2#n#0#227&1', '其他债权投资#3#n#0#227&2', '其他债权投资#4#n#0#227&3', '其他权益工具投资#1#n#0#228&2',
           '长期股权投资#1#n#0#233&1', '融券业务#1#n#0#244&4', '资产减值准备变动表#1#n#0#245&1', '金融工具及其他项目预期信用损失准备表#1#n#0#245&2',
           '金融工具及其他项目预期信用损失准备表#2#n#0#246&1', '应付短期融资款#1#n#0#247&1', '应付短期融资款#2#n#0#248&1',
           '交易性金融负债#1#n#0#249&3', '交易性金融负债#2#n#0#249&4', '卖出回购金融资产款#1#n#0#250&1', '卖出回购金融资产款#2#n#0#250&2',
           '卖出回购金融资产款#3#n#0#250&3', '应付债券#1#n#0#254&1', '应付债券#2#n#0#255&2', '投资收益#1#n#0#268&1', '公允价值变动收益#1#n#0#268&2',
           '信用减值损失#1#n#0#269&3']
# comp_lst = ['光大证券']
comp_lst = ['海通证券']


def parse_data_from_pdf(comp_name, rpt_year):
    for i in haitong:
        item = i.split('#')[0]
        item_no = int(i.split('#')[1])  # 指标项序号
        double_spread = i.split('#')[2].lower().strip()  # 是否跨页：y 跨页， n 不跨页
        spread_no = int(i.split('#')[3])  # 若跨页，跨页的次数，若不跨页，默认为0
        detail_cfg = i.split('#')[4]  # 若跨页，则配置第几页，否则，配置第几页第几个表
        item_page_no = int(detail_cfg.split('&')[0])
        tbls = pdf.pages[item_page_no - 1].extract_tables()
        tbl_name = 'item: %s_%s' % (item, item_no)
        print(tbl_name)
        if double_spread == 'n':
            item_tb_no = int(detail_cfg.split('&')[1])
            tgt_tb = tbls[item_tb_no - 1]
            for row in tgt_tb:
                row_lst = [comp_name, rpt_year]
                for n in row:
                    row_lst.append(n)
                print(row_lst)
            print('-*' * 50)
        elif double_spread == 'y':
            if spread_no <= 1:
                item_tb_no_1 = int(detail_cfg.split('&')[1])
                item_tb_no_2 = int(detail_cfg.split('&')[2])
                tbls_2 = pdf.pages[item_page_no].extract_tables()
                tgt_tb_1 = tbls[item_tb_no_1 - 1]
                tgt_tb_2 = tbls_2[item_tb_no_2 - 1]
                for row in tgt_tb_1:
                    row_lst = [rpt_year, comp_name]
                    for n in row:
                        row_lst.append(n)
                    print(row_lst)
                for row in tgt_tb_2:
                    row_lst = [rpt_year, comp_name]
                    for n in row:
                        row_lst.append(n)
                    print(row_lst)
                print('-*' * 50)
            else:
                item_tb_no_1 = int(detail_cfg.split('&')[1])
                tgt_tb_1 = tbls[item_tb_no_1 - 1]
                for row in tgt_tb_1:
                    row_lst = [rpt_year, comp_name]
                    for n in row:
                        row_lst.append(n)
                    print(row_lst)
                for m in range(item_page_no, item_page_no + spread_no):
                    tgt_tbl_m = pdf.pages[item_page_no].extract_tables()[0]
                    for row in tgt_tbl_m:
                        row_lst = [rpt_year, comp_name]
                        for n in row:
                            row_lst.append(n)
                        print(row_lst)
                tbls_2 = pdf.pages[item_page_no + spread_no - 1].extract_tables()
                item_tb_no_2 = int(detail_cfg.split('&')[2])
                tgt_tb_2 = tbls_2[item_tb_no_2 - 1]
                for row in tgt_tb_2:
                    row_lst = [rpt_year, comp_name]
                    for n in row:
                        row_lst.append(n)
                    print(row_lst)
                print('-*' * 50)
        else:
            print('Configuration item error! Please check!')


# ###### main #########
for comp in comp_lst:
    pdf = pdfplumber.open(r'E:\tmp\%s2019年年度报告.pdf' % comp)
    parse_data_from_pdf(comp, '2020')
