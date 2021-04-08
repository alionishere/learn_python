import sys
import pdfplumber
# import tabula
import cx_Oracle

# 通过表名+页码定位
# 表名#表序号#是否跨页#页码&表定位
# 0：指标
# 1：指标项序号
# 2：是否跨页（y/n）
# 3：跨页次数
# 4：详细配置项（第几页，第几个表），若跨页，2&3的形式标识跨页表的位置
# hualin = ['结算备付金#1#n#0#151&3', '融出资金#1#y#1#151&4&1', '融出资金#2#n#1#152&2', '衍生金融工具#1#y#1#152&3&1', '买入返售金融资产#1#y#1#154&3&1',
#           '买入返售金融资产#1#n#1#155&2', '买入返售金融资产#2#n#1#155&3', '存出保证金#1#n#0#153&2', '交易性金融资产#1#n#0#157&1',
#           '其他债权投资#1#n#0#158&1', '长期股权投资#1#n#1#209&3', '长期股权投资#2#n#1#209&1', '融券业务#1#n#0#151&2', '应付短期融资款#1#y#1#166&4&1',
#           '交易性金融负债#1#n#0#167&3', '卖出回购金融资产款#1#n#1#168&1', '卖出回购金融资产款#1#n#1#168&2', '卖出回购金融资产款#1#n#1#168&3',
#           '投资收益#1#y#1#176&2&1', '投资收益#2#n#1#177&2', '公允价值变动收益#1#n#0#177&4', '信用减值损失#1#n#0#179&2',
#           '业务及管理费-母公司#1#n#0#214&2', '业务及管理费-合并#1#y#1#178&3&1', '手续费及佣金净收入#1#y#1#211&2&1', '手续费及佣金净收入#2#n#0#212&2',
#           '手续费及佣金净收入#3#n#0#212&3', '手续费及佣金净收入#4#n#0#212&4']


def parse_data_from_pdf(comp_name, rpt_year):
    # conn = get_db_conn()
    # cur = conn.cursor()
    tbl_name = 'FINAN_ANALYSE'
    # del_data(cur, tbl_name, comp_name, rpt_year)
    for i in item_lst:
        item = i.split('#')[0]
        item_no = int(i.split('#')[1])  # 指标项序号
        double_spread = i.split('#')[2].lower().strip()  # 是否跨页：y 跨页， n 不跨页
        spread_no = int(i.split('#')[3])  # 若跨页，跨页的次数，若不跨页，默认为0
        detail_cfg = i.split('#')[4]  # 若跨页，则配置第几页，否则，配置第几页第几个表
        item_page_no = int(detail_cfg.split('&')[0])
        tbls = pdf.pages[item_page_no - 1].extract_tables()
        item_name = '%s_%s' % (item, item_no)
        print(item_name)
        if double_spread == 'n':
            seq_no = 1
            item_tb_no = int(detail_cfg.split('&')[1])
            tgt_tb = tbls[item_tb_no - 1]
            for row in tgt_tb:
                row_lst = [seq_no, comp_name, rpt_year, item_name]
                for n in row:
                    row_lst.append(n)
                amend_lst(row_lst)
                print(row_lst)
                # write2db(conn, cur, tbl_name, row_lst)
                seq_no = seq_no + 1
            print('-*' * 50)
        elif double_spread == 'y':
            if spread_no <= 1:
                item_tb_no_1 = int(detail_cfg.split('&')[1])
                item_tb_no_2 = int(detail_cfg.split('&')[2])
                tbls_2 = pdf.pages[item_page_no].extract_tables()
                tgt_tb_1 = tbls[item_tb_no_1 - 1]
                tgt_tb_2 = tbls_2[item_tb_no_2 - 1]
                seq_no = 1
                for row in tgt_tb_1:
                    row_lst = [seq_no, comp_name, rpt_year, item_name]
                    for n in row:
                        row_lst.append(n)
                    amend_lst(row_lst)
                    print(row_lst)
                    # write2db(conn, cur, tbl_name, row_lst)
                    seq_no = seq_no + 1
                seq_no = 1
                for row in tgt_tb_2:
                    row_lst = [seq_no, comp_name, rpt_year, item_name]
                    for n in row:
                        row_lst.append(n)
                    amend_lst(row_lst)
                    print(row_lst)
                    # write2db(conn, cur, tbl_name, row_lst)
                    seq_no = seq_no + 1
                print('-*' * 50)
            else:
                item_tb_no_1 = int(detail_cfg.split('&')[1])
                tgt_tb_1 = tbls[item_tb_no_1 - 1]
                seq_no = 1
                for row in tgt_tb_1:
                    row_lst = [seq_no, comp_name, rpt_year, item_name]
                    for n in row:
                        row_lst.append(n)
                    amend_lst(row_lst)
                    print(row_lst)
                    # write2db(conn, cur, tbl_name, row_lst)
                    seq_no = seq_no + 1
                for m in range(item_page_no, item_page_no + spread_no):
                    tgt_tbl_m = pdf.pages[item_page_no].extract_tables()[0]
                    seq_no = 1
                    for row in tgt_tbl_m:
                        row_lst = [seq_no, comp_name, rpt_year, item_name]
                        for n in row:
                            row_lst.append(n)
                        amend_lst(row_lst)
                        print(row_lst)
                        # write2db(conn, cur, tbl_name, row_lst)
                        seq_no = seq_no + 1
                tbls_2 = pdf.pages[item_page_no + spread_no - 1].extract_tables()
                item_tb_no_2 = int(detail_cfg.split('&')[2])
                tgt_tb_2 = tbls_2[item_tb_no_2 - 1]
                seq_no = 1
                for row in tgt_tb_2:
                    row_lst = [seq_no, comp_name, rpt_year, item_name]
                    for n in row:
                        row_lst.append(n)
                    amend_lst(row_lst)
                    print('....' * 10)
                    print(row_lst)
                    # write2db(conn, cur, tbl_name, row_lst)
                    seq_no = seq_no + 1
                    print(row_lst)
                print('-*' * 50)
        else:
            print('Configuration item error! Please check!')
    # conn.commit()
    # close_db(cur, conn)


def amend_lst(lst):
    while len(lst) < 50:
        lst.append('')
    return lst


def get_db_conn():
    return cx_Oracle.connect('kingstar', 'kingstar', '10.29.7.211:1521/siddc01')


def close_db(cursor, conn):
    cursor.close()
    conn.close()


def del_data(cursor, tb_name, bond_company, rpt_year):
    sql = "delete from sc61.%s where bond_company = '%s' and date_time = '%s'" % (tb_name, bond_company, rpt_year)
    print(sql)
    cursor.execute(sql)


def write2db(conn, cur, tb_name, data_list):
    values = ':' + ',:'.join(str(i) for i in range(1, 51))
    sql = "insert into SC61.FINAN_ANALYSE values ({values})".format(values=values)
    print(sql)
    print(type(data_list))
    cur.execute(sql, data_list)



# ###### main #########
item_lst = ['1#1#y#1#153&1&1', '1#2#y#1#154&2&1', '2#1#n#1#143&1', '2#2#n#1#143&4', '3#1#y#1#181&4&1', '4#1#n#1#148&1', '4#2#n#1#149&1', '4#3#n#1#149&2', '5#1#n#0#139&2', '5#2#n#0#139&3', '7#1#n#0#145&3', '7#2#n#0#146&1', '7#3#n#0#146&2', '7#4#n#0#146&3', '7#5#n#0#147&1', '7#6#n#0#147&2', '8#1#n#0#169&1', '8#2#n#0#169&2', '8#3#n#0#169&3', '8#4#n#0#170&2', '9#1#n#0#151&3', '10#1#n#0#150&1', '10#2#n#0#151&1', '10#3#n#0#151&2', '11#1#n#0#140&1', '11#2#n#0#140&2', '11#3#n#0#140&3', '11#4#n#0#140&4', '11#5#y#1#140&5&1', '12#1#n#0#168&1', '13#1#n#0#203&2', '14#1#n#0#227&1', '14#2#n#0#227&2', '15#1#n#0#183&1', '16#1#n#1#142&1', '16#2#n#1#143&2', '16#3#n#1#143&3', '17#1#n#1#166&1', '18#1#n#0#173&1', '19#1#n#0#150&1', '19#2#n#0#151&1', '19#3#n#0#151&2', '21#1#n#0#163&1', '21#2#n#0#164&1', '21#3#n#0#165&1', '21#4#n#0#165&2', '22-com#1#n#1#180&3', '22-com#1#n#1#226&2', '54#1#y#1#179&1&1', '54#2#n#1#180&2', '62-com#1#n#0#182&4', '62-mon#1#n#0#228&2']
comp_lst = ['兴业证券']

for comp in comp_lst:
    pdf = pdfplumber.open(r'E:\tmp\rpt\%s：2020年年度报告.pdf' % comp)
    parse_data_from_pdf(comp, '2020')
