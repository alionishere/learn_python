import pdfplumber
import datetime


date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#dic = ['海通证券','西南证券','东方证券','国金证券','财通证券','华西证券','招商证券','国海证券',
# '华安证券','南京证券','太平洋跨页','华林证券跨页','兴业证券','浙商证券跨页']
dic = ['东吴证券']
for bond_company in dic:
    print("证券公司名称:" + bond_company)
    pdf = pdfplumber.open('D:/年报/' + bond_company + '.pdf')
    table = []
    list = []
    for page in pdf.pages[1:400]:
        data = page.extract_text()
        if '3、融出资金' in data:
            page_number = page.page_number
            print('开始读取数据页数:'+str(page_number))
            #print(page_number)
            for i in range(page_number - 2, page_number + 2):
                page_use = pdf.pages[i]
                for table_list in page_use.extract_tables():
                    table.append(table_list)
    # #判断是指标
    # # 获取所有的表格
    for i in range(0, len(table[0:]) - 1):
        # 获取范围类的每个表格
        for j in range(0, len(table[0:][i])):
            # 通过关键字获取指定表格
                if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('减：减值') or \
                        table[0:][i][j][0] != None and table[0:][i][j][0].startswith('个人'):
                    list.append(table[0:][i])
                elif table[0:][i][j][0] != None and table[0:][i][j][0].startswith('1-3个月'):
                    list.append(table[0:][i])
                elif table[0:][i][j][0] != None and table[0:][i][j][0].startswith('6个月'):
                    list.append(table[0:][i])
    # 去除重复list
    new_list = []
    for i in list:
        if i not in new_list:
            new_list.append(i)
    # 遍历列表
    for table in new_list:
        for row in table:
            row.append(bond_company)
            row.append(date)
            print(row)

pdf.close()
