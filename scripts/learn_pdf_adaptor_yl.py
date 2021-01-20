import pdfplumber
import datetime
import time
import pymysql
'''
# dic = ['海通证券','西南证券','东方证券','国金证券','财通证券','华西证券','招商证券','国海证券',
# '华安证券','南京证券','太平洋跨页','华林证券跨页','兴业证券','浙商证券跨页''中原证券','东兴证券','东吴证券','国泰君安']
'''
date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class Fin_Analysis(object):
    def Fink_database(self):
        #打开数据库连接
        try:
            db = pymysql.Connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='test', charset='utf8')
        except:
            print("could not connect to mysql server")
            exit(0)
        #使用cursor()方法获取操作游标
        cursor = db.cursor()
        #SQL查询语句
        sql = "select bond_company,keyword,whether_cross_page,whether_around from finance_config"
        #where keyword = % s
        #val = ('4、衍生金融工具')
        cursor.execute(sql)
        #获取所有记录列表
        results = cursor.fetchall()
        return results
        db.close()

    def Fin_Analysis_Mian(self, results):
        #获取参数
        for param in results:
            bond_company = param[0]
            keyword = param[1]
            whether_cross_page = param[2]
            whether_around = param[3]
            print("证券公司名称:" + bond_company)
            pdf = pdfplumber.open('D:/年报/'+bond_company+'.pdf')
            table = []
            list = []
            for page in pdf.pages[1:400]:
                data = page.extract_text()
                if keyword in data:
                    page_number = page.page_number
                    print('开始读取数据页数:' + str(page_number))
                    for i in range(page_number - 2, page_number + 2):
                        page_use = pdf.pages[i]
                        for table_list in page_use.extract_tables():
                            table.append(table_list)
            #判断是指标
            if keyword.endswith('结算备付金'):
                #获取所有的表格
                for i in range(0, len(table[0:]) - 1):
                    #获取范围类的每个表格
                    for j in range(0, len(table[0:][i])):
                        #判断是否跨页 2有跨页
                        if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('客户备付金') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].startswith('公司自有备付金') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].startswith('公司自有'):
                            list.append(table[0:][i])
                        if whether_cross_page == 2:
                            #跨页在后为1
                            if whether_around == 1:
                                if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('公司自有'):
                                    list.append(table[0:][i+1])
                            #跨页在前为-1
                            elif whether_around == -1:
                                if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('客户信用备'):
                                    list.append(table[0:][i-1])
            elif keyword.endswith('融出资金'):
                for i in range(0, len(table[0:]) - 1):
                    for j in range(0, len(table[0:][i])):
                        if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('减：减值') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].startswith('个人') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].startswith('1-3个月') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].startswith('资金'):
                            list.append(table[0:][i])
                        if whether_cross_page == 2:
                            if whether_around == 1:
                                if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('6个月'):
                                    list.append(table[0:][i+1])
                            elif whether_around == -1:
                                if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('6个月'):
                                    list.append(table[0:][i-1])
            elif keyword.endswith('衍生金融工具'):
                for i in range(0, len(table[0:]) - 1):
                    for j in range(0, len(table[0:][i])):
                        if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('商品期货') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].endswith('生工具') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].startswith('权益\n互换') or \
                                table[0:][i][j][0] != None and table[0:][i][j][0].startswith('资金'):
                            list.append(table[0:][i])
                        if whether_cross_page == 2:
                            if whether_around == 1:
                                if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('6个月'):
                                    list.append(table[0:][i+1])
                            elif whether_around == -1:
                                if table[0:][i][j][0] != None and table[0:][i][j][0].startswith('6个月'):
                                    list.append(table[0:][i-1])
            pdf.close()
            #去除重复list
            new_list = []
            for i in list:
                if i not in new_list:
                    new_list.append(i)
            #遍历列表
            for table in new_list:
                for row in table:
                    row.append(bond_company)
                    row.append(date)
                    print(row)
            return bond_company

    # def get_data(self,bond_comany):
    #     print(bond_comany)

    def Start(self):
        results = self.Fink_database()
        self.Fin_Analysis_Mian(results)
        # self.get_data(bond_comany)

Fin_Analysis().Start()
