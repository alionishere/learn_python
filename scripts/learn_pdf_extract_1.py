import pdfplumber, time
from .models import ScenarioInfo   #这里是引入模型中的ScenarioInfo表
with pdfplumber.open(r'C:\Users\hisoft\Desktop\(2019-09-25)_21460.pdf') as pdf:
    m = 0  # 测试步骤表格的数量（按表头数算）
    s = '<table><tbody>'
    mjcd = ''  # 末级菜单
    for i in range(29, len(pdf.pages)):  # 从第30页开始获取表格数据
        page = pdf.pages[i]  # 设置操作页面
        n = 1  # 当前页的第n个表格
        for table in page.extract_tables():
            k = True
            for row in range(len(table)):
                if str(table[row]) == r"['Step', '案例步骤', '预期结果', '测试信息', '耗时(秒\n)', '结果']":  # 这是根据表格行内容判断是否是一个新的表格开头，因为表头内容都是固定的
                    if i == 29 and n == 1:  # 判断是否是第30页的第一个表格
                        pass
                    else:
                        s += '</tbody></table>'
                        print('i=', i)
                        # print('s=', s)
                        # print('mjcd=', mjcd)
                        m += 1
                        ScenarioInfo.objects.create(test_steps=s, lrry='test', mjcd=mjcd)  # 存进数据库，s是拼接好的表格字符串，富文本会识别表格标签显示
                        time.sleep(0.6)
                        mjcd = ''
                        s = '<table><tbody>'
                for j in range(len(table[row])):
                    if j == 0:  # 判断是否是当前行的第一列
                        s += '<tr><td>'
                    s += table[row][j]
                    if j == 1:  # 判断是否是当前行的第二列，将末级菜单取出来,若碰到有多个末级菜单的只取第一个
                        if k:
                            if len(table[row][j].split('搜索打开菜单:【')) > 1:
                                mjcd = table[row][j].split('搜索打开菜单:【')[1].split('】')[0]
                                k = False
                    if j == len(table[row]) - 1:  # 判断是否是当前行的最后一列
                        s += '</td></tr>'
                    else:
                        s += '</td><td>'
            if i == len(pdf.pages) - 1:  # 判断是否是最后一页
                if n == len(page.extract_tables()):  # 判断是否是最后一个表格
                    for row in range(len(table)):
                        if row == len(table) - 1:  # 判断是否是当前表格的最后一行
                            for j in range(len(table[row])):
                                if j == len(table[row]) - 1:  # 判断是否是当前行的最后一列
                                    s += '</tbody></table>'
                                    print('i=', i)
                                    print('end_s=:', s)
                                    m += 1
                                    print('m=', m)
                                    print('mjcd=', mjcd)
                                    # ScenarioInfo.objects.create(test_steps=s, lrry='test', mjcd=mjcd)  #存进数据库
            n += 1
