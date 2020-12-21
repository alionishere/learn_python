import pdfplumber
import xlwt

workbook = xlwt.Workbook()
sheet = workbook.add_sheet('Sheet1')
i = 0

pdf = pdfplumber.open('C:/Users/13637/Desktop/dwzq.pdf')

print('\n')
print('开始读取数据')
print('\n')

for page in pdf.pages[107:108]:

    for table in page.extract_tables():

        for row in table:
            if '利息净收入' in row:
                print(row)
                for j in range(len(row)):
                    sheet.write(i, j, row[j])
                    i += 1

pdf.close()

workbook.save('C:/Users/13637/Desktop/us2.xls')
print('\n')
print('写入excel成功')
print('保存位置：')
print('C:/Users/13637/Desktop/us2.xls')
print('\n')
input('PDF取读完毕，按任意键退出')