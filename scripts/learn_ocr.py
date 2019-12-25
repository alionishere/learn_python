# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 14:20:36 2019

"""
import cv2 as cv
from PIL import Image
from wand.image import Image as IM
from aip import AipOcr
import pytesseract
import time


def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()


def Pdf_Jpg(file_name):
    image_pdf = IM(filename=file_name, resolution=300)
    image_jpeg = image_pdf.convert('jpg')
    req_image = []
    for img in image_jpeg.sequence:
        img_page = IM(image=img)
        req_image.append(img_page.make_blob('jpg'))
    long = len(req_image)
    i = 0
    for img in req_image:
        ff = open(r'E:/SCOOCHOW/2_dcp/4_test/tmp/' + str(i) + '.jpg', 'wb')
        ff.write(img)
        ff.close()
        i += 1


def recognize_text(src):
    gray = cv.resize(src, None, fx=0.3, fy=0.3, interpolation=cv.INTER_CUBIC)
    cv.imshow("src2", src)
    gray = cv.cvtColor(src, cv.COLOR_BGR2GRAY)
    cv.imshow("src3", gray)
    ret, binary = cv.threshold(gray, 0, 255, cv.THRESH_BINARY_INV | cv.THRESH_OTSU)
    kernel = cv.getStructuringElement(cv.MORPH_RECT, (1, 2))  # 去除线
    binl = cv.morphologyEx(binary, cv.MORPH_OPEN, kernel)
    kernel = cv.getStructuringElement(cv.MORPH_RECT, (2, 1))
    open_out = cv.morphologyEx(binl, cv.MORPH_OPEN, kernel)
    cv.bitwise_not(open_out, open_out)  # 黑色背景变为白色背景
    cv.imshow('open_out', open_out)
    textImage = Image.fromarray(open_out)  # 从np.array 转换成<class 'PIL.Image.Image'>，pytesseract需要接受此类型
    text = pytesseract.image_to_string(textImage, lang='chi_sim')
    print("This OK:%s" % text)


def baidu_Ocr(fname):
    APP_ID = '18006233 '
    API_KEY = 'xHOB4XGSIIsAoW3XpLyejUF4'
    SECRET_KEY = '6ajg5BGqHxhh6OFMlgtnzBvQsAuE1udU'
    client = AipOcr(APP_ID, API_KEY, SECRET_KEY)
    image = get_file_content(fname)
    # print(image)
    ll = client.basicAccurate(image)
    # print(ll)
    ll = ll['words_result']
    n = len(ll)
    for l in range(n):
        print(ll[l]['words'])


def Work():
    file_name = r'E:/SCOOCHOW/2_dcp/4_test/tmp/1.pdf'
    Pdf_Jpg(file_name)
    file = r'E:/SCOOCHOW/2_dcp/4_test/tmp/1.jpg'
    baidu_Ocr(file)
    # src =cv.imread('C:/Temp/0.jpg')
    # cv.imshow("src", src)
    # recognize_text(src)
    # text = pytesseract.image_to_string(file,lang='chi_sim')
    # cv.waitKey(0)
    # cv.destroyAllWindows()
    # print(text)


def main():
    Work()


if __name__ == '__main__':
    start = time.time()
    print(start)
    main()
    end = time.time()
    print(end)
    m, s = divmod(end - start, 60)
    h, m = divmod(m, 60)
    print("运行时长：%02d:%02d:%02d" % (h, m, s))