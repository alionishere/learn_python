from PIL import Image
import sys, os
from PIL import Image, ImageDraw,ImageEnhance
from PIL import Image
import pytesseract
import time, urllib.request


def test(path):
    img = Image.open(path)

    # 模式L”为灰色图像，它的每个像素用8个bit表示，0表示黑，255表示白，其他数字表示不同的灰度。
    Img = img.convert('L')
    # 自定义灰度界限，大于这个值为黑色，小于这个值为白色
    threshold = 170

    table = []
    for i in range(256):
        if i < threshold:
            table.append(0)
        else:
            table.append(1)

    # 图片二值化
    photo = Img.point(table, '1')

    return photo

# 二值数组
t2val = {}

def twoValue(image, G):
    for y in range(0, image.size[1]):
        for x in range(0, image.size[0]):
            g = image.getpixel((x, y))
            if g > G:
                t2val[(x, y)] = 1
            else:
                t2val[(x, y)] = 0


# 根据一个点A的RGB值，与周围的8个点的RBG值比较，设定一个值N（0 <N <8），当A的RGB值与周围8个点的RGB相等数小于N时，此点为噪点
# G: Integer 图像二值化阀值
# N: Integer 降噪率 0 <N <8
# Z: Integer 降噪次数
# 输出
#  0：降噪成功
#  1：降噪失败
def clearNoise(image, N, Z):
    for i in range(0, Z):
        t2val[(0, 0)] = 1
        t2val[(image.size[0] - 1, image.size[1] - 1)] = 1

        for x in range(1, image.size[0] - 1):
            for y in range(1, image.size[1] - 1):
                nearDots = 0
                L = t2val[(x, y)]
                if L == t2val[(x - 1, y - 1)]:
                    nearDots += 1
                if L == t2val[(x - 1, y)]:
                    nearDots += 1
                if L == t2val[(x - 1, y + 1)]:
                    nearDots += 1
                if L == t2val[(x, y - 1)]:
                    nearDots += 1
                if L == t2val[(x, y + 1)]:
                    nearDots += 1
                if L == t2val[(x + 1, y - 1)]:
                    nearDots += 1
                if L == t2val[(x + 1, y)]:
                    nearDots += 1
                if L == t2val[(x + 1, y + 1)]:
                    nearDots += 1

                if nearDots < N:
                    t2val[(x, y)] = 1


def saveImage(filename, size):
    image = Image.new("1", size)
    draw = ImageDraw.Draw(image)

    for x in range(0, size[0]):
        for y in range(0, size[1]):
            draw.point((x, y), t2val[(x, y)])

    image.save(filename)






def recognize_captcha(img_path):
    im = Image.open(img_path)
    # threshold = 140
    # table = []
    # for i in range(256):
    #     if i < threshold:
    #         table.append(0)
    #     else:
    #         table.append(1)
    #
    # out = im.point(table, '1')
    num = pytesseract.image_to_string(im)
    return num

def downImg(flink):
    # number = random.randint(1000, 10000)
    content2 = urllib.request.urlopen(flink).read()
    with open(u'imgs/原始图片.png', 'wb') as code:
        code.write(content2)
    return 'imgs' + '/原始图片.png'

def dowork(image_name):
    # 下载秃瓢
    # image_name = downImg(image_name)

    # 二值化变色
    # image_name='imgs/原始图片378682.png'
    im = test(image_name)
    im.save('imgs/二值化变色.png')

    # 降噪
    image = Image.open('imgs/二值化变色.png').convert("L")
    twoValue(image, 100)
    clearNoise(image, 4, 4)
    path1 = "imgs/降噪.jpg"
    saveImage(path1, image.size)

    # 对比度增强
    # imageCode = Image.open(path1)
    # sharp_img = ImageEnhance.Contrast(imageCode).enhance(2.0)
    # sharp_img.save("imgs/对比度增强.png")
    # 识别
    res = recognize_captcha(path1)
    yzm = res.replace(' ', '')
    print(yzm)
    return yzm
    # if len(strs) >= 1:
    #     print(strs[0])

if __name__ == '__main__':
    dowork('http://221.178.136.186:8090/pkt/service/getCode?random=1231312312')
