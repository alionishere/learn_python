from PIL import Image
import pytesseract

im = Image.open('1.png')
# im.show()
# print(pytesseract.image_to_string(Image.open('1.png')))
print(pytesseract.image_to_string(Image.open('1.png'), lang='chi_sim'))
