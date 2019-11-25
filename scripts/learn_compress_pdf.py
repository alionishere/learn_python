# pdffile = "t2.pdf"
# doc = fitz.open(pdffile)
# width, height = fitz.PaperSize("a4")
#
# totaling = doc.pageCount
#
# for pg in range(totaling):
#     page = doc[pg]
#     zoom = int(100)
#     rotate = int(0)
#     print(page)
#     trans = fitz.Matrix(zoom / 100.0, zoom / 100.0).preRotate(rotate)
#     pm = page.getPixmap(matrix=trans, alpha=False)
#
#     lurl = 'pdf/%s.jpg' % str(pg + 1)
#     pm.writePNG(lurl)
