from turtle import *

# help doc website
# https://docs.python.org/3.5/library/turtle.html
color('red', 'yellow')
begin_fill()
while True:
    forward(200)
    left(170)
    if abs(pos()) < 1:
        break
end_fill()
done()