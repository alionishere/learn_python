"""
'b'：布尔值
'i'：符号整数
'u'：无符号整数
'f'：浮点
'c'：复数浮点
'm'：时间间隔
'M'：日期时间
'O'：Python 对象
'S', 'a'：字节串
'U'：Unicode
'V'：原始数据（void）
"""
import numpy as np

a = np.array([1, 2, 3])
b = np.array([[1, 2], [3, 4]])
c = np.array([1, 2, 3, 4, 5], ndmin=3)
d = np.array([1, 2, 3], dtype=complex)
e = np.array([1.1, 2.3, 3.6], dtype=int)
student = np.dtype([('name', 'S20'), ('age', 'i1'), ('marks', 'f4')])
st = np.array([('abc', 21, 50), ('xyz', 18, 75)], dtype=student)
# print(a)
# print(b)
# print(c)
# print(d)
# print(e)
print(student)
a = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
b = np.array([[1, 2, 3], [4, 5, 6]])
b.shape = (3, 2)
x = np.arange(24)
print(x.ndim)
y = x.reshape(2, 4, 3)
print(y.ndim)
print('*' * 50)
print(y)
# print(st)
print('-' * 50)
x = np.array([1, 2, 3, 4, 5])
print(x.flags)
print('-' * 50)
x = np.empty([3, 3], dtype=int)  # 因为没有初始化，所以打印的为随机值
print(x)
print('-' * 50)
x = np.zeros([3, 3])
print(x)
print(np.zeros([3, 3], dtype=np.int))
print('-' * 50)
print(np.ones(5))
print(np.ones(5, dtype=int))
print(np.ones([3, 3], dtype=int))
print('-' * 50)
print(np.asarray([(1, 2, 3), (4, 5)]))
print(np.arange(10, 19, 2))
print(np.linspace(10, 20, 11))
print(np.arange(10)[2:18:2])
print(np.arange(10)[2:])
# aa = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
# print(a)
# print('--' * 50)
# print(a[0])
# print(a[..., 1:])
# print(a[1:])

x = np.array([[10, 71, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]])
print(x[x > 5])
a = np.arange(0, 60, 5)
a = a.reshape(3, 4)
print(a)
print(a.T)
for x in np.nditer(a.T):
    print(x, end=' ')
