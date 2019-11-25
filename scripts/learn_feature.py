# 学习目标：使用DictVectorizer对使用字典存储的数据进行特征抽取和向量化
from sklearn.feature_extraction import DictVectorizer

# 定义一组字典列表，用来表示多个数据样本（每个字典代表一个数据样本）
measurements = [{'city': 'Beijing', 'temperature': 33.}, {'city': 'London', 'temperature': 12.},
                {'city': 'San Fransisco', 'temperature': 18.}]
# 从sklearn.feature_extraction导入DictVectorizer

# from sklearn2


vec = DictVectorizer()
# 输出转化后的特征矩阵
print(vec.fit_transform(measurements).toarray())
# 输出各个维度的特征含义
print(vec.get_feature_names())

measurements = [{'city': 'Beijing', 'country': 'CN', 'temperature': 33.},
                {'city': 'London', 'country': 'UK', 'temperature': 12.},
                {'city': 'San Fransisco', 'country': 'USA', 'temperature': 18.}]
