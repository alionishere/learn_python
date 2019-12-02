# -*- coding: utf-8 -*-


class Student(object):
    def __init__(self, name, score):
        self.__name = name
        self.score = score

    def get_grade(self):
        if self.score >= 90:
            return 'A'
        elif self.score >= 60:
            return 'B'
        else:
            return 'C'

    def get_name(self):
        return self.__name

    def get_score(self):
        return self.__score


# lisa = Student('Lisa', 90)
# bart = Student('Bart', 85)
# print(lisa._Student__name, lisa.get_grade())
# print(bart.get_name(), bart.get_grade())
# lisa.age = 9
# print(lisa.age)
# bart.__name = 'Barty'
# print(bart.get_name(), bart.get_grade())
class Animal(object):
    def run(self):
        print('Animal is running...')


class Dog(Animal):
    pass


class Cat(Animal):
    pass


dog = Dog()
dog.run()

cat = Cat()
cat.run()


class MyDog(object):
    def __len__(self):
        return 100


dog = MyDog()
print(len(dog))
