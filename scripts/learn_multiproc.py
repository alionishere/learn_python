import time
from threading import Thread


def main(name):
    for i in range(3):
        print('Hello %s: %s' % (name, i))
        time.sleep(1)


t_1 = Thread(target=main, args=('Mical',))
t_2 = Thread(target=main, args=('Jack',))

t_1.start()
t_2.start()


class MyThread(Thread):
    def __init__(self, name):
        # 注意super().__init__()一定要写，而且需要写在第一行，
        # 否则会报错
        super().__init__()
        self.name = name

    def run(self):
        for i in range(3):
            print('Hello %s: %s' % (self.name, i))
            time.sleep(1)


if __name__ == '__main__':
    th_1 = MyThread('Ordaily')
    th_2 = MyThread('Hurrffn')

    print('-' * 50)
    th_1.start()
    th_2.start()
