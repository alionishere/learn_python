import datetime

print('Hello World!')
print('Time is %s' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print('__name__value: %s' % __name__)


def main(msg):
    print('Main msg: %s' %msg)


if __name__ == '__main__':
    main('This is first!')
