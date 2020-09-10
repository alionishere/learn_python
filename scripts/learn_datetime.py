from datetime import datetime, date, timedelta

hour = datetime.now().hour

if hour == '10':
    print(hour)
else:
    print('....')

start_date = date.today() + timedelta(days=-22)
print(start_date)
for i in range(107):
    print(start_date + timedelta(days=-i), end=',')
