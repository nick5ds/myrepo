from datetime import datetime,timedelta
from pytz import timezone
import numpy as np

def update_avi(avi_length, list_of_num_days, last_visit_date):    
    #setting everything to pacific since user activity stats does a timezone conversion and subtracting pacific/eastern time  from UTC coule yield an incorrect result
    pac=timezone('US/Pacific')
    last_purchase = datetime.strptime(last_visit_date, '%Y-%m-%d %H:%M:%S')
    last_purchase = last_purchase.replace(tzinfo=pac)
    today=pac.localize(datetime.now()) 
    curr_days = float((today-last_purchase).days)
    days_list = map(float, list_of_num_days.split(','))
    if days_list[0] == 0:
        #a zero in the begining of the array happens becasue it is the num days from previous visit for their first visit, which is null but gets reported as zero so needs to be dropped
        days_list.pop(0)

    days_list.append(curr_days)
    avi = np.mean(days_list[-avi_length:])
    return avi


if __name__ == "__main__":

    #the below row is a row of data from the query to populate custoza in the format
    #user_id
    #last_6_num_days
    #last_purchase_date
    #first_purchase_date
    #num_purchases
    #is_active
    #last_avi_6v
    #first_store
    #last_store
    #num_visits
    #frequency
    #recency
    #t
    
    
    
    
    row = [
        '0970a009-59c2-4ea3-a000-88247bd51522',
        '30.00,3.00,28.00,15.00,16.00,32.00,106.0',
        '2016-09-26 00:00:00',
        '2016-03-23 00:00:00',
         8,
        'not active',
        20.66,
        '121 Spear St.',
        '121 Spear St.',
        8,
        7,
        27,
        45
    ]
    pac = timezone('US/Pacific')
    curr_days = float(
        (pac.localize(datetime.now())-
        datetime.strptime('2016-09-26 00:00:00','%Y-%m-%d %H:%M:%S').replace(tzinfo=pac))
        .days
        )
    days1 = '30.00,3.00,28.00,15.00,16.00,32.00,106.0'
    days2 = '0.00,2'
    days3 = '0.00'
    days4 = '0,2,3'
    days1answer = np.mean([32.0, 106.0, curr_days])
    days2answer = np.mean([2.0, curr_days])
    days3answer = np.mean([curr_days])
    days4answer = np.mean([2.0, 3.0, curr_days])
    
    print update_avi(3, days1, row[2]) == days1answer
    print update_avi(3, days2, row[2]) == days2answer
    print update_avi(3, days3, row[2]) == days3answer
    print update_avi(3, days4, row[2]) == days4answer
