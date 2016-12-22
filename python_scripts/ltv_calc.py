from pprint import pprint
from datetime import datetime




def create_ltv(user_activity,days_old):
    ltv=0
    activity=dict(item.split(":") for item in user_activity.split(",")) 
    activity_dates=list(activity.keys())
    activity_dates.sort()
    pprint(activity_dates)
    install_date=datetime.strptime(min(activity_dates),"%Y-%m-%d")
    for day in activity_dates:
        if (datetime.strptime(day,"%Y-%m-%d")-install_date).days>days_old:  
            break
        if (datetime.strptime(day,"%Y-%m-%d")-install_date).days<=days_old:
            ltv+=int(activity[day])
    return ltv

if __name__ == "__main__":
    data="2016-06-01:499,2016-06-04:799,2016-06-05:599"
    ltvday=2
    print(data)
    print(create_ltv(data,ltvday))
    print(create_ltv(data,3))
    print(create_ltv(data,7))
