import numpy as np

def price_array(bowl_distribution,discount_per_bin,avg_price):
    center=np.asarray([-2, -1, 0, 1, 2])
    center=center*discount_per_bin
    center_dot=np.dot(center,bowl_distribution)
    centers_bc=center-center_dot
    final_price=centers_bc+avg_price
    return final_price

def min_max_price(discount_range,bowl_distribution,avg_price):
    final=[]
    for i in list(drange(discount_range[0],discount_range[1],discount_range[2])):
        pa=price_array(bowl_distribution,i,avg_price)
        min_p=min(pa)
        max_p=max(pa)
        prices={'min_price':min_p,'max_price':max_p,'discount':i}
        final.append(prices)
    return final

discount_range=[0,3,0.1]
bowl_distribution=[0.139 , 0.188, 0.142, 0.103, 0.427]
avg_price=8.44


print price_array(bowl_distribution,1,8.08)
