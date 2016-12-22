from pprint import pprint
from datetime import datetime



def calulate_expected_visits(prob_list,transition_mult=1,max_visits=0):
    ev=0.0
    prob_list=prob_list.split(',')
    for count,prob in enumerate(prob_list):
         #calculate probability of n visits
        pn=1.0
        for i in range(count+1):
            if float(prob_list[i])*transition_mult>=1.0:
                print(1)
                pn=pn
            else:
                print(float(prob_list[i])*transition_mult)
                pn=pn*float(prob_list[i])*transition_mult
            end_prob=pn
        ev=ev+pn 
        #print(end_prob) 
    if max_visits>len(prob_list):
       for i in range(len(prob_list),max_visits):
            ev=ev+end_prob 
            print(ev)
    return ev




if __name__ == "__main__":
    data="1.0000,1.0000,1.0000,0.9429,0.9152,0.9470,0.9231,0.9091,0.9000,0.9167,0.9091,0.9556,0.8488,0.9178,0.8358,0.9643,0.9074,0.8980,0.9091,0.9250,0.9189,0.9118,0.8710,0.8889,0.8750,0.8571,0.8889,0.8750,0.8571,0.9167,0.9091,0.9000,0.8889,0.8750,1.0000,0.8571,1.0000,1.0000,0.8333,0.8000,1.0000,1.0000,0.7500,1.0000,1.0000,0.6667,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,0.5000,1.0000,1.0000,1.0000"
    print(len(data.split(',')))
    print(data)
    print(calulate_expected_visits(data,0.9,10))
