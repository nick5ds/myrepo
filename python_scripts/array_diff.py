from pprint import pprint
from datetime import datetime




def common_elements(array1,array2):
    num_same=0
    array1=array1.split(',')
    array2=array2.split(',')
    for item in array2:
        if item in array1:
            num_same+=1
    final= str(len(array2))+','+str(num_same)
    return final


if __name__ == "__main__":
    array1="guac,onions,quinoa,beans"
    array2="basil,lemon,guac,tofu,rice,beans"

    print(common_elements(array1,array2))
