from lifetimes import BetaGeoFitter
from lifetimes.datasets import load_cdnow
from lifetimes.plotting import plot_frequency_recency_matrix
from lifetimes.utils import summary_data_from_transaction_data
import pandas as pd
from pandas import Series, DataFrame
import numpy as np
from sqlalchemy import create_engine
import os
from collections import Counter
import psycopg2
import decimal
from pprint import pprint
import json



def get_creds(file_loc='../config.json'):
    with open(file_loc) as config:
        conf=json.load(config)
    redshift=conf['redshift']
    FIVETRAN_HOST=redshift['FIVETRAN_HOST']
    FIVETRAN_USER = redshift['FIVETRAN_USER']
    FIVETRAN_PASSWORD = redshift['FIVETRAN_PASSWORD']
    FIVETRAN_DATABASE = redshift['FIVETRAN_DATABASE']


def generate_ndb_data(end_date,interval,**kwargs):

#generates the base data needed for bg_nbd. Note that the interval sets the period length of the model


    
    conn = psycopg2.connect(
        dbname=kwargs['FIVETRAN_DATABASE'],
        host=kwargs['FIVETRAN_HOST'],
        port=5439,
        user=kwargs['FIVETRAN_USER'],
        password=kwargs['FIVETRAN_PASSWORD']
    )
    conn.autocommit = True
    query="""




select id
,max_p as last_purchase_date
,min_p as vist_purchase_date
,frequency as num_purchases
,case when frequency>=3 and datediff('day',max_p,'{0}')<=28 then '28 day active'
when frequency>=3 and datediff('day',max_p,'{0}')<=84 then '84 day active'
else 'not active' end as is_active
,last_avi_6v
, frequency-1 as frequency
,datediff('{1}',min_p,max_p) as recency
, datediff('{1}',min_p,'{0}') as T from
(select id,last_avi_6v,count(distinct date) frequency,min(date) min_p,max(date) max_p from

 (       select distinct activity_date as date, user_account as id,last_value(avi_6v) over (partition by user_account order by activity_date rows between unbounded preceding and unbounded following) as last_avi_6v
    from customer_identity.t_user_activity_stats
    where first_store like '%Spear%'
and    activity_date<='{0}' and activity_type!='inactive' and is_test_user=false and is_employee_user=false )
    
group by 1,2 )


    """.format(end_date, interval)
    transaction_data = pd.read_sql(query, conn)
    return transaction_data

def fit_nbd_model(data,model_columns=['frequency', 'recency', 't']):
    bgf = BetaGeoFitter(penalizer_coef=0.0)
    bgf.fit(data[model_columns[0]], data[model_columns[1]], data[model_columns[2]])
    return bgf


def make_preds(bgf,data,num_periods_to_predict,model_columns=['frequency','recency','t']):
    #note returns a dictionary not just the data. This is because the column name is needed in the following fuction
    x=bgf.conditional_expected_number_of_purchases_up_to_time(
                num_periods_to_predict,data[model_columns[0]]
                , data[model_columns[1]]
                , data[model_columns[2]])
    result=pd.concat([data,x],axis=1)
    col_name='projected_transactions_next_'+str(num_periods_to_predict)+'_periods'
    result.rename(columns={0:col_name},inplace=True)
    return {'colname':col_name,'data':result}


def make_elsatic_pred(row,**kwargs):
    #function that is applied onto the generate NBD data frame. Multiplies the predicted number of orders with an elasticity 
    avi_column=kwargs['avi_column']
    transactions_column=kwargs['transactions_column']
    elasticity=kwargs['elasticity']
    low_price=kwargs['low_price']
    old_price=kwargs['old_price']
    high_price=kwargs['high_price']
    max_value=kwargs['max_value']
    bins=kwargs['bins'] 
    discount_size=kwargs['discount_size']
     

    x=row[transactions_column]
    avi=row[avi_column]
    if avi==0:
        new_x=x*(1+((high_price)/old_price-1)*elasticity)
        return new_x
    for index,value in enumerate(bins):
        if avi>float(value):
            
            increase=(1+((high_price-discount_size*index)/old_price-1)*elasticity) 
            new_x=x*increase
            if new_x>float(max_value):
                return max_value
            elif new_x<0.0:
                return 0.0
            else: 
                return new_x
                
def drange(x,y,jump):
    x=decimal.Decimal(x)
    y=decimal.Decimal(y)
    while True:
        yield float(x)
        if x>=y:
            break
        x+=decimal.Decimal(jump)

def make_elastic_matrix(data,col,old_price,e_range, discount_range, bowl_dist, avg_price):
    #loops through a rangGe of elasticities and discounts to create a dataset of possible values
    final_data=data
    original_data=data
    prices=min_max_price(discount_range,bowl_dist,avg_price)
    for e in list(drange(e_range[0], e_range[1], e_range[2])):
        for price in prices:
            lp=price['min_price']
            hp=price['max_price']
            d=price['discount']
            print "elasticity is " + str(e) +" and high_price is" + str(hp)
            data['elasticity']=e
            data['high_price']=hp
            data['low_price']=lp
            data['discount_size']=d
            #import pdb; pdb.set_trace()
            data['elastic purchase']=data.apply(make_elsatic_pred
                    ,avi_column='last_avi_6v',transactions_column=col
                    ,elasticity=e,low_price=lp,old_price=old_price,high_price=hp,axis=1,bins=[28,22,15,8,0],max_value=20,discount_size=d)

#            final_data=pd.concat([final_data,data],axis=0)
            file_name="../projections_low_price/"+str(d)+str(hp)+str(lp)+str(e)+".csv"
            data.to_csv(file_name,index=False,header=False)
            data=original_data
    return final_data

def price_array(bowl_distribution, discount_per_bin, avg_price):
    center=np.asarray([-2, -1, 0, 1, 2])
    center=center*discount_per_bin
    center_dot=np.dot(center,bowl_distribution)
    centers_bc=center-center_dot
    final_price=centers_bc+avg_price
    return final_price

def min_max_price(discount_range, bowl_distribution, avg_price):
    final=[]    
    for i in list(drange(discount_range[0], discount_range[1], discount_range[2])):
        pa=price_array(bowl_distribution, i, avg_price)
        min_p=min(pa)
        max_p=max(pa)
        prices={'min_price':min_p, 'max_price':max_p, 'discount':i}
        final.append(prices)
    return final

if __name__ == "__main__":
    with open('../config.json') as config:
        conf=json.load(config)
    redshift=conf['redshift']
    FIVETRAN_HOST=redshift['FIVETRAN_HOST']
    FIVETRAN_USER = redshift['FIVETRAN_USER']
    FIVETRAN_PASSWORD = redshift['FIVETRAN_PASSWORD']
    FIVETRAN_DATABASE = redshift['FIVETRAN_DATABASE']
    discount_range=[0,3,0.25]
    bowl_distribution=[0.139 , 0.188, 0.142, 0.103, 0.427]
    avg_price=6.7
    old_price=7.85
    elastic_range=[-4, 0, 0.1]
    data=generate_ndb_data('2016-10-01','day',FIVETRAN_HOST=FIVETRAN_HOST,FIVETRAN_USER=FIVETRAN_USER,FIVETRAN_PASSWORD=FIVETRAN_PASSWORD,FIVETRAN_DATABASE=FIVETRAN_DATABASE)
    model=fit_nbd_model(data)
    pred_data=data.query('is_active!="not active"')
    preds=make_preds(model,pred_data, 4)
    col= preds['colname']
    elastic_test= preds['data']
    preds['data'].to_csv('../base_projection.csv',index=False)
    elastic_matrix=make_elastic_matrix(elastic_test,col,old_price,elastic_range,discount_range,bowl_distribution,avg_price)
    elastic_matrix.to_csv('test.csv',index=False)
