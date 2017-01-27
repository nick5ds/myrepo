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

def generate_ndb_data(end_date,interval):
    FIVETRAN_HOST ='eatsa-warehouse-fivetran.c5xyqeaknls5.us-east-1.redshift.amazonaws.com'
    FIVETRAN_USER = 'awsuser'
    FIVETRAN_PASSWORD ='CxoqbgkkqtWXILseCR0qeGWn3s72iFDPML8T8rMwFbpjyzfraOBBxtihPkDZq6C3'
    FIVETRAN_DATABASE = 'fivetran'
    conn = psycopg2.connect(
        dbname=FIVETRAN_DATABASE,
        host=FIVETRAN_HOST,
        port=5439,
        user=FIVETRAN_USER,
        password=FIVETRAN_PASSWORD
    )
    conn.autocommit = True
    where=' and activation_date_3rd_visit<='+"'"+end_date+"'" + ' and activity_date<='+"'"+end_date+"'" 
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
    where first_store like '%1 Cal%'
and    activity_date<='{0}' and activity_type!='inactive' and is_test_user=false and is_employee_user=false)
    
group by 1,2 )


    """.format(end_date,interval)
    transaction_data = pd.read_sql(query, conn)
    return transaction_data

def fit_nbd_model(data,model_columns=['frequency','recency','t']):
    bgf = BetaGeoFitter(penalizer_coef=0.0)
    bgf.fit(data[model_columns[0]], data[model_columns[1]], data[model_columns[2]])
    return bgf


def make_preds(bgf,data,num_periods_to_predict,model_columns=['frequency','recency','t']):
    x=bgf.conditional_expected_number_of_purchases_up_to_time(
                num_periods_to_predict,data[model_columns[0]]
                , data[model_columns[1]]
                , data[model_columns[2]])
    result=pd.concat([data,x],axis=1)
    col_name='projected_transactions_next_'+str(num_periods_to_predict)+'_periods'
    result.rename(columns={0:col_name},inplace=True)
    return {'colname':col_name,'data':result}


def make_elsatic_pred(row,**kwargs):
    avi_column=kwargs['avi_column']
    transactions_column=kwargs['transactions_column']
    elasticity=kwargs['elasticity']
    low_price=kwargs['low_price']
    old_price=kwargs['old_price']
    high_price=kwargs['high_price']
    max_value=20
    bins=[0,8,15,22,28]
    discount_size=(high_price-old_price)/(len(bins)-1)
     
    bins.reverse()
    x=row[transactions_column]
    avi=row[avi_column]
    for index,value in enumerate(bins):
        new_x=x*(1+((high_price-discount_size*index)/old_price-1)*elasticity) 
        return new_x
def drange(x,y,jump):
    x=decimal.Decimal(x)
    y=decimal.Decimal(y)
    while not x>y:
        yield float(x)
        x+=decimal.Decimal(jump)

def make_elastic_matrix(data,col,e_start,e_end,e_step,d_start,d_end,d_step):
    final_data=data
    original_data=data
    for e in list(drange(e_start,e_end,e_step)):
        for d in list(drange(d_start,d_end,d_step)):
            print "elasticity is " + str(e) +" and discount is" + str(d)
            data['elasticity']=e
            data['high_price']=d
            data['elastic purchase']=data.apply(make_elsatic_pred,avi_column='last_avi_6v',transactions_column=col
                          ,elasticity=e,low_price=4.95,old_price=6.95,high_price=d,axis=1)
            final_data=pd.concat([final_data,data],axis=0)
            data=original_data
    return final_data

if __name__ == "__main__":
    data=generate_ndb_data('2016-10-01','week')
    model=fit_nbd_model(data)
    preds=make_preds(model,data,4)
    col= preds['colname']
    elastic_test= preds['data'].head()
 #  elastic_test['elastic_price']=elastic_test.apply(make_elsatic_pred,avi_column='last_avi_6v',transactions_column=col
 #  ,elasticity=-2,low_price=4.95,old_price=6.95,discount_size=1,axis=1)
 #  elastic_test.to_csv('../elastic_test.csv',index=False)
    elastic_matrix=make_elastic_matrix(elastic_test,col,-4.0,0.0,0.5,6.95,10.95,0.5)
    elastic_matrix.to_csv('../elastic_m.csv',index=False)
#    data=14
#    avi=7
#    print make_elsatic_pred(data,avi,-2,4.95,6.95,1,20)

#bgf = BetaGeoFitter(penalizer_coef=0.0)
#bgf.fit(data['frequency'], data['recency'], data['T'])
#print bgf
#plot_frequency_recency_matrix(bgf)
