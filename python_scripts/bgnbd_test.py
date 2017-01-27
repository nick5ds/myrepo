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
    print query
    transaction_data = pd.read_sql(query, conn)
    return transaction_data


data=generate_ndb_data('2016-10-01','week')
data.to_csv('~/data_work/nbd_data.csv',sep=',',index=False)
#data=pd.read_csv('~/data_work/nbd_data.csv',sep=',')
#data.head()

bgf = BetaGeoFitter(penalizer_coef=0.0)
bgf.fit(data['frequency'], data['recency'], data['t'])
x=bgf.conditional_expected_number_of_purchases_up_to_time(4,data['frequency'],data['recency'],data['t'])
x.rename("purchases in next 4 weeks")
result=pd.concat([data,x],axis=1)
result.rename(columns={0:'projected_transactions_next_4_weeks'},inplace=True)
print result.head()
result.to_csv('~/data_work/projected_transactions.csv',index=False)


print bgf

#bgf = BetaGeoFitter(penalizer_coef=0.0)
#bgf.fit(data['frequency'], data['recency'], data['T'])
#print bgf
#plot_frequency_recency_matrix(bgf)
