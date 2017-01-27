create or replace function pivot_columns(keys varchar(2000),values varchar(2000),col_names varchar(2000))
  returns varchar(2000)
stable
as $$
import pandas as pd
colnames=col_names.split(',')
keys=keys.split(',')
values=values.split(',')



df=pd.DataFrame(columns=colnames,index=[1])
for i,v in enumerate(keys):
    print v
    df.set_value(1,v,str(values[i]))


df.apply(str)
b = '\n'.join(','.join('%s' %x for x in y) for y in df.values)
return b
$$ language plpythonu;



create view user_features.v_item_clicks_and_purchases_raw as (
select user_account,is_test_user,is_employee_user,user_id,order_id,item_name
,case when line_item_id is not null then True else False end as did_purchase
,count(distinct timestamp) as num_clicks
from
(select distinct v.item_name,v.item_id,t.timestamp,line_items.id line_item_id,c.order_id,o.user_id,ua.user_account,ua.is_employee_user,ua.is_test_user from segment.viewed_product v
join segment.completed_order c on (v.ordeR_session_id=c.order_session_id) 
join segment.tracks t on (v.message_id=t.message_id)
join kwwhub_public.orders o on (c.order_id=o.id)
left join kwwhub_public.line_items
on (c.order_id=line_items.order_id and line_items.item_id=v.item_id)
left join
customer_identity.t_user_accounts ua
on (o.user_id=ua.user_id)
 ) where order_id is not null
 group by 1,2,3,4,5,6,7)
 

select user_account,order_id,pivot_columns(keys,values,colnames) from

(select user_account,order_id,listagg(item_name,',') within group (order by item_name) as keys, listagg(num_clicks,',') 
within group (order by item_name) as values from
 user_features.v_item_clicks_and_purchases_raw where ordeR_id='00000d79-3cc8-406b-bc04-526e77707c2c'
 group by 1,2)
 join(
select listagg(item_name,',') within group (order by item_name) as colnames from 
(select distinct item_name from user_features.v_item_clicks_and_purchases_raw ))
on (1=1)

