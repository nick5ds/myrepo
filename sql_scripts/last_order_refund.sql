select lo.*, case when refunds.user_id is not null then 'refunded' else 'not refunded' end as did_refund, case when reorder.user_id is not null then 'reordered' else 'not reordered' end as did_reorder
,case when ever_refunds.user_id is not null then 'has recieved a refund' else 'never recieved refund' end as has_refund
,case when ever_reorder.user_id is not null then 'has reordered' else 'never reordered' end as has_reorder,case when last_order_date::date=created_at::date then 'churned' else 'engaged' end as did_churn
,coalesce(ci.churn_days,84) as churn_days
from
(select
user_id,address,fulfilment_time,dow,created_at,served_at,is_mobile,total,running_average,ranked
,order_id,last_order_date
from
(select user_id,store_id,created_at,id as order_id
,datediff("second",created_at,served_at) fulfilment_time
,max(created_at) over (partition by user_id ) as  last_order_date
,avg(datediff("second",created_at,served_at)) over (partition by user_id order by created_at asc rows between unbounded preceding and 1 preceding )  as running_average
,rank() over (partition by user_id order by created_at asc) ranked
,coalesce(is_mobile,'false') is_mobile
,total,extract("dayofweek" from created_at) dow,served_at
from 
kwwhub_public.orders
where user_id not in (select id from kwwhub_public.v_test_users)
and user_id not in (select id  from kwwhub_public.users where coalesce(account_email,in_store_receipt_email)  like '%eatsa.com'and  coalesce(account_email,in_store_receipt_email)  like '%keenwawa.com')
order by user_id,5
) fo
join
(select distinct id,address from kwwhub_public.stores) s
on(fo.store_id=s.id)

) lo
left join

(select user_id, created_at::date refund_date from kwwhub_public.orders where is_refunded=true group by 1,2) refunds
on (lo.created_at::date=refunds.refund_date and refunds.user_id=lo.user_id)

left join
(select user_id
,orders.created_at::date as reorder_date 
from kwwhub_public.orders join kwwhub_public.line_items 
on (orders.id=line_items.order_id and line_items.reorder_id is not null) group by 1,2 ) reorder
on (lo.user_id=reorder.user_id and lo.created_at::date=reorder.reorder_date)

left join

(select user_id from kwwhub_public.orders where is_refunded=true group by 1) ever_refunds
on (ever_refunds.user_id=lo.user_id)

left join
(select  user_id
 
from kwwhub_public.orders join kwwhub_public.line_items 
on (orders.id=line_items.order_id and line_items.reorder_id is not null) group by 1 ) ever_reorder
on (lo.user_id=ever_reorder.user_id)

left join
(select ac.user_id,max(avg_visit_interval)*4 as churn_days,activity_date::date as activity_date from
 reporting.user_activity_analysis an join reporting.t_user_accounts ac on (an.user_account=ac.user_account)
 where visit_number>3
group by 1,3) ci
on (lo.user_id=ci.user_id and created_at::date=activity_date) 

where created_at::date=refunds.refund_date or created_at::date=reorder.reorder_date