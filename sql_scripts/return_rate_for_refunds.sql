select 
to_char(refund_date,'YYYY-MM') as month
,count(distinct refund_order) as total_refunds
, count(last_order_date) as total_returns
, sum(case when total_orders>=3 then 1 else 0 end) as engaged_users
, avg(case when total_orders>=3 then datediff('day',refund_date,last_order_date)/total_orders else null end ) order_interval
 from (

select refunds.user_id,users.first_name,users.last_name,in_store_receipt_email, refund_order,refund_date,max(d.created_at) as last_order_date,count(distinct d.id) as total_orders from
(select r.order_id refund_order,ro.user_id user_id,ro.created_at as refund_date
from kwwhub_public.refunds r
join
kwwhub_public.orders ro
on (r.order_id=ro.id)) refunds
left join
 kwwhub_public.orders d
 on (refunds.user_id=d.user_id and refunds.refund_date<d.created_at)
join
kwwhub_public.users
on(refunds.user_id=users.id)
where refunds.user_id not in (select id from kwwhub_public.v_test_users)
group by 1,2,3,4,5,6) alls


group by 1


