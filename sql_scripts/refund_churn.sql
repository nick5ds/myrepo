select refunds.user_id,users.first_name,users.last_name
,in_store_receipt_email, refund_order,refund_date,time_to_refund
,max(d.created_at) as last_order_date,count(distinct d.id) as total_orders,sum(d.order_total) as total_spent_after_refund from
(select r.order_id refund_order,ro.user_id user_id,ro.created_at as refund_date,datediff("minute",ro.created_at,r.created_at) as time_to_refund,
from kwwhub_public.refunds r
join
kwwhub_public.orders ro
on (r.order_id=ro.id)) refunds
left join
 kwwhub_public.orders d
 on (refunds.user_id=d.user_id and refunds.refund_date<d.created_at )
join
kwwhub_public.users
on(refunds.user_id=users.id)
where refunds.user_id not in (select id from kwwhub_public.v_test_users)
group by 1,2,3,4,5,6,7