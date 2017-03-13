 
select user_id,'"pricing"'||':'||'{'||'"do_reset"'||':"'||do_reset||'",'||'"timestamps"'||':'||'['||'"'||order_ts||'"'||']}' from

(select coalesce(t_user_accounts.user_id,b.user_account) user_id, do_reset,order_ts
from
(select user_account,
case when last_order::date<'2016-02-20' then 'True' else 'False' end as do_reset,
listagg(created_at,'","') within group(order by created_at asc) order_ts
from

(select coalesce(user_account,orders.user_id) as user_account,orders.created_at,max(orders.created_at) over (partition by user_account)  last_order from

(select *, rank() over (partition by coalesce(user_account,user_id) order by created_at::date desc ) ranked
from
(select 
distinct
user_account,orders.user_id,created_at::date
from kwwhub_public.orders
left join customer_identity.t_user_accounts on (orders.user_id=t_user_accounts.user_id and status=500)

)
) a
join kwwhub_public.orders on (orders.user_id=a.user_id and a.created_at=orders.created_at::date and ranked<=6 and status=500)
)
group by 1,2 ) b
left join
customer_identity.t_user_accounts 
on (b.user_account=t_user_accounts.user_account)
)
