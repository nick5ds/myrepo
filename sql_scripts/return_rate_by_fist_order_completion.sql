select
user_id,address,fulfilment_time,case when next_order is null then 0 else 1 end as retained ,dow,created_at,served_at,is_mobile,total

from
(select user_id,store_id,created_at
,datediff("second",created_at,served_at) fulfilment_time
,lead(created_at,1) over (partition by user_id order by created_at asc) next_order
, rank() over (partition by user_id order by created_at asc) rank
,coalesce(is_mobile,'false') is_mobile
,total,extract("dayofweek" from created_at) dow,served_at
from 
kwwhub_public.orders
where user_id not in (select id from kwwhub_public.v_test_users)
order by user_id,5
) fo
join
(select distinct id,address from kwwhub_public.stores) s
on(fo.store_id=s.id)
where rank=1
and served_at is not null
