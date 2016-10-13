select first_name,last_name ,coalesce(account_email,in_store_receipt_email) as email, bowl_id
from
	(select user_id
	,last_purchase_date
	from

		(select user_id
		,min(activity_date_pst) as first_purchase_date
		,max(activity_date_pst) as last_purchase_date 
		,count(*) as total_purchases  
		from sandbox.t_daily_activity_stats
		where  daypart='lunch' 
		and not  activity_type_daypart like 'churned%'
        and activity_date_pst<'2016-08-01'
		group by 1) a
	where 
	datediff("day",first_purchase_date,'2016-08-01')<=180 
	and datediff("day",last_purchase_date,'2016-08-01')>=28
	and total_purchases<=2
	) target
join kwwhub_public.users users 
on (target.user_id=users.id)
join
(
select user_id
,max(item_id) as bowl_id 
from (
select id,user_id, rank() over( partition by user_id order by created_at desc) as rank  from kwwhub_public.orders where daypart is null or daypart=1) a
join
(select item_id,order_id,name from kwwhub_public.line_items  
join
kwwhub_public.items
on (items.id=line_items.item_id and item_type='entree' and latest_version=true) ) b
on (id=order_id) 
where rank=1 
group by 1
) bowl
on (target.user_id=bowl.user_id)
where
users.in_store_receipt_email not like '%keenwawa%'
and users.in_store_receipt_email not like '%eatsa%'
and target.user_id not in (select id from kwwhub_public.v_test_users)