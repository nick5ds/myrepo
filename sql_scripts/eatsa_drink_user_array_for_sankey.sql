
select count(*),drinks,visit_number as total_orders from 
(
select user_account, listagg(name,',') within group (order by od asc) drinks, max(od) as last_order_date from
(

select  ua.user_account AS user_account,orders.served_at::date od,items.name name,rank() over (partition by user_account order by orders.served_at::date) as "rank"
FROM kwwhub_public.orders AS orders
LEFT JOIN kwwhub_public.line_items AS line_items ON orders.id = line_items.order_id
LEFT JOIN kwwhub_public.items AS items ON line_items.item_id = items.id
AND items.version_id != ''
AND (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', orders.created_at), 'YYYY-MM-DD HH24:MI:SS')) >= (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', items.effective_date), 'YYYY-MM-DD HH24:MI:SS')) 
AND (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', orders.created_at), 'YYYY-MM-DD HH24:MI:SS')) < (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', items.expiration_date), 'YYYY-MM-DD HH24:MI:SS'))
left join
(select distinct a.user_account,user_id,min(first_visit_date) as first_visit_date from reporting.t_user_accounts a join reporting.user_activity_analysis b on (a.user_account=b.user_account) group by 1,2) ua
on (orders.user_id=ua.user_id)
where
 items.item_type='drink' and items.name like 'eatsa%'
 and datediff("day",orders.created_at,first_visit_date)<=60

)
where "rank"<=10
group by 1
) ol
join
reporting.user_activity_analysis d
on(ol.user_account=d.user_account and ol.last_order_date=d.activity_date)

group by 2,3

