select 
split_part(date_given,'-',2) as month
,count(distinct promo_code) as total_codes
, count(distinct order_id) as return_visits
, sum(case when total_orders>=3 then 1 else 0 end) as engaged_users
, avg(case when total_orders>=3 then datediff('day',order_date,last_order_date)/total_orders else null end )
 from (

select c.*,max(d.created_at) last_order_date ,count(distinct d.id) as total_orders from
(select a.* , b.created_at as order_date from
(Select date_given,promo_code,redemption_code,order_id,user_id,kwwhub_public.promo_authorizations.created_at as promo_use_date

,case when position('+' in customer_name)>0
then split_part(customer_name,'+',1)
else split_part(customer_name,' ',1) end as fn



, case when position('+' in customer_name)>0
then split_part(customer_name,'+',2)
else split_part(customer_name,' ',2) end as ln
 from csv.cs_promo
join kwwhub_public.promos on (promo_code=redemption_code)
left join kwwhub_public.promo_authorizations
on (kwwhub_public.promo_authorizations.promo_id=kwwhub_public.promos.id)
where lower(customer_name) !='scott bruggman') a 
left join
 kwwhub_public.orders  b
on b.id=a.order_id) c
left join
 kwwhub_public.orders d
 on (c.user_id=d.user_id and c.order_date<d.created_at)
 group by 1,2,3,4,5,6,7,8,9) alls


group by 1

