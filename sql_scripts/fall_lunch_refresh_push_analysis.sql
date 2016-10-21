select push_group,count(distinct push_user) as no_push_sent,

count( distinct case when open_date is not null then user_id else null end

) as same_day_app_opens
, count(distinct user_id) as total_orders,
count( distinct case when 
"Hummus & Falafel Bowl Count" >0 then user_id
when "aloha bowl Count" >0 then user_id
when "Tres Chiles Bowl Count" >0 then user_id
when  "Berry Chia Parfait count" > 0then user_id
when "Falafel & Harissa" >0 then user_id 
when  "Hummus & Pita Chips" >0 then user_id 
else null end ) "users that order a new item"

,sum("Hummus & Falafel Bowl Count") as "Hummus & Falafel Bowl Count",
sum( "aloha bowl Count") as "aloha bowl Count",
sum( "Tres Chiles Bowl Count") as "Tres Chiles Bowl Count",
sum( "Berry Chia Parfait count") as "Berry Chia Parfait count",
sum("Falafel & Harissa") as "Falafel & Harissa",
sum( "Hummus & Pita Chips")as "Hummus & Pita Chips"
from
(select p.user_id push_user ,p.group as push_group,'2016-10-13' as push_sent_date,o.*,min(ts) as open_date
from csv.lunch_refresh_push_fall_2016 p

left join(
select  user_id,max(timestamp) as ts  from   
segment.tracks 
where context_app_name='eatsa' and timestamp::date='2016-10-13'
group by 1
union all
select  user_id,max(timestamp) as ts from   
segment_android.tracks 
where context_app_name='eatsa'and timestamp='2016-10-13'
group by 1
union all
select b.user_id,max(time) as ts from 
mixpanel._event a join mixpanel._session_started b on (a.event_id=b.event_id)
where b.client='eatsa iPhone' and time::date= '2016-10-13' group by 1
)m
on (p.user_id=m.user_id)

left join

(
SELECT 

  orders.user_id AS user_id,
    COUNT(DISTINCT CASE WHEN (items.name = 'Hummus & Falafel Bowl') THEN line_items.id ELSE NULL END) AS "Hummus & Falafel Bowl Count",
    COUNT(DISTINCT CASE WHEN (items.name = 'Aloha Bowl') THEN line_items.id ELSE NULL END) AS "aloha bowl Count",
COUNT(DISTINCT CASE WHEN (items.name = 'Tres Chiles') THEN line_items.id ELSE NULL END) AS "Tres Chiles Bowl Count",
COUNT(DISTINCT CASE WHEN (items.name = 'Berry Chia Parfait') THEN line_items.id ELSE NULL END) AS "Berry Chia Parfait count",
COUNT(DISTINCT CASE WHEN (items.name = 'Falafel & Harissat') THEN line_items.id ELSE NULL END) AS "Falafel & Harissa",
COUNT(DISTINCT CASE WHEN (items.name = 'Hummus & Pita Chips') THEN line_items.id ELSE NULL END) AS "Hummus & Pita Chips"
FROM kwwhub_public.orders AS orders
LEFT JOIN kwwhub_public.line_items AS line_items ON orders.id = line_items.order_id
LEFT JOIN kwwhub_public.items AS items ON line_items.item_id = items.id
AND items.version_id != ''
AND (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', orders.created_at), 'YYYY-MM-DD HH24:MI:SS')) >= (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', items.effective_date), 'YYYY-MM-DD HH24:MI:SS')) 
AND (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', orders.created_at), 'YYYY-MM-DD HH24:MI:SS')) < (TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', items.expiration_date), 'YYYY-MM-DD HH24:MI:SS'))
where ((orders.created_at >= CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TIMESTAMP '2016-10-13'))) 
group by 1

) o
on(p.user_id=o.user_id)

group by 1,2,3,4,5,6,7,8,9,10) b
group by 1

