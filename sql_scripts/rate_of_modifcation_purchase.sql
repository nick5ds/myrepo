--drop view user_features.rate_of_modification_purchase;
create view user_features.rate_of_modification_purchase as

(select a.*,b.name as modifier_name,b.item_type, b.reduced_portion,b.total_orders_with_modifier from
(select user_account,user_id,is_employee_user,is_test_user,CASE
WHEN COALESCE(daypart, 1) = 0 THEN 'Breakfast' 
WHEN COALESCE(daypart, 1) = 1 THEN 'Lunch' 
ELSE 'UNKNOWN' 
END AS daypart
,count(distinct order_id) as total_orders
from kwwhub_public.v_orders_items_unified
where status=500
group by 1,2,3,4,5) a
join
(select user_account,m.name,item_type,CASE
WHEN COALESCE(daypart, 1) = 0 THEN 'Breakfast' 
WHEN COALESCE(daypart, 1) = 1 THEN 'Lunch' 
ELSE 'UNKNOWN' 
END AS daypart 
,reduced_portion,count(distinct order_id) as total_orders_with_modifier
from kwwhub_public.v_orders_items_unified u
join kwwhub_public.modifications mods on( u."id _line_items_"=mods.line_item_id)
join kwwhub_public.modifiers m on (mods.modifier_id=m.id and mods.created_at between m.effective_date and m.expiration_date)
where status=500
group by 1,2,3,4,5,6) b
on (a.user_Account=b.user_account and a.daypart=b.daypart)
)

