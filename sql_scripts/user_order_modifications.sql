drop view user_features.user_order_modifications; create view user_features.user_order_modifications as 



with mods as 
(select line_item_id,o.id as order_id,o.created_at as order_created_at
,i.id as item_id
,i.name as item_name
,o.user_id
,modifier_id
,m.name as modifier_name,'order'
,count(distinct mo.id) as num_mods
from kwwhub_public.modifications mo
join kwwhub_public.modifiers m on
(mo.modifier_id=m.id and mo.created_at between m.effective_date and m.expiration_date)
join kwwhub_public.line_items li on ( mo.line_item_id=li.id)
join kwwhub_public.items i on (li.item_id=i.id
and li.created_at between i.effective_date and i.expiration_date)
join kwwhub_public.orders o
on (li.order_id=o.id)
where o.status=500
group by 1,2,3,4,5,6,7,8,9


) 

select ua.user_account,is_test_user,is_employee_user,d.*,store_address,is_internal_user,is_test_order,store_locality,is_comped,is_refunded,promo_redemption_amount,num_items,num_total_bowls,num_chefs_bowl,num_sides,num_beverages from
(select
line_item_id,order_id,order_created_at,item_id,item_name,user_id,modifier_id,modifier_name,sum(num_mods) as num_mods from

(select * from mods

union all

select mods.line_item_id,mods.order_id,mods.order_created_at,mods.item_id,mods.item_name,mods.user_id,dm.modifier_id,m.name,'default',count( distinct dm.modifier_id)*-1
from mods
join kwwhub_public.default_modifiers dm
on (dm.item_id=mods.item_id and mods.order_Created_at between dm.effective_date and dm.expiration_date)
join kwwhub_public.modifiers m
on(dm.modifier_id=m.id and mods.order_Created_at between m.effective_date and m.expiration_date)
group by 1,2,3,4,5,6,7,8,9
)
group by 1,2,3,4,5,6,7,8) d

join reporting.t_pos_orders pos on (d.order_id=pos.order_id)
join
customer_identity.t_user_accounts ua on (d.user_id=ua.user_id)


