
select * from reporting.t_pos_orders o 
join
(select user_id,order_id,sum(cost_per_portion) as cost from
(select user_id,order_id,item_name,line_item_id,cost_per_portion,name,min(coalesce(cost_per_portion,0)) over (partition by order_id) mincost from
(select user_id,order_id,coalesce(m.modifier_id,"id _items_") as item_id,"id _line_items_" as line_item_id,line_item_total,name 
from kwwhub_public.v_orders_items_unified u
left join kwwhub_public.modifications m
on ("id _line_items_"=m.line_item_id and m.reduced_portion=false )
) a
left join csv.ingredient_costs b
on ("a".item_id =b.item_id) )
where mincost >0 
group by 1,2 ) costs
on (o.order_id=costs.order_id) 
where day_part='lunch' and num_total_bowls=1 and is_test_order=false and is_internal_user=false and store_address in ('121 Spear St.','1 California St.') and promo_redemption_amount=0
and total_refunded=0 and is_comped=false
limit 100







