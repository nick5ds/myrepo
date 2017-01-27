select user_account,order_id,created_at,pivot_columns (keys,value,colnames),colnames from

(select user_account,order_created_at::date created_at,order_id
,listagg(modifier_name,',') within group (order by modifier_name asc) as keys
,listagg(mod_count) within group (order by modifier_name asc) as value

from
(select user_account,order_created_at,order_id,modifier_name,count(distinct modifier_id) mod_count from
(select user_account,order_created_at,line_item_id,order_id,modifier_name,modifier_id,min(has_pas) over (partition by order_id) as filter from

 user_features.tmp_mods_with_pas created

where is_test_user=false and is_employee_user=false and num_total_bowls=1
--group by 1,2,3,4,5


)

where filter=1
group by 1,2,3,4)
group by 1,2,3
)


join 
(select listagg( name,',' ) within group (order by name)as colnames from
(select distinct name from kwwhub_public.modifiers) ) 
on (1=1)

