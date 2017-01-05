create or replace function common_elements(array1 varchar(2000),array2 varchar(2000))
    returns varchar(60)
stable
as $$
    num_same=0
    array1=array1.split(',')
    array2=array2.split(',')
    for item in array2:
        if item in array1:
            num_same+=1
    final= str(len(array2))+','+str(num_same)
    return final
$$ language plpythonu;



create view user_features.item_modifier_purchase_similarity_and_frequency as (
with all_variants as(

select user_account,item_name,variant,num_variant_ordered
,sum(num_variant_ordered) over (partition by user_account,item_name) as num_item_ordered
,sum(num_variant_ordered) over (partition by user_account) as total_items_ordered
,rank() over (partition by user_account order by num_variant_ordered desc) overall_rank
,rank() over (partition by user_account,item_name order by num_variant_ordered desc) rank_within_item
from
(select user_account,item_name,variant,count(distinct order_id) as num_variant_ordered from
(select user_account,item_name,order_id,listagg(modifier_name,',') within group (order by modifier_name) variant
from user_features.user_order_modifications where
is_test_user=false and  is_employee_user=false and num_total_bowls=1 
--and not  user_account in('bcc836c5-4df1-4513-b659-f44f3124f3b4','02680098-97cb-4947-9d3f-87c60801a16c','cb88b9db-731d-441b-b905-ebb8ca07ae1d','6374aa1c-49a1-4730-9dc4-14dc5d7cc2c3','na@gmail.com')
group by 1,2,3) a
 group by 1,2,3)


) 



select a.*,b.top_variant_for_item--,c.top_variant
,split_part(common_elements(variant,top_variant_for_item),',',1) as num_elements, 
split_part(common_elements(variant,top_variant_for_item),',',2) num_common_elements_with_top_variant_for_item
,split_part(common_elements(variant,top_variant),',',2)  num_common_elements_with_top_variant
from 
all_variants a
join
( select * from (select distinct user_account,item_name,variant as top_variant_for_item,rank() over(partition by user_account,item_name order by RANDOM()) rownum
 from all_variants where rank_within_item=1) where rownum=1   ) b
on (a.user_account=b.user_account and a.item_name=b.item_name)
join 
( select * from (select distinct user_account,variant as top_variant,rank() over(partition by user_account order by RANDOM()) rownum
 from all_variants where overall_rank=1) where rownum=1   ) c
on (a.user_account=c.user_account )

)





