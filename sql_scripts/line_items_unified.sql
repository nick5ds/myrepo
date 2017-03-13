drop table if exists reporting.t_line_items;
create table reporting.t_line_items
as
SELECT period_start_date ,
       period_end_date ,
       address ,
       region ,
       coast ,
       ord.order_id ,
       ord.user_id ,
       user_account ,
       first_store ,
       prev_visit_date ,
       first_visit_date ,
       last_visit_date ,
       next_visit_date ,
       avi_6v ,
       visit_number ,
       order_created_at_local ,
       order_status ,
       order_channel ,
       order_type ,
       order_daypart ,
       order_is_employee_or_test ,
       entree_count AS order_entree_count ,
       bags_count AS order_bags_count ,
       side_count AS order_side_count ,
       drink_count AS order_drink_count ,
       total_revenue AS order_total_revenue ,
       line_items.created_at as line_item_created_at,
       line_items.id AS line_item_id ,
       items.name AS line_item_name ,
       items.item_type AS line_item_type ,
       item_subtype as line_item_subtype,
       line_items.personalized_base_item_price AS line_item_personalized_base_price ,
       line_items.seed_bowl_id AS line_item_seed_bowl_id ,
       line_items.personalized_item_name as line_item_personalized_name,
       line_items.situational_filters as line_item_situational_filters,
       line_items.reorder_id ,
       line_items.refund_id ,
       ir.rating ,
       coalesce(line_items.gross_total,line_item_total) AS line_item_gross_total ,
       rank() over (partition BY user_account, items.id
                    ORDER BY order_created_at_local::date) AS line_item_repeat_order_number
FROM reporting.t_orders ord
LEFT JOIN kwwhub_public.line_items line_items ON (ord.order_id=line_items.order_id)
LEFT JOIN kwwhub_public.item_ratings ir ON (ir.line_item_id=line_items.id)
LEFT JOIN
(select distinct id,name,item_type,effective_date,expiration_date,item_subtype from kwwhub_public.items  where items.version_id::text <> ''::text) items
 ON (line_items.item_id::text = items.id::text
                                        AND line_items.created_at >= items.effective_date
                                        AND line_items.created_at <= items.expiration_date
                                       )
