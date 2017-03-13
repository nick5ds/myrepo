DROP TABLE IF EXISTS reporting.t_elements;


CREATE TABLE reporting.t_elements AS
SELECT line_items.* ,
       modifications.id AS modification_id ,
       modifications.modification_total AS modification_total ,
       modifiers.price AS element_price ,
       modifiers.name AS element_name ,
       modifiers.id AS element_id ,
       modifier_groups.name AS element_group_name ,
       modifier_groups.short_name AS element_group_short_name ,
       modifier_groups.modifier_group_type_label AS element_group_type_label ,
       rank() over (partition BY user_Account,modifiers.id
                    ORDER BY order_created_at::date) AS element_repeat_order_number
FROM reporting.t_line_items line_items
JOIN kwwhub_public.modifications modifications ON line_items.line_item_id::text = modifications.line_item_id::text
LEFT JOIN
  (SELECT DISTINCT id ,
                   name,
                   modifier_group_id,
                   price,
                   effective_date,
                   expiration_date
   FROM kwwhub_public.modifiers
   WHERE modifiers.version_id::text <> ''::text) modifiers 
 ON(modifications.modifier_id::text = modifiers.id::text
    AND order_created_at>= modifiers.effective_date
    AND order_created_at<=modifiers.expiration_date)
LEFT JOIN kwwhub_public.modifier_groups ON (modifier_groups.id=modifiers.modifier_group_id)