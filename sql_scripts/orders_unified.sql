DROP TABLE IF EXISTS reporting.t_orders;


CREATE TABLE reporting.t_orders AS
SELECT o.*,
       pos.total_bowls_revenue,
       pos.total_sides_revenue,
       pos.total_beverages_revenue,
       pos.total_revenue
FROM
  (SELECT DATE (periods.start_date) AS "period_start_date",
          DATE (periods.end_date) AS "period_end_date",
          LEFT(CAST(CASE WHEN orders.is_mobile = 1
                    AND orders.scheduled_time IS NOT NULL THEN CONVERT_TIMEZONE ('UTC',stores.timezone,COALESCE(orders.scheduled_to_fire_at, orders.queued_at)) ELSE CONVERT_TIMEZONE ('UTC',stores.timezone,orders.queued_at) END AS CHAR(19)), 10) AS "order_date_string",
          stores.store_number,
          stores.address,
          stores.region,
          xxx.coast,
          orders.id AS "order_id",
          orders.human_readable_id,
          orders.user_id,
          users.first_name AS user_first_name,
          t_user_accounts.user_account,
          uas.first_store,
          uas.prev_visit_date,
          uas.first_visit_date,
          uas.last_visit_date,
          uas.next_visit_date,
          uas.avi_6v,
          visit_number,
          CASE
              WHEN (visit_number)>=3
                   AND datediff('day',prev_visit_date,activity_date) BETWEEN 0 AND 28 THEN '28 day active'
              WHEN (visit_number)>=3
                   AND datediff('day',prev_visit_date,activity_date) BETWEEN 0 AND 84 THEN '84 day active'
              WHEN (visit_number)<3 THEN 'new or onboarding'
              ELSE 'not active'
          END AS activity_type,
          DATEDIFF('days',users.created_at,orders.served_at) AS days_since_account_creation,
          CASE
              WHEN COALESCE(orders.daypart,1) = 0 THEN 'Breakfast'
              WHEN COALESCE(orders.daypart,1) = 1 THEN 'Lunch'
              ELSE 'UNKNOWN'
          END AS "order_daypart",
          CASE
              WHEN orders.is_mobile IS NULL
                   OR (orders.is_mobile IS FALSE
                       AND orders.scheduled_time IS NULL) THEN 'kiosk'
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NULL THEN 'mobile - now'
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NOT NULL THEN 'mobile - schedule ahead'
          END AS "order_type",
          CASE
              WHEN orders.is_mobile IS NULL
                   OR (orders.is_mobile IS FALSE
                       AND orders.scheduled_time IS NULL) THEN 'kiosk'
              WHEN orders.is_mobile IS TRUE
                   AND orders.order_channel = 3 THEN 'mobile - web'
              WHEN orders.is_mobile IS TRUE
                   AND (orders.order_channel = 2
                        OR orders.order_channel IS NULL) THEN 'mobile - ios'
              WHEN orders.is_mobile IS TRUE
                   AND orders.order_channel = 1 THEN 'mobile - android'
              ELSE 'UNKNOWN'
          END AS "order_channel",
          CASE
              WHEN orders.status = 0 THEN 'In Queue'
              WHEN orders.status = 100 THEN 'Scheduled'
              WHEN orders.status = 300 THEN 'On the Line'
              WHEN orders.status = 400 THEN 'Ready for Pickup'
              WHEN orders.status = 500 THEN 'Delivered to Customer'
              WHEN orders.status = 700 THEN 'Customer Cancelled'
              WHEN orders.status = 800 THEN 'Attendant Cancelled'
              WHEN orders.status = 900 THEN 'Hold for Recubby'
              WHEN orders.status = 1000 THEN 'Ready to Recubby'
          END AS "order_status",
          CONVERT_TIMEZONE('UTC',stores.timezone,orders.created_at) AS "order_created_at_local",
          CASE
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NOT NULL THEN CONVERT_TIMEZONE ('UTC',stores.timezone,COALESCE(orders.scheduled_to_fire_at, orders.queued_at))
              ELSE CONVERT_TIMEZONE ('UTC',stores.timezone,orders.queued_at)
          END AS "order_queued_at_local",
          CASE
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NOT NULL THEN DATEDIFF ('second',COALESCE(orders.scheduled_to_fire_at, orders.queued_at),orders.started_at)
              ELSE DATEDIFF ('second',orders.queued_at,orders.started_at)
          END AS "queue_time",
          CONVERT_TIMEZONE('UTC',stores.timezone,orders.started_at) AS "order_started_at_local",
          DATEDIFF('second',orders.started_at,orders.served_at) AS "prep_time",
          CONVERT_TIMEZONE('UTC',stores.timezone,orders.served_at) AS "order_served_at_local",
          CONVERT_TIMEZONE('UTC',stores.timezone,orders.scheduled_time) AS "order_scheduled_ahead_expected_local",
          CONVERT_TIMEZONE('UTC',stores.timezone,orders.delivered_at) AS "order_delivered_at_local",
          CASE
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NOT NULL THEN DATEDIFF ('second',COALESCE(orders.scheduled_to_fire_at, orders.queued_at), orders.served_at)
              ELSE DATEDIFF ('second',orders.queued_at,orders.served_at)
          END AS "fulfillment_time",
          CASE
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NOT NULL THEN DATEDIFF ('second',orders.scheduled_time,orders.served_at)
              ELSE DATEDIFF ('second',DATEADD ('second',orders.initial_eta,orders.created_at),orders.served_at)
          END AS "eta_error",
          CASE
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NOT NULL THEN NULL
              ELSE orders.initial_eta
          END AS "initial_eta",
          CASE
              WHEN orders.is_mobile = 1
                   AND orders.scheduled_time IS NOT NULL THEN ABS(DATEDIFF ('second',orders.scheduled_time,orders.served_at))
              ELSE ABS(DATEDIFF ('second',DATEADD ('second',orders.initial_eta,orders.created_at),orders.served_at))
          END AS "absolute_eta_error",
          CASE
              WHEN order_modifications.order_id IS NULL THEN 0
              ELSE 1
          END AS "order_has_truffled_eggs",
          orders.expo_station_id AS "expo_station_id",
          CASE
              WHEN override_store_hours.id IS NULL
                   AND DATE_PART('hour',(convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at))))*60 + DATE_PART('minute',(convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at)))) BETWEEN CONVERT(INT,LEFT (store_hours.open_time,2))*60 + CONVERT(INT,SUBSTRING(store_hours.open_time,4,2)) AND CONVERT(INT,LEFT (store_hours.close_time,2))*60 + CONVERT(INT,SUBSTRING(store_hours.close_time,4,2)) THEN 1
              ELSE 0
          END AS "during_normal_store_hours",
          CASE
              WHEN override_store_hours.closed IS TRUE THEN 0
              WHEN override_store_hours.closed IS FALSE
                   AND DATE_PART('hour',(convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at))))*60 + DATE_PART('minute',(convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at)))) BETWEEN CONVERT(INT,LEFT (override_store_hours.alt_open_time,2))*60 + CONVERT(INT,SUBSTRING(override_store_hours.alt_open_time,4,2)) AND CONVERT(INT,LEFT (override_store_hours.alt_close_time,2))*60 + CONVERT(INT,SUBSTRING(override_store_hours.alt_close_time,4,2)) THEN 1
              ELSE 0
          END AS "during_override_store_hours",
          CASE
              WHEN convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at)) >= xxx.soft_launch_date
                   AND convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at)) < xxx.grand_open_date THEN 1
              ELSE 0
          END AS "during_soft_launch",
          CASE
              WHEN convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at)) >= xxx.grand_open_date THEN 1
              ELSE 0
          END AS "after_grand_opening",
          CASE
              WHEN t_user_accounts.is_test_user=TRUE THEN 1
              WHEN t_user_accounts.is_employee_user=TRUE THEN 1
              ELSE 0
          END AS "order_is_employee_or_test",
          CASE
              WHEN orders.held_at IS NOT NULL
                   OR orders.recubby_holding_area IS NOT NULL THEN 1
              ELSE 0
          END "order_was_held",
          CASE
              WHEN ABS(DATEDIFF ('second',orders.served_at,orders.last_cubbied_at)) > 5 THEN 1
              ELSE 0
          END "order_was_recubbied",
          CASE
              WHEN orders.status = 500
                   AND orders.last_cubbied_at IS NULL THEN 1
              ELSE 0
          END "order_was_not_assigned_cubby",
          NVL(order_modifications.truffled_eggs,0) AS "num_truffled_eggs",
          COUNT(DISTINCT CASE WHEN (items.item_type = 'entree') THEN line_items.id ELSE NULL END) AS "entree_count",
          COUNT(DISTINCT CASE WHEN (items.item_type = 'bags') THEN line_items.id ELSE NULL END) AS "bags_count",
          COUNT(DISTINCT CASE WHEN (items.item_type = 'side') THEN line_items.id ELSE NULL END) AS "side_count",
          COUNT(DISTINCT CASE WHEN (items.item_type = 'drink') THEN line_items.id ELSE NULL END) AS "drink_count",
          MAX(CASE WHEN line_items.reorder_id IS NOT NULL THEN 1 ELSE 0 END) "order_had_reorder",
          COUNT(DISTINCT CASE WHEN (items.item_type = 'entree'
                                    AND line_items.reorder_id IS NOT NULL) THEN line_items.id ELSE NULL END) AS "reorder_entree_count",
          COUNT(DISTINCT CASE WHEN (items.item_type = 'side'
                                    AND line_items.reorder_id IS NOT NULL) THEN line_items.id ELSE NULL END) AS "reorder_side_count",
          COUNT(DISTINCT CASE WHEN (items.item_type = 'drink'
                                    AND line_items.reorder_id IS NOT NULL) THEN line_items.id ELSE NULL END) AS "reorder_drink_count" ,
          sum(line_items.personalized_base_item_price) AS line_item_persosnalized_total ,
          avg(item_ratings.rating) AS avg_line_item_rating 

   -- Main sequence of joins 

   FROM kwwhub_public.orders orders
   LEFT JOIN kwwhub_public.users AS users ON (orders.user_id = users.id)
   LEFT JOIN kwwhub_public.stores stores ON(orders.store_id::text = stores.id::text
                                            AND orders.created_at >= stores.effective_date
                                            AND orders.created_at <= stores.expiration_date
                                            AND stores.version_id::text <> ''::text
                                            AND orders.created_at < stores.expiration_date)
   LEFT JOIN csv.store_supplement xxx ON xxx.store_id = orders.store_id
   LEFT JOIN kwwhub_public.store_hours ON (orders.store_id = store_hours.store_id
                                           AND DATE_PART (dow,convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at))) = (store_hours.day_of_week%7))
   LEFT JOIN kwwhub_public.override_store_hours ON (orders.store_id = override_store_hours.store_id
                                                    AND TRUNC (convert_timezone ('UTC',stores.timezone,NVL (orders.scheduled_to_fire_at,orders.queued_at))) BETWEEN override_store_hours.override_start_date AND override_store_hours.override_end_date)
   LEFT JOIN kwwhub_public.line_items line_items ON orders.id::text = line_items.order_id::text
   LEFT JOIN kwwhub_public.items items ON line_items.item_id::text = items.id::text
   AND line_items.created_at >= items.effective_date
   AND line_items.created_at <= items.expiration_date
   AND items.version_id::text <> ''::text
   LEFT JOIN kwwhub_public.item_ratings ON line_items.id = item_ratings.line_item_id
   LEFT JOIN customer_identity.t_user_accounts t_user_accounts ON orders.user_id::text = t_user_accounts.user_id::text
   JOIN "csv".periods periods ON orders.created_at >= periods.start_date
   AND orders.created_at <= periods.end_date
   --This says whether or not an order is a reorder or has a truffled egg
   LEFT JOIN 

     (SELECT orders.id AS order_id,
             count(DISTINCT CASE WHEN modifiers.name::text = 'Truffled Egg'::text THEN line_items.id ELSE NULL END) AS truffled_eggs,
             COALESCE("max"(reorder.is_reorder), 0) AS is_reorder
      FROM kwwhub_public.orders orders
      LEFT JOIN kwwhub_public.line_items line_items ON orders.id::text = line_items.order_id::text
      LEFT JOIN kwwhub_public.modifications modifications ON line_items.id::text = modifications.line_item_id::text
      LEFT JOIN kwwhub_public.modifiers modifiers ON modifications.modifier_id::text = modifiers.id::text
      AND modifiers.version_id::text <> ''::text
      AND to_char(convert_timezone('UTC'::text, 'America/Los_Angeles'::text, orders.created_at), 'YYYY-MM-DD HH24:MI:SS'::text) >= to_char(convert_timezone('UTC'::text, 'America/Los_Angeles'::text, modifiers.effective_date), 'YYYY-MM-DD HH24:MI:SS'::text)
      AND to_char(convert_timezone('UTC'::text, 'America/Los_Angeles'::text, orders.created_at), 'YYYY-MM-DD HH24:MI:SS'::text) < to_char(convert_timezone('UTC'::text, 'America/Los_Angeles'::text, modifiers.expiration_date), 'YYYY-MM-DD HH24:MI:SS'::text)
      LEFT JOIN
        (SELECT DISTINCT line_items.reorder_id,
                         1 AS is_reorder
         FROM kwwhub_public.line_items) reorder ON reorder.reorder_id::text = orders.id::text
      GROUP BY orders.id) "order_modifications" ON orders.id::text = "order_modifications".order_id::text
       -- ends join

   LEFT JOIN 
 customer_identity.t_user_activity_stats uas ON (uas.user_account=t_user_accounts.user_Account
                                                 AND activity_type!='inactive'
                                                 AND CONVERT_TIMEZONE('UTC',stores.timezone,orders.started_at)::date::date=uas.activity_date::date) 
                                       

   WHERE orders.started_at IS NOT NULL
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15 ,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
            33,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            46,
            47,
            48) o
LEFT JOIN reporting.t_pos_orders pos ON (pos.order_id=o.order_id) ;

