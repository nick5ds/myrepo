 -- base query
        with ua_stats as(
    select user_account, activity_date, 'all' as daypart, max_order_id,
             row_number() over (partition by user_account order by activity_date asc) as visit_number,
               datediff('day', lag(activity_date, 1) over (partition by user_account order by activity_date asc), activity_date) as num_days_from_prev_visit,
               datediff('day', activity_date, lead(activity_date, 1) over (partition by user_account order by activity_date asc)) as num_days_to_next_visit,
               lag(activity_date, 1) over (partition by user_account order by activity_date asc) as prev_visit_date,
               lead(activity_date, 1) over (partition by user_account order by activity_date asc) as next_visit_date,
               first_value(activity_date) over(partition by user_account order by activity_date asc rows between unbounded preceding and unbounded following) as first_visit_date,
               last_value(activity_date) over(partition by user_account order by activity_date asc rows between unbounded preceding and unbounded following) as last_visit_date

                from (
          		select user_account,

                 DATE(CONVERT_TIMEZONE('UTC', stores.timezone, orders.created_at)) as activity_date,
                     CASE  WHEN orders.daypart IS NULL OR orders.daypart = 1 THEN 'lunch'
                            WHEN orders.daypart = 0 then 'breakfast'
                    ELSE 'unknown'
                     END AS daypart,
                     MAX(orders.id) AS max_order_id

          from
                  kwwhub_public.orders
              JOIN customer_identity.t_user_accounts u on orders.user_id = u.user_id
              JOIN kwwhub_public.stores on orders.store_id = stores.id
              AND orders.created_at >= stores.effective_date and orders.created_at < stores.expiration_date
              AND stores.version_id != ''

        --  and u.user_account = ' larena24@yahoo.com'

          group by 1,2,3)
         )
select t.user_account,t.activity_date,t.daypart,t.max_order_id,t.visit_number,t.num_days_from_prev_visit,t.num_days_to_next_visit,t.prev_visit_date,
               t.next_visit_date,
               t.first_visit_date,
               last_visit_date,



 p.user_id_list, p.is_test_user, p.is_employee_user, pos.store_address as order_store,
          first_value(pos.store_address) over(partition by t.user_account order by t.activity_date asc rows between unbounded preceding and unbounded following) as first_store,
          last_value(pos.store_address) over(partition by t.user_account order by t.activity_date asc rows between unbounded preceding and unbounded following) as last_store,
      q.activation_date_3rd_visit,
      periods.start_date as period_start_date,
      periods.end_date as period_end_date,
      periods.name as period,
      pos.total_revenue,
      pos.total_charged_to_card,
      pos.day_part as order_daypart,
      pos.num_total_bowls,
      pos.num_chefs_bowl,
      pos.num_build_a_bowl,
      pos.num_sides,
      pos.num_beverages,
      pos.num_bags,
      pos.is_comped,
      pos.is_refunded,
      pos.promo_code,
      pos.promo_category,
      pos.promo_redemption_amount,
      pos.comped_amount
      , 0 as lifetime_avg_visit_interval,
      0 as windowed_avg_visit_interval,
      'unknown' as activity_type,
      'unknown'  as prev_activity_type,
      'unknown'  as next_activity_type,
      'unknown'  as last_activity_type,
       0 as activity_window,
       0 as last_activity_window,
       0 as avg_window_visit,
       0 as churn_interval
   from
   ua_stats t join
   (select user_account,
          listagg(user_id,',') user_id_list,
          bool_or(is_test_user) as is_test_user,
          bool_or(is_employee_user) as is_employee_user
    from customer_identity.t_user_accounts
   group by 1) p
   on t.user_account = p.user_account

left join (select distinct user_account,activity_date as activation_date_3rd_visit  from ua_stats t where visit_number=3) q
  on q.user_account=t.user_Account

   left join reporting.t_pos_orders pos on pos.order_id = t.max_order_id
   LEFT JOIN csv.periods AS periods ON date(t.activity_date) between (DATE(periods.start_date)) and (DATE(periods.end_date))
order by activity_date


--adds churn events
insert into csv.activity_metrics_final
select user_account, dateadd(day, floor(churn_interval)::int, activity_date) as activity_date, daypart, max_order_id, 
visit_number, 
floor(churn_interval::int) num_days_from_prev_visit, 
datediff('day', dateadd(day, floor(churn_interval)::int, activity_date), next_visit_date) num_days_to_next_visit, 
activity_date as prev_visit_date, 
next_visit_date as next_visit_date, 
first_visit_date, 
last_visit_date, 
user_id_list,
is_test_user,
is_employee_user,
order_store,
first_store,
last_store,
activation_date_3_rd_visit,
period_start_date,
period_end_date,
period,
0 as total_revenue,
0 as total_charged_to_card,
null as order_daypart,
0 as num_total_bowls,
0 as num_chefs_bowl,
0 as num_build_a_bowl,
0 as num_sides,
0 as num_Beverages,
0 as num_bags,
'False' as is_comped,
'False' as is_refunded,
null as promo_code,
null as promo_category,
null as promo_redemption_amount,
null as comped_amount,
churn_interval,
avg_window_visit, 
windowed_avg_visit_interval,
lifetime_avg_visit_interval, 
activity_window, 
 'inactive' as activity_type,
activity_type as prev_activity_type, 
next_activity_type, 
last_activity_type, 
last_activity_window

from csv.activity_metrics_final t
where 1=1 
-- intermediate churns
and 
(next_activity_type='reactivated' OR
-- user has churned since the last visit
(t.next_visit_date is null and dateadd(day, floor(churn_interval)::int, activity_date) <= date(convert_timezone('UTC', 'US/Pacific', getdate()))))



--updates for churn events
update csv.activity_metrics_final 
set prev_activity_type = p.prev_activity_type,
   next_activity_type = p.next_activity_type,
   last_activity_type = p.last_activity_type
from (
select t.user_account, t.activity_date, t.daypart, 
              t.activity_type,
              lag(activity_type, 1) over (partition by t.user_account order by activity_date asc) as prev_activity_type,
              lead(activity_type, 1) over (partition by t.user_account order by activity_date asc) as next_activity_type,
              last_value(activity_type) over(partition by t.user_account order by activity_date asc rows between unbounded preceding and unbounded following) as last_activity_type
from  csv.activity_metrics_final t
-- where t.user_account = 'bkapoor@gmail.com'
) p
where csv.activity_metrics_final.user_account=p.user_account
and csv.activity_metrics_final.activity_date = p.activity_date
and csv.activity_metrics_final.daypart=p.daypart
and csv.activity_metrics_final.activity_type=p.activity_type
