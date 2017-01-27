select *, '28 Day Active' as activty_definition from (
select 'daily' as date_interval,date,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
(select *, median(avi_6v) over (partition by date,first_store) as median_6v_avi 
from


(select rank() over (partition by user_account,date order by activity_date,random() desc) as order_rank,a.date,b.* as report_date from

 csv.date_scaffold a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 28)
 ))
)
where order_rank=1
) 
group by 1,2,3,4,5

union all
select 'weekly',date ,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
( select *,median(avi_6v) over (partition by date,first_store) as median_6v_avi from
(select rank() over (partition by user_account,date order by activity_date desc,random() ) as order_rank,a.date,b.* as report_date 
from
(select distinct date_trunc('week',date) as date from csv.date_scaffold ) a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where daypart='all' and is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 28)
))
)
where order_rank=1 )
group by 1,2,3,4,5


union all
select 'monthly',date ,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
( select *,median(avi_6v) over (partition by date,first_store) as median_6v_avi from
(select rank() over (partition by user_account,date order by activity_date desc,random() ) as order_rank,a.date,b.* as report_date 
from
(select distinct date_trunc('month',date) as date from csv.date_scaffold ) a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where daypart='all' and is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 28)
))
)
where order_rank=1 )
group by 1,2,3,4,5
union all
select 'period',date ,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
( select *,median(avi_6v) over (partition by date,first_store) as median_6v_avi from
(select rank() over (partition by user_account,date order by activity_date desc,random() ) as order_rank,a.date,b.* as report_date 
from
(select distinct date_scaffold.date as date from csv.date_scaffold join csv.periods on (periods.end_date=date_scaffold.date) ) a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where daypart='all' and is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 28)
))
)
where order_rank=1 )
group by 1,2,3,4,5
) union all
select *, '91 Day Active' as activty_definition from (
select 'daily' as date_interval,date,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
(select *, median(avi_6v) over (partition by date,first_store) as median_6v_avi 
from


(select rank() over (partition by user_account,date order by activity_date,random() desc) as order_rank,a.date,b.* as report_date from

 csv.date_scaffold a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 91)
 ))
)
where order_rank=1
) 
group by 1,2,3,4,5

union all
select 'weekly',date ,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
( select *,median(avi_6v) over (partition by date,first_store) as median_6v_avi from
(select rank() over (partition by user_account,date order by activity_date desc,random() ) as order_rank,a.date,b.* as report_date 
from
(select distinct date_trunc('week',date) as date from csv.date_scaffold ) a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where daypart='all' and is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 91)
))
)
where order_rank=1 )
group by 1,2,3,4,5


union all
select 'monthly',date ,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
( select *,median(avi_6v) over (partition by date,first_store) as median_6v_avi from
(select rank() over (partition by user_account,date order by activity_date desc,random() ) as order_rank,a.date,b.* as report_date 
from
(select distinct date_trunc('month',date) as date from csv.date_scaffold ) a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where daypart='all' and is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 91)
))
)
where order_rank=1 )
group by 1,2,3,4,5
union all
select 'period',date ,first_Store,median_6v_avi,round(avi_6v,0)as avi_6v,count(distinct user_account) as users from
( select *,median(avi_6v) over (partition by date,first_store) as median_6v_avi from
(select rank() over (partition by user_account,date order by activity_date desc,random() ) as order_rank,a.date,b.* as report_date 
from
(select distinct date_scaffold.date as date from csv.date_scaffold join csv.periods on (periods.end_date=date_scaffold.date) ) a
join

(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where daypart='all' and is_test_user=false and is_employee_user=false and activity_type!='inactive') b
on(date<=current_date and b.visit_number>=3 
and ((datediff('day',activity_date,date) between 0 and 91)
))
)
where order_rank=1 )
group by 1,2,3,4,5
)
