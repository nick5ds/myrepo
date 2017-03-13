with avis as
(select 

count(distinct case when visit_number<=2 then user_account else null end ) as "onboarding_or_new"
,count(distinct case when avi>=29 and visit_number>2 then user_account else null end) as "avi>=28"
,count(distinct case when (avi between 22 and 28) and visit_number>2 then user_account else null end) as "avi22-28"
,count(distinct case when (avi between 15 and 21) and visit_number>2 then user_account else null end) as "avi15-21"
,count(distinct case when (avi between 8 and 14) and visit_number>2 then user_account else null end) as "avi8-14"
,count(distinct case when (avi between 1 and 7) and visit_number>2 then user_account else null end) as "avi1-7"
,count(distinct user_account) as total
,order_store,activity_date
from(
select *,round(avi_6v,0) as avi
--,round(avg(num_days_from_prev_visit) over (partition by user_account order by activity_date rows between 2 preceding and current row),0) as avi 
 from customer_identity.t_user_activity_stats where 
 activity_type!='inactive'
and is_employee_user=false and is_test_user=false
)
group by order_store,activity_date
) 



select * from (
select  *
,'one week ago' as metric
,lag(total,case when order_store in ('6320 Topanga Canyon Blvd.','2334 Telegraph Ave.') then 7 else 5 end) over (partition by order_store order by activity_date) total_pred
,lag("onboarding_or_new",case when order_store in ('6320 Topanga Canyon Blvd.','2334 Telegraph Ave.') then 7 else 5 end) over (partition by order_store order by activity_date) onboarding_pred
,lag("avi>=28",case when order_store in ('6320 Topanga Canyon Blvd.','2334 Telegraph Ave.') then 7 else 5 end) over (partition by order_store order by activity_date) "avi28_pred"
,lag("avi22-28",case when order_store in ('6320 Topanga Canyon Blvd.','2334 Telegraph Ave.') then 7 else 5 end) over (partition by order_store order by activity_date) "avi22-28_predg"
,lag("avi15-21",case when order_store in ('6320 Topanga Canyon Blvd.','2334 Telegraph Ave.') then 7 else 5 end) over (partition by order_store order by activity_date) "avi15-21_pred"
,lag("avi8-14",case when order_store in ('6320 Topanga Canyon Blvd.','2334 Telegraph Ave.') then 7 else 5 end) over (partition by order_store order by activity_date) "avi8-14_pred"
,lag("avi1-7",case when order_store in ('6320 Topanga Canyon Blvd.','2334 Telegraph Ave.') then 7 else 5 end) over (partition by order_store order by activity_date) "avi1-7_pred"

from avis
union all select *
,'one day ago'
,lag(total,1) over (partition by order_store order by activity_date) total_one_day_lag
,lag("onboarding_or_new",1) over (partition by order_store order by activity_date) onboarding_one_day_lag
,lag("avi>=28",1) over (partition by order_store order by activity_date) "avi28_one_day_lag"
,lag("avi22-28",1) over (partition by order_store order by activity_date) "avi22-28_one_day_lag"
,lag("avi15-21",1) over (partition by order_store order by activity_date) "avi15-21_one_day_lag"
,lag("avi8-14",1)  over (partition by order_store order by activity_date) "avi8-14_one_day_lag"
,lag("avi1-7",1) over (partition by order_store order by activity_date) "avi1-7_one_day_lag"

from avis union all
select *,'one week average'
,avg(total) over (partition by order_store order by activity_date rows between 7 preceding and 1 preceding ) total_one_week_avg
,avg("onboarding_or_new")   over (partition by order_store order by activity_date rows between 7   preceding and 1 preceding ) onboarding_one_week_avg
,avg("avi>=28")   over (partition by order_store order by activity_date rows between 7 preceding and 1 preceding ) "avi28_one_week_avg"
,avg("avi22-28")   over (partition by order_store order by activity_date rows between 7 preceding and 1 preceding ) "avi22-28_one_week_avg"
,avg("avi15-21")   over (partition by order_store order by activity_date rows between 7 preceding and 1 preceding ) "avi15-21_one_week_avg"
,avg("avi8-14")  over (partition by order_store order by activity_date rows between 7 preceding and 1 preceding ) "avi8-14_one_week_avg"
,avg("avi1-7")   over (partition by order_store order by activity_date rows between 7 preceding and 1 preceding ) "avi1-7_one_week_avg"

from avis union all
select * ,'5 day average'
,avg(total) over (partition by order_store order by activity_date rows between 5 preceding and 1 preceding ) total_5_day_avg
,avg("onboarding_or_new") over (partition by order_store order by activity_date rows between 5   preceding and 1 preceding ) onboarding_5day_avg
,avg("avi>=28")  over (partition by order_store order by activity_date rows between 5 preceding and 1 preceding ) "avi28_5day_avg"
,avg("avi22-28")  over (partition by order_store order by activity_date rows between 5 preceding and 1 preceding ) "avi22-28_5day_avg"
,avg("avi15-21")   over (partition by order_store order by activity_date rows between 5 preceding and 1 preceding ) "avi15-21_5day_avg"
,avg("avi8-14") over (partition by order_store order by activity_date rows between 5 preceding and 1 preceding ) "avi8-14_5day_avg"
,avg("avi1-7")  over (partition by order_store order by activity_date rows between 5 preceding and 1 preceding ) "avi1-7_5day_avg"  
from avis
)
where activity_date between'2016-09-01' and '2016-10-31'
