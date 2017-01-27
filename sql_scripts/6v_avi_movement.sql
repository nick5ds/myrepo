with period_avi as 

(select distinct period_end_date,user_account
,last_value(avi_6v)
 over (partition by user_account,period_end_date 
 order by activity_Date rows between unbounded preceding and unbounded following) avi_6v
  from
(select *
,avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 6 preceding and current row) as avi_6v
,sum(total_revenue) over (partition by user_account order by activity_date asc rows between unbounded preceding and current row) as lifetime_revenue
 from customer_identity.t_user_activity_stats where is_test_user=false and is_employee_user=false 
 and activity_type!='inactive'and visit_number>=3 
)

) 

select a.user_account, a.avi_6v
,date_add('week',-4,a.period_end_date::date) as prev_period
,date_add('week',4,a.period_end_date::date) as next_period
,min(a.period_end_date) over (partition by a.user_account) as first_period
,a.period_end_date,b.avi_6v as next_period_avi, c.avi_6v as prev_period_avi
from period_avi a
left join
period_avi b
on (a.period_end_date=date_add('week',-4,b.period_end_date) and a.user_Account=b.user_account) 
left join
period_avi c
on (a.period_end_date=date_add('week',4,c.period_end_date)and a.user_Account=c.user_account) 


