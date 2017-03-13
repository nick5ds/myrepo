select ua.user_id as user_id
,coalesce(num_days_array,'0.00') as last_6_num_days

,max_p as last_purchase_date
,min_p as first_purchase_date
,frequency as num_purchases
,case when frequency>=3 and datediff('day',max_p,getdate())<=28 then '28 day active'
when frequency>=3 and datediff('day',max_p,getdate())<=84 then '84 day active'
else 'not active' end as is_active
,last_avi_6v
,coalesce(first_store,'unknown') first_store
,coalesce(last_store,'unknown') last_store
,frequency as num_visits
, frequency-1 as frequency
,datediff('week',min_p,max_p) as recency
, datediff('week',min_p,getdate()) as T from
(select id,last_avi_6v, first_store,last_store,count(distinct date) frequency,min(date) min_p,max(date) max_p from

 (       select distinct activity_date as date, first_store,last_store,user_account as id
 ,last_value(avi_6v) over (partition by user_account order by activity_date rows between unbounded preceding and unbounded following) as last_avi_6v
 ,last_value( avi_3v) over (partition by user_account order by activity_date rows between unbounded preceding and unbounded following) as last_avi_3v from
(select *,  avg(num_days_from_prev_visit) over (partition by user_account order by activity_date asc rows between 2 preceding and current row) avi_3v
    from customer_identity.t_user_activity_stats
    where
 activity_type!='inactive'  ) )

group by 1,2,3,4 ) uas
join
customer_identity.t_user_accounts ua
on(uas.id=ua.user_account)
join
(
select user_Account,listagg(num_days_from_prev_visit::varchar(5),',') within group (order by activity_date asc) as num_days_array from
(select user_account,activity_date,num_days_from_prev_visit,rank() over (partition by user_account order by activity_date desc) as ranked   from customer_identity.t_user_activity_stats
) where ranked<=7
group by 1) nums
on uas.id=nums.user_account


