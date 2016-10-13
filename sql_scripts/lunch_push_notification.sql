1.1

select distinct ua.user_id from 
reporting.t_user_accounts ua join
(select  user_id,max(timestamp) last_sign_in from   
segment.tracks 
where context_app_name='eatsa'
group by 1
union all
select  user_id,max(timestamp) last_sign_in from   
segment_android.tracks 
where context_app_name='eatsa' group by 1
union all
select b.user_id,max(time) last_sign_in from 
mixpanel._event a join mixpanel._session_started b on (a.event_id=b.event_id)
where b.client='eatsa iPhone' group by 1
)m
on(ua.user_id=m.user_id) 
where 
last_sign_in between '2016-06-01' and '2016-10-03'
and
user_email not in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Lunch Refresh - Pre-launch','Lunch Refresh 10/4 - Active','Triggered Reactivation Email - Control - Tranche A'
'Triggered Reactivation Email - Control - Tranche B' 
)) -- and email_action !='0-did_not_open')

and user_account not in ( select distinct user_account from reporting.t_user_daily_activity where activity_date>='2016-10-04')

1.2


select distinct ua.user_id from 
reporting.t_user_accounts ua join
(select  user_id,max(timestamp) last_sign_in from   
segment.tracks 
where context_app_name='eatsa'
group by 1
union all
select  user_id,max(timestamp) last_sign_in from   
segment_android.tracks 
where context_app_name='eatsa' group by 1
union all
select b.user_id,max(time) last_sign_in from 
mixpanel._event a join mixpanel._session_started b on (a.event_id=b.event_id)
where b.client='eatsa iPhone' group by 1
)m
on(ua.user_id=m.user_id) 
where 
last_sign_in between '2016-06-01' and '2016-10-03'
and
user_email not in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Triggered Reactivation Email - Control - Tranche A'
'Triggered Reactivation Email - Control - Tranche B' 
)) -- and email_action !='0-did_not_open')
 and
user_email not in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Lunch Refresh - Pre-launch','Lunch Refresh 10/4 - Active'

)  and email_action !='0-did_not_open'

)
and user_account not in ( select distinct user_account from reporting.t_user_daily_activity where activity_date>='2016-10-04')


2.2

select count(distinct user_account) from 
reporting.t_user_accounts ua join
(select  user_id,max(timestamp) last_sign_in from   
segment.tracks 
where context_app_name='eatsa'
group by 1
union all
select  user_id,max(timestamp) last_sign_in from   
segment_android.tracks 
where context_app_name='eatsa' group by 1
union all
select b.user_id,max(time) last_sign_in from 
mixpanel._event a join mixpanel._session_started b on (a.event_id=b.event_id)
where b.client='eatsa iPhone' group by 1
)m
on(ua.user_id=m.user_id) 
where 
last_sign_in >= '2016-06-01' 
and
user_email not in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Triggered Reactivation Email - Control - Tranche A'
'Triggered Reactivation Email - Control - Tranche B' 
)) --and email_action !='0-did_not_open')
and
user_email  in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Lunch Refresh - Pre-launch','Lunch Refresh 10/4 - Active'

)  and email_action !='0-did_not_open'

) 

and user_account not in ( select distinct user_account from reporting.t_user_daily_activity where activity_date>='2016-10-04')





2.2

select count(distinct user_account) from 
reporting.t_user_accounts ua join
(select  user_id,max(timestamp) last_sign_in from   
segment.tracks 
where context_app_name='eatsa'
group by 1
union all
select  user_id,max(timestamp) last_sign_in from   
segment_android.tracks 
where context_app_name='eatsa' group by 1
union all
select b.user_id,max(time) last_sign_in from 
mixpanel._event a join mixpanel._session_started b on (a.event_id=b.event_id)
where b.client='eatsa iPhone' group by 1
)m
on(ua.user_id=m.user_id) 
where 
last_sign_in >= '2016-06-01' 
and
user_email not in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Triggered Reactivation Email - Control - Tranche A'
'Triggered Reactivation Email - Control - Tranche B' 
)) -- and email_action !='0-did_not_open')
/* and
user_email  in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Lunch Refresh - Pre-launch','Lunch Refresh 10/4 - Active'

)  and email_action !='0-did_not_open'

) */

and user_account not in ( select distinct user_account from reporting.t_user_daily_activity where activity_date>='2016-10-04')

2.2

select count(distinct user_account) from 
reporting.t_user_accounts ua join
(select  user_id,max(timestamp) last_sign_in from   
segment.tracks 
where context_app_name='eatsa'
group by 1
union all
select  user_id,max(timestamp) last_sign_in from   
segment_android.tracks 
where context_app_name='eatsa' group by 1
union all
select b.user_id,max(time) last_sign_in from 
mixpanel._event a join mixpanel._session_started b on (a.event_id=b.event_id)
where b.client='eatsa iPhone' group by 1
)m
on(ua.user_id=m.user_id) 
where 
last_sign_in >= '2016-09-28' 
and
user_email not in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Triggered Reactivation Email - Control - Tranche A'
'Triggered Reactivation Email - Control - Tranche B' 
)) -- and email_action !='0-did_not_open')
/* and
user_email  in (select distinct email_address from mailchimp_blendo.v_segment_member_Action where
name in 
 ('Lunch Refresh - Pre-launch','Lunch Refresh 10/4 - Active'

)  and email_action !='0-did_not_open'

) */

and user_account not in ( select distinct user_account from reporting.t_user_daily_activity where activity_date>='2016-10-04')





