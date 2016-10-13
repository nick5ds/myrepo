select segment_create_time,campaign_id,send_time,campaign_name,segment_id,segment_name,email_Address,email_action, store_address as post_campaign_store,last_Activity_status
,max(first_visit_date) first_visit_date
,max(last_Activity_date) last_Activity_date
,max(post_campaign_activity_date) as post_campaign_activity_date
,max(visit_interval_before_campaign) visit_interval_before_campaign
,max(last_activity_date_before_campaign) last_activity_date_before_campaign
,max(current_visit_interval) current_visit_interval
,max(first_opened)first_opened
,max(visit_number) as post_campaing_visit_number


 


from 
(select post_email.*,pre_ua.avg_visit_interval visit_interval_before_campaign
,pre_ua.activity_date last_activity_date_before_campaign,first_visit_date 
,rank() over (partition by user_account,campaign_id order by pre_ua.activity_date desc) rank2
from 

(select *
from 
(
select email_opens.*,visit_number,days_since_prev_Visit,store_address,last_activity_status,activity_date as post_campaign_activity_date
,rank() over (partition by user_account,campaign_id order by activity_date asc)
,last_value(avg_visit_interval) over (partition by user_account order by activity_date asc rows between unbounded preceding and unbounded following ) as current_visit_interval
,last_value(activity_date) over (partition by user_account order by activity_date asc rows between unbounded preceding and unbounded following ) as last_Activity_date
from

(select b.created_at as segment_create_time ,c.id as campaign_id
,send_time
,settings_title as campaign_name
,s.segment_id as segment_id
,b.name as segment_name
,s.email_address
,coalesce(email_action,'recieved') as email_action
,first_opened

 from  mailchimp_blendo.blendo_segment_members s
 join mailchimp_blendo.blendo_segments b 
 on(s.segment_id=b.id)
left join mailchimp_blendo.blendo_campaigns c
 on (coalesce(c.recipients_segment_opts_saved_segment_id,recipients_segment_opts_conditions_value) =s.segment_id)
left join (
select   email_address, coalesce(sr.parent_campaign_id,campaign_id) as campaign_id,
 max(case action when 'bounce' then '0-bounce' when 'open' then '1-open' when 'click' then '2-click' end) as email_action
 , min(ea.timestamp) as first_opened
from 
 mailchimp_blendo.blendo_report_email_activity ea
 left join mailchimp_blendo.blendo_sub_reports sr
 on (ea.campaign_id=sr.id)
group by 1,2 
) ea
on(s.email_address=ea.email_address and c.id=ea.campaign_id)) email_opens
left join
(
select
user_email,t_user_Accounts.user_account,activity_date,avg_visit_interval,visit_number,store_Address,is_promo_order,next_Activity_date,last_activity_status,days_since_prev_visit
from reporting.t_user_accounts 
left join
reporting.user_Activity_analysis
on (t_user_Accounts.user_account=user_activity_analysis.user_account)
where lower(activity_type) not like '%churned%'
group by 1,2,3,4,5,6,7,8,9,10) ua
on( email_opens.email_address=ua.user_email and send_time<activity_date)
--group by 1,2,3,4,5,6,7,8,9,10

)  mrkt where rank=1  
) post_email

left join
(
select
user_email,t_user_Accounts.user_account,activity_date,avg_visit_interval,first_visit_date
from reporting.t_user_accounts 
left join
reporting.user_Activity_analysis
on (t_user_Accounts.user_account=user_activity_analysis.user_account)
where lower(activity_type) not like '%churned%'
group by 1,2,3,4,5) pre_ua
on( post_email.email_address=pre_ua.user_email and post_email.segment_Create_time::date>activity_date)) campaign_users where rank2=1 
group by 1,2,3,4,5,6,7,8,9,10