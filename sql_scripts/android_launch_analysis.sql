
select cam.*
,sum(ua1_o.rev) as total_rev
,sum(case when ua1_o.activity_date<campaign_date then ua1_o.rev else 0 end) pre_rev
,sum(case when ua1_o.activity_date<campaign_date then ua1_o.is_mobile else 0 end) Pre_mob_orders
,sum(case when ua1_o.activity_date<campaign_date then ua1_o.is_refund else 0 end) Pre_mob_refund
,sum(case when ua1_o.activity_date<campaign_date then 1 else 0 end) Pre_orders
,sum(case when ua1_o.activity_date>campaign_date then ua1_o.rev else 0 end) post_rev
,sum(case when ua1_o.activity_date>campaign_date then ua1_o.is_mobile else 0 end) post_mob_orders
,sum(case when ua1_o.activity_date>campaign_date then ua1_o.is_refund else 0 end) post_mob_refund
,sum(case when ua1_o.activity_date<campaign_date then 1 else 0 end) post_orders


,sum(case when ua2_o.activity_date<campaign_date then ua2_o.rev else 0 end) pre_rev
,sum(case when ua2_o.activity_date<campaign_date then ua2_o.is_mobile else 0 end) Pre_mob_orders
,sum(case when ua2_o.activity_date<campaign_date then ua2_o.is_refund else 0 end) Pre_mob_refund
,sum(case when ua2_o.activity_date<campaign_date then 1 else 0 end) Pre_orders
,sum(case when ua2_o.activity_date>campaign_date then ua2_o.rev else 0 end) post_rev
,sum(case when ua2_o.activity_date>campaign_date then ua2_o.is_mobile else 0 end) post_mob_orders
,sum(case when ua2_o.activity_date>campaign_date then ua2_o.is_refund else 0 end) post_mob_refund
,sum(case when ua2_o.activity_date<campaign_date then 1 else 0 end) post_orders

from
(select a.email_address,b.name,a.merge_fields_mmerge15 as promo_given,pa.user_id,pa.order_id,b.created_at as campaign_date,pa.created_at::date promo_date,ua.user_account as ua_email,ua2.user_account as ua_promo
,case when ua.user_account=ua2.user_account then 'true' else 'false' end as same_ua from
mailchimp_blendo.blendo_segment_members a join
mailchimp_blendo.blendo_segments b
on(a.segment_id=b.id)
join kwwhub_public.promos p
on(p.redemption_code =a.merge_fields_mmerge15)
left join kwwhub_public.promo_authorizations pa
on (pa.promo_id=p.id)
left join
reporting.t_user_accounts ua
on(a.email_address=ua.user_email)

left join
reporting.t_user_accounts ua2
on (pa.user_id=ua2.user_id)

where b.name like 'And%') cam


left join
(select user_account,created_At::date as activity_date,sum(total) rev ,max(case when is_mobile=true then 1 else 0 end) as is_mobile, max(case when is_refunded=true then 1 else 0 end) as is_refund

 from kwwhub_public.orders o
join reporting.t_user_accounts ua3
on(o.user_id=ua3.user_id)
group by 1,2 ) ua1_o
on (cam.ua_email=ua1_o.user_account) 

left join
(select user_account,created_At::date as activity_date,sum(total) rev ,max(case when is_mobile=true then 1 else 0 end) as is_mobile, max(case when is_refunded=true then 1 else 0 end) as is_refund

 from kwwhub_public.orders o
join reporting.t_user_accounts ua3
on(o.user_id=ua3.user_id)
group by 1,2 ) ua2_o
on (cam.ua_promo=ua2_o.user_account) 
group by 1,2,3,4,5,6,7,8,9,10  





