drop view user_features.order_session_summary;

create view user_features.order_session_summary as (
select app_session.*,
ord.created_at,ord.served_at,ord.delivered_at,pos.num_items,pos.num_total_bowls
,pos.num_chefs_bowl,pos.num_sides,pos.num_beverages,pos.num_bags,pos.total_revenue,ord.user_id,uas.visit_number,uas.user_account,uas.avi_6v,uas.activation_date_3_rd_visit,uas.order_store
 from
(select client_type,ss.order_session_id,cor.order_id, min(st.timestamp) as first_session_started_at,min(pv.entered_menu_at) as first_entered_menu_at
,max(st.timestamp) last_session_started_at,max(pv.entered_menu_at) last_entered_menu_at
,max(started_checkout_at) started_checkout_at
,max(cor.order_completed_at) order_completed_at
,max(pv.num_products_viewed) num_products_viewed
from segment.session_started ss
join segment.tracks st
on (st.message_id=ss.message_id)

join(select order_session_id,count(distinct vp.message_id) as num_products_viewed,min(vpt.timestamp) as entered_menu_at
from segment.viewed_product vp
join segment.tracks vpt
on (vp.message_id=vpt.message_id)
group by 1) pv
on (pv.order_Session_id=ss.order_session_id)

join (select order_session_id,max(sct.timestamp) started_checkout_at from
segment.started_checkout sc
join segment.tracks sct
on (sc.message_id=sct.message_id)
group by 1) sch
on (sch.order_session_id=ss.order_session_id)

join (select order_Session_id,max(cot.timestamp) order_completed_at,order_id from segment.completed_order co join segment.tracks cot
on (co.message_id=cot.message_id)  group by 1,3) cor
on(ss.order_Session_id=cor.order_session_id)  
where client_type in ('eatsa-android','eatsa-iOS','kiosk')
group by 1,2,3) app_session
join reporting.t_pos_orders pos
on (app_session.order_id=pos.order_id and is_internal_user=false and is_test_order=false)
join kwwhub_public.orders ord
on(app_session.order_id=ord.id)

join customer_identity.t_user_accounts ua on (ord.user_id=ua.user_id)

join customer_identity.t_user_activity_stats uas on (ua.user_account=uas.user_account and pos.created_at_local::date=activity_date::date and activity_type!='inactive')
)



