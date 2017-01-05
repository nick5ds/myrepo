create view user_features.kiosk_session_summary as (
select app_session.*, 
ord.created_at,ord.served_at,ord.delivered_at,pos.num_items,pos.num_total_bowls
,pos.num_chefs_bowl,pos.num_sides,pos.num_beverages,pos.num_bags,pos.total_revenue
 from
(select ss.order_session_id,cor.order_id, st.timestamp as session_started_at,pv.entered_menu_at,started_checkout_at,cor.order_completed_at
,pv.num_products_viewed
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
where client_type='kiosk'
) app_session
join reporting.t_pos_orders pos
on (app_session.order_id=pos.order_id and is_internal_user=false and is_test_order=false)
join kwwhub_public.orders ord
on(app_session.order_id=ord.id)
)



