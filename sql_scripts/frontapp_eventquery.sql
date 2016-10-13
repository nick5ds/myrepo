
select 
reciepient_email
,conversation_id
,conversation_start_time
,first_value(message_blurb) over (partition by conversation_id order by message_time asc rows between unbounded preceding and current row) as first_message
,message_time
,message_id
,message_blurb
,type	as message_type
,emitted_at	as taged_time
,tag_id,
tag_name
,did_untag
from
(select conv.*,messages.message_time,messages.meta_id as message_id, messages.message_blurb
,messages.type,tags.emitted_at,tags.event_id as tag_id ,tags.meta_name as tag_name,tags.did_untag
,rank() over (partition by tags.event_id order by datediff(second,message_time,tags.emitted_at) asc) as tag_rank

 from

(SELECT reciepient_email,conversation_id,min(emitted_at) as conversation_start_time
FROM csv.frontapp_events 
group by 1,2) as conv
left join
( select event_id,conversation_id,meta_id,message_blurb,type,min(emitted_at) as message_time  
FROM csv.frontapp_events where type in ('inbound','out_reply')
group by 1,2,3,4,5) as messages
on (conv.conversation_id=messages.conversation_id)
left join
(
select t.conversation_id,t.event_id,t.type,t.meta_id,t.meta_name,t.emitted_at,max( case when u.conversation_id is null then 0 else 1 end) as did_untag  from
(select * FROM csv.frontapp_events where type='tag') t
left join 
(select conversation_id,meta_id, max(emitted_at) emitted_at from 
csv.frontapp_events where type='untag' group by 1,2) u
on(u.emitted_at>=t.emitted_at and u.conversation_id=t.conversation_id)
group by 1,2,3,4,5,6
 ) tags
 on(tags.conversation_id=conv.conversation_id and tags.emitted_at>=messages.message_time)
 ) fa
 where tag_rank=1
