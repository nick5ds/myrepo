select 
first_value(message_blurb) 
  over (partition by messages.conversation_id 
  order by message_time asc rows between unbounded preceding and current row) as first_message
,first_value("from") 
  over (partition by messages.conversation_id 
  order by message_time asc rows between unbounded preceding and current row) as initiated_by
,first_value(message_time) 
  over (partition by messages.conversation_id 
  order by message_time asc rows between unbounded preceding and current row) as conversation_started

,messages."from",messages."to"
,messages.message_time,messages.meta_id as message_id, messages.message_blurb
,messages.type,tags.emitted_at,tags.event_id as tag_id ,tags.meta_name as tag_name,tags.did_untag

 from

( select "from","to",event_id,conversation_id,meta_id,message_blurb,type,min(emitted_at) as message_time  
FROM csv.frontapp_events where type in ('inbound','out_reply')
group by 1,2,3,4,5,6,7) as messages
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
 on(tags.conversation_id=messages.conversation_id)



