create view frontapp.v_conversations_unified as

 SELECT tags.from_email, tags.conversation_id, o_map.order_id, archive.archived_at,tags.created_at as conversation_created_at, inb.first_message_recieved_at, outb.first_message_sent_at,outb.last_message_sent_at, pg_catalog.listagg(tags.tag_name::text, ','::text) WITHIN GROUP(
  ORDER BY tags.tag_name) AS taglist
   FROM frontapp.tags
   LEFT JOIN ( SELECT DISTINCT front_order_mappingrows.conversation_id, front_order_mappingrows.order_id
           FROM mailchimp_blendo.front_order_mappingrows) o_map ON tags.conversation_id::text = o_map.conversation_id::text
   LEFT JOIN ( SELECT messages.conversation_id, "max"(messages.created_at) AS archived_at
      FROM frontapp.messages
     WHERE messages.message_type::text = 'archive'::text
     GROUP BY messages.conversation_id) archive ON archive.conversation_id::text = tags.conversation_id::text
   LEFT JOIN ( SELECT messages.conversation_id, min(messages.created_at) AS first_message_recieved_at
   FROM frontapp.messages
  WHERE messages.message_type::text = 'inbound'::text
  GROUP BY messages.conversation_id) inb ON tags.conversation_id::text = inb.conversation_id::text
   LEFT JOIN ( SELECT messages.conversation_id, min(messages.created_at) AS first_message_sent_at,max(messages.created_at) as last_message_sent_at
   FROM frontapp.messages
  WHERE messages.message_type::text = 'outbound'::text OR messages.message_type::text = 'out_reply'::text
  GROUP BY messages.conversation_id) outb ON tags.conversation_id::text = outb.conversation_id::text
  GROUP BY tags.from_email, tags.conversation_id, o_map.order_id, archive.archived_at,tags.created_at, inb.first_message_recieved_at, outb.first_message_sent_at,outb.last_message_sent_at
