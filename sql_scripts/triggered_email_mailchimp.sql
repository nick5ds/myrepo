CREATE VIEW mailchimp_blendo.v_segment_member_action
AS
(
		SELECT DISTINCT sm.*
			,s.NAME
			,ea.last_opened
			,ea.first_opened
			,COALESCE(ea.email_action, '0-did_not_open') AS email_action
			,ea.campaign_send_time
			,ea.campaign_id
		FROM mailchimp_blendo.blendo_segment_members sm
		INNER JOIN mailchimp_blendo.blendo_segments s ON (sm.segment_id = s.id)
		LEFT JOIN (
			SELECT COALESCE(b.id, c.campaign_id) AS campaign_id
				,COALESCE(c.segment_id, recipients_segment_opts_conditions_value) AS segment_id
				,COALESCE(b.settings_title, c.settings_title) AS campaign_title
				,email_address
				,COALESCE(c.send_time, b.send_time) AS campaign_send_time
				,MAX(CASE action
						WHEN 'bounce'
							THEN '1-bounce'
						WHEN 'open'
							THEN '2-open'
						WHEN 'click'
							THEN '3-click'
						END) AS email_action
				,MAX(TIMESTAMP) AS last_opened
				,MIN(TIMESTAMP) AS first_opened
			FROM mailchimp_blendo.blendo_report_email_activity a
			--join for campaigns with no variants
			LEFT JOIN mailchimp_blendo.blendo_campaigns b ON (a.campaign_id = b.id)
			--join for multivariate campaigns
			LEFT JOIN (
				SELECT DISTINCT sr.id
					,settings_title
					,cr.id AS campaign_id
					,cr.recipients_segment_opts_conditions_value segment_id
					,cr.send_time
				FROM mailchimp_blendo.blendo_sub_reports sr
				INNER JOIN mailchimp_blendo.blendo_campaigns cr ON (sr.parent_campaign_id = cr.id)
				) c ON (a.campaign_id = c.id)
			GROUP BY 1
				,2
				,3
				,4
				,5
			) ea ON (
				s.id = ea.segment_id
				AND sm.email_address = ea.email_address
				)
		)