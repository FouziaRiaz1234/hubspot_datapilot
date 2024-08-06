
WITH source_data AS

(SELECT
 app_name,
 id AS event_id,
 app_id,
 portal_id,
 created,
 CAST(email_campaign_id AS STRING) AS campaign_id,
 recipient,
 `from`,
 reply_to,
 cc,
 bcc,
 subject,
 type AS event_type,
 sentby_id,
 sentby_created,
 smtp_id,
 email_campaign_group_id,
 obsoletedBy_id,
 obsoletedBy_created
 FROM {{ source('hubspot_tables', 'sent_event_table') }}
)

SELECT * FROM source_data

