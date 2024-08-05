WITH source_data AS
(
    SELECT 
    CAST(id AS STRING)AS event_id,
    source,
    source_id,
    subscriptions,
    recipient,
    created,
    type AS event_type,
    portal_id,
    sentby_id,
    sentby_created,
    app_id,
    CAST(email_campaign_id AS STRING) AS campaign_id,
    email_campaign_group_id,
    portal_subscription_status
FROM {{ source('hubspot_tables', 'status_change_event_table') }}
)
SELECT * FROM source_data