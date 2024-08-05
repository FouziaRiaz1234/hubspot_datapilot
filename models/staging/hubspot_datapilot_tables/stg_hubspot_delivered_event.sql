WITH source_data AS
    (
    SELECT 
    CAST(id AS STRING)AS event_id,
    type AS event_type,
    recipient,
    CAST (email_campaign_id AS STRING) AS campaign_id,
    email_campaign_group_id,
    created	AS created_at,
    sentby_id,
    sentby_created,
FROM {{ source('hubspot_tables', 'delivered_event_table') }}
)
SELECT * FROM source_data
