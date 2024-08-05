WITH source_data AS
( 
    SELECT
    CAST(id AS STRING)AS event_id,
    CAST(email_campaign_id AS STRING) AS campaign_id,
    type AS event_type,
    category,
    sentby_id,
    response,
    recipient,
    email_campaign_group_id,
    sentby_created,
    status,
FROM {{ source('hubspot_tables', 'bounce_event_table') }}
)

SELECT * FROM source_data
