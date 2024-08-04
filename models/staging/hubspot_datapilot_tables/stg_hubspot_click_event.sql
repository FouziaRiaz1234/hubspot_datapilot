WITH source_data AS
    (
    SELECT 
    id AS event_id,
    type AS event_type,
    recipient,
    CAST (email_campaign_id AS STRING) AS campaign_id,
    email_campaign_group_id,
    location_country AS country,
    location_state	as state,
    location_city AS city,
    location_latitude,
    location_longitude,
    location_zipcode,
    created	AS created_at,
    device_type	AS device_type,
    sentby_id,
    sentby_created,
FROM {{ source('hubspot_tables', 'click_event_table') }}
)
SELECT * FROM source_data
