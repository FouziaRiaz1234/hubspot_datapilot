WITH source_data AS 
(
  SELECT
    CAST (emailCampaignId AS STRING) AS campaign_id,
    `from` AS from_name,
    created AS created_at,
    id AS event_id,
    deviceType AS device_type,
    type AS event_type,
    sentBy_created,
    category
  FROM {{ source('hubspot_tables', 'all_events_table') }}
  WHERE type IN ('OPEN' , 'CLICK'))

SELECT * FROM source_data
