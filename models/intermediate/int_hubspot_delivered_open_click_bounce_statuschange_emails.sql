
WITH all_email_events AS (
  SELECT
    event_id,
    campaign_id,
    CAST(NULL AS STRING) AS category,
    event_type,
    CAST(NULL AS STRING) AS recipient,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS state,
    CAST(NULL AS STRING) AS city,
    CAST(NULL AS STRING) AS device_type,
    CAST(NULL AS STRING) AS sentby_id,
    sentby_created,
    CAST(NULL AS STRING) AS open_duration
  FROM {{ ref('stg_hubspot_all_email_events') }}
),

click_events AS (
  SELECT
    event_id,
    campaign_id,
    CAST(NULL AS STRING) AS category,
    event_type,
    recipient,
    NULLIF(country, 'Unknown') AS country,
    NULLIF(state, 'Unknown') AS state,
    NULLIF(city, 'Unknown') AS city,
    device_type,
    sentby_id,
    sentby_created,
    CAST(NULL AS STRING) AS open_duration
  FROM {{ ref('stg_hubspot_click_event') }}
),

open_events AS (
  SELECT
    event_id,
    campaign_id,
    CAST(NULL AS STRING) AS category,
    event_type,
    recipient,
    NULLIF(country, 'Unknown') AS country,
    NULLIF(state, 'Unknown') AS state,
    NULLIF(city, 'Unknown') AS city,
    device_type,
    sentby_id,
    sentby_created,
    CAST(NULL AS STRING) AS open_duration
  FROM {{ ref('stg_hubspot_open_event') }}
),

delivered_events AS (
  SELECT
    event_id,
    campaign_id,
    CAST(NULL AS STRING) AS category,
    event_type,
    recipient,
    CAST(NULL AS STRING) country,
    CAST(NULL AS STRING) state,
    CAST(NULL AS STRING) city,
    CAST(NULL AS STRING) device_type,
    sentby_id,
    sentby_created,
    CAST(NULL AS STRING) open_duration
  FROM {{ ref('stg_hubspot_delivered_event') }}
),

bounce_events AS (
  SELECT
    event_id,
    campaign_id,
    category,
    event_type,
    recipient,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS state,
    CAST(NULL AS STRING) AS city,
    CAST(NULL AS STRING) AS device_type,
    sentby_id,
    sentby_created,
    CAST(NULL AS STRING) AS open_duration
  FROM {{ ref('stg_hubspot_bounce_event') }}
),

status_change_events AS (
  SELECT
    event_id,
    campaign_id,
    CAST(NULL AS STRING) AS category,
    event_type,
    recipient,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS state,
    CAST(NULL AS STRING) AS city,
    CAST(NULL AS STRING) AS device_type,
    sentby_id,
    sentby_created,
    CAST(NULL AS STRING) AS open_duration
  FROM {{ ref('stg_hubspot_status_change_event') }}
),

sent_events AS (
  SELECT
    event_id,
    campaign_id,
    CAST(NULL AS STRING) AS category,
    event_type,
    recipient,
    CAST(NULL AS STRING) country,
    CAST(NULL AS STRING) state,
    CAST(NULL AS STRING) city,
    CAST(NULL AS STRING) device_type,
    sentby_id,
    sentby_created,
    CAST(NULL AS STRING) open_duration
  FROM {{ ref('stg_hubspot_sent_event') }}
),

combined_events AS (
  SELECT * FROM all_email_events
  UNION ALL
  SELECT * FROM click_events
  UNION ALL
  SELECT * FROM open_events
  UNION ALL
  SELECT * FROM delivered_events
  UNION ALL
  SELECT * FROM bounce_events
  UNION ALL
  SELECT * FROM status_change_events
  UNION ALL
  SELECT * FROM sent_events
)

SELECT
  event_id,
  MAX(campaign_id) AS campaign_id,
  MAX(device_type) AS device_type,
  MAX(event_type) AS event_type,
  MAX(sentby_created) AS sentby_created,
  MAX(category) AS category,
  MAX(recipient) AS recipient,
  MAX(country) AS country,
  MAX(state) AS state,
  MAX(city) AS city,
  MAX(sentby_id) AS sentby_id,
  MAX(open_duration) AS open_duration
FROM combined_events
GROUP BY event_id
