-- models/joined_email_events.sql
{{
    config(
        materialized='table'
    )
}}

WITH all_email_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY created_at) AS row_num
  FROM {{ ref('stg_hubspot_all_email_events') }}
),

click_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY created_at) AS row_num
  FROM {{ ref('stg_hubspot_click_event') }}
),

open_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY created_at) AS row_num
  FROM {{ ref('stg_hubspot_open_event') }}
)

SELECT
  a.campaign_id,
  a.from_name,
  a.created_at,
  a.event_id,
  a.device_type,
  a.event_type,
  a.sentBy_created,
  a.category,
  COALESCE(c.recipient, o.recipient) AS recipient,
  COALESCE(c.country, o.country) AS country,
  COALESCE(c.state, o.state) AS state,
  COALESCE(c.city, o.city) AS city,
  COALESCE(c.device_type, o.device_type) AS event_device_type,
  COALESCE(c.sentby_id, o.sentby_id) AS event_sentby_id,
  COALESCE(c.sentby_created, o.sentby_created) AS event_sentby_created,
  o.duration AS open_duration -- This field will be NULL for click events
FROM
  all_email_events a
  LEFT JOIN click_events c ON a.campaign_id = c.campaign_id AND a.event_type = 'CLICK' AND c.row_num = 1
  LEFT JOIN open_events o ON a.campaign_id = o.campaign_id AND a.event_type = 'OPEN' AND o.row_num = 1
WHERE a.row_num = 1
