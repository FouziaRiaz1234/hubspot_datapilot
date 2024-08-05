
WITH marketing_campaign_emails AS (
  SELECT
    *
  FROM {{ ref('int_hubspot_marketing_campaign_emails_joined') }}
),

all_email_events AS (
  SELECT
    *
  FROM {{ ref('int_hubspot_delivered_open_click_bounce_statuschange_emails') }}
),

combined_events AS (
  SELECT 
    i.email_id,
    i.author,
    i.from_name,
    i.authorName,
    i.campaign_name,
    i.published_at,
    i.contentTypeCategory,
    i.currentState,
    i.mailingListsExcluded,
    i.mailingListsIncluded,
    i.vidsIncluded,
    i.content_id,
    i.campaign_subject,
    i.campaign_type,
    i.campaign_id,
    t.event_id,
    t.device_type,
    t.event_type,
    t.sentby_created,
    t.category,
    t.recipient,
    t.country,
    t.state,
    t.city,
    t.sentby_id,
    t.open_duration
  FROM 
    marketing_campaign_emails i
  LEFT JOIN 
    all_email_events t
  ON 
    i.campaign_id = t.campaign_id
)

SELECT 
  event_type,
  recipient,
  sentby_created,
  published_at,
  country,
  state,
  city,
  author,
  from_name,
  authorName,
  campaign_id,
  campaign_name,
  campaign_subject,
  campaign_type,
  email_id,
  category,
  MAX(contentTypeCategory) AS contentTypeCategory,
  MAX(currentState) AS currentState,
  MAX(mailingListsExcluded) AS mailingListsExcluded,
  MAX(mailingListsIncluded) AS mailingListsIncluded,
  MAX(vidsIncluded) AS vidsIncluded,
  MAX(sentby_id) AS sentby_id,
  MAX(open_duration) AS open_duration
FROM 
  combined_events
GROUP BY 
  event_type,
  recipient,
  sentby_created,
  country,
  state,
  city,
  author,
  from_name,
  authorName,
  campaign_id,
  campaign_name,
  campaign_subject,
  campaign_type,
  email_id,
  published_at,
  category
  