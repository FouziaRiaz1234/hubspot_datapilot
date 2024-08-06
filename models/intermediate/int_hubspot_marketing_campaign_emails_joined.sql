WITH emails AS (
  SELECT
    *
  FROM {{ ref('stg_hubspot_marketing_emails') }}
),

campaigns AS (
  SELECT
    *
  FROM {{ ref('stg_hubspot_campaigns') }}
)

SELECT DISTINCT
  e.email_id,
  e.email_name,
  e.author,
  e.from_name,
  e.authorName,
  c.campaign_name,
  e.published_at,
  e.contentTypeCategory,
  e.currentState,
  e.mailingListsExcluded,
  e.mailingListsIncluded,
  e.vidsIncluded,
  c.content_id,
  c.campaign_subject,
  c.campaign_type,
  c.campaign_id
FROM emails e
LEFT JOIN campaigns c
ON e.email_name = c.campaign_name

