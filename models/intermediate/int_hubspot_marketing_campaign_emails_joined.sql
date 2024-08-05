
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
  
FROM campaigns c
INNER JOIN emails e
ON CAST(e.campaign_id AS STRING) = CAST(c.campaign_id AS STRING)
