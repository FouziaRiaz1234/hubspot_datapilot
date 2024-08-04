{{
    config(
        materialized='table'
    )
}}

WITH emails AS (
  SELECT
    id AS email_id,
    name AS email_name,
    emailType AS email_type,
    subcategory,
    subject,
    fromName AS from_name,
    publishedAt AS published_at,
    author,
    authorName,
    categoryId,
    contentTypeCategory,
    currentState,
    mailingListsExcluded,
    mailingListsIncluded,
    categoryId AS email_category_id,
    subscription,
    subscriptionName,
    vidsExcluded,
    vidsIncluded,
    primaryEmailCampaignId,
    emailCampaignGroupId,
    campaign,
    campaignName,
    campaignUtm,
    SPLIT(REPLACE(REPLACE(allEmailCampaignIds, '[', ''), ']', ''), ',') AS allEmailCampaignIds_array
  FROM {{ source('hubspot_tables', 'marketing_email_table') }}
)

SELECT
    email_id,
    SAFE_CAST(TRIM(campaign_id) AS INT64) AS campaign_id,
    email_name,
    email_type,
    subcategory,
    subject,
    from_name,
    published_at,
    author,
    authorName,
    email_category_id,
    contentTypeCategory,
    currentState,
    mailingListsExcluded,
    mailingListsIncluded,
    subscription,
    subscriptionName,
    vidsExcluded,
    vidsIncluded,
    primaryEmailCampaignId,
    emailCampaignGroupId,
    campaign,
    campaignName,
    campaignUtm,
FROM emails, UNNEST(allEmailCampaignIds_array) AS campaign_id
WHERE SAFE_CAST(TRIM(campaign_id) AS INT64) IS NOT NULL
AND from_name IN ('Data Pilot', 'Hamza Malik', 'Tooba Shah')

