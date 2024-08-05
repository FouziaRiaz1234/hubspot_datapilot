-- models/joined_email_events.sql
{{
    config(
        materialized='table'
    )
}}

WITH all_email_events AS (
  SELECT *
  FROM {{ ref('int_hubspot_campaign_emails_events') }}
),

contacts_lists AS (
  SELECT *
  FROM {{ ref('int_contact_list_joined') }}
)

SELECT
c.contact_id,
c.static_list_id,
c.list_size,
c.list_name,
c.full_name,
c.email,
c.annual_revenue,
c.industry,
c.company_size,
c.job_title,
a.event_type,
a.recipient,
a.campaign_id,
a.campaign_name,
a.from_name,
a.campaign_type,
a.campaign_subject,
a.sentby_created,
a.category,
COALESCE(a.country, c.country) AS country,
COALESCE(a.state, c.state) AS state,
COALESCE(a.city, c.city) AS city

FROM all_email_events a
INNER JOIN contacts_lists c
ON a.recipient = c.email


