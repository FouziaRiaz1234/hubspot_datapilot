WITH all_email_events AS (
  SELECT *
  FROM {{ ref('int_hubspot_campaign_emails_events') }}
),

contacts_lists AS (
  SELECT DISTINCT
    contact_id,
    static_list_id,
    list_size,
    list_name,
    full_name,
    email,
    annual_revenue,
    industry,
    company_size,
    job_title,
    country AS contact_country,
    state AS contact_state,
    city AS contact_city
  FROM {{ ref('int_contact_list_joined') }}
),

combined_data AS (
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
    COALESCE(a.country, c.contact_country) AS country,
    COALESCE(a.state, c.contact_state) AS state,
    COALESCE(a.city, c.contact_city) AS city,
    ROW_NUMBER() OVER (
      PARTITION BY c.contact_id, c.static_list_id, c.list_size, c.list_name, c.full_name, c.email, c.annual_revenue, c.industry, c.company_size, c.job_title, a.event_type, a.recipient, a.campaign_id, a.campaign_name, a.from_name, a.campaign_type, a.campaign_subject, a.sentby_created, a.category
      ORDER BY COALESCE(a.country, c.contact_country), COALESCE(a.state, c.contact_state), COALESCE(a.city, c.contact_city)
    ) AS rn
  FROM contacts_lists c
  LEFT JOIN all_email_events a
  ON c.email = a.recipient
)

SELECT *
FROM combined_data
WHERE rn = 1
