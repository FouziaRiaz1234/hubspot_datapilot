WITH source_data AS 
(
  SELECT
    id AS contact_id,
    CONCAT(properties_firstname, ' ', properties_lastname) AS full_name,
    properties_createdate AS created_at,
    properties_annualrevenue AS annual_revenue,
    properties_country AS country,
    properties_city AS city,
    properties_company AS company,
    properties_company_size AS company_size,
    properties_email AS email,
    properties_hs_is_unworked,
    properties_hs_lead_status AS hs_lead_status,
    properties_industry AS industry,
    properties_jobtitle AS job_title,
    properties_lastmodifieddate AS last_modified_at,
    properties_phone,
    properties_state AS state,
    properties_total_revenue AS total_revenue
  FROM {{ source('hubspot_tables', 'contacts_table') }}
)

SELECT * FROM source_data
