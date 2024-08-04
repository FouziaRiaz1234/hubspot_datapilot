WITH source_data AS
(
    SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    content_id,	
    subject	AS campaign_subject,
    campaign_name,
    type AS campaign_type
 FROM {{ source('hubspot_tables', 'campaigns_table') }})

 SELECT * from source_data
