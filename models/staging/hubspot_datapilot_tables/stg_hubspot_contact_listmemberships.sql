WITH source_data AS 
(
  SELECT
 CAST(list_id AS STRING) AS contact_list_id,
 cast(vid as string) AS contact_id,
 portal_id,
 is_contact,
 CAST(static_list_id AS STRING) AS static_list_id,
 internal_list_id,
 timestamp,
 is_member
  FROM {{ source('hubspot_tables', 'contacts_list_memberships_table') }}
)

SELECT * FROM source_data