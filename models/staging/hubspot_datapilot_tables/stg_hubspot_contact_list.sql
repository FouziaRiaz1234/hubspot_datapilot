WITH source_data AS 
(
  SELECT 
  CAST(listId AS STRING) AS contact_list_id,
  createdAt AS created_at,
  updatedAt AS updated_at,
  name AS list_name,
  listType AS list_type,
  metaData_size AS list_size,
FROM {{ source('hubspot_tables', 'contacts_list_table') }})

SELECT * FROM source_data


