-- models/joined_contacts.sql
{{
    config(
        materialized='table'
    )
}}
with contacts_list_membership as (
    select 
        cast(contact_id as string) as contact_id,
        portal_id,
        is_contact,
        static_list_id,
        internal_list_id
        timestamp,
        is_member
    from {{ ref('stg_hubspot_contact_listmemberships') }}
),

contacts as (
    select 
        cast(contact_id as string) as contact_id,
        full_name,
        created_at,
        annual_revenue,
        country,
        state,
        city,
        company,
        company_size,
        email,
        properties_hs_is_unworked,
        hs_lead_status,
        industry,
        job_title,
        last_modified_at
    from {{ ref('stg_hubspot_contacts') }}
),

contact_list as (
    select 
        contact_list_id,
        list_name,
        list_size,
        list_type,
    from {{ ref('stg_hubspot_contact_list') }}
)

SELECT DISTINCT
    clm.contact_id,
    clm.portal_id,
    clm.is_contact,
    clm.static_list_id,
    clm.timestamp,
    clm.is_member,
    cl.list_name,
    cl.list_size,
    cl.list_type,
    c.full_name,
    c.created_at,
    c.annual_revenue,
    c.country,
    c.state,
    c.city,
    c.company,
    c.company_size,
    c.email,
    c.properties_hs_is_unworked,
    c.hs_lead_status,
    c.industry,
    c.job_title,
    c.last_modified_at
FROM
    contacts_list_membership clm
INNER JOIN
    contacts c
ON
    clm.contact_id = c.contact_id
INNER JOIN
    contact_list cl
ON
    clm.static_list_id = cl.contact_list_id