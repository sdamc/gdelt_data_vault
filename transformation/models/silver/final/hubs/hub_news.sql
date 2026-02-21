{{
  config(
    materialized='incremental',
    schema='raw_vault',
    unique_key='news_hkey',
    tags=['hub', 'news','gdelt']
  )
}}

-- Hub: Unique News Articles
-- Business Key: GKG Record ID

WITH source_data AS (
    SELECT
        gkg_record_id,
        load_datetime
    FROM {{ source('staging', 'stg_gkg_news') }}
    WHERE gkg_record_id IS NOT NULL
),

hashed AS (
    SELECT
        MD5(CAST(gkg_record_id AS VARCHAR)) AS news_hkey,
        gkg_record_id,
        load_datetime,
        'stg_gkg_news' AS record_source
    FROM source_data
)

SELECT DISTINCT
    news_hkey,
    gkg_record_id,
    load_datetime,
    record_source
FROM hashed

{% if is_incremental() %}
    WHERE news_hkey NOT IN (SELECT news_hkey FROM {{ this }})
{% endif %}
