{{
  config(
    materialized='incremental',
    schema='raw_vault',
    unique_key='news_country_hkey',
    tags=['link', 'news_country','gdelt']
  )
}}

WITH source_data AS (
    SELECT
        gkg_record_id,
        UNNEST(string_to_array(trim(both '{}' from countries), ',')) AS country_code,
        load_datetime
    FROM {{ source('staging', 'stg_gkg_news') }}
    WHERE gkg_record_id IS NOT NULL
      AND countries IS NOT NULL
      AND countries != '{}'
      AND countries != ''
),

hashed AS (
    SELECT
        MD5(CAST(gkg_record_id AS VARCHAR)) AS news_hkey,
        MD5(CAST(country_code AS VARCHAR)) AS country_hkey,
        load_datetime,
        'stg_gkg_news' AS record_source
    FROM source_data
),

with_link_key AS (
    SELECT
        MD5(CONCAT_WS('|', news_hkey, country_hkey)) AS news_country_hkey,
        news_hkey,
        country_hkey,
        load_datetime,
        record_source
    FROM hashed
)

SELECT DISTINCT
    news_country_hkey,
    news_hkey,
    country_hkey,
    load_datetime,
    record_source
FROM with_link_key

{% if is_incremental() %}
    WHERE news_country_hkey NOT IN (SELECT news_country_hkey FROM {{ this }})
{% endif %}
