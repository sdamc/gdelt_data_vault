{{
  config(
    materialized='incremental',
    schema='raw_vault',
    unique_key='country_hkey',
    tags=['hub', 'country','gdelt']
  )
}}

-- Hub: Unique Countries
-- Business Key: ISO 2-letter Country Code

WITH source_data AS (
    SELECT DISTINCT
        UNNEST(string_to_array(trim(both '{}' from countries), ',')) AS country_code,
        load_datetime
    FROM {{ source('staging', 'stg_gkg_news') }}
    WHERE countries IS NOT NULL 
      AND countries != '{}'
      AND countries != ''
),

hashed AS (
    SELECT
        MD5(CAST(country_code AS VARCHAR)) AS country_hkey,
        country_code,
        load_datetime,
        'stg_gkg_news' AS record_source
    FROM source_data
)

SELECT DISTINCT
    country_hkey,
    country_code,
    load_datetime,
    record_source
FROM hashed

{% if is_incremental() %}
    WHERE country_hkey NOT IN (SELECT country_hkey FROM {{ this }})
{% endif %}
