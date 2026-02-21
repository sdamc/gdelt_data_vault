{{
  config(
    materialized='incremental',
    schema='raw_vault',
    unique_key=['news_hkey', 'load_datetime'],
    tags=['satellite', 'news','gdelt']
  )
}}

-- Satellite: News Tone and Attributes
-- Parent Hub: hub_news

WITH source_data AS (
    SELECT
        gkg_record_id,
        event_datetime,
        source_domain,
        source_url,
        tone_overall,
        tone_positive,
        tone_negative,
        tone_polarity,
        tone_activity,
        word_count,
        load_datetime
    FROM {{ source('staging', 'stg_gkg_news') }}
    WHERE gkg_record_id IS NOT NULL
),

hashed AS (
    SELECT
        MD5(CAST(gkg_record_id AS VARCHAR)) AS news_hkey,
        event_datetime,
        source_domain,
        source_url,
        tone_overall,
        tone_positive,
        tone_negative,
        tone_polarity,
        tone_activity,
        word_count,
        load_datetime,
        'stg_gkg_news' AS record_source,
        MD5(CONCAT_WS('|',
            COALESCE(CAST(event_datetime AS VARCHAR), ''),
            COALESCE(source_domain, ''),
            COALESCE(CAST(tone_overall AS VARCHAR), ''),
            COALESCE(CAST(tone_positive AS VARCHAR), ''),
            COALESCE(CAST(tone_negative AS VARCHAR), ''),
            COALESCE(CAST(tone_polarity AS VARCHAR), ''),
            COALESCE(CAST(tone_activity AS VARCHAR), ''),
            COALESCE(CAST(word_count AS VARCHAR), '')
        )) AS hashdiff
    FROM source_data
)

SELECT
    news_hkey,
    load_datetime,
    event_datetime,
    source_domain,
    source_url,
    tone_overall,
    tone_positive,
    tone_negative,
    tone_polarity,
    tone_activity,
    word_count,
    hashdiff,
    record_source
FROM hashed

{% if is_incremental() %}
    WHERE (news_hkey, hashdiff) NOT IN (
        SELECT news_hkey, hashdiff FROM {{ this }}
    )
{% endif %}
