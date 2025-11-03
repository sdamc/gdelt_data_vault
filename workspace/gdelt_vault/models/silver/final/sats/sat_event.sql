{{ config(
    materialized = 'incremental',
    unique_key = 'event_hk',
    schema = 'silver'
) }}

-- Versão que lê direto da tabela staging_gkg_corruption via source('staging','staging_gkg_corruption')
-- Faz parsing tab-delimited de alguns campos do registro GKG.

WITH raw AS (
    SELECT
        split_part(gkg_record, E'\t', 1) AS gkg_id,
        split_part(gkg_record, E'\t', 2) AS gkg_datetime_raw,
        split_part(gkg_record, E'\t', 8) AS themes,
        split_part(gkg_record, E'\t', 10) AS locations,
        split_part(gkg_record, E'\t', 11) AS persons,
        split_part(gkg_record, E'\t', 12) AS organizations,
        split_part(gkg_record, E'\t', 13) AS v2tone_raw, -- formato: Tone,PositiveScore,NegativeScore,Polarity,ActivityDensity,SelfGroupDensity,WordCount
        split_part(gkg_record, E'\t', 15) AS gcam_raw,
        filename,
        date_id,
        gkg_record
    FROM {{ source('staging', 'staging_gkg_corruption') }}
), prepared AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['gkg_id']) }} AS event_hk,
        gkg_id,
        themes,
        locations,
        persons,
        organizations,
        -- Extrai métricas individuais de v2tone_raw
        CASE WHEN split_part(v2tone_raw, ',', 1) ~ '^[-0-9\.]+$' THEN split_part(v2tone_raw, ',', 1)::REAL END AS tone,
        CASE WHEN split_part(v2tone_raw, ',', 2) ~ '^[-0-9\.]+$' THEN split_part(v2tone_raw, ',', 2)::REAL END AS positive_score,
        CASE WHEN split_part(v2tone_raw, ',', 3) ~ '^[-0-9\.]+$' THEN split_part(v2tone_raw, ',', 3)::REAL END AS negative_score,
        CASE WHEN split_part(v2tone_raw, ',', 4) ~ '^[-0-9\.]+$' THEN split_part(v2tone_raw, ',', 4)::REAL END AS polarity,
        CASE WHEN split_part(v2tone_raw, ',', 5) ~ '^[-0-9\.]+$' THEN split_part(v2tone_raw, ',', 5)::REAL END AS activity_density,
        CASE WHEN split_part(v2tone_raw, ',', 6) ~ '^[-0-9\.]+$' THEN split_part(v2tone_raw, ',', 6)::REAL END AS selfgroup_density,
        CASE WHEN split_part(v2tone_raw, ',', 7) ~ '^[0-9]+$' THEN split_part(v2tone_raw, ',', 7)::INT END AS word_count,
        gcam_raw AS gcam,
        CASE
            WHEN length(gkg_datetime_raw) >= 8 AND gkg_datetime_raw ~ '^[0-9]+' THEN TO_DATE(left(gkg_datetime_raw,8), 'YYYYMMDD')
            ELSE NULL
        END AS date_event,
        CURRENT_TIMESTAMP AS load_date
    FROM raw
)
SELECT * FROM prepared
{% if is_incremental() %}
WHERE event_hk NOT IN (SELECT event_hk FROM {{ this }})
{% endif %}
