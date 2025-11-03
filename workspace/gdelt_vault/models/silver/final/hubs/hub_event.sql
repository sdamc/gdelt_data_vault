{{ config(
    materialized = 'incremental',
    unique_key = 'event_hk',
    schema = 'silver'
) }}

WITH raw AS (
    SELECT
        -- Campo 1: identificador único da linha
        split_part(gkg_record, E'\t', 1) AS gkg_id,
        -- Campo 2: data/hora completa, mas usamos para derivar date_id
        split_part(gkg_record, E'\t', 2) AS gkg_datetime_raw,
        filename,
        date_id,  -- já armazenado pelo script de carga
        gkg_record
    FROM {{ source('staging', 'staging_gkg_corruption') }}
), prepared AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['gkg_id']) }} AS event_hk,
        gkg_id,
        -- Deriva date_id primário baseado nos 8 primeiros dígitos do segundo campo, se existir. Fallback para date_id já salvo.
        CASE
            WHEN length(gkg_datetime_raw) >= 8 AND gkg_datetime_raw ~ '^[0-9]+' THEN left(gkg_datetime_raw,8)::BIGINT
            ELSE date_id
        END AS date_id,
        CURRENT_TIMESTAMP AS load_date
    FROM raw
)
SELECT * FROM prepared
{% if is_incremental() %}
WHERE event_hk NOT IN (SELECT event_hk FROM {{ this }})
{% endif %}
