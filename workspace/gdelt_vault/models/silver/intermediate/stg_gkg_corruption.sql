{{ config(
    materialized = 'incremental',
    unique_key = 'gkgrecordid'
) }}

WITH raw AS (

    -- Lê diretamente o arquivo bronze em TSV (opcional) via external table
    -- Ou espera um script Python pré-carregar a tabela staging_gkg_corruption

    SELECT
        gkgrecordid::text,
        date_id::bigint,
        source_name::text,
        document_identifier::text,
        themes::text,
        enhanced_themes::text,
        locations::text,
        persons::text,
        organizations::text,
        tone::real,
        positive_score::real,
        negative_score::real,
        polarity::real,
        activity_density::real,
        selfgroup_density::real,
        word_count::int,
        filename::text

    FROM staging_gkg_corruption

    {% if is_incremental() %}
    WHERE date_id > (SELECT MAX(date_id) FROM {{ this }})
    {% endif %}

)

SELECT * FROM raw;
