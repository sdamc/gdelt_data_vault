{{
  config(
    materialized='incremental',
    database='pg_vault',
    schema='gold',
    unique_key='news_hkey',
    tags=['gold', 'ai_insights', 'gdelt'],
    on_schema_change='sync_all_columns'
  )
}}

-- Data populated by insights/ai_insights_generator.py

SELECT 
    CAST(NULL AS VARCHAR) as news_hkey,
    CAST(NULL AS TIMESTAMP) as event_datetime,
    CAST(NULL AS VARCHAR) as source_domain,
    CAST(NULL AS VARCHAR) as source_url,
    CAST(NULL AS FLOAT) as tone_overall,
    CAST(NULL AS FLOAT) as tone_positive,
    CAST(NULL AS FLOAT) as tone_negative,
    CAST(NULL AS FLOAT) as tone_polarity,
    CAST(NULL AS TEXT) as ai_abstract,
    CAST(NULL AS TIMESTAMP) as generated_at
WHERE 1=0  -- Return no rows, just schema
