{{
  config(
    materialized='incremental',
    database='pg_vault',
    schema='gold',
    tags=['gold', 'ai_insights', 'fact'],
    on_schema_change='sync_all_columns'
  )
}}

-- Gold Table: AI-Generated News Insights
-- Contains AI-powered abstracts for AML news articles
-- Data populated by insights/ai_insights_generator.py

-- Using incremental materialization so dbt doesn't drop/recreate the table
-- This preserves the unique constraint needed for ON CONFLICT

-- Schema only definition (data loaded via Python script)
SELECT 
    CAST(NULL AS VARCHAR) as news_hkey,
    CAST(NULL AS VARCHAR) as source_domain,
    CAST(NULL AS VARCHAR) as source_url,
    CAST(NULL AS TIMESTAMP) as event_datetime,
    CAST(NULL AS VARCHAR) as category,  -- 'top_negative', 'top_positive', 'most_polarizing'
    CAST(NULL AS VARCHAR) as continent,
    CAST(NULL AS VARCHAR) as country_name,
    CAST(NULL AS FLOAT) as tone_overall,
    CAST(NULL AS FLOAT) as tone_positive,
    CAST(NULL AS FLOAT) as tone_negative,
    CAST(NULL AS FLOAT) as tone_polarity,
    CAST(NULL AS TEXT) as ai_abstract,  -- AI-generated 40-word summary
    CAST(NULL AS TIMESTAMP) as generated_at
WHERE 1=0  -- Return no rows, just schema
