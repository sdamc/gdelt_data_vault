{{
  config(
    materialized='table',
    database='pg_vault',
    schema='gold',
    unique_key=['news_hkey', 'category'],
    tags=['gold', 'ai_insights', 'fact']
  )
}}

-- Gold Table: AI-Generated News Insights
-- Contains AI-powered abstracts for top sentiment news articles
-- Populated by ai_news_insights.py script

-- This table is managed by the Python ingestion script
-- Run: python ingestion/ai_news_insights.py

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
    CAST(NULL AS INTEGER) as report_count,
    CAST(NULL AS TEXT) as ai_abstract,  -- AI-generated 40-word summary
    CAST(NULL AS TIMESTAMP) as generated_at
WHERE 1=0  -- Return no rows, just schema
