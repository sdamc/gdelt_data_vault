{{
  config(
    materialized='incremental',
    database='pg_vault',
    schema='gold',
    unique_key=['week_date', 'country_name'],
    incremental_strategy='merge',
    tags=['gold', 'gdelt', 'country']
  )
}}

-- Derived from fact_ai_news joined with Data Vault link_news_country

WITH curated_articles AS (
    SELECT
        fan.news_hkey,
        fan.event_datetime,
        fan.tone_overall,
        fan.tone_positive,
        fan.tone_negative,
        fan.tone_polarity,
        fan.source_domain,
        dc.country_name,
        dc.continent
    FROM {{ ref('fact_ai_news_insights') }} fan
    INNER JOIN {{ source('raw_vault', 'link_news_country') }} lnc 
        ON fan.news_hkey = lnc.news_hkey
    INNER JOIN {{ source('raw_vault', 'hub_country') }} hc 
        ON lnc.country_hkey = hc.country_hkey
    INNER JOIN {{ ref('dim_country') }} dc 
        ON hc.country_code = dc.country_code
    WHERE fan.tone_overall IS NOT NULL
        AND fan.event_datetime IS NOT NULL
        {% if is_incremental() %}
        AND DATE_TRUNC('week', fan.event_datetime) >= (
            SELECT COALESCE(MAX(week_date), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
        {% endif %}
),

weekly_country_facts AS (
    SELECT
        DATE_TRUNC('week', event_datetime) as week_date,        
        country_name,
        continent,        
        COUNT(DISTINCT news_hkey) as article_count,
        COUNT(DISTINCT source_domain) as unique_sources,        
        ROUND(AVG(tone_overall)::numeric, 2) as avg_tone_overall,
        ROUND(AVG(tone_positive)::numeric, 2) as avg_tone_positive,
        ROUND(AVG(tone_negative)::numeric, 2) as avg_tone_negative,
        ROUND(AVG(tone_polarity)::numeric, 2) as avg_tone_polarity,        
        ROUND(MIN(tone_overall)::numeric, 2) as min_tone_overall,
        ROUND(MAX(tone_overall)::numeric, 2) as max_tone_overall,
        ROUND(STDDEV(tone_overall)::numeric, 2) as stddev_tone_overall
        
    FROM curated_articles
    GROUP BY 
        DATE_TRUNC('week', event_datetime),
        country_name,
        continent
)

SELECT
    week_date,
    country_name,
    continent,    
    article_count,
    unique_sources,    
    avg_tone_overall,
    avg_tone_positive,
    avg_tone_negative,
    avg_tone_polarity,    
    min_tone_overall,
    max_tone_overall,
    stddev_tone_overall,    
    ROUND(
        CASE 
            WHEN avg_tone_overall >= 0 THEN 0
            ELSE ABS(avg_tone_overall) * 10
        END, 
        2
    ) as risk_intensity_score,    
    CASE
        WHEN avg_tone_overall < -5 THEN 'Very Negative'
        WHEN avg_tone_overall < -2 THEN 'Negative'
        WHEN avg_tone_overall < 2 THEN 'Neutral'
        WHEN avg_tone_overall < 5 THEN 'Positive'
        ELSE 'Very Positive'
    END as sentiment_category,    
    CASE
        WHEN stddev_tone_overall IS NULL THEN 'Unknown'
        WHEN stddev_tone_overall > 3 THEN 'High Volatility'
        WHEN stddev_tone_overall > 1.5 THEN 'Moderate Volatility'
        ELSE 'Low Volatility'
    END as sentiment_volatility,    
    CURRENT_TIMESTAMP as fact_created_at
FROM weekly_country_facts
ORDER BY week_date DESC, article_count DESC
