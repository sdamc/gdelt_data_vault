{{
  config(
    materialized='incremental',
    database='pg_vault',
    schema='gold',
    unique_key=['month_date', 'country_code'],
    incremental_strategy='merge',
    tags=['gold', 'fact', 'monthly']
  )
}}

-- Fact Table: Monthly AML News Sentiment by Country
-- Aggregates news coverage and tone metrics at monthly grain

WITH monthly_country_facts AS (
    SELECT
        -- Time dimension (grain: month)
        DATE_TRUNC('month', s.event_datetime) as month_date,
        
        -- Country dimension
        h_c.country_code,
        
        -- Volume metrics
        COUNT(DISTINCT s.news_hkey) as unique_news_count,
        COUNT(DISTINCT s.source_domain) as unique_sources,
        
        -- Tone metrics - Central tendency
        ROUND(AVG(s.tone_overall), 2) as avg_tone_overall,
        ROUND(AVG(s.tone_positive), 2) as avg_tone_positive,
        ROUND(AVG(s.tone_negative), 2) as avg_tone_negative,
        ROUND(AVG(s.tone_polarity), 2) as avg_tone_polarity        
    
    FROM pg_vault.main_raw_vault.link_news_country l
    INNER JOIN pg_vault.main_raw_vault.hub_country h_c
        ON l.country_hkey = h_c.country_hkey
    INNER JOIN pg_vault.main_raw_vault.sat_news_tone s
        ON l.news_hkey = s.news_hkey
    WHERE s.tone_overall IS NOT NULL
        AND s.event_datetime IS NOT NULL
        {% if is_incremental() %}
        -- Only process data from the most recent complete month onwards
        AND DATE_TRUNC('month', s.event_datetime) >= (
            SELECT COALESCE(MAX(month_date), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
        {% endif %}
    GROUP BY 
        DATE_TRUNC('month', s.event_datetime),
        h_c.country_code
)

SELECT
    month_date,
    country_code,
    unique_news_count,
    unique_sources,
    
    -- Primary sentiment indicators
    avg_tone_overall,
    avg_tone_positive,
    avg_tone_negative,
    avg_tone_polarity,  
 
    
    -- Derived metrics for AML risk assessment
    -- Risk Intensity: Higher negative tone = higher risk
    ROUND(
        CASE 
            WHEN avg_tone_overall >= 0 THEN 0
            ELSE ABS(avg_tone_overall) * 10  -- Scale -10 to 0 → 100 to 0
        END, 
        2
    ) as risk_intensity_score,    
    -- Sentiment Category
    CASE
        WHEN avg_tone_overall < -5 THEN 'Very Negative'
        WHEN avg_tone_overall < -2 THEN 'Negative'
        WHEN avg_tone_overall < 2 THEN 'Neutral'
        WHEN avg_tone_overall < 5 THEN 'Positive'
        ELSE 'Very Positive'
    END as sentiment_category,
    
    CURRENT_TIMESTAMP as fact_created_at

FROM monthly_country_facts
ORDER BY month_date DESC, unique_news_count DESC
