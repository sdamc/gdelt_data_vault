{{
  config(
    materialized='incremental',
    database='pg_vault',
    schema='gold',
    unique_key=['week_date', 'country_name'],
    incremental_strategy='merge',
    tags=['gold', 'fact', 'weekly', 'aml']
  )
}}

-- Fact Table: Weekly AML News Sentiment by Country
-- Aggregates ONLY curated/validated AML articles from fact_ai_news_insights
-- This ensures sentiment metrics only reflect true money laundering news

WITH curated_articles AS (
    SELECT
        news_hkey,
        event_datetime,
        country_name,
        -- Extract country code from country name (or use a lookup if available)
        -- For now, we'll use the country name directly
        continent,
        tone_overall,
        tone_positive,
        tone_negative,
        tone_polarity,
        category,
        source_domain
    FROM {{ ref('fact_ai_news_insights') }}
    WHERE tone_overall IS NOT NULL
        AND event_datetime IS NOT NULL
        {% if is_incremental() %}
        -- Only process data from the most recent complete week onwards
        AND DATE_TRUNC('week', event_datetime) >= (
            SELECT COALESCE(MAX(week_date), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
        {% endif %}
),

weekly_country_facts AS (
    SELECT
        -- Time dimension (grain: week, Monday as start)
        DATE_TRUNC('week', event_datetime) as week_date,
        
        -- Geographic dimension
        country_name,
        continent,
        
        -- Volume metrics (only validated AML articles)
        COUNT(DISTINCT news_hkey) as validated_article_count,
        COUNT(DISTINCT source_domain) as unique_sources,
        
        -- Category distribution
        COUNT(DISTINCT CASE WHEN category = 'top_negative' THEN news_hkey END) as negative_articles,
        COUNT(DISTINCT CASE WHEN category = 'top_positive' THEN news_hkey END) as positive_articles,
        COUNT(DISTINCT CASE WHEN category = 'most_polarizing' THEN news_hkey END) as polarizing_articles,
        
        -- Tone metrics - Central tendency
        ROUND(AVG(tone_overall)::numeric, 2) as avg_tone_overall,
        ROUND(AVG(tone_positive)::numeric, 2) as avg_tone_positive,
        ROUND(AVG(tone_negative)::numeric, 2) as avg_tone_negative,
        ROUND(AVG(tone_polarity)::numeric, 2) as avg_tone_polarity,
        
        -- Tone metrics - Distribution
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
    
    -- Use country_name as key (since we don't have country_code in ai_insights)
    -- If you add country_code to ai_insights, replace this
    country_name as country_key,
    
    -- Volume metrics
    validated_article_count,
    unique_sources,
    negative_articles,
    positive_articles,
    polarizing_articles,
    
    -- Primary sentiment indicators
    avg_tone_overall,
    avg_tone_positive,
    avg_tone_negative,
    avg_tone_polarity,
    
    -- Distribution metrics
    min_tone_overall,
    max_tone_overall,
    stddev_tone_overall,
    
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
    
    -- Volatility indicator (high stddev = unstable sentiment)
    CASE
        WHEN stddev_tone_overall IS NULL THEN 'Unknown'
        WHEN stddev_tone_overall > 3 THEN 'High Volatility'
        WHEN stddev_tone_overall > 1.5 THEN 'Moderate Volatility'
        ELSE 'Low Volatility'
    END as sentiment_volatility,
    
    CURRENT_TIMESTAMP as fact_created_at

FROM weekly_country_facts
ORDER BY week_date DESC, validated_article_count DESC
