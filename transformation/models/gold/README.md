# Gold Layer - Dimensional Model

## Overview
The gold layer uses **DuckDB as the analytical engine** to process data from PostgreSQL raw vault and materialize dimensional models back to PostgreSQL.

## Architecture
```
Raw Vault (PostgreSQL) 
  → DuckDB Analytics Engine 
  → Gold Dimensional Model (PostgreSQL)
```

---

## Dimension Table

### `dim_country` (Seed)
Reference dimension with country metadata.

**Grain:** One row per country

**Columns:**
- `country_code` (PK): ISO 3166-1 alpha-2 code
- `country_name`: Full country name
- `capital_latitude`, `capital_longitude`: Capital coordinates
- `continent`: Continent classification
- `region`: UN geographic region

**Load:** Hardcoded via `dbt seed`

---

## Fact Table

### `fact_monthly_country_sentiment`
Monthly aggregated AML news sentiment metrics by country.

**Grain:** One row per country per month

**Foreign Keys:**
- `country_code` → `dim_country.country_code`

**Measure Groups:**

#### 1. Volume Metrics
- `unique_news_count`: Number of distinct news articles
- `unique_sources`: Number of distinct news domains
- `total_word_count`: Total words analyzed

#### 2. Tone Metrics - Understanding GDELT Tone

**`avg_tone_overall` (-10 to +10)** ⭐ PRIMARY METRIC
- Overall sentiment score
- **Negative values = Bad news** (e.g., -8 = very negative AML coverage)
- **Positive values = Good news** (e.g., +3 = positive economic news)
- **Use for:** Primary risk indicator

**`avg_tone_positive` (0-100)**
- Percentage of positive words in text
- **Use for:** Identifying optimistic narratives

**`avg_tone_negative` (0-100)** ⭐ SEVERITY METRIC
- Percentage of negative words in text
- **Higher = more severe/harsh language**
- **Use for:** Measuring how bad the bad news is

**`avg_tone_polarity` (0-20+)**
- Absolute difference between positive and negative percentages
- **High values = controversial/polarizing topics**
- **Use for:** Identifying divisive issues

**`avg_tone_activity` (0-100)**
- Action/energy level in text
- **Higher = more dynamic/active language**
- **Use for:** Detecting urgent/breaking situations

#### 3. Distribution Metrics
- `stddev_tone_overall`: Sentiment consistency
- `median_tone_overall`: Middle value (less affected by outliers)
- `p25_tone_overall`, `p75_tone_overall`: Quartiles
- `min_tone_overall`, `max_tone_overall`: Range extremes

#### 4. Derived Risk Metrics

**`risk_intensity_score` (0-100)**
- Converts negative sentiment to risk score
- Formula: `abs(avg_tone_overall) * 10` when negative
- **100 = extreme risk, 0 = no risk**

**`coverage_intensity_score`**
- Log-scaled news volume
- **Higher = more media attention**

**`sentiment_volatility_flag`**
- High/Medium/Low classification
- **High volatility = conflicting narratives**

**`negativity_ratio`**
- Ratio of negative to positive tone
- **> 1 = negative dominates**

**`sentiment_category`**
- Very Negative / Negative / Neutral / Positive / Very Positive

---

## How to Interpret for AML Risk

### High Risk Signals
✅ `avg_tone_overall` < -5 (Very negative sentiment)  
✅ `avg_tone_negative` > 15 (High severity language)  
✅ `risk_intensity_score` > 60  
✅ `unique_news_count` > 10 (High media attention)  
✅ `sentiment_volatility_flag` = 'High' (Evolving situation)

### Example Query: Find Highest Risk Countries This Month
```sql
SELECT 
    d.country_name,
    f.unique_news_count,
    f.avg_tone_overall,
    f.avg_tone_negative,
    f.risk_intensity_score,
    f.sentiment_category
FROM gold.fact_monthly_country_sentiment f
JOIN gold.dim_country d 
    ON f.country_code = d.country_code
WHERE f.month_date = DATE_TRUNC('month', CURRENT_DATE)
    AND f.risk_intensity_score > 50
ORDER BY f.risk_intensity_score DESC
LIMIT 10;
```

---

## Execution

### 1. Load Dimension (One-time)
```bash
dbt seed
```

### 2. Build Fact Table
```bash
# Full pipeline
python /workspace/transformation/scripts/load_staging.py
dbt run --select tag:gdelt      # Build raw vault
dbt run --select tag:gold        # Build gold layer
```

### 3. Incremental Updates
```bash
# After new bronze files arrive
python /workspace/transformation/scripts/load_staging.py
dbt run --select tag:gdelt+      # Rebuild raw vault
dbt run --select tag:gold        # Rebuild fact (full refresh monthly)
```

---

## Performance Notes

DuckDB processes the following operations:
- Multi-table joins (hubs, links, satellites)
- Window functions (percentiles, moving averages)
- Statistical aggregations (stddev, median)
- Date truncation and grouping

Results are materialized to PostgreSQL `gold` schema as regular tables for BI tool consumption.
