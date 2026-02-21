# AI News Insights Generator

This module generates AI-powered insights from GDELT news articles using Ollama (running locally in Docker).

## Features

The AI insights generator:
- ✅ Analyzes news from the **last 7 days**
- ✅ Identifies **Top 10 most negative** news by sentiment
- ✅ Identifies **Top 10 most positive** news by sentiment  
- ✅ Finds **most polarizing news** from each continent
- ✅ Fetches full article content from URLs
- ✅ Generates **40-word AI summaries** using Ollama (Llama 3.2)
- ✅ Stores results in gold table: `pg_vault.gold.fact_ai_news_insights`

## Gold Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `news_hkey` | VARCHAR | News article hash key |
| `source_domain` | VARCHAR | News source domain |
| `source_url` | VARCHAR | Full article URL |
| `event_datetime` | TIMESTAMP | When event occurred |
| `category` | VARCHAR | `top_negative`, `top_positive`, `most_polarizing` |
| `continent` | VARCHAR | Continent from seed table |
| `country_name` | VARCHAR | Country name |
| `tone_overall` | FLOAT | Overall sentiment score |
| `tone_positive` | FLOAT | Positive sentiment |
| `tone_negative` | FLOAT | Negative sentiment |
| `tone_polarity` | FLOAT | Polarity score |
| `report_count` | INTEGER | Number of reports for this URL |
| `ai_abstract` | TEXT | **AI-generated 40-word summary** |
| `generated_at` | TIMESTAMP | When insight was generated |

## Setup

### 1. Start Services

```bash
docker-compose up -d
```

### 2. Download Ollama Model

```bash
bash setup_ollama.sh
```

Or manually:
```bash
docker exec ollama ollama pull llama3.2:3b
```

### 3. Create Gold Table

```bash
cd transformation
dbt run --select fact_ai_news_insights
```

## Usage

### Generate Insights

Run the Python script inside the Airflow container (has all dependencies):

```bash
docker exec -it airflow python /workspace/ingestion/ai_news_insights.py
```

### Query Results

```sql
-- View latest insights
SELECT 
    category,
    continent,
    source_domain,
    tone_overall,
    ai_abstract,
    source_url
FROM pg_vault.gold.fact_ai_news_insights
ORDER BY generated_at DESC
LIMIT 20;

-- Top negative news
SELECT 
    source_domain,
    ROUND(tone_overall::NUMERIC, 2) as sentiment,
    ai_abstract,
    source_url
FROM pg_vault.gold.fact_ai_news_insights
WHERE category = 'top_negative'
ORDER BY tone_overall ASC;

-- Most polarizing by continent
SELECT 
    continent,
    source_domain,
    ROUND(tone_polarity::NUMERIC, 2) as polarity,
    ai_abstract
FROM pg_vault.gold.fact_ai_news_insights
WHERE category = 'most_polarizing'
ORDER BY continent, ABS(tone_polarity) DESC;
```

## How It Works

1. **Query Selection**: Queries `sat_news_tone` joined with country/continent data
2. **Filtering**: Applies last 7 days filter and category logic (top 10 negative/positive, most polarizing per continent)
3. **Fetch Content**: Downloads article content using `newspaper3k` + `BeautifulSoup` fallback
4. **AI Generation**: Calls Ollama API to generate 40-word summaries
5. **Storage**: Inserts results into `fact_ai_news_insights` gold table

## Data Flow

```
sat_news_tone (Silver)
    ↓
link_news_country
    ↓
hub_country → dim_country (continent mapping)
    ↓
Python Script:
  - Fetch URLs
  - Download articles
  - Generate AI abstracts
    ↓
fact_ai_news_insights (Gold)
```

## Configuration

Edit `ingestion/ai_news_insights.py` to customize:

- **Model**: Change `llama3.2:3b` to other Ollama models
- **Word count**: Adjust `max_words` parameter
- **Days back**: Modify `INTERVAL '7 days'`
- **Top N**: Change `LIMIT 10` in queries
- **Temperature**: Adjust AI creativity (0-1)

## Troubleshooting

### Ollama not responding
```bash
docker logs ollama
docker restart ollama
```

### Model not found
```bash
docker exec ollama ollama list
docker exec ollama ollama pull llama3.2:3b
```

### Article fetch failures
- Many 2016 URLs may be dead/broken
- Some sites block scrapers
- Script includes fallback logic for failed fetches

### Slow performance
- Uses smaller model (3B parameters) for speed
- Add rate limiting: `time.sleep(2)` between requests
- Process in batches

## Scheduling

To run daily, add to Airflow DAG:

```python
from airflow.operators.bash import BashOperator

generate_insights = BashOperator(
    task_id='generate_ai_insights',
    bash_command='python /workspace/ingestion/ai_news_insights.py',
    dag=dag
)
```

## Resources

- **CPU**: Llama 3.2 3B can run on CPU (slower)
- **GPU**: Add GPU support to Ollama container for 10-100x speedup
- **Memory**: ~4GB RAM for model + processing
- **Disk**: Models stored in `./data/ollama/`

## Results

Expected output:
- ~30 articles analyzed (10 negative + 10 positive + ~10 polarizing)
- Processing time: ~2-5 minutes (CPU), ~30-60 seconds (GPU)
- Storage: ~5-10KB per insight
