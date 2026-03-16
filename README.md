# 🌍 GDELT AML Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791.svg)](https://www.postgresql.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.9+-yellow.svg)](https://duckdb.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11+-orange.svg)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8+-red.svg)](https://airflow.apache.org/)
[![Ollama](https://img.shields.io/badge/Ollama-Llama_3.2-purple.svg)](https://ollama.ai/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

> **Worldwide Anti-Money Laundering (AML) news articles powered by AI**  
> A data platform for learning purposes that processes global news events from GDELT, procecess through a well structured transformation pipeline, validates AML relevance using LLMs, and delivers actionable insights through interactive dashboards.

---

## 📡 Data Source

This platform processes data from the **GDELT Project** (Global Database of Events, Language, and Tone) - the largest, most comprehensive, and highest-resolution open database of human society ever created. GDELT monitors print, broadcast, and web news media in over 100 languages from around the world, processing and cataloging events, counts, themes, sources, emotions, locations, and tone from every corner of the planet every 15 minutes.

**Learn more:** [GDELT 2.0: Our Global World in Realtime](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)

---

## 📊 Live Dashboard

**[View Interactive Tableau Dashboard →](https://public.tableau.com/app/profile/michel.gon.alves/viz/GdeltDashboard/GDELTAMLDashboard)**

Track the top 5 most negative AML news globally with real-time sentiment analysis, geographic filtering, and AI-generated summaries.

---

## 🎯 Project Overview

This platform automates the **end-to-end lifecycle** of AML risk intelligence:

1. **Ingests** GDELT Global Knowledge Graph (GKG) data (GDELT publishes every 15 min, we process daily)
2. **Transforms** raw events into a **Data Vault 2.0** model (hubs, links, satellites)
3. **Validates** AML relevance using **Ollama Llama 3.2** (GPU-accelerated)
4. **Aggregates** weekly country-level sentiment metrics
5. **Exports** curated insights to Tableau Public for visualization

**Key Differentiators:**
- ✅ **Data Vault 2.0 Architecture** - Enterprise-grade data modeling for auditability and scalability
- ✅ **AI-Powered Content Validation** - LLM-based filtering eliminates false positives
- ✅ **GPU Acceleration** - NVIDIA RTX 3050 for 10x faster inference
- ✅ **Incremental Processing** - Only new data is processed, optimized for cost and speed
- ✅ **Automated Testing** - dbt data quality tests ensure accuracy

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         GDELT AML DATA PLATFORM                          │
└──────────────────────────────────────────────────────────────────────────┘

📥 INGESTION LAYER (Bronze)
   │
   ├─► GDELT GKG API (15-min intervals)
   │   └─► Downloads: locations, themes, tone, sources
   │
   └─► Storage: Local CSV files (data/bronze/)


🔄 TRANSFORMATION LAYER (Silver - Data Vault 2.0)
   │
   ├─► PostgreSQL Staging (stg_gkg_news)
   │   └─► Parse locations, extract tone metrics
   │
   ├─► DuckDB + postgres_scanner
   │   └─► Incremental Data Vault construction:
   │       ├─ hub_news (unique articles)
   │       ├─ hub_country (unique countries)
   │       ├─ sat_news_tone (sentiment satellites)
   │       └─ link_news_country (many-to-many relationships)
   │
   └─► dbt-duckdb (unified transformation engine)


🤖 AI VALIDATION LAYER
   │
   ├─► Ollama (Llama 3.2:3b) + NVIDIA GPU
   │   ├─ Web scraping (newspaper3k)
   │   ├─ AML relevance classification (Yes/No)
   │   └─ Abstract generation (40-word summaries)
   │
   └─► Watermark tracking (meta.ai_insights_watermark)
       └─► Day-by-day chronological processing


📊 ANALYTICS LAYER (Gold)
   │
   ├─► fact_ai_news_insights (article-level facts)
   │   └─► Columns: news_hkey, event_datetime, source, tone_*, ai_abstract
   │
   ├─► fact_countries (weekly country aggregates)
   │   └─► Metrics: article_count, avg_tone, risk_intensity, sentiment_category
   │
   └─► dim_country (seed data)
       └─► 160+ countries (ISO + FIPS codes)


📤 EXPORT LAYER
   │
   ├─► CSV Export (data/gold/)
   │   └─► Tableau Public data extracts
   │
   └─► PostgreSQL (pg_vault.main_gold)
       └─► Persistent storage with indexes


🔧 ORCHESTRATION
   │
   └─► Apache Airflow (9-task DAG, daily at 2 AM UTC)
       └─► Tasks: download → stage → vault → AI → aggregate → export → test
```

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestration** | Apache Airflow 2.8+ | Workflow scheduling & monitoring |
| **Storage** | PostgreSQL 15 | Persistent Data Vault & Gold layer |
| **Analytics Engine** | DuckDB 0.9+ | High-performance SQL transformations |
| **Transformation** | dbt-duckdb 1.11+ | Data modeling & testing framework |
| **AI/ML** | Ollama (Llama 3.2:3b) | LLM-based content validation & summarization |
| **GPU Acceleration** | NVIDIA CUDA | 10x faster inference (RTX 3050) |
| **Web Scraping** | newspaper3k | Article content extraction |
| **Containerization** | Docker Compose | Reproducible local deployment |
| **Visualization** | Tableau Public | Interactive BI dashboards |
| **Data Source** | GDELT GKG API | Global news event stream |

---

## 📂 Project Structure

```
gdelt_data_vault/
│
├── airflow/
│   └── dags/
│       └── gdelt_data_vault_pipeline.py    # 9-task orchestration DAG
│
├── data/
│   ├── bronze/                             # Raw GDELT CSV files
│   ├── silver/                             # PostgreSQL data directory
│   ├── gold/                               # CSV exports for Tableau
│   └── ollama/                             # LLM model cache
│
├── ingestion/
│   ├── gkg_downloader.py                   # GDELT API client (bronze layer)
│   └── state_table.py                      # Ingestion watermark management
│
├── transformation/
│   ├── models/
│   │   └── silver/ 
│   │       ├── hub_country.sql       # Countries as entities
│   │       ├── hub_news.sql          # News articles as entities
│   │       ├── link_news_country.sql # key foi linking news, tones and countries
│   │       ├── sat_news_tone.sql     # Tone as attribute
│   │       └── schema.yml            # dbt tests & documentation               
│   │   └── gold/
│   │       ├── fact_ai_news_insights.sql   # AI-curated articles
│   │       ├── fact_countries.sql          # Weekly country aggregates
│   │       ├── sources.yml                 # Raw vault source definitions
│   │       └── schema.yml                      # dbt tests & documentation
│   ├── seeds/
│   │   └── dim_country.csv                 # 160+ country reference data
│   ├── scripts/
│   │   └── load_staging.py                 # Bronze → PostgreSQL staging
│   ├── dbt_project.yml                     # dbt configuration
│   └── profiles.yml                        # DuckDB + Postgres connection
│
├── insights/
│   ├── ai_insights_generator.py            # LLM validation pipeline
│   └── test_ai_insights.py                 # AI module tests
│
├── docker-compose.yml                      # Multi-container orchestration
├── Dockerfile.airflow                      # Airflow custom image
├── export_gold_to_csv.py                   # Gold → CSV for Tableau
├── requirements.txt                        # Python dependencies
└── README.md                               # This file
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** with NVIDIA Container Toolkit (for GPU support)
- **NVIDIA GPU** (optional, CPU fallback available)
- **8GB RAM** minimum
- **20GB disk space**

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/sdamc/gdelt_data_vault.git
   cd gdelt_data_vault
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env and set ALL required variables:
   # - POSTGRES_USER, POSTGRES_PASSWORD (required)
   # - POSTGRES_HOST=postgres, POSTGRES_DB=gdelt, POSTGRES_PORT=5432
   # - OLLAMA_HOST=http://ollama:11434, OLLAMA_MODEL=llama3.2:3b
   # - HOST_BIND=127.0.0.1 (localhost-only, change to 0.0.0.0 for network access)
   # - WORKSPACE_ROOT=/workspace
   # - BRONZE_PATH=/workspace/data/bronze
   # - GOLD_PATH=/workspace/data/gold
   ```

3. **Start the platform**
   ```bash
   docker-compose up -d
   ```

4. **Initialize Ollama model**
   ```bash
   docker exec -it ollama ollama pull llama3.2:3b
   ```

5. **Access Airflow UI**
   - URL: `http://localhost:8080`
   - Default credentials: `admin` / `admin`
   - Enable the `gdelt_aml_data_vault` DAG

6. **Trigger the pipeline**
   - Click **"Trigger DAG"** in Airflow UI
   - Monitor task progress in Graph View

### Verify Installation

```bash
# Check GPU availability
docker exec -it ollama nvidia-smi

# Query processed articles
docker exec -it dv_postgres psql -U $POSTGRES_USER -d $POSTGRES_DB
  -c "SELECT COUNT(*) FROM main_gold.fact_ai_news_insights;"

# Check watermark progress
docker exec -it dv_postgres psql -U $POSTGRES_USER -d $POSTGRES_DB
  -c "SELECT * FROM meta.ai_insights_watermark;"
```

---

## ⚙️ Configuration

### Environment Variables

**Security Note:** All environment variables must be set in `.env` file - no hardcoded defaults in code.

| Variable | Example Value | Description |
|----------|---------------|-------------|
| `POSTGRES_HOST` | `postgres` | PostgreSQL container hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `gdelt` | Database name |
| `POSTGRES_USER` | `gdelt_user` | Database username |
| `POSTGRES_PASSWORD` | `your_secure_password` | Database password |
| `OLLAMA_HOST` | `http://ollama:11434` | Ollama API endpoint |
| `OLLAMA_MODEL` | `llama3.2:3b` | LLM model identifier |
| `HOST_BIND` | `127.0.0.1` | Docker port binding (localhost-only for security) |
| `WORKSPACE_ROOT` | `/workspace` | Container workspace path |
| `BRONZE_PATH` | `/workspace/data/bronze` | Bronze layer data directory |
| `GOLD_PATH` | `/workspace/data/gold` | Gold layer CSV export directory |

### Airflow DAG Schedule

Default: **Daily at 2 AM UTC** (`0 2 * * *`)

To change frequency, edit `airflow/dags/gdelt_data_vault_pipeline.py`:
```python
schedule_interval="0 2 * * *",  # Cron expression
```

### Security Configuration

**Port Binding:** By default, all services (PostgreSQL, Airflow, Ollama) are bound to `127.0.0.1` (localhost-only). To expose services to your network, set `HOST_BIND=0.0.0.0` in `.env` file.

**Docker Compose:**
```yaml
ports:
  - "${HOST_BIND:-127.0.0.1}:8080:8080"  # Airflow UI
  - "${HOST_BIND:-127.0.0.1}:5432:5432"  # PostgreSQL
  - "${HOST_BIND:-127.0.0.1}:11434:11434" # Ollama API
```

---

## 📋 Pipeline Details

### Task Breakdown (9 Tasks)

| # | Task ID | Description | Runtime |
|---|---------|-------------|---------|
| 1 | `download_gdelt_gkg` | Download GKG files from GDELT API | ~2-5m |
| 2 | `load_staging_table` | Parse CSV → PostgreSQL staging | ~1-3m |
| 3 | `dbt_seed_dimensions` | Load dim_country seed data | ~5s |
| 4 | `dbt_run_data_vault` | Build Data Vault (incremental) | ~2-4m |
| 5 | `dbt_create_gold_schemas` | Create fact table schemas | ~10s |
| 6 | `generate_ai_insights` | **AI validation + summarization** | ~10-30m/day |
| 7 | `dbt_run_fact_countries` | Aggregate weekly country metrics | ~30s |
| 8 | `export_gold_to_csv` | Export 3 CSV files for Tableau Public | ~15s |
| 9 | `dbt_test_quality` | Run data quality tests | ~30s |

**Total Pipeline Runtime:** ~15-45 minutes (depends on daily news volume)

### AI Insights Generation (Task 6)

**Process:**
1. Query all articles for target date from Data Vault
2. For each article:
   - Fetch full text via web scraping (newspaper3k)
   - Send to Ollama: *"Is this AML-relevant? Yes/No + 40-word summary"*
   - Save validated articles to `fact_ai_news_insights`
3. Update watermark to mark date as processed

**Optimizations:**
- **GPU acceleration**: 10x faster inference (RTX 3050)
- **Incremental processing**: Day-by-day chronological (no reprocessing)
- **Deduplication**: `ON CONFLICT DO NOTHING` prevents duplicates
- **Watermark tracking**: `meta.ai_insights_watermark` ensures progress

**Sample Prompt:**
```
You are analyzing news for AML compliance. Determine if this article is AML-related.

Article: [scraped text]

1. Is this AML-relevant? (Yes/No)
2. If Yes, provide 40-word summary.
```

---

## 🗂️ Data Model

### Data Vault 2.0 (Silver Layer)

**Hubs** (Business Keys)
- `hub_news` - Unique news articles (news_hkey)
- `hub_country` - Unique countries (country_hkey)

**Satellites** (Descriptive Attributes)
- `sat_news_tone` - Sentiment metrics (tone_overall, tone_positive, tone_negative)

**Links** (Relationships)
- `link_news_country` - Many-to-many articles ↔ countries

### Dimensional Model (Gold Layer)

**Facts**
- `fact_ai_news_insights` - Article-level grain (AI-validated AML news)
  - Primary Key: `news_hkey`
  - Columns: `event_datetime`, `source_domain`, `source_url`, `tone_*`, `ai_abstract`

- `fact_countries` - Weekly country aggregates
  - Composite Key: `(week_date, country_name)`
  - Metrics: `article_count`, `avg_tone_overall`, `risk_intensity_score`

**Dimensions**
- `dim_country` - Country reference data (160+ countries)
  - Codes: ISO 3166-1 + FIPS 10-4
  - Attributes: `country_name`, `continent`, `capital_city`, `region`

---

## 🧪 Data Quality & Testing

### dbt Tests

```bash
# Run all tests
docker exec -it airflow dbt test --project-dir /workspace/transformation

# Test specific model
docker exec -it airflow dbt test --select fact_ai_news_insights
```

**Test Coverage:**
- ✅ **Not Null**: `news_hkey`, `event_datetime`, `tone_overall`, `ai_abstract`
- ✅ **Unique**: `news_hkey` (primary key)
- ✅ **Referential Integrity**: Country codes exist in `dim_country`

---

## 🏎️ Performance Optimization

### GPU Acceleration

**Hardware:** NVIDIA RTX 3050 (8GB VRAM)

**Configuration** (`docker-compose.yml`):
```yaml
ollama:
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            count: all
            capabilities: [gpu]
```

**Benchmarks:**
- CPU (Intel i7): ~3 seconds per article
- GPU (RTX 3050): ~0.3 seconds per article
- **Speedup: 10x**

### Database Indexing

**Fact Table Indexes:**
```sql
CREATE INDEX idx_event_datetime ON fact_ai_news_insights(event_datetime DESC);
CREATE INDEX idx_tone_overall ON fact_ai_news_insights(tone_overall);
```

**Query Performance:**
- Top 10 negative news (7 days): **<50ms**
- Weekly country aggregation: **<200ms**

---

## 📈 Monitoring & Observability

### Airflow Metrics

- **DAG Run History**: http://localhost:8080/dags/gdelt_aml_data_vault/grid
- **Task Duration**: Monitor slowest tasks (typically AI generation)
- **Retry Behavior**: Max 2 retries, 5-minute delay

### Database Metrics

```sql
-- Check processing coverage
SELECT 
    last_processed_date,
    updated_at,
    CURRENT_DATE - last_processed_date::date AS days_behind
FROM meta.ai_insights_watermark;

-- Count articles by date
SELECT 
    event_datetime::date AS date,
    COUNT(*) AS articles
FROM main_gold.fact_ai_news_insights
GROUP BY event_datetime::date
ORDER BY date DESC;
```

---

## 🔮 Future Enhancements

- [ ] **Real-time streaming** - Replace batch processing with Kafka + Flink
- [ ] **Multi-model ensembles** - Combine Llama, Mistral, GPT for higher accuracy
- [ ] **Named Entity Recognition** - Extract persons, organizations, locations
- [ ] **Graph analytics** - Network analysis of AML actors
- [ ] **Alerting system** - Email/Slack notifications for high-risk events
- [ ] **Kubernetes deployment** - Production-grade orchestration

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **GDELT Project** - Global event database ([https://www.gdeltproject.org](https://www.gdeltproject.org))
- **Ollama** - Local LLM inference engine ([https://ollama.ai](https://ollama.ai))
- **dbt Labs** - Modern data transformation framework ([https://www.getdbt.com](https://www.getdbt.com))
- **Data Vault 2.0** - Dan Linstedt's modeling methodology

---

**Built with ❤️ for enterprise-grade AML intelligence**
