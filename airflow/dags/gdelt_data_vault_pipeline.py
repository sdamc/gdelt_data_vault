"""
GDELT AML Data Vault Pipeline
==============================

Orchestrates end-to-end GDELT news sentiment analysis for AML risk assessment.

Architecture:
- Bronze Layer: Raw GDELT GKG CSV files (local storage)
- Staging Layer: PostgreSQL staging table (stg_gkg_news)
- Silver Layer: Data Vault 2.0 (hubs, links, satellites) - DuckDB → PostgreSQL
- Gold Layer: Curated AML insights + aggregated sentiment metrics

Pipeline Flow:
1. Download GDELT GKG files → Bronze CSV
2. Load staging → Parse locations & tone, load to PostgreSQL
3. dbt seed → Load 131-country dimension
4. dbt run (silver) → Build Data Vault incrementally
5. dbt run (gold schemas) → Create fact table structures
6. AI insights generation → Web scrape + LLM validation + summarization
7. dbt run (weekly aggregation) → Aggregate weekly AML sentiment from curated articles
8. Export gold tables → CSV files for Tableau Public
9. dbt test → Validate data quality

Technology Stack:
- PostgreSQL 15: Persistent storage
- DuckDB: Analytical engine with postgres_scanner
- dbt-duckdb: Single adapter for silver + gold
- Python: Bronze layer ingestion + staging ETL + AI insights
- Ollama (Llama 3.2): LLM for AML validation & summarization
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Container paths
WORKSPACE_ROOT = "/workspace"
BRONZE_DIR = f"{WORKSPACE_ROOT}/data/bronze"
DBT_PROJECT_DIR = f"{WORKSPACE_ROOT}/transformation"
DOWNLOAD_SCRIPT = f"{WORKSPACE_ROOT}/ingestion/gkg_downloader.py"
STAGING_SCRIPT = f"{DBT_PROJECT_DIR}/scripts/load_staging.py"
AI_INSIGHTS_SCRIPT = f"{WORKSPACE_ROOT}/insights/ai_insights_generator.py"
EXPORT_CSV_SCRIPT = f"{WORKSPACE_ROOT}/export_gold_to_csv.py"

# dbt command prefix
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"

# Environment variables for tasks that need database access
TASK_ENV = {
    "PYTHONUNBUFFERED": "1",
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.getenv("POSTGRES_DB", "gdelt"),
    "POSTGRES_USER": os.getenv("POSTGRES_USER", "gdelt_user"),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "gdelt_pass"),
    "OLLAMA_HOST": os.getenv("OLLAMA_HOST", "http://ollama:11434"),
    "OLLAMA_MODEL": os.getenv("OLLAMA_MODEL", "llama3.2:3b"),
}

default_args = {
    "owner": "aml_analytics",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="gdelt_aml_data_vault",
    default_args=default_args,
    description="GDELT AML sentiment pipeline with AI-curated insights and weekly aggregations",
    schedule_interval="0 * * * *",  # Every hour at minute 0
    start_date=datetime(2026, 1, 1),  # Start from 2026 (2026-only data)
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "aml", "data_vault", "dbt", "llm"],
)

# Task 1: Download GDELT GKG files to bronze layer (local storage)
download_bronze = BashOperator(
    task_id="download_gdelt_gkg",
    bash_command=f"python {DOWNLOAD_SCRIPT} --output {BRONZE_DIR}",
    env=TASK_ENV,
    dag=dag,
)

# Task 2: Load bronze CSV to PostgreSQL staging table
load_staging = BashOperator(
    task_id="load_staging_table",
    bash_command=f"python {STAGING_SCRIPT}",
    env=TASK_ENV,
    dag=dag,
)

# Task 3: Load country dimension seed 
dbt_seed = BashOperator(
    task_id="dbt_seed_dimensions",
    bash_command=f"{DBT_CMD} seed --select dim_country",
    dag=dag,
)

# Task 4: Build silver layer - Data Vault (hubs, links, satellites)
dbt_run_silver = BashOperator(
    task_id="dbt_run_data_vault",
    bash_command=f"{DBT_CMD} run --select tag:gdelt",
    dag=dag,
)

# Task 5: Build gold layer schemas (creates empty fact tables)
dbt_run_gold_schemas = BashOperator(
    task_id="dbt_create_gold_schemas",
    bash_command=f"{DBT_CMD} run --select fact_ai_news_insights fact_monthly_country_sentiment",
    dag=dag,
)

# Task 6: Generate AI insights (web scraping + LLM validation + summarization)
generate_ai_insights = BashOperator(
    task_id="generate_ai_insights",
    bash_command=f"python {AI_INSIGHTS_SCRIPT}",
    env=TASK_ENV,
    dag=dag,
)

# Task 7: Export gold tables to CSV for Tableau Public
export_gold_csv = BashOperator(
    task_id="export_gold_to_csv",
    bash_command=f"python {EXPORT_CSV_SCRIPT}",
    env=TASK_ENV,
    dag=dag,
)

# Task 8: Run dbt tests
dbt_test = BashOperator(
    task_id="dbt_test_quality",
    bash_command=f"{DBT_CMD} test",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Pipeline dependencies
# Bronze → Staging → Silver → Gold Schemas → AI Insights → CSV Export → Tests
download_bronze >> load_staging >> dbt_seed >> dbt_run_silver >> dbt_run_gold_schemas >> generate_ai_insights >> export_gold_csv >> dbt_test

if __name__ == "__main__":
    dag.test()
