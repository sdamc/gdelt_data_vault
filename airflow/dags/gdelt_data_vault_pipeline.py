from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Container paths
WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT", "/workspace")
BRONZE_DIR = os.getenv("BRONZE_PATH", f"{WORKSPACE_ROOT}/data/bronze")
DBT_PROJECT_DIR = f"{WORKSPACE_ROOT}/transformation"
DOWNLOAD_SCRIPT = f"{WORKSPACE_ROOT}/ingestion/gkg_downloader.py"
STAGING_SCRIPT = f"{DBT_PROJECT_DIR}/scripts/load_staging.py"
AI_INSIGHTS_SCRIPT = f"{WORKSPACE_ROOT}/insights/ai_insights_generator.py"
EXPORT_CSV_SCRIPT = f"{WORKSPACE_ROOT}/export_gold_to_csv.py"

# dbt command prefix
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"

# Environment variables for tasks that need database access
# Filter out None values to avoid subprocess errors
TASK_ENV = {
    k: v for k, v in {
        "PYTHONUNBUFFERED": "1",
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "OLLAMA_HOST": os.getenv("OLLAMA_HOST"),
        "OLLAMA_MODEL": os.getenv("OLLAMA_MODEL"),
        "WORKSPACE_ROOT": os.getenv("WORKSPACE_ROOT", "/workspace"),
        "BRONZE_PATH": os.getenv("BRONZE_PATH"),
        "GOLD_PATH": os.getenv("GOLD_PATH"),
    }.items() if v is not None
}

default_args = {
    "owner": "aml_analytics",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="gdelt_aml_data_vault",
    default_args=default_args,
    description="GDELT AML sentiment pipeline with AI-curated insights and weekly aggregations",
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    start_date=datetime(2026, 1, 1),  
    catchup=False,
    max_active_runs=1,
    tags=["gdelt"],
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
    bash_command=f"{DBT_CMD} run --select fact_ai_news_insights",
    dag=dag,
)

# Task 6: Generate AI insights (web scraping + LLM validation + summarization)
generate_ai_insights = BashOperator(
    task_id="generate_ai_insights",
    bash_command=f"python {AI_INSIGHTS_SCRIPT}",
    env=TASK_ENV,
    dag=dag,
)

# Task 7: Build fact_countries (aggregates from AI-curated articles)
dbt_run_fact_countries = BashOperator(
    task_id="dbt_run_fact_countries",
    bash_command=f"{DBT_CMD} run --select fact_countries",
    dag=dag,
)

# Task 8: Export gold tables to CSV for Tableau Public
export_gold_csv = BashOperator(
    task_id="export_gold_to_csv",
    bash_command=f"python {EXPORT_CSV_SCRIPT}",
    env=TASK_ENV,
    dag=dag,
)

# Task 9: Run dbt tests
dbt_test = BashOperator(
    task_id="dbt_test_quality",
    bash_command=f"{DBT_CMD} test",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Pipeline dependencies
# Bronze → Staging → Silver → Gold Schemas → AI Insights → Country Aggregates → CSV Export → Tests
download_bronze >> load_staging >> dbt_seed >> dbt_run_silver >> dbt_run_gold_schemas >> generate_ai_insights >> dbt_run_fact_countries >> export_gold_csv >> dbt_test

if __name__ == "__main__":
    dag.test()
