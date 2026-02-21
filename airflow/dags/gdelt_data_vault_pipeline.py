"""
GDELT AML Data Vault Pipeline
==============================

Orchestrates end-to-end GDELT news sentiment analysis for AML risk assessment.

Architecture:
- Bronze Layer: Raw GDELT GKG CSV files (local storage)
- Staging Layer: PostgreSQL staging table (stg_gkg_news)
- Silver Layer: Data Vault 2.0 (hubs, links, satellites) - DuckDB → PostgreSQL
- Gold Layer: Dimensional star schema (dim_country + fact_monthly_country_sentiment)

Pipeline Flow:
1. Download GDELT GKG files → Bronze CSV
2. Load staging → Parse locations & tone, load to PostgreSQL
3. dbt seed → Load 131-country dimension
4. dbt run (silver) → Build Data Vault incrementally
5. dbt run (gold) → Aggregate monthly AML sentiment metrics
6. dbt test → Validate data quality

Technology Stack:
- PostgreSQL 15: Persistent storage
- DuckDB: Analytical engine with postgres_scanner
- dbt-duckdb: Single adapter for silver + gold
- Python: Bronze layer ingestion + staging ETL
"""
from __future__ import annotations

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

# dbt command prefix
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"

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
    description="GDELT AML sentiment pipeline: Bronze → Staging → Silver (Data Vault) → Gold (Star Schema)",
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "aml", "data_vault", "dbt"],
)

# Task 1: Download GDELT GKG files to bronze layer (local storage)
download_bronze = BashOperator(
    task_id="download_gdelt_gkg",
    bash_command=f"python {DOWNLOAD_SCRIPT} --output {BRONZE_DIR}",
    env={"PYTHONUNBUFFERED": "1"},
    dag=dag,
)

# Task 2: Load bronze CSV to PostgreSQL staging table
load_staging = BashOperator(
    task_id="load_staging_table",
    bash_command=f"python {STAGING_SCRIPT}",
    env={"PYTHONUNBUFFERED": "1"},
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

# Task 5: Build gold layer - Monthly AML sentiment fact table
dbt_run_gold = BashOperator(
    task_id="dbt_run_gold_analytics",
    bash_command=f"{DBT_CMD} run --select tag:gold",
    dag=dag,
)

# Task 6: Run dbt tests
dbt_test = BashOperator(
    task_id="dbt_test_quality",
    bash_command=f"{DBT_CMD} test",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Pipeline dependencies 
load_staging >> dbt_seed >> dbt_run_silver >> dbt_run_gold >> dbt_test

if __name__ == "__main__":
    dag.test()
