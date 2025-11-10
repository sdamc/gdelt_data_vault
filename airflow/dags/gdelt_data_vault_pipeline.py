"""DAG: GDELT Data Vault pipeline.

Steps:
1. Download raw GKG files (bronze)
2. Optional bronze transform
3. Load staging table (public.staging_gkg_corruption)
4. dbt deps (conditional)
5. dbt run hub_event + sat_event (incremental)
6. dbt tests (optional)
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Base paths inside container
WORKSPACE_DIR_ROOT = "/workspace"
DBT_PROJECT_DIR = f"{WORKSPACE_DIR_ROOT}/workspace"
SCRIPTS_DIR = f"{DBT_PROJECT_DIR}/scripts"
RAW_BRONZE_DIR = f"{DBT_PROJECT_DIR}/data/raw/bronze"

# Use DBT_PROJECT_DIR for dbt commands and set profiles dir there.
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && DBT_PROFILES_DIR={DBT_PROJECT_DIR}"

default_args = {
    "owner": "data_vault",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="gdelt_data_vault_pipeline",
    default_args=default_args,
    description="Orchestrates GDELT ingestion -> staging -> Data Vault (hub/sat) with dbt",
    schedule_interval="@daily",  # adjust to desired cadence
    start_date=datetime(2025, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "data_vault"],
)

# 1. Download raw GKG files (bronze)

download_raw = BashOperator(
    task_id="download_raw_gkg",
    bash_command=f"python {DBT_PROJECT_DIR}/download_gkg_files.py --output {RAW_BRONZE_DIR} || true",
    env={"PYTHONUNBUFFERED": "1"},
    dag=dag,
)

# 2. Bronze transform (optional)
bronze_transform = BashOperator(
    task_id="bronze_transform_optional",
    bash_command=f"python {SCRIPTS_DIR}/transform_bronze_gkg_aml.py --input {RAW_BRONZE_DIR} --output {RAW_BRONZE_DIR} || echo 'Skip transform'",
    env={"PYTHONUNBUFFERED": "1"},
    dag=dag,
)

# 3. Load staging table
load_staging = BashOperator(
    task_id="load_staging_table",
    bash_command=f"python {SCRIPTS_DIR}/load_staging_gkg_corruption.py",
    env={"PYTHONUNBUFFERED": "1"},
    dag=dag,
)

# 4. dbt deps (conditional)
dbt_deps = BashOperator(
    task_id="dbt_deps",
    bash_command=f"if [ -d {DBT_PROJECT_DIR}/dbt_packages/dbt_utils ]; then echo 'Skip dbt deps (already installed)'; else {DBT_CMD_PREFIX} dbt deps; fi",
    dag=dag,
)

# 5. dbt run hub + sat
dbt_run_vault = BashOperator(
    task_id="dbt_run_vault_models",
    bash_command=(
        "mkdir -p /tmp/dbt_logs /tmp/dbt_target && "
        f"cd {DBT_PROJECT_DIR} && "
        f"DBT_LOG_PATH=/tmp/dbt_logs DBT_TARGET_PATH=/tmp/dbt_target DBT_PROFILES_DIR={DBT_PROJECT_DIR} "
        f"dbt run --select hub_event sat_event"
    ),
    dag=dag,
)

# 6. dbt tests (optional)
dbt_test = BashOperator(
    task_id="dbt_tests_optional",
    bash_command=f"{DBT_CMD_PREFIX} dbt test --select source:staging.staging_gkg_corruption hub_event sat_event || true",
    trigger_rule=TriggerRule.ALL_DONE,  # run even if previous tasks failed (adjust if desired)
    dag=dag,
)

# Simple linear flow for now
download_raw >> bronze_transform >> load_staging >> dbt_deps >> dbt_run_vault >> dbt_test

if __name__ == "__main__":
    dag.test()  # allows 'python gdelt_data_vault_pipeline.py' local debugging in Airflow 2.7+
