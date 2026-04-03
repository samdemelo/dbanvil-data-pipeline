from datetime import datetime
import sys

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = "/mnt/c/Users/Sam/Documents/dbanvil-data-pipeline/dbanvil-data-pipeline"
SRC_ROOT = f"{PROJECT_ROOT}/src"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbanvil"

if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from orchestration.airflow_functions import (
    run_users_raw_load,
    run_diagram_raw_load,
    run_diagram_table_raw_load,
    run_diagram_index_raw_load,
    run_diagram_column_raw_load,
)


@dag(
    dag_id="dbanvil_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbanvil", "snowflake", "dbt", "airflow"],
)
def dbanvil_pipeline():
    @task
    def get_load_timestamp() -> str:
        import pandas as pd
        return pd.Timestamp.utcnow().isoformat()

    @task
    def load_users_task(load_timestamp: str) -> None:
        run_users_raw_load(load_timestamp)

    @task
    def load_diagram_task(load_timestamp: str) -> None:
        run_diagram_raw_load(load_timestamp)

    @task
    def load_diagram_table_task(load_timestamp: str) -> None:
        run_diagram_table_raw_load(load_timestamp)

    @task
    def load_diagram_index_task(load_timestamp: str) -> None:
        run_diagram_index_raw_load(load_timestamp)

    @task
    def load_diagram_column_task(load_timestamp: str) -> None:
        run_diagram_column_raw_load(load_timestamp)

    load_timestamp = get_load_timestamp()

    load_users = load_users_task(load_timestamp)
    load_diagram = load_diagram_task(load_timestamp)
    load_diagram_table = load_diagram_table_task(load_timestamp)
    load_diagram_index = load_diagram_index_task(load_timestamp)
    load_diagram_column = load_diagram_column_task(load_timestamp)

    DBT_PROJECT_DIR = "/mnt/c/Users/Sam/Documents/dbanvil-data-pipeline/dbanvil-data-pipeline/dbanvil"
    DBT_PROFILES_DIR = "/mnt/c/Users/Sam/.dbt"
    DBT_VENV = "/home/sam/dbt-venv-wsl"

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select staging
        """,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select staging
        """,
    )

    dbt_run_core_diagram_table = BashOperator(
        task_id="dbt_run_core_diagram_table",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select core_diagram_table
        """,
    )

    dbt_run_core_diagram_index = BashOperator(
        task_id="dbt_run_core_diagram_index",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select core_diagram_index
        """,
    )

    dbt_run_core_diagram_column = BashOperator(
        task_id="dbt_run_core_diagram_column",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select core_diagram_column
        """,
    )

    dbt_run_core_diagram = BashOperator(
        task_id="dbt_run_core_diagram",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select core_diagram
        """,
    )

    dbt_run_core_users = BashOperator(
        task_id="dbt_run_core_users",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select core_users
        """,
    )

    dbt_test_core = BashOperator(
        task_id="dbt_test_core",
        bash_command=f"""
        source {DBT_VENV}/bin/activate &&
        dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select core
        """,
    )

    [load_users, load_diagram, load_diagram_table, load_diagram_index, load_diagram_column] >> dbt_run_staging >> dbt_test_staging >> dbt_run_core_diagram_table >> dbt_run_core_diagram_index >> dbt_run_core_diagram_column >> dbt_run_core_diagram >> dbt_run_core_users >> dbt_test_core


dbanvil_pipeline()