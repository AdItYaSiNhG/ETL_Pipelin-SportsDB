"""
Apache Airflow DAG: TheSportsDB end-to-end ETL.

Tasks:
  1. ingest_thesportsdb_data   - Python ingestion  -> MinIO raw-zone
  2. duckdb_process_data       - DuckDB transform  -> MinIO analytics-zone
  3. dbt_deps                  - Install dbt packages
  4. dbt_run_models            - Build dbt models in DuckDB
  5. dbt_test_models           - Run dbt tests
  6. analytics_zone_health_check - Sanity check on Parquet output
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

ENV_VARS = {
    "THESPORTSDB_API_KEY": os.getenv("THESPORTSDB_API_KEY", "1"),
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    "MINIO_ENDPOINT": "http://minio:9000",
    "S3_ENDPOINT": "minio:9000",
    "S3_USE_SSL": "false",
    "S3_URL_STYLE": "path",
    "S3_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "minioadmin"),
    "S3_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    "PYTHONUNBUFFERED": "1",
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "HOME": "/home/airflow",
}


def analytics_health_check(**_context) -> None:
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    expected = [
        "cricket/leagues_clean.parquet",
        "cricket/teams_clean.parquet",
        "cricket/events_clean.parquet",
    ]
    found = []
    for key in expected:
        try:
            s3.head_object(Bucket="analytics-zone", Key=key)
            found.append(key)
        except Exception as exc:
            print(f"Missing: s3://analytics-zone/{key} ({exc})")
    if not found:
        raise ValueError("No Parquet outputs found in analytics-zone/cricket/. DuckDB step may have failed.")
    print(f"Health check OK. Found {len(found)}/{len(expected)} expected outputs: {found}")


with DAG(
    dag_id="thesportsdb_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end ETL for TheSportsDB: ingest -> DuckDB -> dbt.",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["thesportsdb", "etl", "duckdb", "dbt", "minio", "cricket"],
) as dag:
    ingest_task = BashOperator(
        task_id="ingest_thesportsdb_data",
        bash_command="python /opt/airflow/scripts/ingest_thesportsdb.py",
        env=ENV_VARS,
    )

    duckdb_task = BashOperator(
        task_id="duckdb_process_data",
        bash_command=(
            "python -W ignore /opt/airflow/scripts/duckdb_process.py"
        ),
        env=ENV_VARS,
    )

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "export PATH=/home/airflow/.local/bin:$PATH && "
            "mkdir -p /tmp/dbt/duckdb && "
            "cd /opt/airflow/dbt_project/sports_analytics && "
            "dbt deps --profiles-dir /opt/airflow/dbt_project"
        ),
        env=ENV_VARS,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run_models",
        bash_command=(
            "export PATH=/home/airflow/.local/bin:$PATH && "
            "mkdir -p /tmp/dbt/duckdb && "
            "cd /opt/airflow/dbt_project/sports_analytics && "
            "dbt run --profiles-dir /opt/airflow/dbt_project"
        ),
        env=ENV_VARS,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test_models",
        bash_command=(
            "export PATH=/home/airflow/.local/bin:$PATH && "
            "mkdir -p /tmp/dbt/duckdb && "
            "cd /opt/airflow/dbt_project/sports_analytics && "
            "dbt test --profiles-dir /opt/airflow/dbt_project"
        ),
        env=ENV_VARS,
    )

    health_check_task = PythonOperator(
        task_id="analytics_zone_health_check",
        python_callable=analytics_health_check,
    )

    ingest_task >> duckdb_task >> dbt_deps_task >> dbt_run_task >> dbt_test_task >> health_check_task
