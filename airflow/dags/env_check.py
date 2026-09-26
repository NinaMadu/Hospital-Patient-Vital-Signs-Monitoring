"""Environment check DAG: proves Airflow can submit a Spark job to the standalone cluster.

Trigger it manually from the Airflow UI after `docker compose up`. It is not part of the pipeline.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="env_check",
    description="Smoke test: Airflow -> Spark cluster -> Parquet lake -> Postgres",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup"],
) as dag:
    show_versions = BashOperator(
        task_id="show_versions",
        bash_command="java -version 2>&1 | head -1 && python --version && spark-submit --version 2>&1 | grep -m1 version",
    )

    spark_env_check = SparkSubmitOperator(
        task_id="spark_env_check",
        conn_id="spark_default",
        application="/opt/project/scripts/spark_env_check.py",
        name="env-check",
        verbose=False,
    )

    show_versions >> spark_env_check
