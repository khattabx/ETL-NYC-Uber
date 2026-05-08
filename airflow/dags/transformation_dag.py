"""
transformation_dag.py
DAG ID  : uber_transformation_pipeline
Trigger : triggered by uber_ingestion_pipeline
"""

from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

SPARK_SUBMIT = "docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"
SPARK_APPS   = "/opt/spark-apps/jobs"

DEFAULT_ARGS = {
    "owner": "Data Engineering Team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="uber_transformation_pipeline",
    description="Clean + Transform Uber data → Snowflake Star Schema",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # triggered by ingestion DAG
    catchup=False,
    max_active_runs=1,
    tags=["uber", "transformation", "spark", "snowflake"],
) as dag:

    # 1. Clean
    clean = BashOperator(
        task_id="clean_data",
        bash_command=f"{SPARK_SUBMIT} {SPARK_APPS}/clean.py",
        execution_timeout=timedelta(minutes=30),
    )

    # 2. Transform → Snowflake
    transform = BashOperator(
        task_id="transform_to_star",
        bash_command=f"{SPARK_SUBMIT} {SPARK_APPS}/transform.py",
        execution_timeout=timedelta(minutes=30),
    )

    # 3. Success log
    success_log = BashOperator(
        task_id="log_success",
        bash_command="""
            echo "════════════════════════════════"
            echo "  [Done] Transformation Complete"
            echo "  DAG  : {{ dag.dag_id }}"
            echo "  Date : {{ ds }}"
            echo "════════════════════════════════"
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # 4. Failure alert
    fail_alert = BashOperator(
        task_id="alert_on_failure",
        bash_command="""
            echo "════════════════════════════════"
            echo "  [Failed] Transformation FAILED"
            echo "  DAG  : {{ dag.dag_id }}"
            echo "  Date : {{ ds }}"
            echo "════════════════════════════════"
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Flow
    clean >> transform >> success_log
    [clean, transform] >> fail_alert