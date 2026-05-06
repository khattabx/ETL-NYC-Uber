from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

HADOOP_SCRIPTS = "/opt/airflow/hadoop/scripts"

DEFAULT_ARGS = {
    "owner": "Data Engineering Team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

with DAG(
    dag_id="uber_ingestion_pipeline",
    description="yellow/green_tripdata_YYYY-MM.parquet → HDFS every 15 min",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["uber", "ingestion", "hdfs", "raw-layer"],
) as dag:

    # 1. Ingest 
    ingest = BashOperator(
        task_id="ingest_to_hdfs",
        bash_command=(
            "bash /opt/airflow/hadoop/scripts/ingest_to_hdfs.sh "
            "{{ ds }} "
            "{{ execution_date.strftime('%H') }} "
            "{{ execution_date.strftime('%M') }}"
        ),
        env={"HADOOP_USER_NAME": "hadoop"},
        execution_timeout=timedelta(minutes=10),
    )

    # 2. Validate
    validate = BashOperator(
        task_id="validate_hdfs",
        bash_command="python3 /opt/airflow/hadoop/scripts/validate_hdfs.py {{ ds }}",
        env={"HADOOP_USER_NAME": "hadoop"},
        execution_timeout=timedelta(minutes=5),
    )

    # 3. Success log
    success_log = BashOperator(
        task_id="log_success",
        bash_command="""
            echo "════════════════════════════════"
            echo "  [Done] Run Complete"
            echo "  DAG  : {{ dag.dag_id }}"
            echo "  Date : {{ ds }}"
            echo "  Time : {{ execution_date.strftime('%H:%M') }}"
            echo "════════════════════════════════"
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # 4. Failure alert
    fail_alert = BashOperator(
        task_id="alert_on_failure",
        bash_command="""
            echo "════════════════════════════════"
            echo "  [Failed] Pipeline FAILED"
            echo "  DAG  : {{ dag.dag_id }}"
            echo "  Date : {{ ds }}"
            echo "  Time : {{ execution_date.strftime('%H:%M') }}"
            echo "════════════════════════════════"
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Flow 
    ingest >> validate >> success_log
    [ingest, validate] >> fail_alert