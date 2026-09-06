from datetime import timedelta

import pendulum
from airflow.operators.python import PythonOperator
from analytics_tasks import evaluate, submit, task_failed
from legal_sensor import LegalOffsetsSensor, acknowledge_sensor

from airflow import DAG

with DAG(
    "analytics_legal_ingestion",
    schedule="@continuous",
    start_date=pendulum.datetime(2026, 9, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": task_failed,
    },
) as dag:
    wait = LegalOffsetsSensor(task_id="wait_for_legal_events")
    bronze = PythonOperator(
        task_id="legal_kafka_to_bronze",
        python_callable=submit,
        op_kwargs={
            "action": "stream",
            "arguments": ["--channel", "legal", "--layer", "kafka_to_bronze"],
        },
    )
    silver = PythonOperator(
        task_id="legal_bronze_to_silver",
        python_callable=submit,
        op_kwargs={
            "action": "stream",
            "arguments": ["--channel", "legal", "--layer", "bronze_to_silver"],
        },
    )
    metrics = PythonOperator(task_id="evaluate_dq_and_metrics", python_callable=acknowledge_sensor)
    alerts = PythonOperator(task_id="evaluate_alerts", python_callable=evaluate)
    wait >> bronze >> silver >> metrics >> alerts
