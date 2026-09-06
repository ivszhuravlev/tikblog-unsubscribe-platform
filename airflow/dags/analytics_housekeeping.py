from datetime import timedelta

import pendulum
from airflow.operators.python import PythonOperator
from analytics_tasks import submit, task_failed

from airflow import DAG

with DAG(
    "analytics_housekeeping",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": task_failed,
    },
) as dag:
    previous = None
    for name in (
        "optimize_silver",
        "optimize_gold_if_needed",
        "enforce_bronze_retention",
        "vacuum",
        "monitoring_retention_cleanup",
    ):
        task = PythonOperator(
            task_id=name,
            python_callable=submit,
            op_kwargs={"action": "housekeeping", "arguments": ["--step", name]},
        )
        if previous:
            previous >> task
        previous = task
