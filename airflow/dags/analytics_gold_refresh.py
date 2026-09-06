import os
from datetime import timedelta

import pendulum
from airflow.operators.python import PythonOperator
from analytics_tasks import evaluate, find_touched_dates, rebuild_gold_dates, submit, task_failed

from airflow import DAG
from analytics.common.config import settings

with DAG(
    "analytics_gold_refresh",
    schedule=settings()["gold_schedule"],
    start_date=pendulum.parse(os.environ.get("ANALYTICS_START_DATE", "2026-09-05T00:00:00Z")),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": task_failed,
    },
) as dag:
    dates = PythonOperator(task_id="find_touched_dates", python_callable=find_touched_dates)
    rebuild = PythonOperator(task_id="rebuild_gold_dates", python_callable=rebuild_gold_dates)
    dq = PythonOperator(task_id="gold_dq", python_callable=evaluate)
    metrics = PythonOperator(
        task_id="write_metrics", python_callable=submit, op_kwargs={"action": "freshness"}
    )
    alerts = PythonOperator(task_id="evaluate_alerts", python_callable=evaluate)
    dates >> rebuild >> dq >> metrics >> alerts
