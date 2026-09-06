from datetime import timedelta

import pendulum
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from analytics_tasks import run_backfill, task_failed

from airflow import DAG

with DAG(
    "analytics_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "layer": Param("gold", enum=["silver", "gold"]),
        "from_date": Param("2026-09-01", type="string", format="date"),
        "to_date": Param("2026-09-05", type="string", format="date"),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": task_failed,
    },
) as dag:
    PythonOperator(task_id="replay_and_rebuild", python_callable=run_backfill)
