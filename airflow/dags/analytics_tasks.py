"""Airflow calls the shared writer; it never creates a Spark driver."""

import hashlib
import json
import logging
import os
from pathlib import Path
from urllib.request import Request, urlopen


def call_writer(path, payload):
    url = os.environ.get("ANALYTICS_WRITER_URL", "http://analytics-driver:8088")
    request = Request(
        url + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=3600) as response:
        return json.loads(response.read())


def submit(action, arguments=None, **context):
    payload = {"action": action, "run_id": context["run_id"]}
    args = arguments or []
    for index in range(0, len(args), 2):
        payload[args[index].removeprefix("--").replace("-", "_")] = args[index + 1]
    return call_writer("/run", payload)


def find_touched_dates(**context):
    return call_writer(
        "/run",
        {
            "action": "touched",
            "run_id": context["run_id"],
            "start": context["data_interval_start"].isoformat(),
            "end": context["data_interval_end"].isoformat(),
        },
    )


def rebuild_gold_dates(**context):
    dates = context["ti"].xcom_pull(task_ids="find_touched_dates")
    call_writer("/run", {"action": "gold", "run_id": context["run_id"], "dates": dates})


def run_backfill(**context):
    params = context["params"]
    call_writer(
        "/run",
        {
            "action": "backfill",
            "run_id": context["run_id"],
            "layer": params["layer"],
            "start": params["from_date"],
            "end": params["to_date"],
        },
    )


def evaluate(**context):
    logging.info(
        "Analytics run summary: %s", call_writer("/summary", {"run_id": context["run_id"]})
    )


def task_failed(context):
    identity = f"{context['run_id']}:{context['task_instance'].task_id}"
    payload = {
        "run_id": context["run_id"],
        "pipeline_name": context["dag"].dag_id,
        "alert_id": hashlib.sha256(identity.encode()).hexdigest(),
        "message": str(context.get("exception", "Airflow task failed"))[:2000],
    }
    logging.error("ALERT_STUB %s", json.dumps(payload))
    spool = Path("/app/alert-spool")
    spool.mkdir(parents=True, exist_ok=True)
    row = dict(
        payload,
        source="ALL",
        alert_type="AIRFLOW_TASK_FAILURE",
        severity="RED",
        status="RED",
        metric_name="task_failure",
        current_value=1.0,
        threshold=1.0,
    )
    temporary = spool / (payload["alert_id"] + ".tmp")
    temporary.write_text(json.dumps(row))
    temporary.replace(spool / (payload["alert_id"] + ".json"))
    try:
        call_writer("/failure", payload)
    except Exception:
        logging.exception("ALERT_STUB persistence failed: shared writer unavailable")
