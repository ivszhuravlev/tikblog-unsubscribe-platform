import hashlib
import json
from datetime import datetime

from pyspark.sql import functions as F

from analytics.common.config import monitoring, status
from analytics.common.delta import records


def now():
    return datetime.utcnow()


def emit_alerts(spark, cfg, rows, kind):
    alerts = []
    for row in rows:
        if row["status"] not in ("YELLOW", "RED"):
            continue
        name = row.get("metric_name", row.get("check_name"))
        value = row.get("metric_value", row.get("failed_rate"))
        identity = ":".join(
            str(row.get(k))
            for k in (
                "run_id",
                "pipeline_name",
                "source",
                "metric_name",
                "check_name",
                "kafka_topic",
                "kafka_partition",
            )
        )
        item = dict(
            alert_id=hashlib.sha256(identity.encode()).hexdigest(),
            created_at=now(),
            pipeline_name=row["pipeline_name"],
            source=row["source"],
            alert_type=kind,
            severity=row["status"],
            status=row["status"],
            metric_name=name,
            current_value=float(value or 0),
            threshold=row.get("_threshold"),
            message=f"{name}: {value} ({row['status']})",
            run_id=row["run_id"],
        )
        alerts.append(item)
        print("ALERT_STUB " + json.dumps(item, default=str), flush=True)
    records(spark, cfg, "monitoring.alert_events", alerts)


def dq_result(run_id, pipeline, layer, source, check, total, failed, force_red=False):
    thresholds = monitoring()[
        "duplicate_failed_rate" if check.startswith("duplicate_") else "dq_failed_rate"
    ]
    rate = failed / total if total else 0.0
    level = "RED" if force_red else status(rate, thresholds)
    return dict(
        run_id=run_id,
        pipeline_name=pipeline,
        layer=layer,
        source=source or "UNKNOWN",
        check_name=check,
        status=level,
        total_rows=int(total),
        failed_rows=int(failed),
        failed_rate=rate,
        checked_at=now(),
        _threshold=float(thresholds.get(level.lower(), 0)) if level != "GREEN" else None,
    )


def write_dq(spark, cfg, rows):
    records(spark, cfg, "monitoring.dq_results", rows)
    emit_alerts(spark, cfg, rows, "DQ")


def metric(
    run_id, pipeline, source, name, value, unit, start=None, end=None, topic=None, partition=None
):
    choices = monitoring()["metrics"].get(name, {})
    thresholds = choices.get(source, choices.get("default"))
    level = status(value, thresholds)
    return dict(
        observed_at=now(),
        window_start=start,
        window_end=end,
        pipeline_name=pipeline,
        source=source,
        metric_name=name,
        metric_value=float(value) if value is not None else None,
        unit=unit,
        status=level,
        run_id=run_id,
        kafka_topic=topic,
        kafka_partition=partition,
        _threshold=float(thresholds[level.lower()])
        if thresholds and level in ("YELLOW", "RED")
        else None,
    )


def write_metrics(spark, cfg, rows):
    records(spark, cfg, "monitoring.pipeline_metrics", rows)
    emit_alerts(spark, cfg, rows, "METRIC")


def latency_metrics(frame, expression, name, run_id, pipeline):
    rows = (
        frame.groupBy("source")
        .agg(F.percentile_approx(expression, [0.5, 0.95], 10000).alias("values"))
        .collect()
    )
    return [
        metric(run_id, pipeline, r.source, f"{name}_p{percent}_seconds", value, "seconds")
        for r in rows
        for percent, value in zip((50, 95), r["values"], strict=True)
    ]
