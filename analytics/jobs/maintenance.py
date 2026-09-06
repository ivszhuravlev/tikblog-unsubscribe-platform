from delta.tables import DeltaTable
from pyspark.sql import functions as F

from analytics.common.config import location
from analytics.common.delta import read, retry
from analytics.common.dq import validate_and_merge
from analytics.jobs.gold import refresh


def housekeeping(spark, cfg, step):
    if step == "optimize_silver":
        spark.sql(
            f"OPTIMIZE delta.`{location(cfg, 'silver.unsubscribe_events')}` ZORDER BY (event_id)"
        )
    elif step == "optimize_gold_if_needed" and cfg["optimize_gold"]:
        spark.sql(f"OPTIMIZE delta.`{location(cfg, 'gold.unsubscribe_daily')}`")
    elif step == "enforce_bronze_retention":
        for channel in ("priority", "legal"):
            table = DeltaTable.forPath(spark, location(cfg, f"bronze.unsubscribe_{channel}"))
            # Retain replay data by arrival time, including malformed or late events.
            retry(
                cfg,
                lambda table=table: table.delete(
                    F.col("bronze_ingested_at")
                    < F.current_timestamp()
                    - F.expr(f"INTERVAL {int(cfg['bronze_retention_days'])} DAYS")
                ),
            )
    elif step == "vacuum":
        for table in (
            "bronze.unsubscribe_priority",
            "bronze.unsubscribe_legal",
            "silver.unsubscribe_events",
            "gold.unsubscribe_daily",
        ):
            DeltaTable.forPath(spark, location(cfg, table)).vacuum(cfg["vacuum_retention_hours"])
    elif step == "monitoring_retention_cleanup" and cfg["monitoring_retention_days"]:
        for name, timestamp in [
            ("dq_results", "checked_at"),
            ("dq_quarantine", "quarantined_at"),
            ("pipeline_metrics", "observed_at"),
            ("alert_events", "created_at"),
        ]:
            table = DeltaTable.forPath(spark, location(cfg, f"monitoring.{name}"))
            retry(
                cfg,
                lambda table=table, timestamp=timestamp: table.delete(
                    F.col(timestamp)
                    < F.current_timestamp()
                    - F.expr(f"INTERVAL {int(cfg['monitoring_retention_days'])} DAYS")
                ),
            )


def backfill(spark, cfg, layer, start, end, run_id):
    if start > end:
        raise ValueError("from_date must not exceed to_date")
    if layer == "silver":
        for channel in ("priority", "legal"):
            events = read(spark, cfg, f"bronze.unsubscribe_{channel}").filter(
                F.col("event_date").between(start, end)
            )
            validate_and_merge(spark, cfg, events, f"{run_id}:{channel}", "backfill_silver")
    silver = read(spark, cfg, "silver.unsubscribe_events").filter(
        F.col("event_date").between(start, end)
    )
    gold = read(spark, cfg, "gold.unsubscribe_daily").filter(
        F.col("event_date").between(start, end)
    )
    dates = [
        r.event_date
        for r in silver.select("event_date").union(gold.select("event_date")).distinct().collect()
    ]
    refresh(spark, cfg, dates, run_id)
