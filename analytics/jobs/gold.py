from datetime import timedelta

from pyspark.sql import functions as F

from analytics.common.config import location
from analytics.common.delta import read, retry
from analytics.common.observability import (
    dq_result,
    latency_metrics,
    metric,
    write_dq,
    write_metrics,
)
from analytics.common.writer import silver_gold_lock


def touched(spark, cfg, start, end):
    # A small overlap heals commits whose processing timestamp precedes commit
    # visibility. Airflow catchup also replays missed scheduled intervals.
    start = start - timedelta(minutes=cfg["gold_lookback_minutes"])
    silver = read(spark, cfg, "silver.unsubscribe_events")
    interval_dates = silver.filter(
        (F.col("silver_processed_at") >= F.lit(start)) & (F.col("silver_processed_at") < F.lit(end))
    ).select("event_date")
    # Repair missed runs without rebuilding unaffected Gold partitions. Together
    # with the local Silver/Gold critical section, this also covers slow commits.
    latest = silver.groupBy("event_date", "source").agg(
        F.max("silver_processed_at").alias("silver_latest")
    )
    gold = read(spark, cfg, "gold.unsubscribe_daily")
    pending = latest.join(gold, ["event_date", "source"], "left").filter(
        F.col("gold_updated_at").isNull() | (F.col("silver_latest") > F.col("gold_updated_at"))
    )
    return [
        r.event_date
        for r in interval_dates.union(pending.select("event_date")).distinct().collect()
    ]


def refresh(spark, cfg, dates, run_id):
    with silver_gold_lock:
        return _refresh(spark, cfg, dates, run_id)


def _refresh(spark, cfg, dates, run_id):
    pipeline = "gold_refresh"
    if not dates:
        return
    events = (
        read(spark, cfg, "silver.unsubscribe_events")
        .filter(F.col("event_date").isin(dates))
        .cache()
    )
    try:
        counts = events.groupBy("event_date", "source").agg(
            F.count("*").alias("unsubscribe_count"),
            F.countDistinct("user_id").alias("unique_readers"),
            F.countDistinct("writer_id").alias("unique_writers"),
        )
        gold = counts.withColumn("gold_updated_at", F.current_timestamp()).cache()
        try:
            gold.count()
            # Replacing whole touched partitions also removes stale groups.
            predicate = "event_date IN (" + ",".join(f"DATE '{d.isoformat()}'" for d in dates) + ")"
            retry(
                cfg,
                lambda: (
                    gold.write.format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", predicate)
                    .save(location(cfg, "gold.unsubscribe_daily"))
                ),
            )
            actual = read(spark, cfg, "gold.unsubscribe_daily").filter(
                F.col("event_date").isin(dates)
            )
            joined = actual.alias("g").join(counts.alias("s"), ["event_date", "source"], "full")
            checks = {
                "nonnegative_count": F.col("g.unsubscribe_count") >= 0,
                "reader_bound": F.col("g.unique_readers") <= F.col("g.unsubscribe_count"),
                "writer_bound": F.col("g.unique_writers") <= F.col("g.unsubscribe_count"),
                "silver_reconciliation": F.col("g.unsubscribe_count")
                == F.col("s.unsubscribe_count"),
            }
            results = []
            for r in (
                joined.groupBy("source")
                .agg(
                    F.count("*").alias("total"),
                    *[
                        F.sum((~F.coalesce(rule, F.lit(False))).cast("long")).alias(name)
                        for name, rule in checks.items()
                    ],
                )
                .collect()
            ):
                results.extend(
                    dq_result(run_id, pipeline, "GOLD", r.source, name, r.total, r[name])
                    for name in checks
                )
            ready = events.join(
                actual.select("event_date", "source", "gold_updated_at"), ["event_date", "source"]
            )
            metrics = latency_metrics(
                ready,
                F.col("gold_updated_at").cast("double") - F.col("requested_at").cast("double"),
                "e2e",
                run_id,
                pipeline,
            )
            write_dq(spark, cfg, results)
            write_metrics(spark, cfg, metrics)
        finally:
            gold.unpersist()
    finally:
        events.unpersist()


def freshness(spark, cfg, run_id):
    # Measure unresolved new Silver rows, not old unchanged historical partitions.
    silver = read(spark, cfg, "silver.unsubscribe_events")
    gold = read(spark, cfg, "gold.unsubscribe_daily")
    pending = silver.join(gold, ["event_date", "source"], "left").filter(
        F.col("gold_updated_at").isNull()
        | (F.col("silver_processed_at") > F.col("gold_updated_at"))
    )
    pending_age = {
        r.source: float(r.age or 0)
        for r in pending.groupBy("source")
        .agg(F.max(F.unix_timestamp() - F.unix_timestamp("silver_processed_at")).alias("age"))
        .collect()
    }
    rows = [
        metric(
            run_id,
            "gold_refresh",
            source,
            "gold_freshness_seconds",
            pending_age.get(source, 0),
            "seconds",
        )
        for source in ("UI", "CUSTOMER_SUCCESS", "LEGAL")
    ]
    write_metrics(spark, cfg, rows)
    write_dq(
        spark,
        cfg,
        [
            dict(
                dq_result(
                    run_id,
                    "gold_refresh",
                    "GOLD",
                    r["source"],
                    "dashboard_freshness",
                    1,
                    int(r["status"] in ("YELLOW", "RED")),
                ),
                status=r["status"],
            )
            for r in rows
        ],
    )
