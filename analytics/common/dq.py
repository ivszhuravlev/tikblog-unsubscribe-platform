"""Shared inline Bronze -> Silver validation for both ingestion paths and replay."""

from pyspark.sql import functions as F

from analytics.common.delta import insert_once, read, silver_merge
from analytics.common.observability import dq_result, latency_metrics, write_dq, write_metrics
from analytics.common.tables import EVENT
from analytics.common.writer import silver_gold_lock


def validate_and_merge(spark, cfg, batch, run_id, pipeline):
    with silver_gold_lock:
        return _validate_and_merge(spark, cfg, batch, run_id, pipeline)


def _validate_and_merge(spark, cfg, batch, run_id, pipeline):
    if batch.isEmpty():
        return
    expected = spark.createDataFrame([], EVENT).schema
    actual = {f.name: f.dataType for f in batch.schema.fields}
    required = {f.name for f in expected.fields}
    target = read(spark, cfg, "silver.unsubscribe_events").schema
    incompatible = [
        f.name
        for f in target.fields
        if (f.name in actual and actual[f.name] != f.dataType)
        or (f.name in required and f.name not in actual)
    ]
    expected = target
    if incompatible:
        # Do not cast incompatible fields into accepted values. Preserve the raw
        # row for diagnosis, with typed NULLs in the standard quarantine columns.
        batch = batch.withColumn("raw_value", F.to_json(F.struct("*")))
        for field in expected.fields:
            if actual.get(field.name) != field.dataType:
                batch = batch.withColumn(field.name, F.lit(None).cast(field.dataType))
        batch = batch.withColumn(
            "decode_error", F.lit("schema_type_mismatch:" + ",".join(incompatible))
        )
    checks = {
        "event_id_required": F.col("event_id").isNotNull() & (F.trim("event_id") != ""),
        "user_id_required": F.col("user_id").isNotNull() & (F.trim("user_id") != ""),
        "writer_id_required": F.col("writer_id").isNotNull() & (F.trim("writer_id") != ""),
        "source_valid": F.col("source").isin("UI", "CUSTOMER_SUCCESS", "LEGAL"),
        "requested_at_required": F.col("requested_at").isNotNull(),
        "requested_at_not_future": F.col("requested_at") <= F.current_timestamp(),
        "schema_and_decode": F.col("decode_error").isNull(),
    }
    failures = [
        F.when(~F.coalesce(rule, F.lit(False)), F.lit(name)) for name, rule in checks.items()
    ]
    checked = batch.withColumn(
        "failed_checks", F.filter(F.array(*failures), lambda x: x.isNotNull())
    )
    checked = checked.withColumn("source", F.coalesce("source", F.lit("UNKNOWN"))).cache()
    try:
        summaries = (
            checked.groupBy("source")
            .agg(
                F.count("*").alias("total"),
                *[
                    F.sum(F.array_contains("failed_checks", name).cast("long")).alias(name)
                    for name in checks
                ],
            )
            .collect()
        )
        results = [
            dq_result(
                run_id,
                pipeline,
                "SILVER",
                r.source,
                name,
                r.total,
                r[name],
                force_red=name == "schema_and_decode" and r[name] > 0,
            )
            for r in summaries
            for name in checks
        ]
        quarantine = checked.filter(F.size("failed_checks") > 0).select(
            F.lit(run_id).alias("run_id"),
            F.lit(pipeline).alias("pipeline_name"),
            "source",
            "event_id",
            "user_id",
            "writer_id",
            "requested_at",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "failed_checks",
            F.concat_ws(",", "failed_checks").alias("failure_reason"),
            F.current_timestamp().alias("quarantined_at"),
            "raw_value",
        )
        insert_once(spark, cfg, "monitoring.dq_quarantine", quarantine)
        valid = checked.filter(F.size("failed_checks") == 0).drop("failed_checks")
        # Event ID is the only deduplication key. Reader/writer pairs are retained.
        unique = valid.dropDuplicates(["event_id"])
        stats = (
            valid.groupBy("source")
            .agg(F.count("*").alias("total"), F.countDistinct("event_id").alias("unique"))
            .collect()
        )
        for r in stats:
            results.append(
                dq_result(
                    run_id,
                    pipeline,
                    "SILVER",
                    r.source,
                    "duplicate_event_id_in_batch",
                    r.total,
                    r.total - r["unique"],
                )
            )
        existing = read(spark, cfg, "silver.unsubscribe_events").select("event_id")
        already = {
            r.source: r["count"]
            for r in unique.join(existing, "event_id", "left_semi")
            .groupBy("source")
            .count()
            .collect()
        }
        for r in stats:
            results.append(
                dq_result(
                    run_id,
                    pipeline,
                    "SILVER",
                    r.source,
                    "duplicate_event_id_already_in_silver",
                    r["unique"],
                    already.get(r.source, 0),
                )
            )
        # Record pre-MERGE checks first; retry inserts preserve the first results.
        write_dq(spark, cfg, results)
        prepared = unique.withColumn("silver_processed_at", F.current_timestamp()).cache()
        try:
            prepared.count()  # Freeze timestamps across MERGE and metrics actions.
            silver_merge(spark, cfg, prepared)
            metrics = latency_metrics(
                prepared,
                F.col("silver_processed_at").cast("double")
                - F.col("bronze_ingested_at").cast("double"),
                "processing",
                run_id,
                pipeline,
            )
            write_metrics(spark, cfg, metrics)
        finally:
            prepared.unpersist()
    finally:
        checked.unpersist()
