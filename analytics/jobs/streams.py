import json

from confluent_kafka import Consumer, TopicPartition

from analytics.common.avro import decode
from analytics.common.config import checkpoint, location
from analytics.common.delta import append_bronze
from analytics.common.dq import validate_and_merge
from analytics.common.observability import metric, write_metrics


def report_progress(spark, cfg, pipeline, progress):
    run_id = f"{progress['id']}:{progress['batchId']}"
    source = "LEGAL" if pipeline.startswith("legal") else "PRIORITY_SHARED"
    rows = [
        metric(
            run_id,
            pipeline,
            source,
            "microbatch_duration_ms",
            progress.get("durationMs", {}).get("triggerExecution", 0),
            "milliseconds",
        ),
        metric(
            run_id,
            pipeline,
            source,
            "configured_executor_count",
            cfg["executor_instances"],
            "executors",
        ),
        metric(
            run_id,
            pipeline,
            source,
            "active_executor_count",
            max(0, spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1),
            "executors",
        ),
    ]
    if pipeline.endswith("kafka_to_bronze"):
        consumer = Consumer(
            {
                "bootstrap.servers": cfg["kafka_bootstrap"],
                "group.id": "analytics-lag-probe",
                "enable.auto.commit": False,
            }
        )
        try:
            for item in progress.get("sources", []):
                offsets = item.get("endOffset")
                if isinstance(offsets, str):
                    offsets = json.loads(offsets)
                for topic, partitions in (offsets or {}).items():
                    rows.append(
                        metric(
                            run_id,
                            pipeline,
                            source,
                            "kafka_partition_count",
                            len(partitions),
                            "partitions",
                            topic=topic,
                        )
                    )
                    for partition, processed in partitions.items():
                        _, latest = consumer.get_watermark_offsets(
                            TopicPartition(topic, int(partition)), timeout=10, cached=False
                        )
                        # Both values are exclusive next offsets, avoiding off-by-one lag.
                        rows.append(
                            metric(
                                run_id,
                                pipeline,
                                source,
                                "kafka_lag",
                                max(0, latest - int(processed)),
                                "records",
                                topic=topic,
                                partition=int(partition),
                            )
                        )
        finally:
            consumer.close()
    write_metrics(spark, cfg, rows)


def run(spark, cfg, channel, layer, available=False, wait=True):
    pipeline = f"{channel}_{layer}"
    bronze = f"bronze.unsubscribe_{channel}"
    identity = checkpoint(cfg, pipeline)
    if layer == "kafka_to_bronze":
        stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg["kafka_bootstrap"])
            .option("subscribe", cfg[f"{channel}_topic"])
            .option("startingOffsets", cfg["starting_offsets"])
            .option("failOnDataLoss", str(cfg["fail_on_data_loss"]).lower())
            .option("maxOffsetsPerTrigger", cfg["max_offsets_per_trigger"])
            .load()
        )

        def process(batch, batch_id):
            if batch.isEmpty():
                return
            decoded = decode(spark, batch, cfg)
            if decoded is None:
                return
            append_bronze(spark, cfg, bronze, decoded, identity, batch_id)
    else:
        stream = (
            spark.readStream.format("delta")
            .option("skipChangeCommits", "true")
            .load(location(cfg, bronze))
        )

        def process(batch, batch_id):
            validate_and_merge(spark, cfg, batch, f"{identity}:{batch_id}", pipeline)

    writer = stream.writeStream.foreachBatch(process).option("checkpointLocation", identity)
    writer = (
        writer.trigger(availableNow=True)
        if available
        else writer.trigger(processingTime=f"{cfg['trigger_seconds']} seconds")
    )
    query = writer.queryName(pipeline).start()
    if not wait:
        return query
    last = None
    try:
        while True:
            terminated = query.awaitTermination(2)
            progress = query.lastProgress
            if progress and (progress["id"], progress["batchId"]) != last:
                report_progress(spark, cfg, pipeline, progress)
                last = (progress["id"], progress["batchId"])
            if terminated:
                break
    finally:
        if query.isActive:
            query.stop()
