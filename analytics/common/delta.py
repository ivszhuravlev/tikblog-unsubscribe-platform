import random
import time

from delta.tables import DeltaTable

from analytics.common.config import location
from analytics.common.tables import KEYS, TABLES


def retry(cfg, operation):
    for attempt in range(cfg["merge_attempts"]):
        try:
            return operation()
        except Exception as exc:
            conflict = any(
                name in str(exc)
                for name in (
                    "ConcurrentAppendException",
                    "ConcurrentWriteException",
                    "ConcurrentDeleteReadException",
                    "ConcurrentDeleteDeleteException",
                    "MetadataChangedException",
                    "ProtocolChangedException",
                    "DELTA_CONCURRENT",
                )
            )
            if not conflict or attempt + 1 == cfg["merge_attempts"]:
                raise
            time.sleep(cfg["merge_backoff_seconds"] * 2**attempt + random.random())


def bootstrap(spark, cfg):
    for table, schema in TABLES.items():
        path = location(cfg, table)
        partition = "PARTITIONED BY (event_date)" if not table.startswith("monitoring.") else ""
        spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{path}` ({schema}) USING DELTA
            {partition} TBLPROPERTIES ('delta.isolationLevel'='Serializable',
            'delta.enableChangeDataFeed'='false')""")


def read(spark, cfg, table):
    return spark.read.format("delta").load(location(cfg, table))


def insert_once(spark, cfg, table, frame, keys=None):
    keys = keys or KEYS[table]
    condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)

    def write():
        target = DeltaTable.forPath(spark, location(cfg, table))
        (target.alias("t").merge(frame.alias("s"), condition).whenNotMatchedInsertAll().execute())

    return retry(cfg, write)


def records(spark, cfg, table, rows):
    if rows:
        frame = spark.createDataFrame(rows, TABLES[table])
        insert_once(spark, cfg, table, frame)


def append_bronze(spark, cfg, table, frame, run_id, batch_id):
    def write():
        (
            frame.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("txnAppId", run_id)
            .option("txnVersion", batch_id)
            .partitionBy("event_date")
            .save(location(cfg, table))
        )

    retry(cfg, write)


def silver_merge(spark, cfg, frame):
    # A full-key predicate + Serializable isolation makes concurrent writers
    # retry rather than accepting independently inserted copies of an event_id.
    insert_once(spark, cfg, "silver.unsubscribe_events", frame, ["event_id"])
