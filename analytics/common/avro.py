"""Decode each writer schema with Spark Avro, never a Python row UDF."""

import json
from functools import reduce
from urllib.request import urlopen

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from analytics.common.tables import EVENT


def registry_schema(url, schema_id):
    with urlopen(f"{url}/schemas/ids/{schema_id}", timeout=15) as response:
        return json.loads(response.read())["schema"]


def decode(spark, batch, cfg):
    header_ok = (F.length("value") >= 5) & (F.hex(F.substring("value", 1, 1)) == "00")
    framed = batch.withColumn(
        "schema_id",
        F.when(header_ok, F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int")),
    )
    ids = [row.schema_id for row in framed.select("schema_id").distinct().collect()]
    expected = spark.createDataFrame([], EVENT).schema
    frames = []
    for schema_id in ids:
        part = framed.filter(F.col("schema_id").eqNullSafe(F.lit(schema_id)))
        if schema_id is None:
            decoded = part
            problem = "invalid_confluent_frame"
        else:
            schema = registry_schema(cfg["schema_registry_url"], schema_id)
            # Use the writer schema for each ID; no "latest schema" assumption.
            decoded = part.withColumn(
                "_event",
                from_avro(
                    F.expr("substring(value, 6, length(value)-5)"), schema, {"mode": "PERMISSIVE"}
                ),
            )
            event_type = decoded.schema["_event"].dataType
            actual = {field.name: field.dataType for field in event_type.fields}
            incompatible = [f.name for f in expected.fields if actual.get(f.name) != f.dataType]
            problem = "schema_type_mismatch:" + ",".join(incompatible) if incompatible else None
        metadata = [
            F.col("schema_id"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("bronze_ingested_at"),
            F.base64("value").alias("raw_value"),
        ]
        if problem:
            columns = [F.lit(None).cast(f.dataType).alias(f.name) for f in expected.fields]
            frames.append(decoded.select(*columns, *metadata, F.lit(problem).alias("decode_error")))
        else:
            frames.append(
                decoded.select(
                    "_event.*",
                    *metadata,
                    F.when(F.col("_event.event_id").isNull(), F.lit("invalid_avro_payload"))
                    .otherwise(F.lit(None).cast("string"))
                    .alias("decode_error"),
                )
            )
    if not frames:
        return None
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames).withColumn(
        "event_date", F.to_date("requested_at")
    )
