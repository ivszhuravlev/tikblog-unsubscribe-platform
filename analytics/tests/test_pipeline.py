"""Real Spark/Delta regressions using temporary local tables, never live MinIO.

Run with the project's Spark image and its Delta packages:
    spark-submit --master 'local[1]' analytics/tests/test_pipeline.py
Ordinary unittest discovery skips these tests if Spark is not installed.
"""

import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from analytics.common import delta, dq
    from analytics.common.tables import TABLES
    from analytics.jobs import gold, maintenance
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, "Requires the Spark image with Delta packages")
class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("analytics-regressions")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="analytics-test-")
        self.addCleanup(self.directory.cleanup)
        self.path = lambda cfg, table: str(Path(self.directory.name) / table.replace(".", "/"))
        for module in (delta, gold, maintenance):
            patcher = patch.object(module, "location", self.path)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.cfg = {"merge_attempts": 2, "merge_backoff_seconds": 0, "gold_lookback_minutes": 30}
        delta.bootstrap(self.spark, self.cfg)
        self.requested = datetime.utcnow() - timedelta(days=10)

    def event(self, event_id, offset, **overrides):
        row = dict(
            event_id=event_id,
            user_id="reader-1",
            writer_id="writer-1",
            source="UI",
            requested_at=self.requested,
            request_id="request-1",
            cs_agent_id=None,
            legal_batch_id=None,
            schema_id=1,
            kafka_topic="unsubscribe-priority",
            kafka_partition=0,
            kafka_offset=offset,
            kafka_timestamp=self.requested,
            bronze_ingested_at=datetime.utcnow(),
            event_date=self.requested.date(),
            decode_error=None,
            raw_value="AA==",
        )
        row.update(overrides)
        return row

    def frame(self, rows, extra_schema=""):
        return self.spark.createDataFrame(
            rows, TABLES["bronze.unsubscribe_priority"] + extra_schema
        )

    def table(self, name):
        return delta.read(self.spark, self.cfg, name)

    def merge(self, rows, run="test-run", extra_schema=""):
        dq.validate_and_merge(self.spark, self.cfg, self.frame(rows, extra_schema), run, "test")

    def test_technical_duplicates_and_business_requests_survive_replay(self):
        rows = [
            self.event("ui-event", 0),
            self.event("ui-event", 1),
            self.event("cs-event", 2, source="CUSTOMER_SUCCESS"),
            self.event("legal-event", 3, source="LEGAL"),
        ]
        frame = self.frame(rows)
        # A retry of the same Kafka microbatch cannot append Bronze twice.
        for _ in range(2):
            delta.append_bronze(
                self.spark, self.cfg, "bronze.unsubscribe_priority", frame, "test-checkpoint", 0
            )
        self.assertEqual(self.table("bronze.unsubscribe_priority").count(), 4)
        self.merge(rows)
        before = {
            r.event_id: r.silver_processed_at
            for r in self.table("silver.unsubscribe_events").collect()
        }
        self.assertEqual(set(before), {"ui-event", "cs-event", "legal-event"})
        self.merge(rows, run="replay")
        after = {
            r.event_id: r.silver_processed_at
            for r in self.table("silver.unsubscribe_events").collect()
        }
        self.assertEqual(after, before, "A technical duplicate must never update an existing row")
        result = self.table("monitoring.dq_results").filter(
            "run_id = 'test-run' AND source = 'UI' AND check_name = 'duplicate_event_id_in_batch'"
        )
        self.assertEqual(result.first().failed_rows, 1)
        self.assertEqual(self.table("monitoring.dq_quarantine").count(), 0)

    def test_invalid_rows_are_quarantined_and_reported(self):
        self.merge(
            [
                self.event("valid", 0),
                self.event("empty-user", 1, user_id="  "),
                self.event("future", 2, requested_at=datetime.utcnow() + timedelta(days=1)),
                self.event("missing-source", 3, source=None),
            ]
        )
        self.assertEqual(
            [r.event_id for r in self.table("silver.unsubscribe_events").collect()], ["valid"]
        )
        bad = {
            r.event_id: r.failed_checks for r in self.table("monitoring.dq_quarantine").collect()
        }
        self.assertIn("user_id_required", bad["empty-user"])
        self.assertIn("requested_at_not_future", bad["future"])
        self.assertIn("source_valid", bad["missing-source"])
        self.assertGreater(self.table("monitoring.alert_events").count(), 0)

    def test_breaking_type_change_is_red_and_never_cast_to_valid_data(self):
        incompatible = self.frame([self.event("wrong-type", 1)]).withColumn("user_id", F.lit(123))
        dq.validate_and_merge(self.spark, self.cfg, incompatible, "breaking", "test")
        self.assertEqual(self.table("silver.unsubscribe_events").count(), 0)
        row = self.table("monitoring.dq_quarantine").first()
        self.assertIsNone(row.user_id)
        self.assertIn("123", row.raw_value)
        check = (
            self.table("monitoring.dq_results").filter("check_name = 'schema_and_decode'").first()
        )
        self.assertEqual((check.status, check.failed_rows), ("RED", 1))

    def test_additive_nullable_field_reaches_silver_without_changing_old_rows(self):
        self.merge([self.event("old", 0)])
        self.merge(
            [self.event("new", 1, campaign="summer")],
            run="evolved",
            extra_schema=", campaign STRING",
        )
        rows = {
            r.event_id: r.campaign
            for r in self.table("silver.unsubscribe_events")
            .select("event_id", "campaign")
            .collect()
        }
        self.assertEqual(rows, {"old": None, "new": "summer"})

    def test_late_legal_updates_historical_gold_and_backfill_is_idempotent(self):
        old_day = self.requested.date()
        ui = self.frame([self.event("ui", 0)])
        legal = self.frame(
            [self.event("legal", 1, source="LEGAL", kafka_topic="unsubscribe-legal")]
        )
        delta.append_bronze(self.spark, self.cfg, "bronze.unsubscribe_priority", ui, "ui", 0)
        delta.append_bronze(self.spark, self.cfg, "bronze.unsubscribe_legal", legal, "legal", 0)
        for run_id in ("backfill-1", "backfill-2"):
            maintenance.backfill(self.spark, self.cfg, "silver", old_day, old_day, run_id)
            self.assertEqual(self.table("silver.unsubscribe_events").count(), 2)
            counts = {
                r.source: (r.unsubscribe_count, r.unique_readers, r.unique_writers)
                for r in self.table("gold.unsubscribe_daily").collect()
            }
            self.assertEqual(counts, {"UI": (1, 1, 1), "LEGAL": (1, 1, 1)})
        end = datetime.utcnow() + timedelta(seconds=1)
        self.assertIn(old_day, gold.touched(self.spark, self.cfg, end - timedelta(minutes=5), end))
        silver_before = self.table("silver.unsubscribe_events").collect()
        maintenance.backfill(self.spark, self.cfg, "gold", old_day, old_day, "gold-only")
        self.assertCountEqual(self.table("silver.unsubscribe_events").collect(), silver_before)
        self.assertEqual(self.table("gold.unsubscribe_daily").count(), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
