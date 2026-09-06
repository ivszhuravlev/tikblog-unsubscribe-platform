"""One Spark JVM owns all Delta writes; Airflow submits small named jobs over HTTP."""

import json
import logging
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from analytics.common.config import settings
from analytics.common.delta import bootstrap, read, records
from analytics.common.observability import now
from analytics.common.spark import session
from analytics.jobs.main import execute
from analytics.jobs.streams import report_progress, run


def serve():
    cfg = settings()
    spark = session("tikblog-analytics-writer", cfg)
    bootstrap(spark, cfg)
    queries = [
        run(spark, cfg, "priority", layer, wait=False)
        for layer in ("kafka_to_bronze", "bronze_to_silver")
    ]
    batch_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, data):
            raw = json.dumps(data, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            if self.path == "/health":
                ready = all(q.isActive for q in queries)
                self.respond(200 if ready else 503, {"ready": ready})
            else:
                self.respond(404, {"error": "not found"})

        def do_POST(self):
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if size < 1 or size > 1000000:
                    raise ValueError("Invalid request size")
                payload = json.loads(self.rfile.read(size))
                if self.path == "/run":
                    # Serialize short-lived jobs within this process. The two
                    # streams keep running; Delta handles their transaction conflicts.
                    with batch_lock:
                        result = execute(spark.newSession(), cfg, SimpleNamespace(**payload))
                    self.respond(200, result)
                elif self.path == "/summary":
                    rows = (
                        read(spark, cfg, "monitoring.dq_results")
                        .filter(F.col("run_id") == payload["run_id"])
                        .groupBy("status")
                        .count()
                        .collect()
                    )
                    self.respond(200, [r.asDict() for r in rows])
                elif self.path == "/failure":
                    row = dict(
                        alert_id=payload["alert_id"],
                        created_at=now(),
                        pipeline_name=payload["pipeline_name"],
                        source="ALL",
                        alert_type="AIRFLOW_TASK_FAILURE",
                        severity="RED",
                        status="RED",
                        metric_name="task_failure",
                        current_value=1.0,
                        threshold=1.0,
                        message=payload["message"],
                        run_id=payload["run_id"],
                    )
                    records(spark, cfg, "monitoring.alert_events", [row])
                    self.respond(200, {"recorded": True})
                else:
                    self.respond(404, {"error": "not found"})
            except Exception as exc:
                logging.exception("Analytics request failed")
                self.respond(500, {"error": str(exc)})

    server = ThreadingHTTPServer(("0.0.0.0", 8088), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    previous = {}
    spool = Path("/app/alert-spool")
    try:
        while True:
            for file in spool.glob("*.json"):
                row = json.loads(file.read_text())
                row["created_at"] = now()
                records(spark, cfg, "monitoring.alert_events", [row])
                file.unlink(missing_ok=True)
            for query in queries:
                if not query.isActive:
                    raise RuntimeError(str(query.exception() or "Priority query stopped"))
                progress = query.lastProgress
                if progress and progress["batchId"] != previous.get(query.name):
                    report_progress(spark.newSession(), cfg, query.name, progress)
                    previous[query.name] = progress["batchId"]
            threading.Event().wait(3)
    finally:
        server.shutdown()
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    serve()
