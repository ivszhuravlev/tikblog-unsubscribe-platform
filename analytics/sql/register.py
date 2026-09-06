"""Register external Delta tables in the Thrift Server's metastore via beeline."""

import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.common.config import location, settings
from analytics.common.tables import TABLES


def run_beeline(sql, host="spark-sql", port="10000"):
    jdbc = f"jdbc:hive2://{host}:{port}/default"
    result = subprocess.run(
        ["/opt/spark/bin/beeline", "-u", jdbc, "-n", "analytics", "-e", sql],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"beeline failed: {result.stderr}")
    return result.stdout


def main():
    cfg = settings()
    host = os.environ.get("SPARK_SQL_HOST", "spark-sql")
    port = os.environ.get("SPARK_SQL_PORT", "10000")

    max_attempts = 120
    for attempt in range(max_attempts):
        try:
            run_beeline("SELECT 1", host, port)
            break
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(5)

    statements = []
    for namespace in ("bronze", "silver", "gold", "monitoring"):
        statements.append(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    for table in TABLES:
        path = location(cfg, table).replace("'", "''")
        statements.append(f"CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{path}'")
        alias = table.replace(".", "_")
        statements.append(f"CREATE OR REPLACE VIEW default.{alias} AS SELECT * FROM {table}")

    sql = "; ".join(statements)
    run_beeline(sql, host, port)
    for table in TABLES:
        alias = table.replace(".", "_")
        print(f"Registered {table} and default.{alias}")


if __name__ == "__main__":
    main()
