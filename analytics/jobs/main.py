"""Small CLI used by both Compose services and Airflow tasks."""

import sys
from datetime import date, datetime
from pathlib import Path

# spark-submit executes this file by path, not as an installed package.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analytics.common.delta import bootstrap
from analytics.jobs.gold import freshness, refresh, touched
from analytics.jobs.maintenance import backfill, housekeeping
from analytics.jobs.streams import run


def execute(spark, cfg, args):
    if args.action == "bootstrap":
        bootstrap(spark, cfg)
    elif args.action == "stream":
        if args.channel != "legal" or args.layer not in ("kafka_to_bronze", "bronze_to_silver"):
            raise ValueError("HTTP batch stream must be a Legal stage")
        run(spark, cfg, args.channel, args.layer, available=True)
    elif args.action == "touched":
        return [
            d.isoformat()
            for d in touched(
                spark, cfg, datetime.fromisoformat(args.start), datetime.fromisoformat(args.end)
            )
        ]
    elif args.action == "gold":
        refresh(spark, cfg, [date.fromisoformat(d) for d in args.dates], args.run_id)
    elif args.action == "freshness":
        freshness(spark, cfg, args.run_id)
    elif args.action == "housekeeping":
        housekeeping(spark, cfg, args.step)
    elif args.action == "backfill":
        if args.layer not in ("silver", "gold"):
            raise ValueError("backfill layer must be silver or gold")
        backfill(
            spark,
            cfg,
            args.layer,
            date.fromisoformat(args.start),
            date.fromisoformat(args.end),
            args.run_id,
        )
    else:
        raise ValueError("Unknown job action")
