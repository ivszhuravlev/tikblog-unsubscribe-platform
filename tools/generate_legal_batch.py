"""Create a small Legal CSV batch in local MinIO.

uv run python tools/generate_legal_batch.py --rows 100 --broken-share 0.1
"""

import argparse
import csv
import io
import random
from datetime import UTC, datetime, timedelta

from tikblog_storage import StorageConfig, create_object_store

HEADER = ["user_id", "writer_id", "requested_at"]
BREAKAGES = ("empty_user", "missing_writer", "bad_date", "extra_column", "missing_column")
INBOX_PREFIX = "legal_inbox/"

MINIO = StorageConfig(
    endpoint_url="http://localhost:9000",
    bucket="tikblog-legal",
    access_key_id="tikblog",
    secret_access_key="tikblog-local",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--broken-share", type=float, default=0.0)
    parser.add_argument("--name", default="")
    return parser.parse_args()


def good_row(rng: random.Random) -> list[str]:
    requested_at = datetime.now(UTC) - timedelta(minutes=rng.randrange(60 * 24 * 30))
    user_id = f"user-{rng.randrange(20):04d}"
    writer_id = f"writer-{rng.randrange(5):03d}"
    return [user_id, writer_id, requested_at.isoformat()]


def break_row(row: list[str], breakage: str) -> list[str]:
    match breakage:
        case "empty_user":
            return ["", row[1], row[2]]
        case "missing_writer":
            return [row[0], "", row[2]]
        case "bad_date":
            return [row[0], row[1], "17 августа, где-то вечером"]
        case "extra_column":
            return [*row, "surprise"]
        case _:
            return row[:2]


def build_batch(rows: int, broken_share: float) -> tuple[str, int]:
    """Build a CSV and return it with its invalid-row count."""
    rng = random.Random()
    broken_count = 0
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(HEADER)

    for _ in range(rows):
        row = good_row(rng)

        if rng.random() < broken_share:
            row = break_row(row, rng.choice(BREAKAGES))
            broken_count += 1

        writer.writerow(row)

    return buffer.getvalue(), broken_count


def main() -> None:
    args = parse_args()
    name = args.name or f"legal_unsubscribes_{datetime.now(UTC):%Y%m%d_%H%M%S_%f}.csv"
    content, broken_count = build_batch(args.rows, args.broken_share)

    store = create_object_store(MINIO)
    store.write(f"{INBOX_PREFIX}{name}", content.encode("utf-8"))

    print(f"Wrote {args.rows} rows ({broken_count} broken) to {INBOX_PREFIX}{name}")


if __name__ == "__main__":
    main()
