import logging

from tikblog_kafka import UnsubscribeProducer
from tikblog_runtime import configure_logging
from tikblog_storage import create_object_store

from legal_ingestion.config import Settings
from legal_ingestion.landing_zone import LandingZone, Reject
from legal_ingestion.parsing import RowError, build_event

logger = logging.getLogger(__name__)


def process_batch(
    name: str,
    zone: LandingZone,
    producer: UnsubscribeProducer,
) -> tuple[int, list[Reject]]:
    """Read one CSV and send every valid row to Kafka."""
    rows = zone.read_rows(name)
    next(rows, None)  # Skip the CSV header.

    rows_read = 0
    rejects: list[Reject] = []

    for line_number, row in enumerate(rows, start=2):
        rows_read += 1

        try:
            producer.send(build_event(row, legal_batch_id=name))
        except RowError as error:
            rejects.append(Reject(line_number, row, str(error)))

    return rows_read, rejects


def main() -> None:
    """Process every CSV currently waiting in the MinIO inbox, then exit."""
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)

    store = create_object_store(settings.storage_config())
    zone = LandingZone(
        store,
        settings.inbox_prefix,
        settings.processed_prefix,
        settings.rejects_prefix,
    )
    producer = UnsubscribeProducer(settings.producer_config())

    files_done = 0
    rows_read = 0
    events_sent = 0
    rows_rejected = 0

    for name in zone.list_batches():
        if zone.already_processed(name):
            logger.info("batch_skipped_already_processed", extra={"batch": name})
            continue

        delivered_before = producer.delivered_count

        try:
            file_rows, rejects = process_batch(name, zone, producer)
            still_queued = producer.flush()
            delivered = producer.delivered_count - delivered_before
            expected = file_rows - len(rejects)

            if still_queued > 0 or delivered != expected:
                raise RuntimeError(
                    f"Kafka confirmed {delivered} of {expected} events; "
                    f"{still_queued} are still queued"
                )

            if rejects:
                zone.write_rejects(name, rejects)

            zone.mark_processed(name)
        except Exception:
            # The file stays in inbox. Continuing a partly published file without
            # duplicate Kafka records is outside this demo.
            producer.flush()
            logger.exception("batch_failed", extra={"batch": name})
            raise

        files_done += 1
        rows_read += file_rows
        events_sent += delivered
        rows_rejected += len(rejects)

        logger.info(
            "batch_processed",
            extra={
                "batch": name,
                "rows_read": file_rows,
                "events_sent": delivered,
                "rows_rejected": len(rejects),
            },
        )

    logger.info(
        "ingestion_finished",
        extra={
            "files_processed": files_done,
            "rows_read": rows_read,
            "events_sent": events_sent,
            "rows_rejected": rows_rejected,
        },
    )


if __name__ == "__main__":
    main()
