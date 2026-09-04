import logging
import sys

from tikblog_kafka import UnsubscribeProducer
from tikblog_runtime import configure_logging, install_shutdown_handler

from event_producers.config import Settings
from event_producers.generator import EventGenerator

logger = logging.getLogger(__name__)


def main() -> None:
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)

    stopped = install_shutdown_handler()
    generator = EventGenerator(settings)
    producer = UnsubscribeProducer(settings.producer_config())
    interval_seconds = 1 / settings.events_per_second

    logger.info(
        "producer_started",
        extra={
            "role": settings.role,
            "topic": settings.topic,
            "events_per_second": settings.events_per_second,
        },
    )

    # Stop creating events on shutdown, then wait for queued messages below.
    try:
        while not stopped.is_set():
            batch = generator.next_batch()

            for event in batch.events:
                producer.send(event)

            for payload in batch.garbage:
                producer.send_raw(payload)

            stopped.wait(interval_seconds)
    finally:
        still_queued = producer.flush()

        logger.info(
            "producer_stopped",
            extra={
                "sent": producer.sent_count,
                "delivered": producer.delivered_count,
                "failed": producer.failed_count,
                "still_queued": still_queued,
            },
        )

    # A non-zero exit tells Docker that some messages were lost.
    if producer.failed_count > 0 or still_queued > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
