import logging
import time

from tikblog_kafka import UnsubscribeConsumer
from tikblog_runtime import configure_logging, install_shutdown_handler

from unsubscribe_consumer.client import PermanentRejection, TransientError, UnsubscribeMeClient
from unsubscribe_consumer.config import Settings

logger = logging.getLogger(__name__)


def main() -> None:
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)
    stopped = install_shutdown_handler()

    consumer = UnsubscribeConsumer(settings.consumer_config())
    client = UnsubscribeMeClient(settings.unsubscribeme_url, settings.http_timeout_seconds)

    try:
        for record in consumer.messages(stopped):
            if record.event is None:
                logger.error(
                    "deserialization_failed",
                    extra={
                        "error": record.deserialization_error,
                        "topic": record.message.topic(),
                        "partition": record.message.partition(),
                        "offset": record.message.offset(),
                    },
                )
                consumer.seek(record.message)
                time.sleep(settings.retry_pause_seconds)
                continue

            event = record.event

            try:
                # One call for both deployments: the event carries its own
                # source, so priority and legal never branch apart here.
                outcome = client.unsubscribe(event)
            except (PermanentRejection, TransientError) as error:
                # Step 7 will branch here (DLQ vs. bounded retry). For now every
                # failure just blocks this partition and retries forever, which
                # is the deliberate, intentionally crude behavior for this step.
                logger.error(
                    "unsubscribe_call_failed",
                    extra={
                        "event_id": event.event_id,
                        "request_id": event.request_id,
                        "error": str(error),
                        "topic": record.message.topic(),
                        "partition": record.message.partition(),
                        "offset": record.message.offset(),
                    },
                )
                consumer.seek(record.message)
                time.sleep(settings.retry_pause_seconds)
                continue

            # Commit only after UnsubscribeMe confirms the event: a crash between
            # the call and this commit just replays one event into an idempotent
            # endpoint, which is safe. Committing first could silently drop a
            # consent record, which is not acceptable for unsubscribes.
            consumer.commit(record.message)

            logger.info(
                "unsubscribe_processed",
                extra={
                    "event_id": event.event_id,
                    "request_id": event.request_id,
                    "user_id": event.user_id,
                    "writer_id": event.writer_id,
                    "source": event.source.value,
                    "outcome": outcome.outcome,
                    "topic": record.message.topic(),
                    "partition": record.message.partition(),
                    "offset": record.message.offset(),
                },
            )
    finally:
        client.close()
        consumer.close()
        logger.info(
            "consumer_stopped",
            extra={
                "received": consumer.received_count,
                "processed": consumer.processed_count,
                "failed": consumer.failed_count,
            },
        )


if __name__ == "__main__":
    main()
