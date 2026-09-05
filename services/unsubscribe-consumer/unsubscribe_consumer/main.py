import logging
import time

from confluent_kafka import Message
from contracts.model import UnsubscribeEvent
from tikblog_kafka import DeadLetterProducer, UnsubscribeConsumer
from tikblog_runtime import configure_logging, install_shutdown_handler

from unsubscribe_consumer.client import PermanentRejection, TransientError, UnsubscribeMeClient
from unsubscribe_consumer.config import Settings

logger = logging.getLogger(__name__)


def _message_location(message: Message) -> dict[str, object]:
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
    }


def deliver(
    event: UnsubscribeEvent,
    message: Message,
    client: UnsubscribeMeClient,
    dlq: DeadLetterProducer,
    settings: Settings,
) -> None:
    """Call UnsubscribeMe for one event, or park the message in the DLQ.

    Returns normally in both cases: the caller commits the offset either way.
    Not committing would mean re-reading the same message forever, which turns a
    single unprocessable event into an outage for everything queued behind it.
    """
    delay = settings.backoff_initial_seconds

    for attempt in range(1, settings.max_attempts + 1):
        try:
            client.unsubscribe(event)
            return

        except PermanentRejection as error:
            # The request itself is wrong: a retry produces the same rejection.
            # Park it immediately instead of spending the retry budget.
            dlq.park(message, reason="permanent_rejection", error=str(error), attempts=attempt)
            logger.error(
                "unsubscribe_permanently_rejected",
                extra={
                    "event_id": event.event_id,
                    "request_id": event.request_id,
                    "error": str(error),
                    **_message_location(message),
                },
            )
            return

        except TransientError as error:
            if attempt == settings.max_attempts:
                dlq.park(
                    message,
                    reason="transient_exhausted",
                    error=str(error),
                    attempts=attempt,
                )
                logger.error(
                    "unsubscribe_retries_exhausted",
                    extra={
                        "event_id": event.event_id,
                        "request_id": event.request_id,
                        "attempts": attempt,
                        "error": str(error),
                        **_message_location(message),
                    },
                )
                return

            logger.warning(
                "unsubscribe_call_retry",
                extra={
                    "event_id": event.event_id,
                    "request_id": event.request_id,
                    "attempt": attempt,
                    "next_delay_seconds": delay,
                    "error": str(error),
                },
            )
            time.sleep(delay)
            delay = min(delay * settings.backoff_multiplier, settings.backoff_max_seconds)


def main() -> None:
    settings = Settings()
    configure_logging(settings.service_name, settings.log_level)
    stopped = install_shutdown_handler()

    consumer = UnsubscribeConsumer(settings.consumer_config())
    dlq = DeadLetterProducer(settings.dlq_config())
    client = UnsubscribeMeClient(settings.unsubscribeme_url, settings.http_timeout_seconds)

    try:
        for record in consumer.messages(stopped):
            if record.event is None:
                # Bytes that do not match the schema will never decode, however
                # often they are re-read. They go to the DLQ as they arrived, so
                # the original payload stays available for inspection.
                dlq.park(
                    record.message,
                    reason="deserialization_failed",
                    error=record.deserialization_error or "unknown",
                    attempts=0,
                )
                logger.error(
                    "deserialization_failed",
                    extra={
                        "error": record.deserialization_error,
                        **_message_location(record.message),
                    },
                )
                consumer.commit(record.message)
                continue

            deliver(record.event, record.message, client, dlq, settings)

            # Commit only after the event is either accepted by UnsubscribeMe or
            # parked in the DLQ. A crash before this line replays one event into
            # an idempotent endpoint, which is safe; committing earlier could drop
            # a consent record, which is not.
            consumer.commit(record.message)

    finally:
        client.close()
        dlq.close()
        consumer.close()
        logger.info(
            "consumer_stopped",
            extra={
                "received": consumer.received_count,
                "processed": consumer.processed_count,
                "parked_in_dlq": dlq.sent_count,
            },
        )


if __name__ == "__main__":
    main()
