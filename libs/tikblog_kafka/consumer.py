import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from contracts.avro import UnsubscribeEventAvro
from contracts.model import UnsubscribeEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """Settings required to read events from Kafka."""

    bootstrap_servers: str
    schema_registry_url: str
    topic: str
    group_id: str
    client_id: str
    session_timeout_ms: int
    max_poll_interval_ms: int
    poll_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class ConsumedRecord:
    """One Kafka message paired with its decoded event, if decoding succeeded.

    `event` is None when Avro deserialization failed; `deserialization_error`
    then carries the reason so the caller can apply the same not-committed,
    seek-back, retry-forever handling as an HTTP failure (see main.py).
    """

    message: Message
    event: UnsubscribeEvent | None
    deserialization_error: str | None = None


class UnsubscribeConsumer:
    """Reads UnsubscribeEvent from Kafka, one Avro message per event."""

    def __init__(self, settings: ConsumerConfig) -> None:
        self._poll_timeout_seconds = settings.poll_timeout_seconds

        # Converts Avro bytes back to events.
        self._avro = UnsubscribeEventAvro(settings.schema_registry_url, settings.topic)

        # `processed_count` only grows on commit(); it means "safely handed off",
        # not merely "seen".
        self.received_count = 0
        self.processed_count = 0
        self.failed_count = 0

        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.group_id,
                "client.id": settings.client_id,
                # We commit ourselves, only after the HTTP call succeeds (see main.py).
                "enable.auto.commit": False,
                # A fresh consumer group must not skip events already on the topic.
                "auto.offset.reset": "earliest",
                "session.timeout.ms": settings.session_timeout_ms,
                "max.poll.interval.ms": settings.max_poll_interval_ms,
                # A rebalance hands over only the partitions that moved.
                "partition.assignment.strategy": "cooperative-sticky",
                # Put Kafka client errors into our JSON logs.
                "error_cb": self._report_client_error,
            }
        )
        self._consumer.subscribe([settings.topic])

    def messages(self, stopped: threading.Event) -> Iterator[ConsumedRecord]:
        """Yield one record per Kafka message until shutdown is requested.

        The shutdown flag is checked only between polls, so a message already
        being handled always finishes before the loop exits.
        """
        while not stopped.is_set():
            msg = self._consumer.poll(self._poll_timeout_seconds)
            if msg is None:
                continue

            error = msg.error()
            if error is not None:
                if error.code() != KafkaError._PARTITION_EOF:
                    logger.error(
                        "kafka_message_error",
                        extra={"error": str(error), "error_code": error.name()},
                    )
                continue

            self.received_count += 1

            # A message with no error() always carries a value; the library's
            # stubs just type it Optional to cover the error case above.
            value = msg.value()
            assert value is not None, "message without error() must have a value"

            try:
                event = self._avro.from_bytes(value)
            except Exception as exc:
                # Malformed bytes are handled like a failed HTTP call in this step:
                # log, don't commit, seek back, retry forever. Step 7 replaces this
                # with bounded retry + DLQ.
                self.failed_count += 1
                yield ConsumedRecord(message=msg, event=None, deserialization_error=str(exc))
                continue

            yield ConsumedRecord(message=msg, event=event)

    def commit(self, message: Message) -> None:
        """Commit synchronously, one message at a time.

        Slow but obviously correct: a crash between the HTTP call and this commit
        just replays one event into an idempotent endpoint. Batching commits for
        throughput is an optimization to make once it is measured to matter.
        """
        self._consumer.commit(message=message, asynchronous=False)
        self.processed_count += 1

    def seek(self, message: Message) -> None:
        """Rewind to re-deliver a message that was not committed."""
        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
        # A message we just consumed always carries its own coordinates; the
        # library's stubs type them Optional only to cover unrelated cases.
        assert topic is not None and partition is not None and offset is not None
        self._consumer.seek(TopicPartition(topic, partition, offset))

    def close(self) -> None:
        """Leave the consumer group cleanly instead of waiting out the session timeout."""
        self._consumer.close()

    def _report_client_error(self, error: KafkaError) -> None:
        """Log a Kafka client error not tied to one message."""
        logger.error(
            "kafka_client_error",
            extra={"error": str(error), "fatal": error.fatal()},
        )
