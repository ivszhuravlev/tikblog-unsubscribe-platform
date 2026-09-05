import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DlqConfig:
    """Settings required to publish to the dead-letter topic."""

    bootstrap_servers: str
    topic: str
    client_id: str
    flush_timeout_seconds: float


class DeadLetterProducer:
    """Publishes messages the consumer could not process.

    Deliberately byte-for-byte: the original value is forwarded as-is, without
    Avro serialization. A message that failed *because* it does not match the
    schema has to land here too, and an encoder would reject it a second time.
    The reason for parking it travels in the headers, where it can be read
    without decoding the payload.
    """

    def __init__(self, settings: DlqConfig) -> None:
        self._topic = settings.topic
        self._flush_timeout_seconds = settings.flush_timeout_seconds

        self.sent_count = 0
        self.failed_count = 0

        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                # The DLQ is the last place a message can be saved, so the same
                # durability as the main path: no acknowledgement without all
                # in-sync replicas.
                "acks": "all",
                "enable.idempotence": True,
                "client.id": settings.client_id,
            }
        )

    def park(self, message: Message, reason: str, error: str, attempts: int) -> None:
        """Copy one failed message to the dead-letter topic."""
        # The client's signature allows str, bytes or None per header value, so the
        # list has to be typed that way even though every value here is bytes.
        headers: list[tuple[str, str | bytes | None]] = [
            ("dlq_reason", reason.encode()),
            ("dlq_error", error[:500].encode()),
            ("dlq_attempts", str(attempts).encode()),
            ("dlq_failed_at", datetime.now(tz=UTC).isoformat().encode()),
            ("source_topic", str(message.topic()).encode()),
            ("source_partition", str(message.partition()).encode()),
            ("source_offset", str(message.offset()).encode()),
        ]

        self._producer.produce(
            topic=self._topic,
            key=message.key(),
            value=message.value(),
            headers=headers,
            on_delivery=self._on_delivery,
        )
        # Block until Kafka confirms: parking a message is the last chance to keep
        # it, so the consumer must not commit the offset before that is certain.
        self._producer.flush(self._flush_timeout_seconds)

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self.failed_count += 1
            logger.error("dlq_delivery_failed", extra={"error": str(error)})
            return

        self.sent_count += 1
        logger.warning(
            "message_parked_in_dlq",
            extra={
                "dlq_topic": message.topic(),
                "dlq_partition": message.partition(),
                "dlq_offset": message.offset(),
            },
        )

    def close(self) -> None:
        self._producer.flush(self._flush_timeout_seconds)
