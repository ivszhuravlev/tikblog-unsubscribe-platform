import logging
import time
from dataclasses import dataclass
from functools import partial

from confluent_kafka import KafkaError, Message, Producer
from contracts.avro import UnsubscribeEventAvro, message_key
from contracts.model import UnsubscribeEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProducerConfig:
    """Settings required to send events to Kafka."""

    bootstrap_servers: str
    schema_registry_url: str
    topic: str
    client_id: str
    linger_ms: int
    batch_size: int
    compression_type: str
    queue_full_timeout_seconds: float
    flush_timeout_seconds: float


class UnsubscribeProducer:
    """Turn unsubscribe events into Avro and pass them to the Kafka client."""

    def __init__(self, settings: ProducerConfig) -> None:
        self._topic = settings.topic
        self._queue_full_timeout_seconds = settings.queue_full_timeout_seconds
        self._flush_timeout_seconds = settings.flush_timeout_seconds

        # Converts events to Avro bytes.
        self._avro = UnsubscribeEventAvro(settings.schema_registry_url, settings.topic)

        # send() only puts a message into the Kafka client's local queue.
        # sent_count changes there; delivered_count and failed_count change later,
        # when poll() or flush() processes Kafka's reply.
        self.sent_count = 0
        self.delivered_count = 0
        self.failed_count = 0

        self._producer = Producer(
            {
                # Addresses used to find the Kafka cluster.
                "bootstrap.servers": settings.bootstrap_servers,
                # Kafka reports success only after all in-sync replicas save the message.
                "acks": "all",
                # If the client retries the same send, Kafka does not save it twice.
                "enable.idempotence": True,
                # Wait at most this long to collect several messages into one batch.
                "linger.ms": settings.linger_ms,
                # Maximum batch size in bytes.
                "batch.size": settings.batch_size,
                # Compression for the whole batch.
                "compression.type": settings.compression_type,
                "client.id": settings.client_id,
                # Kafka calls this function for errors not tied to one message.
                "error_cb": self._report_client_error,
            }
        )

    def send(self, event: UnsubscribeEvent) -> None:
        """Convert one event to Avro and put it into the local send queue."""
        try:
            payload = self._avro.to_bytes(event)
        except Exception:
            # Invalid events must not reach Kafka.
            logger.exception(
                "serialization_failed",
                extra={"event_id": event.event_id, "request_id": event.request_id},
            )
            raise

        self._enqueue(
            key=message_key(event),
            payload=payload,
            event_id=event.event_id,
            request_id=event.request_id,
            log_context={"event_id": event.event_id, "request_id": event.request_id},
        )

    def send_raw(self, payload: bytes) -> None:
        """Bypass Avro and queue deliberately broken bytes for failure testing."""
        self._enqueue(
            key=b"garbage",
            payload=payload,
            event_id=None,
            request_id=None,
            log_context={"anomaly": "garbage_bytes"},
        )

    def _enqueue(
        self,
        *,
        key: bytes,
        payload: bytes,
        event_id: str | None,
        request_id: str | None,
        log_context: dict[str, object],
    ) -> None:
        """Put one message into the local queue, waiting briefly if it is full."""
        deadline = time.monotonic() + self._queue_full_timeout_seconds
        waiting_logged = False

        while True:
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=key,
                    value=payload,
                    # Kafka calls _report_delivery later. partial() remembers which
                    # event this callback belongs to.
                    on_delivery=partial(self._report_delivery, event_id, request_id),
                )
            except BufferError:
                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    self.failed_count += 1
                    logger.error("produce_queue_full", extra=log_context)
                    raise

                if not waiting_logged:
                    logger.warning("produce_queue_full_waiting", extra=log_context)
                    waiting_logged = True

                # Handle replies already received from Kafka. Completed messages then
                # leave the local queue, making room for this message.
                self._producer.poll(min(1.0, remaining))
                continue

            self.sent_count += 1
            self.poll()
            return

    def poll(self) -> None:
        """Process replies already received from Kafka and run their callbacks."""
        self._producer.poll(0)

    def flush(self) -> int:
        """Wait while queued messages are sent and Kafka confirms their delivery.

        Delivery callbacks run while waiting. Return the number of messages still
        unfinished when the timeout expires.
        """
        return self._producer.flush(timeout=self._flush_timeout_seconds)

    def _report_client_error(self, error: KafkaError) -> None:
        """Write a general Kafka client error to our JSON log."""
        logger.error(
            "kafka_client_error",
            extra={"error": str(error), "fatal": error.fatal()},
        )

    def _report_delivery(
        self,
        event_id: str | None,
        request_id: str | None,
        error: KafkaError | None,
        message: Message,
    ) -> None:
        """Update counters when Kafka reports success or failure for one message."""
        if error is not None:
            self.failed_count += 1
            logger.error(
                "delivery_failed",
                extra={
                    "event_id": event_id,
                    "request_id": request_id,
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "error": str(error),
                    "error_code": error.name(),
                    "retriable": error.retriable(),
                },
            )
            return

        self.delivered_count += 1
