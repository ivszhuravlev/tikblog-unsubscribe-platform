import logging
import random
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from contracts.model import UnsubscribeEvent, UnsubscribeSource

from event_producers.config import Settings

logger = logging.getLogger(__name__)

# Fake Customer Success agents used in generated events.
CS_AGENT_POOL_SIZE = 3
FUTURE_SHIFT = timedelta(days=365)
MEANINGLESS_FLAVOURS = ("empty_user_id", "requested_at_in_future", "unknown_writer_id")


@dataclass(frozen=True, slots=True)
class GeneratedBatch:
    """Events and invalid bytes created in one iteration."""

    events: tuple[UnsubscribeEvent, ...]
    garbage: tuple[bytes, ...]


class EventGenerator:
    """Creates fake unsubscribe events for UI or Customer Success."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._source = UnsubscribeSource(settings.role)
        # UI and Customer Success must generate different sequences.
        self._random = random.Random(f"{settings.random_seed}:{settings.role}")
        self._users = [f"user-{number:04d}" for number in range(settings.user_pool_size)]
        self._writers = [f"writer-{number:03d}" for number in range(settings.writer_pool_size)]
        self._cs_agents = [f"agent-{number:02d}" for number in range(CS_AGENT_POOL_SIZE)]

    def next_batch(self) -> GeneratedBatch:
        """Create one normal event and any enabled bad events."""
        user_id = self._random.choice(self._users)
        writer_id = self._random.choice(self._writers)

        events = [self._make_event(user_id, writer_id, self._source)]

        if self._happens(self._settings.technical_duplicate_probability):
            # Send the same request twice to test deduplication later.
            events.append(events[0])
            self._log_anomaly("technical_duplicate", events[0])

        if self._happens(self._settings.meaningless_event_probability):
            events.append(self._make_meaningless_event(user_id, writer_id))

        garbage = []

        if self._happens(self._settings.garbage_bytes_probability):
            payload = self._make_garbage()
            garbage.append(payload)
            logger.warning(
                "anomaly_generated",
                extra={"anomaly": "garbage_bytes", "payload_size": len(payload)},
            )

        return GeneratedBatch(events=tuple(events), garbage=tuple(garbage))

    def _make_event(
        self,
        user_id: str,
        writer_id: str,
        source: UnsubscribeSource,
    ) -> UnsubscribeEvent:
        return UnsubscribeEvent(
            event_id=self._new_id(),
            user_id=user_id,
            writer_id=writer_id,
            source=source,
            requested_at=datetime.now(UTC),
            request_id=self._new_id(),
            cs_agent_id=(
                self._random.choice(self._cs_agents)
                if source is UnsubscribeSource.CUSTOMER_SUCCESS
                else None
            ),
        )

    def _make_meaningless_event(self, user_id: str, writer_id: str) -> UnsubscribeEvent:
        """Create an Avro-valid event with bad business data."""
        event = self._make_event(user_id, writer_id, self._source)
        flavour = self._random.choice(MEANINGLESS_FLAVOURS)

        match flavour:
            case "empty_user_id":
                event = replace(event, user_id="")
            case "requested_at_in_future":
                event = replace(event, requested_at=event.requested_at + FUTURE_SHIFT)
            case "unknown_writer_id":
                event = replace(
                    event, writer_id=f"writer-outside-pool-{self._random.randrange(1000)}"
                )
            case _:
                raise ValueError(f"Unknown meaningless flavour: {flavour}")

        self._log_anomaly(f"meaningless_{flavour}", event)
        return event

    def _make_garbage(self) -> bytes:
        """Create bytes that are not a valid Confluent Avro message."""
        return b"not-avro:" + self._random.randbytes(16)

    def _happens(self, probability: float) -> bool:
        # A disabled option must not change the seeded sequence.
        return probability > 0 and self._random.random() < probability

    def _new_id(self) -> str:
        # IDs stay unique after a restart.
        return str(uuid4())

    def _log_anomaly(self, anomaly: str, event: UnsubscribeEvent) -> None:
        logger.warning(
            "anomaly_generated",
            extra={
                "anomaly": anomaly,
                "event_id": event.event_id,
                "request_id": event.request_id,
            },
        )
