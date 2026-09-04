from datetime import datetime
from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from contracts.model import UnsubscribeEvent, UnsubscribeSource

SCHEMA_PATH = Path(__file__).with_name("unsubscribe_event.avsc")


def event_to_dict(
    event: UnsubscribeEvent,
    _context: SerializationContext,
) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "user_id": event.user_id,
        "writer_id": event.writer_id,
        "source": event.source.value,
        "requested_at": event.requested_at,
        "request_id": event.request_id,
        "cs_agent_id": event.cs_agent_id,
        "legal_batch_id": event.legal_batch_id,
    }


def event_from_dict(
    values: dict[str, Any],
    _context: SerializationContext,
) -> UnsubscribeEvent:
    requested_at = values["requested_at"]

    if not isinstance(requested_at, datetime):
        raise TypeError("requested_at must be datetime")

    return UnsubscribeEvent(
        event_id=values["event_id"],
        user_id=values["user_id"],
        writer_id=values["writer_id"],
        source=UnsubscribeSource(values["source"]),
        requested_at=requested_at,
        request_id=values["request_id"],
        cs_agent_id=values["cs_agent_id"],
        legal_batch_id=values["legal_batch_id"],
    )


class UnsubscribeEventAvro:
    """Convert unsubscribe events between Python objects and Avro bytes."""

    def __init__(self, registry_url: str, topic: str) -> None:
        schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
        registry_client = SchemaRegistryClient({"url": registry_url})

        self._context = SerializationContext(topic, MessageField.VALUE)
        self._encode = AvroSerializer(
            registry_client,
            schema_text,
            to_dict=event_to_dict,
        )
        self._decode = AvroDeserializer(
            registry_client,
            schema_text,
            from_dict=event_from_dict,
        )

    def to_bytes(self, event: UnsubscribeEvent) -> bytes:
        payload = self._encode(event, self._context)

        if not isinstance(payload, bytes):
            raise TypeError("Serializer did not return bytes")

        return payload

    def from_bytes(self, payload: bytes) -> UnsubscribeEvent:
        event = self._decode(payload, self._context)

        if not isinstance(event, UnsubscribeEvent):
            raise TypeError("Deserializer did not return UnsubscribeEvent")

        return event


def message_key(event: UnsubscribeEvent) -> bytes:
    return f"{event.user_id}:{event.writer_id}".encode()
