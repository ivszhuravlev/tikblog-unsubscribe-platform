from datetime import datetime
from uuid import uuid4

from contracts.model import UnsubscribeEvent, UnsubscribeSource

EXPECTED_HEADER = ["user_id", "writer_id", "requested_at"]


class RowError(ValueError):
    """The CSV row cannot become an unsubscribe event."""


def build_event(row: list[str], legal_batch_id: str) -> UnsubscribeEvent:
    """Turn one CSV row into a canonical event, or raise RowError."""
    if len(row) != len(EXPECTED_HEADER):
        raise RowError(f"expected {len(EXPECTED_HEADER)} columns, got {len(row)}")

    user_id, writer_id, requested_at = (value.strip() for value in row)

    if not user_id:
        raise RowError("user_id is empty")

    if not writer_id:
        raise RowError("writer_id is empty")

    try:
        parsed_at = datetime.fromisoformat(requested_at)
    except ValueError:
        raise RowError(f"requested_at is not ISO-8601: {requested_at!r}") from None

    if parsed_at.tzinfo is None:
        raise RowError(f"requested_at has no timezone: {requested_at!r}")

    # Ingestion creates IDs and records which file the row came from.
    return UnsubscribeEvent(
        event_id=str(uuid4()),
        user_id=user_id,
        writer_id=writer_id,
        source=UnsubscribeSource.LEGAL,
        requested_at=parsed_at,
        request_id=str(uuid4()),
        legal_batch_id=legal_batch_id,
    )
