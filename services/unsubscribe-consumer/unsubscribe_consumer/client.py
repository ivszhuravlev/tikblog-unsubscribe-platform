from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from contracts.model import UnsubscribeEvent


class PermanentRejection(Exception):
    """The request itself is invalid (4xx other than 408/429). Retrying it would
    fail the same way again; step 7 routes this to the dead-letter queue."""


class TransientError(Exception):
    """UnsubscribeMe, the network, or the database is temporarily unavailable."""


@dataclass(frozen=True, slots=True)
class UnsubscribeOutcome:
    """The parsed 200 response from POST /unsubscribe."""

    user_id: str
    writer_id: str
    status: str
    unsubscribed_at: datetime
    unsubscribe_source: str
    outcome: str


def _event_to_json(event: UnsubscribeEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "user_id": event.user_id,
        "writer_id": event.writer_id,
        "source": event.source.value,
        "requested_at": event.requested_at.isoformat(),
        "request_id": event.request_id,
        "cs_agent_id": event.cs_agent_id,
        "legal_batch_id": event.legal_batch_id,
    }


class UnsubscribeMeClient:
    """One pooled HTTP client for calling UnsubscribeMe."""

    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout_seconds)

    def unsubscribe(self, event: UnsubscribeEvent) -> UnsubscribeOutcome:
        try:
            response = self._client.post(
                "/unsubscribe",
                json=_event_to_json(event),
                headers={"X-Request-Id": event.request_id},
            )
        except httpx.HTTPError as error:
            raise TransientError(f"request to UnsubscribeMe failed: {error}") from error

        if 200 <= response.status_code < 300:
            body: dict[str, Any] = response.json()
            return UnsubscribeOutcome(
                user_id=body["user_id"],
                writer_id=body["writer_id"],
                status=body["status"],
                unsubscribed_at=datetime.fromisoformat(body["unsubscribed_at"]),
                unsubscribe_source=body["unsubscribe_source"],
                outcome=body["outcome"],
            )

        if response.status_code in (408, 429) or response.status_code >= 500:
            raise TransientError(f"UnsubscribeMe returned {response.status_code}: {response.text}")

        raise PermanentRejection(
            f"UnsubscribeMe rejected the request with {response.status_code}: {response.text}"
        )

    def close(self) -> None:
        self._client.close()
