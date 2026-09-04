from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class UnsubscribeSource(StrEnum):
    UI = "UI"
    CUSTOMER_SUCCESS = "CUSTOMER_SUCCESS"
    LEGAL = "LEGAL"


@dataclass(frozen=True, slots=True)
class UnsubscribeEvent:
    event_id: str
    user_id: str
    writer_id: str
    source: UnsubscribeSource
    requested_at: datetime
    request_id: str
    cs_agent_id: str | None = None
    legal_batch_id: str | None = None

    def __post_init__(self) -> None:
        if self.requested_at.tzinfo is None or self.requested_at.utcoffset() is None:
            raise ValueError("requested_at must include timezone")
