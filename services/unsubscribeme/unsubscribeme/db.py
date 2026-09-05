from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg
from psycopg_pool import ConnectionPool

from unsubscribeme.config import Settings

# psycopg returns database rows as tuples.
_Connection = psycopg.Connection[tuple[Any, ...]]

Outcome = Literal["created", "updated", "unchanged"]

# Create the subscription if it is new.
# If it already exists, keep the earliest unsubscribe time.
_UPSERT_SQL = """
    INSERT INTO subscription (
        user_id, writer_id, status, unsubscribed_at, unsubscribe_source, unsubscribe_event_id
    )
    VALUES (%(user_id)s, %(writer_id)s, 'UNSUBSCRIBED', %(requested_at)s, %(source)s, %(event_id)s)
    ON CONFLICT (user_id, writer_id) DO UPDATE SET
        status               = 'UNSUBSCRIBED',
        unsubscribed_at      = EXCLUDED.unsubscribed_at,
        unsubscribe_source   = EXCLUDED.unsubscribe_source,
        unsubscribe_event_id = EXCLUDED.unsubscribe_event_id,
        updated_at           = now()
    WHERE subscription.unsubscribed_at IS NULL
       OR EXCLUDED.unsubscribed_at < subscription.unsubscribed_at
    -- True for INSERT, false for UPDATE.
    RETURNING (xmax = 0) AS inserted, status, unsubscribed_at, unsubscribe_source;
"""

_SELECT_SQL = """
    SELECT status, unsubscribed_at, unsubscribe_source
    FROM subscription
    WHERE user_id = %(user_id)s AND writer_id = %(writer_id)s;
"""


class DatabaseError(Exception):
    """Postgres operation failed."""


@dataclass(frozen=True, slots=True)
class UnsubscribeResult:
    user_id: str
    writer_id: str
    status: str
    unsubscribed_at: datetime | None
    unsubscribe_source: str | None
    outcome: Outcome


def build_pool(settings: Settings) -> ConnectionPool[_Connection]:
    """Create the Postgres connection pool."""
    return ConnectionPool(
        conninfo=settings.dsn(),
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        open=False,
    )


class SubscriptionRepository:
    """Read and update subscriptions in Postgres."""

    def __init__(self, pool: ConnectionPool[_Connection]) -> None:
        self._pool = pool

    def unsubscribe(
        self,
        *,
        event_id: str,
        user_id: str,
        writer_id: str,
        source: str,
        requested_at: datetime,
    ) -> UnsubscribeResult:
        params = {
            "event_id": event_id,
            "user_id": user_id,
            "writer_id": writer_id,
            "source": source,
            "requested_at": requested_at,
        }
        try:
            with self._pool.connection() as conn:
                row = conn.execute(_UPSERT_SQL, params).fetchone()
                if row is not None:
                    # A row was created or updated.
                    inserted, status, unsubscribed_at, unsubscribe_source = row
                    outcome: Outcome = "created" if inserted else "updated"
                else:
                    # Nothing changed. Return the existing subscription.
                    row = conn.execute(_SELECT_SQL, params).fetchone()
                    if row is None:
                        raise DatabaseError(f"subscription row missing for {user_id}:{writer_id}")
                    status, unsubscribed_at, unsubscribe_source = row
                    outcome = "unchanged"
        except psycopg.Error as error:
            raise DatabaseError(str(error)) from error

        return UnsubscribeResult(
            user_id=user_id,
            writer_id=writer_id,
            status=status,
            unsubscribed_at=unsubscribed_at,
            unsubscribe_source=unsubscribe_source,
            outcome=outcome,
        )

    def check_health(self) -> None:
        try:
            with self._pool.connection() as conn:
                conn.execute("SELECT 1")
        except psycopg.Error as error:
            raise DatabaseError(str(error)) from error
