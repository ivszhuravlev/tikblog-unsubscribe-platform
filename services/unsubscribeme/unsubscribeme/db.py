from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg
from psycopg_pool import ConnectionPool

from unsubscribeme.config import Settings

# The default row shape: plain tuples, not dict_row. Keeps the pool's generic
# type a single, unremarkable annotation instead of threading a custom row
# factory type through every signature below.
_Connection = psycopg.Connection[tuple[Any, ...]]

Outcome = Literal["created", "updated", "unchanged"]

# The earliest requested_at wins, not the last write: the row records when the
# user asked, not when we happened to process it, so an out-of-order arrival
# (a Legal batch two weeks later carrying an older request) can never move the
# consent timestamp forward.
#
# The WHERE guard also makes a plain duplicate a true no-op: no new tuple
# version, no WAL, no dead tuple, and updated_at does not move. "Reprocessing
# changes nothing" is then literally true for every column — and, because of
# that same guard, a duplicate returns ZERO rows from RETURNING. That is not
# an error, see SubscriptionRepository.unsubscribe below.
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
    -- xmax = 0 is the standard Postgres tell for "this RETURNING row came from
    -- the INSERT branch of the upsert": a brand-new tuple has no prior deleter,
    -- so its xmax is unset. Without this it looks like an unrelated boolean.
    RETURNING (xmax = 0) AS inserted, status, unsubscribed_at, unsubscribe_source;
"""

_SELECT_SQL = """
    SELECT status, unsubscribed_at, unsubscribe_source
    FROM subscription
    WHERE user_id = %(user_id)s AND writer_id = %(writer_id)s;
"""


class DatabaseError(Exception):
    """Any failure talking to Postgres, translated from psycopg so the API
    layer never has to import it."""


@dataclass(frozen=True, slots=True)
class UnsubscribeResult:
    user_id: str
    writer_id: str
    status: str
    unsubscribed_at: datetime | None
    unsubscribe_source: str | None
    outcome: Outcome


def build_pool(settings: Settings) -> ConnectionPool[_Connection]:
    """Build the pool unopened; the caller opens and closes it in the app lifespan."""
    return ConnectionPool(
        conninfo=settings.dsn(),
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        open=False,
    )


class SubscriptionRepository:
    """Owns the one statement this service exists to run."""

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
                    inserted, status, unsubscribed_at, unsubscribe_source = row
                    outcome: Outcome = "created" if inserted else "updated"
                else:
                    # Duplicate: the WHERE guard skipped the write. Read back
                    # the row that is already there to answer the caller.
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
