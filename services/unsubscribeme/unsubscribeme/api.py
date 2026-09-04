import logging
import random
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from contracts.model import UnsubscribeSource
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel

from unsubscribeme.config import Settings
from unsubscribeme.db import DatabaseError, Outcome, SubscriptionRepository, build_pool

logger = logging.getLogger(__name__)


class UnsubscribeRequest(BaseModel):
    event_id: str
    user_id: str
    writer_id: str
    source: UnsubscribeSource
    # AwareDatetime rejects a naive timestamp with 422, the same rule the
    # event contract enforces in UnsubscribeEvent.__post_init__.
    requested_at: AwareDatetime
    request_id: str
    cs_agent_id: str | None = None
    legal_batch_id: str | None = None


class UnsubscribeResponse(BaseModel):
    user_id: str
    writer_id: str
    status: str
    unsubscribed_at: datetime | None
    unsubscribe_source: str | None
    outcome: Outcome


class FaultInjector:
    """Decides, per request, whether to fail and how long to sleep first.

    Kept in one place instead of scattered ifs: this is the whole point of the
    stub — a later step turns these knobs to break the service on purpose, so
    the mechanism needs to be obvious and configurable purely through env.
    """

    def __init__(self, settings: Settings) -> None:
        self._failure_rate = settings.failure_rate
        self._failure_status = settings.failure_status
        self._latency_ms = settings.latency_ms
        self._random = random.Random(settings.random_seed)

    @property
    def failure_status(self) -> int:
        return self._failure_status

    def should_fail(self) -> bool:
        return self._failure_rate > 0 and self._random.random() < self._failure_rate

    def sleep_before_call(self) -> None:
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000)


def create_app(settings: Settings) -> FastAPI:
    """Build the FastAPI app. The pool is opened and closed by the lifespan below."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = build_pool(settings)
        pool.open()
        app.state.repository = SubscriptionRepository(pool)
        try:
            yield
        finally:
            pool.close()

    app = FastAPI(lifespan=lifespan)
    fault_injector = FaultInjector(settings)

    @app.post("/unsubscribe")
    def unsubscribe(payload: UnsubscribeRequest, request: Request) -> UnsubscribeResponse:
        if fault_injector.should_fail():
            logger.warning("fault_injected", extra={"request_id": payload.request_id})
            raise HTTPException(
                status_code=fault_injector.failure_status, detail="injected failure"
            )

        fault_injector.sleep_before_call()

        repository: SubscriptionRepository = request.app.state.repository
        try:
            result = repository.unsubscribe(
                event_id=payload.event_id,
                user_id=payload.user_id,
                writer_id=payload.writer_id,
                source=payload.source.value,
                requested_at=payload.requested_at,
            )
        except DatabaseError as error:
            logger.error(
                "database_error",
                extra={
                    "event_id": payload.event_id,
                    "request_id": payload.request_id,
                    "error": str(error),
                },
            )
            raise HTTPException(status_code=503, detail="database unavailable") from error

        # One line per handled request. The consent payload itself is not logged.
        logger.info(
            "unsubscribe_handled",
            extra={
                "event_id": payload.event_id,
                "request_id": payload.request_id,
                "user_id": payload.user_id,
                "writer_id": payload.writer_id,
                "source": payload.source.value,
                "outcome": result.outcome,
            },
        )

        return UnsubscribeResponse(
            user_id=result.user_id,
            writer_id=result.writer_id,
            status=result.status,
            unsubscribed_at=result.unsubscribed_at,
            unsubscribe_source=result.unsubscribe_source,
            outcome=result.outcome,
        )

    @app.get("/health")
    def health(request: Request) -> JSONResponse:
        repository: SubscriptionRepository = request.app.state.repository
        try:
            repository.check_health()
        except DatabaseError as error:
            logger.error("health_check_failed", extra={"error": str(error)})
            return JSONResponse(
                status_code=503, content={"status": "error", "database": "unreachable"}
            )

        return JSONResponse(content={"status": "ok", "database": "ok"})

    return app
