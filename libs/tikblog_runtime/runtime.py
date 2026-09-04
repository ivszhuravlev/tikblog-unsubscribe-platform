import json
import logging
import signal
import threading
from datetime import UTC, datetime
from types import FrameType

RESERVED_LOG_RECORD_FIELDS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)))


class JsonFormatter(logging.Formatter):
    """Convert each log record to one JSON line."""

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "level": record.levelname,
            "service": self._service_name,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Add fields supplied through logging's extra={...}.
        for key, value in vars(record).items():
            if key not in RESERVED_LOG_RECORD_FIELDS and key not in payload:
                payload[key] = value

        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=str)


def configure_logging(service_name: str, log_level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(service_name))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)


def install_shutdown_handler() -> threading.Event:
    """Create a flag that Ctrl+C or Docker stop will set."""
    logger = logging.getLogger(__name__)
    stopped = threading.Event()

    def request_shutdown(signum: int, _frame: FrameType | None) -> None:
        logger.info("shutdown_requested", extra={"signal": signum})
        stopped.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    return stopped


def run_until_stopped(service_name: str, log_level: str) -> None:
    """Keep a placeholder service alive until it is stopped."""
    configure_logging(service_name, log_level)
    logger = logging.getLogger(__name__)

    stopped = install_shutdown_handler()

    logger.info("service_started")
    stopped.wait()
    logger.info("service_stopped")
