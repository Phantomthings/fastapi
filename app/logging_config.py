import json
import logging
import sys
from typing import Any, Dict, Optional


class JsonFormatter(logging.Formatter):
    """Structured JSON formatter that keeps a small and predictable schema."""

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting logic
        log: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key in (
            "event",
            "method",
            "path",
            "status_code",
            "duration_ms",
            "request_id",
            "client",
            "user_agent",
            "filters",
            "total",
            "view",
        ):
            value: Optional[Any] = getattr(record, key, None)
            if value is not None:
                log[key] = value

        if record.exc_info:
            log["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(log, ensure_ascii=False)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Align uvicorn loggers with the same structured formatter
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.setLevel(level)
