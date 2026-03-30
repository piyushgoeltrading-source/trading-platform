"""
app/core/logging.py

Structured Logging — PiyushTrade
====================================
All log records include:
  - timestamp (UTC ISO 8601)
  - module name
  - event type (via `event` key in `extra`)
  - relevant IDs (user_id, strategy_id) when available

Log all:
  - WebSocket reconnects
  - Feed degradation
  - Redis failures
  - Data validation failures
  - Backpressure drops

Usage:
    from app.core.logging import get_structured_logger
    logger = get_structured_logger(__name__)
    logger.info("tick written", extra={"event": "tick_written", "instrument_token": 12345})
"""

import json
import logging
import sys
from datetime import timezone, datetime
from typing import Any


class _UTCISOFormatter(logging.Formatter):
    """
    JSON log formatter.

    Output format per line:
    {
        "timestamp": "2025-03-29T10:15:30.123456Z",
        "level": "INFO",
        "module": "options_service",
        "event": "feed_degraded",
        "message": "...",
        ... (any extra fields)
    }
    """

    def format(self, record: logging.LogRecord) -> str:
        # UTC timestamp, always
        utc_dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        timestamp = utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        payload: dict[str, Any] = {
            "timestamp": timestamp,
            "level": record.levelname,
            "module": record.name,
            "event": getattr(record, "event", "unspecified"),
            "message": record.getMessage(),
        }

        # Optional contextual IDs — included only when present
        for field in ("user_id", "strategy_id", "instrument_token", "task_id"):
            val = getattr(record, field, None)
            if val is not None:
                payload[field] = val

        # Absorb any remaining `extra` fields
        skip = {
            "args", "created", "exc_info", "exc_text", "filename",
            "funcName", "levelname", "levelno", "lineno", "message",
            "module", "msecs", "msg", "name", "pathname", "process",
            "processName", "relativeCreated", "stack_info", "taskName",
            "thread", "threadName",
            # fields we already handled
            "event", "user_id", "strategy_id", "instrument_token", "task_id",
        }
        for key, value in record.__dict__.items():
            if key not in skip:
                payload[key] = value

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def get_structured_logger(name: str) -> logging.Logger:
    """
    Return a named logger configured with JSON structured output to stdout.

    Calling this multiple times with the same name is safe — the handler
    is only attached once.
    """
    log = logging.getLogger(name)

    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_UTCISOFormatter())
        log.addHandler(handler)
        log.propagate = False

    # Respect LOG_LEVEL from environment; default INFO
    import os
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log.setLevel(getattr(logging, level_name, logging.INFO))

    return log


# ---------------------------------------------------------------------------
# Convenience: module-level root logger setup
# Call this once from app startup (main.py).
# ---------------------------------------------------------------------------

def configure_root_logging() -> None:
    """
    Configure the root logger for the application.
    Call once at startup before any other imports that log.
    """
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_UTCISOFormatter())
    root.addHandler(handler)

    import os
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    root.setLevel(getattr(logging, level_name, logging.INFO))
