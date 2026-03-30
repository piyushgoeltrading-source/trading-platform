"""
options_service.py

Options Data Service — PiyushTrade
=====================================
Responsibilities:
  - Fetch option chain data from Redis.
  - Enforce staleness policy using UTC timestamps exclusively.
  - Return 503 FEED_DEGRADED when market is open and data is stale.
  - Return data + X-Data-As-Of header when market is closed.
  - Handle Redis unavailability with REDIS_UNAVAILABLE error — NO silent fallback.
  - Validate payload structure on every read.

NON-NEGOTIABLE RULES:
  - All timestamp comparisons use UTC only.
  - Redis is cache only — never the source of truth.
  - If Redis is down: raise RedisUnavailableError. Do NOT call broker API.
  - If market is OPEN and data is stale: raise FeedDegradedError.
  - PostgreSQL is the financial source of truth for all stored data.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.time_utils import (
    from_utc_epoch,
    is_market_open,
    now_utc,
    seconds_since_utc_epoch,
    format_ist,
)
from app.core.logging import get_structured_logger

logger = get_structured_logger(__name__)


# ---------------------------------------------------------------------------
# Standard error codes
# ---------------------------------------------------------------------------

class ErrorCode:
    FEED_DEGRADED = "FEED_DEGRADED"
    REDIS_UNAVAILABLE = "REDIS_UNAVAILABLE"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    INSTRUMENT_NOT_FOUND = "INSTRUMENT_NOT_FOUND"
    MISSING_FIELD = "MISSING_FIELD"


# ---------------------------------------------------------------------------
# Typed exceptions (caught by API layer and converted to HTTP responses)
# ---------------------------------------------------------------------------

class FeedDegradedError(Exception):
    """Raised when market is open and Redis data is stale."""

    def __init__(self, age_seconds: float, threshold: int) -> None:
        self.age_seconds = age_seconds
        self.threshold = threshold
        super().__init__(
            f"Market data is stale: age={age_seconds:.1f}s, threshold={threshold}s"
        )

    def to_dict(self) -> dict:
        return {
            "error_code": ErrorCode.FEED_DEGRADED,
            "message": "Market data is stale. Feed may be degraded.",
            "details": {
                "data_age_seconds": round(self.age_seconds, 2),
                "staleness_threshold_seconds": self.threshold,
            },
        }


class RedisUnavailableError(Exception):
    """Raised when Redis cannot be reached. Never silently fallback."""

    def __init__(self, reason: str = "") -> None:
        self.reason = reason
        super().__init__(f"Redis is unavailable: {reason}")

    def to_dict(self) -> dict:
        return {
            "error_code": ErrorCode.REDIS_UNAVAILABLE,
            "message": "Cache layer is unavailable. Please retry shortly.",
            "details": {"reason": self.reason or "connection_error"},
        }


class InvalidPayloadError(Exception):
    """Raised when a Redis payload fails structural validation."""

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"Payload invalid: field={field}, reason={reason}")

    def to_dict(self) -> dict:
        return {
            "error_code": ErrorCode.INVALID_PAYLOAD,
            "message": "Stored market data is malformed.",
            "details": {"field": self.field, "reason": self.reason},
        }


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------

REQUIRED_PAYLOAD_FIELDS = {
    "_ingested_at_utc",
    "instrument_token",
    "last_price",
    "volume",
    "oi",
    "bid",
    "ask",
    "strike",
    "expiry",
    "option_type",
}


def _validate_payload(data: dict) -> None:
    """
    Validate the structure of a payload read from Redis.
    Raises InvalidPayloadError on any structural issue.
    """
    missing = REQUIRED_PAYLOAD_FIELDS - data.keys()
    if missing:
        raise InvalidPayloadError(
            field=str(sorted(missing)),
            reason="required fields missing from stored payload",
        )

    ingested_at = data.get("_ingested_at_utc")
    if not isinstance(ingested_at, (int, float)):
        raise InvalidPayloadError(
            field="_ingested_at_utc",
            reason=f"expected numeric epoch, got {type(ingested_at).__name__}",
        )

    last_price = data.get("last_price")
    if not isinstance(last_price, (int, float)) or last_price < 0:
        raise InvalidPayloadError(
            field="last_price",
            reason=f"invalid value: {last_price!r}",
        )

    option_type = data.get("option_type")
    if option_type not in ("CE", "PE"):
        raise InvalidPayloadError(
            field="option_type",
            reason=f"must be CE or PE, got {option_type!r}",
        )


# ---------------------------------------------------------------------------
# Core service function
# ---------------------------------------------------------------------------

OPTION_CHAIN_KEY_PREFIX = "option_chain:"


async def get_option_chain(
    redis_client: Any,
    instrument_token: int,
) -> tuple[dict, dict | None]:
    """
    Fetch and validate option chain data for a given instrument token.

    Staleness policy:
      - Market OPEN  + data stale  → raise FeedDegradedError (→ 503)
      - Market OPEN  + data fresh  → return data, no special header
      - Market CLOSED + data exists → return data + X-Data-As-Of header dict
      - Redis key missing          → raise HTTPException 404
      - Redis unavailable          → raise RedisUnavailableError (→ 503)
      - Corrupt payload            → raise InvalidPayloadError (→ 500)

    Args:
        redis_client: Async Redis client.
        instrument_token: NSE instrument token integer.

    Returns:
        Tuple of (payload_dict, extra_headers_dict | None).
        extra_headers_dict contains {"X-Data-As-Of": <IST string>} when
        market is closed.
    """
    key = f"{OPTION_CHAIN_KEY_PREFIX}{instrument_token}"
    now = now_utc()
    market_open = is_market_open(now)

    # --- Fetch from Redis ---
    try:
        raw = await redis_client.get(key)
    except Exception as exc:
        logger.error(
            "Redis unavailable during option chain fetch",
            extra={
                "event": "redis_unavailable",
                "instrument_token": instrument_token,
                "error": str(exc),
                "timestamp_utc": now.isoformat(),
            },
        )
        raise RedisUnavailableError(reason=str(exc)) from exc

    # --- Key not found ---
    if raw is None:
        logger.info(
            "Instrument not found in Redis cache",
            extra={
                "event": "instrument_not_found",
                "instrument_token": instrument_token,
                "market_open": market_open,
                "timestamp_utc": now.isoformat(),
            },
        )
        raise HTTPException(
            status_code=404,
            detail={
                "error_code": ErrorCode.INSTRUMENT_NOT_FOUND,
                "message": f"No data found for instrument {instrument_token}",
                "details": {"instrument_token": instrument_token},
            },
        )

    # --- Deserialize ---
    try:
        data: dict = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.error(
            "Redis payload is not valid JSON",
            extra={
                "event": "payload_corrupt",
                "instrument_token": instrument_token,
                "error": str(exc),
                "timestamp_utc": now.isoformat(),
            },
        )
        raise InvalidPayloadError(
            field="__root__",
            reason="stored value is not valid JSON",
        ) from exc

    # --- Structural validation ---
    _validate_payload(data)

    # --- Staleness check (UTC only) ---
    ingested_at_epoch: float = data["_ingested_at_utc"]
    age_seconds = seconds_since_utc_epoch(ingested_at_epoch)
    threshold = settings.REDIS_STALENESS_THRESHOLD_SECONDS

    if market_open:
        if age_seconds > threshold:
            logger.warning(
                "FEED DEGRADED: market is open and data is stale",
                extra={
                    "event": "feed_degraded",
                    "instrument_token": instrument_token,
                    "data_age_seconds": round(age_seconds, 2),
                    "staleness_threshold_seconds": threshold,
                    "timestamp_utc": now.isoformat(),
                },
            )
            raise FeedDegradedError(age_seconds=age_seconds, threshold=threshold)

        # Market open + data is fresh — return with no special header
        return data, None

    else:
        # Market closed — return data with X-Data-As-Of header
        ingested_at_utc = from_utc_epoch(ingested_at_epoch)
        as_of_ist = format_ist(ingested_at_utc)

        logger.info(
            "Market closed — returning cached data with As-Of header",
            extra={
                "event": "market_closed_response",
                "instrument_token": instrument_token,
                "data_age_seconds": round(age_seconds, 2),
                "as_of_ist": as_of_ist,
                "timestamp_utc": now.isoformat(),
            },
        )

        extra_headers = {"X-Data-As-Of": as_of_ist}
        return data, extra_headers


# ---------------------------------------------------------------------------
# Helper: build standardized HTTP error responses
# (called from the API layer — options.py)
# ---------------------------------------------------------------------------

def build_error_response(
    error_code: str,
    message: str,
    details: dict | None = None,
    status_code: int = 503,
) -> JSONResponse:
    """
    Return a FastAPI JSONResponse with the standard PiyushTrade error envelope.

    Standard format:
        {
            "error_code": "FEED_DEGRADED",
            "message": "...",
            "details": { ... }
        }
    """
    return JSONResponse(
        status_code=status_code,
        content={
            "error_code": error_code,
            "message": message,
            "details": details or {},
        },
    )
