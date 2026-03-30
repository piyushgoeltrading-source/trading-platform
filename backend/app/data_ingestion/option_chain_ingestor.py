"""
option_chain_ingestor.py

NSE Option Chain Ingestor — PiyushTrade
========================================
Responsibilities:
  - Connect to Zerodha WebSocket for live option chain ticks.
  - Validate incoming tick payloads before writing to Redis.
  - Write data to Redis with:
      * UTC epoch timestamp field (MANDATORY on every write)
      * Explicit TTL = REDIS_KEY_TTL_SECONDS (MANDATORY on every write)
  - Emit structured logs for all error and state-change events.
  - Provide backpressure hooks for rate-limiting (stub, Phase 3).

NON-NEGOTIABLE RULES:
  - Redis is cache only. Never treat it as source of truth.
  - All timestamps stored in UTC epoch (float). Never IST in Redis.
  - TTL must always be > REDIS_STALENESS_THRESHOLD_SECONDS.
  - Never call broker API outside the execution layer (here we receive
    push data from a WebSocket — that is acceptable ingestion).
  - Malformed ticks are logged and dropped — never written to Redis.
"""

import json
import logging
import asyncio
from typing import Any

from app.core.config import settings
from app.core.time_utils import now_utc_epoch, now_utc
from app.core.logging import get_structured_logger

logger = get_structured_logger(__name__)

# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------

OPTION_CHAIN_KEY_PREFIX = "option_chain:"


def _redis_key(instrument_token: int) -> str:
    return f"{OPTION_CHAIN_KEY_PREFIX}{instrument_token}"


# ---------------------------------------------------------------------------
# Required fields for a valid option chain tick
# ---------------------------------------------------------------------------

REQUIRED_TICK_FIELDS = {
    "instrument_token",
    "last_price",
    "volume",
    "oi",          # open interest
    "bid",
    "ask",
}

REQUIRED_OPTION_FIELDS = {
    "strike",
    "expiry",
    "option_type",  # CE or PE
}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_tick(tick: dict) -> tuple[bool, str]:
    """
    Validate an incoming tick payload.

    Returns:
        (True, "") if valid.
        (False, reason) if invalid.
    """
    if not isinstance(tick, dict):
        return False, "tick is not a dict"

    missing = REQUIRED_TICK_FIELDS - tick.keys()
    if missing:
        return False, f"missing required fields: {sorted(missing)}"

    missing_option = REQUIRED_OPTION_FIELDS - tick.keys()
    if missing_option:
        return False, f"missing option fields: {sorted(missing_option)}"

    if not isinstance(tick.get("instrument_token"), int):
        return False, "instrument_token must be int"

    last_price = tick.get("last_price")
    if not isinstance(last_price, (int, float)) or last_price < 0:
        return False, f"last_price invalid: {last_price!r}"

    oi = tick.get("oi")
    if not isinstance(oi, int) or oi < 0:
        return False, f"oi (open interest) invalid: {oi!r}"

    option_type = tick.get("option_type")
    if option_type not in ("CE", "PE"):
        return False, f"option_type must be CE or PE, got: {option_type!r}"

    return True, ""


# ---------------------------------------------------------------------------
# Backpressure hook (stub — Phase 3 will implement rate limiting)
# ---------------------------------------------------------------------------

class _BackpressureGuard:
    """
    Stub backpressure guard. Phase 3 will add:
      - Token bucket rate limiter per instrument.
      - Adaptive throttling on Redis write latency spikes.
      - Drop policy with metrics emission.

    This class exists so the call sites are already wired correctly.
    """

    def should_process(self, instrument_token: int) -> bool:  # noqa: ARG002
        """Return True if the tick should be processed, False to drop."""
        # TODO (Phase 3): implement rate-limit logic here.
        return True

    def record_drop(self, instrument_token: int, reason: str) -> None:
        """Record a dropped tick for observability."""
        logger.warning(
            "Tick dropped by backpressure guard",
            extra={
                "event": "backpressure_drop",
                "instrument_token": instrument_token,
                "reason": reason,
            },
        )


_backpressure = _BackpressureGuard()


# ---------------------------------------------------------------------------
# Core Redis write
# ---------------------------------------------------------------------------

async def write_tick_to_redis(redis_client: Any, tick: dict) -> bool:
    """
    Validate and write a single option chain tick to Redis.

    Every write:
      - Includes a `_ingested_at_utc` epoch field.
      - Sets TTL = settings.REDIS_KEY_TTL_SECONDS explicitly.

    Args:
        redis_client: An async Redis client instance (e.g. aioredis).
        tick: Raw tick dict received from WebSocket.

    Returns:
        True if written successfully, False otherwise.
    """
    instrument_token = tick.get("instrument_token", "UNKNOWN")

    # --- Backpressure check ---
    if not _backpressure.should_process(instrument_token):
        _backpressure.record_drop(instrument_token, "rate_limit")
        return False

    # --- Validation ---
    valid, reason = _validate_tick(tick)
    if not valid:
        logger.warning(
            "Tick validation failed — dropping tick",
            extra={
                "event": "tick_validation_failure",
                "instrument_token": instrument_token,
                "reason": reason,
                "timestamp_utc": now_utc().isoformat(),
            },
        )
        return False

    # --- Build payload ---
    # Inject UTC ingestion timestamp into every Redis payload.
    payload = {
        **tick,
        "_ingested_at_utc": now_utc_epoch(),  # UTC epoch float — mandatory
    }

    key = _redis_key(instrument_token)
    ttl = settings.REDIS_KEY_TTL_SECONDS  # Must be > REDIS_STALENESS_THRESHOLD_SECONDS

    # Guard: TTL sanity check (config invariant)
    if ttl <= settings.REDIS_STALENESS_THRESHOLD_SECONDS:
        logger.error(
            "CONFIGURATION ERROR: REDIS_KEY_TTL_SECONDS must be greater than "
            "REDIS_STALENESS_THRESHOLD_SECONDS. Refusing to write.",
            extra={
                "event": "config_error",
                "ttl": ttl,
                "staleness_threshold": settings.REDIS_STALENESS_THRESHOLD_SECONDS,
            },
        )
        return False

    # --- Write to Redis ---
    try:
        await redis_client.set(key, json.dumps(payload), ex=ttl)
        logger.debug(
            "Tick written to Redis",
            extra={
                "event": "tick_written",
                "instrument_token": instrument_token,
                "key": key,
                "ttl_seconds": ttl,
                "ingested_at_utc_epoch": payload["_ingested_at_utc"],
            },
        )
        return True
    except Exception as exc:
        logger.error(
            "Redis write failed",
            extra={
                "event": "redis_write_failure",
                "instrument_token": instrument_token,
                "error": str(exc),
                "timestamp_utc": now_utc().isoformat(),
            },
        )
        return False


# ---------------------------------------------------------------------------
# WebSocket ingestor class
# ---------------------------------------------------------------------------

class OptionChainIngestor:
    """
    Manages the WebSocket connection to Zerodha and feeds ticks
    into Redis with full validation, TTL enforcement, and structured logging.
    """

    def __init__(self, redis_client: Any, websocket_manager: Any) -> None:
        self._redis = redis_client
        self._ws_manager = websocket_manager
        self._running = False

    async def start(self) -> None:
        """Start the ingestion loop."""
        self._running = True
        logger.info(
            "OptionChainIngestor starting",
            extra={"event": "ingestor_start", "timestamp_utc": now_utc().isoformat()},
        )
        await self._ws_manager.connect(on_tick=self._handle_tick)

    async def stop(self) -> None:
        """Gracefully stop the ingestion loop."""
        self._running = False
        await self._ws_manager.disconnect()
        logger.info(
            "OptionChainIngestor stopped",
            extra={"event": "ingestor_stop", "timestamp_utc": now_utc().isoformat()},
        )

    async def _handle_tick(self, tick: dict) -> None:
        """Callback invoked by WebSocket manager for each received tick."""
        if not self._running:
            return
        await write_tick_to_redis(self._redis, tick)
