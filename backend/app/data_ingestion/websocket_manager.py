"""
app/data_ingestion/websocket_manager.py

Manages the Zerodha KiteTicker WebSocket connection.

Responsibilities:
  - Exponential backoff reconnect: 3s → 6s → 12s → 24s → max 60s
  - Heartbeat monitor: if no tick received within threshold → trigger reconnect
  - Feed status tracking: LIVE | DEGRADED | DOWN
  - Broadcast feed status to all consumers via a shared status object
  - Log every reconnect attempt (silent failure is not acceptable)

This module does NOT push to Redis — that is option_chain_ingestor.py's job.
This module owns the connection lifecycle only.

Usage:
    manager = WebSocketManager(on_tick_callback, on_status_change_callback)
    await manager.start()
"""

import asyncio
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Optional
import logging

from app.core.logging import get_structured_logger  # Bug 3 fix: get_logger/log_websocket_reconnect do not exist

logger = get_structured_logger(__name__)


# ---------------------------------------------------------------------------
# Feed status enum
# ---------------------------------------------------------------------------

class FeedStatus(str, Enum):
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"
    NOT_STARTED = "NOT_STARTED"


# ---------------------------------------------------------------------------
# Shared feed state — read by the options API to check staleness
# ---------------------------------------------------------------------------

class FeedState:
    """
    Singleton-style shared state object.
    The options API reads this to determine feed health.
    The ingestor writes to this on every tick and status change.
    """

    def __init__(self):
        self.status: FeedStatus = FeedStatus.NOT_STARTED
        self.last_tick_utc: Optional[datetime] = None
        self.staleness_seconds: Optional[float] = None
        self.reconnect_attempts: int = 0
        self._lock = asyncio.Lock()

    async def update_tick(self):
        """Call on every received tick to reset the staleness clock."""
        async with self._lock:
            self.last_tick_utc = datetime.now(timezone.utc)
            self.status = FeedStatus.LIVE
            self.staleness_seconds = 0.0

    async def update_status(self, status: FeedStatus):
        async with self._lock:
            old = self.status
            self.status = status
            if old != status:
                logger.warning(
                    "Feed status changed",
                    extra={
                        "event": "FEED_STATUS_CHANGE",
                        "old_status": old,
                        "new_status": status,
                    },
                )

    def compute_staleness(self) -> Optional[float]:
        """Return seconds since last tick, or None if no tick ever received."""
        if self.last_tick_utc is None:
            return None
        delta = datetime.now(timezone.utc) - self.last_tick_utc
        return delta.total_seconds()

    def to_dict(self) -> dict:
        staleness = self.compute_staleness()
        return {
            "status": self.status,
            "last_tick": self.last_tick_utc.isoformat() if self.last_tick_utc else None,
            "staleness_seconds": round(staleness, 2) if staleness is not None else None,
            "reconnect_attempts": self.reconnect_attempts,
        }


# Global feed state — imported by options API and ingestor
feed_state = FeedState()


# ---------------------------------------------------------------------------
# Backoff calculator
# ---------------------------------------------------------------------------

class ExponentialBackoff:
    """
    Exponential backoff: 3 → 6 → 12 → 24 → 60 (capped).
    Resets to base on explicit reset() call.
    """

    BASE = 3      # seconds
    MAX = 60      # seconds
    MULTIPLIER = 2

    def __init__(self):
        self._current = self.BASE
        self.attempt = 0

    def next_delay(self) -> int:
        delay = self._current
        self._current = min(self._current * self.MULTIPLIER, self.MAX)
        self.attempt += 1
        return delay

    def reset(self):
        self._current = self.BASE
        self.attempt = 0


# ---------------------------------------------------------------------------
# WebSocket Manager
# ---------------------------------------------------------------------------

class WebSocketManager:
    """
    Manages the Zerodha KiteTicker WebSocket connection lifecycle.

    Args:
        on_tick: Async callback called with raw tick data on every message.
                 Signature: async def on_tick(ticks: list[dict]) -> None
        heartbeat_timeout: Seconds of silence before triggering reconnect.
                           Default 10s — NSE sends ticks every ~1s during
                           market hours so 10s is a generous threshold.
    """

    def __init__(
        self,
        on_tick: Callable,
        heartbeat_timeout: int = 10,
    ):
        self._on_tick = on_tick
        self._heartbeat_timeout = heartbeat_timeout
        self._backoff = ExponentialBackoff()
        self._ticker = None          # KiteTicker instance (set in start())
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self, kite_ticker):
        """
        Start the WebSocket connection with the given KiteTicker instance.

        Args:
            kite_ticker: A configured kiteconnect.KiteTicker object.
                         Credentials must already be set before passing in.
        """
        self._ticker = kite_ticker
        self._running = True
        self._register_callbacks()

        logger.info("WebSocket manager starting")
        await feed_state.update_status(FeedStatus.DOWN)

        await self._connect_with_backoff()

    async def stop(self):
        """Gracefully stop the connection and heartbeat monitor."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._ticker:
            try:
                self._ticker.stop()
            except Exception as e:
                logger.warning("Error stopping ticker", extra={"error": str(e)})
        await feed_state.update_status(FeedStatus.DOWN)
        logger.info("WebSocket manager stopped")

    # ------------------------------------------------------------------
    # KiteTicker callbacks
    # ------------------------------------------------------------------

    def _register_callbacks(self):
        """Wire up KiteTicker event callbacks."""
        self._ticker.on_ticks = self._on_ticks
        self._ticker.on_connect = self._on_connect
        self._ticker.on_close = self._on_close
        self._ticker.on_error = self._on_error
        self._ticker.on_reconnect = self._on_reconnect_event
        self._ticker.on_noreconnect = self._on_noreconnect

    def _on_ticks(self, ws, ticks):
        """Called by KiteTicker on every tick batch."""
        asyncio.create_task(self._handle_ticks(ticks))

    def _on_connect(self, ws, response):
        """Called when WebSocket connection is established."""
        logger.info("WebSocket connected", extra={"response": str(response)})
        self._backoff.reset()
        asyncio.create_task(feed_state.update_status(FeedStatus.LIVE))
        # Start heartbeat monitor
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

    def _on_close(self, ws, code, reason):
        """Called when WebSocket connection is closed."""
        logger.warning(
            "WebSocket closed",
            extra={"code": code, "reason": reason},
        )
        asyncio.create_task(feed_state.update_status(FeedStatus.DOWN))
        if self._running:
            asyncio.create_task(self._connect_with_backoff())

    def _on_error(self, ws, code, reason):
        """Called on WebSocket error."""
        logger.error(
            "WebSocket error",
            extra={"code": code, "reason": reason},
        )
        asyncio.create_task(feed_state.update_status(FeedStatus.DEGRADED))

    def _on_reconnect_event(self, ws, attempts_count):
        """Called by KiteTicker's internal reconnect logic (if enabled)."""
        logger.warning(
            "KiteTicker internal reconnect",
            extra={"attempts": attempts_count},
        )

    def _on_noreconnect(self, ws):
        """Called when KiteTicker exhausts its internal reconnect attempts."""
        logger.error("KiteTicker gave up reconnecting — manager will take over")
        asyncio.create_task(feed_state.update_status(FeedStatus.DOWN))
        if self._running:
            asyncio.create_task(self._connect_with_backoff())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _handle_ticks(self, ticks: list):
        """Process incoming ticks — update feed state, then call consumer."""
        await feed_state.update_tick()
        try:
            await self._on_tick(ticks)
        except Exception as e:
            logger.error(
                "Tick handler raised an exception",
                extra={"error": str(e), "tick_count": len(ticks)},
            )

    async def _connect_with_backoff(self):
        """
        Attempt to connect. On failure, wait with exponential backoff and retry.
        Logs every attempt — silent failure is not acceptable.
        """
        while self._running:
            attempt = self._backoff.attempt + 1
            try:
                logger.info(
                    "WebSocket connect attempt",
                    extra={"attempt": attempt},
                )
                await feed_state.update_status(FeedStatus.DOWN)

                # KiteTicker.connect() is blocking — run in executor
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._ticker.connect, True)

                # If connect() returns without error, we're connected
                # Callbacks take over from here
                return

            except Exception as e:
                delay = self._backoff.next_delay()
                feed_state.reconnect_attempts += 1

                # RULE: Log every reconnect attempt
                # Bug 3 fix: log_websocket_reconnect does not exist — use inline structured log
                logger.warning(
                    "websocket_reconnect_attempt",
                    extra={
                        "event": "WEBSOCKET_RECONNECT",
                        "attempt": attempt,
                        "backoff_seconds": delay,
                        "feed": "NSE_OPTIONS",
                        "reason": str(e),
                    },
                )

                await feed_state.update_status(FeedStatus.DOWN)
                logger.warning(
                    "WebSocket connect failed — backing off",
                    extra={
                        "attempt": attempt,
                        "backoff_seconds": delay,
                        "error": str(e),
                    },
                )
                await asyncio.sleep(delay)

    async def _heartbeat_monitor(self):
        """
        Monitor for tick silence.
        If no tick is received within heartbeat_timeout seconds → reconnect.

        RULE: If no tick received in X seconds → trigger reconnect automatically.
        """
        logger.info(
            "Heartbeat monitor started",
            extra={"timeout_seconds": self._heartbeat_timeout},
        )

        while self._running:
            await asyncio.sleep(self._heartbeat_timeout)

            staleness = feed_state.compute_staleness()

            if staleness is None:
                # No tick ever received — connection may still be initialising
                continue

            if staleness > self._heartbeat_timeout:
                logger.warning(
                    "Heartbeat timeout — no tick received, triggering reconnect",
                    extra={
                        "staleness_seconds": round(staleness, 2),
                        "threshold_seconds": self._heartbeat_timeout,
                    },
                )
                await feed_state.update_status(FeedStatus.DEGRADED)

                # Stop current connection and reconnect
                if self._ticker:
                    try:
                        self._ticker.stop()
                    except Exception:
                        pass

                asyncio.create_task(self._connect_with_backoff())
                return  # This monitor task ends — _on_connect will start a new one
