# app/engines/execution_guard.py
"""
app/engines/execution_guard.py

Execution Safety Guard — PiyushTrade Phase 3, Step 8
=====================================================
Circuit breaker + pre-execution safety gate for all live order flow.

Circuit breaker state machine (per user_id, stored in Redis):

    CLOSED ──(5 failures in 60s)──► OPEN
      ▲                                │
      │                           (cooldown)
      │                                ▼
      └──(probe succeeds)────── HALF_OPEN
                                       │
                                  (probe fails)
                                       │
                                       ▼
                                     OPEN

Redis keys (all scoped to user_id — one user's failures never affect another):
    execution_guard:circuit:<user_id>   →  CircuitState value (str), no TTL
    execution_guard:failures:<user_id>  →  int counter, TTL=cooldown_seconds
                                           Expires naturally — no cron needed
    execution_guard:probe:<user_id>     →  "1", TTL=probe_ttl_seconds
                                           Limits HALF_OPEN to one probe at a time

Rules:
  - Circuit state lives in Redis only — operational state, not financial truth.
  - ZERO backtest logic.
  - ZERO order placement logic — read/write Redis only.
  - All public methods preserve the exact signatures defined in the stub.
  - get_structured_logger is the only logging import.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

import redis.asyncio as aioredis

from app.core.config import settings
from app.core.logging import get_structured_logger
from app.core.time_utils import now_utc

logger = get_structured_logger(__name__)


# ---------------------------------------------------------------------------
# Circuit breaker config defaults
# ---------------------------------------------------------------------------

_DEFAULT_FAILURE_THRESHOLD: int = 5    # Failures within window → OPEN
_DEFAULT_COOLDOWN_SECONDS: int = 60    # Sliding failure window + cooldown TTL
_DEFAULT_PROBE_TTL_SECONDS: int = 30   # HALF_OPEN probe slot lifetime


# ---------------------------------------------------------------------------
# Circuit state enum + GuardDecision (preserved from stub exactly)
# ---------------------------------------------------------------------------

class CircuitState(str, Enum):
    CLOSED    = "CLOSED"     # Normal — all orders pass through
    OPEN      = "OPEN"       # Fault threshold exceeded — all orders blocked
    HALF_OPEN = "HALF_OPEN"  # Recovery probe — one order allowed through


@dataclass
class GuardDecision:
    """Returned by ExecutionGuard.check() before any order is submitted."""
    allowed: bool
    reason: str = ""
    circuit_state: CircuitState = CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Redis key builders
# ---------------------------------------------------------------------------

def _circuit_key(user_id: int) -> str:
    return f"execution_guard:circuit:{user_id}"

def _failures_key(user_id: int) -> str:
    return f"execution_guard:failures:{user_id}"

def _probe_key(user_id: int) -> str:
    return f"execution_guard:probe:{user_id}"


# ---------------------------------------------------------------------------
# Retry policy config
# ---------------------------------------------------------------------------

def retry_policy() -> dict[str, Any]:
    """
    Return the exponential backoff configuration for transient broker errors.

    Intended for use by the execution engine or a Celery retry task.
    Not applied inside this file — separation of concerns.

    Returns a plain dict so callers are not coupled to a specific retry library.

    Fields:
        max_attempts    Total attempts including the first (not just retries).
        base_delay_s    Initial wait in seconds before the first retry.
        max_delay_s     Cap on the computed delay — prevents runaway backoff.
        multiplier      Each delay = previous × multiplier.
        jitter          If True, add uniform random jitter ±20% to each delay.
                        Prevents thundering herd when multiple strategies retry
                        simultaneously after a broker outage.

    Example delays (base=1s, multiplier=2, no jitter):
        Attempt 1 → immediate
        Attempt 2 → 1s
        Attempt 3 → 2s
        Attempt 4 → 4s
        Attempt 5 → 8s  (capped at max_delay_s=30s)
    """
    return {
        "max_attempts": 5,
        "base_delay_s": 1,
        "max_delay_s": 30,
        "multiplier": 2,
        "jitter": True,
    }


# ---------------------------------------------------------------------------
# ExecutionGuard
# ---------------------------------------------------------------------------

class ExecutionGuard:
    """
    Safety gate for all live order execution paths.

    All circuit state is persisted in Redis — no in-memory state after
    __init__ (except config values). This means the guard survives process
    restarts and works correctly across multiple Uvicorn workers.

    Instantiation:
        guard = ExecutionGuard()               # uses settings.REDIS_URL
        guard = ExecutionGuard(redis_client=r) # inject for testing

    Public interface (signatures match stub exactly):
        await guard.check(user_id, strategy_id, order_payload) → GuardDecision
        await guard.record_success(user_id, strategy_id)
        await guard.record_failure(user_id, strategy_id, error)
        guard.get_state()  — sync, returns in-memory fallback (use for tests only)
    """

    def __init__(
        self,
        redis_client: Any = None,
        failure_threshold: int = _DEFAULT_FAILURE_THRESHOLD,
        cooldown_seconds: int = _DEFAULT_COOLDOWN_SECONDS,
        probe_ttl_seconds: int = _DEFAULT_PROBE_TTL_SECONDS,
    ) -> None:
        self._redis_client = redis_client          # Injected client (tests / DI)
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.probe_ttl_seconds = probe_ttl_seconds

        # In-memory fallback — only used by get_state() for test compatibility.
        # All production logic reads from Redis.
        self._state: CircuitState = CircuitState.CLOSED

    # ------------------------------------------------------------------
    # Redis connection helper
    # ------------------------------------------------------------------

    async def _get_redis(self) -> aioredis.Redis:
        """
        Return an async Redis client.

        If a client was injected at construction (e.g. in tests), return it.
        Otherwise create a connection from settings.REDIS_URL.

        Callers are responsible for closing ad-hoc connections — use the
        _redis() context manager pattern in each method.
        """
        if self._redis_client is not None:
            return self._redis_client
        return aioredis.from_url(settings.REDIS_URL, decode_responses=True)

    # ------------------------------------------------------------------
    # Private: circuit state read/write
    # ------------------------------------------------------------------

    async def _read_circuit_state(self, user_id: int) -> CircuitState:
        """
        Read the current circuit state from Redis for this user.

        Returns CLOSED if no state key exists (first-time or after key eviction).
        """
        redis = await self._get_redis()
        try:
            raw = await redis.get(_circuit_key(user_id))
            if raw is None:
                return CircuitState.CLOSED
            try:
                return CircuitState(raw)
            except ValueError:
                # Corrupted value — default to CLOSED and overwrite
                logger.error(
                    "execution_guard_invalid_circuit_state",
                    user_id=user_id,
                    raw_value=raw,
                )
                await redis.set(_circuit_key(user_id), CircuitState.CLOSED.value)
                return CircuitState.CLOSED
        finally:
            if self._redis_client is None:
                await redis.aclose()

    async def _write_circuit_state(self, user_id: int, state: CircuitState) -> None:
        """Persist circuit state to Redis. No TTL — state is explicit."""
        redis = await self._get_redis()
        try:
            await redis.set(_circuit_key(user_id), state.value)
        finally:
            if self._redis_client is None:
                await redis.aclose()

    # ------------------------------------------------------------------
    # Private: circuit state transitions
    # ------------------------------------------------------------------

    async def _check_circuit(self, user_id: int) -> CircuitState:
        """
        Read and return the current circuit state for this user.
        Pure read — no side effects.
        """
        return await self._read_circuit_state(user_id)

    async def _open_circuit(self, user_id: int) -> None:
        """
        Transition circuit to OPEN. Clears any probe slot.

        Called when failure count crosses the threshold.
        Logged at ERROR level — this is an operator-alertable event.
        """
        redis = await self._get_redis()
        try:
            await redis.set(_circuit_key(user_id), CircuitState.OPEN.value)
            await redis.delete(_probe_key(user_id))
        finally:
            if self._redis_client is None:
                await redis.aclose()

        logger.error(
            "execution_guard_circuit_opened",
            user_id=user_id,
            failure_threshold=self.failure_threshold,
            cooldown_seconds=self.cooldown_seconds,
            timestamp_utc=now_utc().isoformat(),
        )

    async def _half_open_circuit(self, user_id: int) -> None:
        """
        Transition circuit to HALF_OPEN.

        Called when the failure counter TTL has expired (counter key gone),
        indicating the cooldown period has elapsed. One probe order is allowed.
        """
        await self._write_circuit_state(user_id, CircuitState.HALF_OPEN)

        logger.info(
            "execution_guard_circuit_half_open",
            user_id=user_id,
            timestamp_utc=now_utc().isoformat(),
        )

    async def _close_circuit(self, user_id: int) -> None:
        """
        Transition circuit to CLOSED. Clears failure counter and probe slot.

        Called when a successful order is placed (probe succeeded or normal op).
        """
        redis = await self._get_redis()
        try:
            await redis.set(_circuit_key(user_id), CircuitState.CLOSED.value)
            await redis.delete(_failures_key(user_id))
            await redis.delete(_probe_key(user_id))
        finally:
            if self._redis_client is None:
                await redis.aclose()

        logger.info(
            "execution_guard_circuit_closed",
            user_id=user_id,
            timestamp_utc=now_utc().isoformat(),
        )

    # ------------------------------------------------------------------
    # Private: failure counter (sliding window via Redis TTL)
    # ------------------------------------------------------------------

    async def _increment_failure_counter(self, user_id: int) -> int:
        """
        Increment the failure counter for this user and return the new count.

        The key TTL is reset on every increment. This creates a sliding window:
        the counter only matters if N failures occurred within cooldown_seconds
        of each other. A quiet period resets the window automatically.

        Returns the new failure count after incrementing.
        """
        redis = await self._get_redis()
        try:
            pipe = redis.pipeline()
            await pipe.incr(_failures_key(user_id))
            await pipe.expire(_failures_key(user_id), self.cooldown_seconds)
            results = await pipe.execute()
            return int(results[0])
        finally:
            if self._redis_client is None:
                await redis.aclose()

    async def _reset_failure_counter(self, user_id: int) -> None:
        """Delete the failure counter key. Used on success to reset the window."""
        redis = await self._get_redis()
        try:
            await redis.delete(_failures_key(user_id))
        finally:
            if self._redis_client is None:
                await redis.aclose()

    # ------------------------------------------------------------------
    # Private: HALF_OPEN probe slot
    # ------------------------------------------------------------------

    async def _claim_probe_slot(self, user_id: int) -> bool:
        """
        Atomically claim the probe slot for a HALF_OPEN circuit.

        Returns True if the probe slot was claimed (this order is the probe).
        Returns False if another order already holds the probe slot.

        SET NX is atomic — no race condition between concurrent requests.
        """
        redis = await self._get_redis()
        try:
            was_set = await redis.set(
                _probe_key(user_id),
                "1",
                nx=True,
                ex=self.probe_ttl_seconds,
            )
            return was_set is True
        finally:
            if self._redis_client is None:
                await redis.aclose()

    # ------------------------------------------------------------------
    # Public interface (signatures match stub exactly)
    # ------------------------------------------------------------------

    async def check(
        self,
        user_id: int,
        strategy_id: int,
        order_payload: dict,
    ) -> GuardDecision:
        """
        Run all pre-execution checks for a proposed order.

        Check sequence:
          1. Read circuit state from Redis.
          2. If OPEN  → check if cooldown has elapsed (failure counter gone).
                        If yes → transition to HALF_OPEN.
                        If no  → block, return GuardDecision(allowed=False).
          3. If HALF_OPEN → attempt to claim the probe slot (SET NX).
                            Only one probe allowed at a time.
                            If slot claimed  → allow this order as the probe.
                            If slot occupied → block.
          4. If CLOSED → allow.

        Args:
            user_id:       ID of the user submitting the order.
            strategy_id:   ID of the strategy generating the order.
            order_payload: Order dict (used for logging context only here —
                           idempotency is handled by execution_engine).

        Returns:
            GuardDecision with allowed=True/False, reason, and circuit_state.
        """
        state = await self._check_circuit(user_id)

        log_context = {
            "user_id": user_id,
            "strategy_id": strategy_id,
            "circuit_state": state.value,
            "symbol": order_payload.get("trading_symbol", ""),
            "timestamp_utc": now_utc().isoformat(),
        }

        # ── OPEN: check if cooldown has elapsed ──────────────────────────
        if state == CircuitState.OPEN:
            redis = await self._get_redis()
            try:
                failure_count_raw = await redis.get(_failures_key(user_id))
            finally:
                if self._redis_client is None:
                    await redis.aclose()

            if failure_count_raw is None:
                # Failure counter TTL expired → cooldown elapsed → HALF_OPEN
                await self._half_open_circuit(user_id)
                state = CircuitState.HALF_OPEN
                logger.info("execution_guard_cooldown_elapsed", **log_context)
            else:
                # Still within cooldown window — block
                logger.warning("execution_guard_circuit_open_blocked", **log_context)
                return GuardDecision(
                    allowed=False,
                    reason="circuit_open",
                    circuit_state=CircuitState.OPEN,
                )

        # ── HALF_OPEN: allow only one probe order ────────────────────────
        if state == CircuitState.HALF_OPEN:
            probe_claimed = await self._claim_probe_slot(user_id)
            if not probe_claimed:
                logger.warning(
                    "execution_guard_half_open_probe_occupied",
                    **log_context,
                )
                return GuardDecision(
                    allowed=False,
                    reason="half_open_probe_occupied",
                    circuit_state=CircuitState.HALF_OPEN,
                )

            logger.info("execution_guard_half_open_probe_granted", **log_context)
            return GuardDecision(
                allowed=True,
                reason="half_open_probe",
                circuit_state=CircuitState.HALF_OPEN,
            )

        # ── CLOSED: normal operation ─────────────────────────────────────
        logger.info("execution_guard_check_passed", **log_context)
        return GuardDecision(
            allowed=True,
            reason="circuit_closed",
            circuit_state=CircuitState.CLOSED,
        )

    async def record_success(self, user_id: int, strategy_id: int) -> None:
        """
        Record a successful broker order placement.

        Always resets the failure counter.
        If the circuit was HALF_OPEN (probe succeeded), transitions to CLOSED.

        Args:
            user_id:     ID of the user whose order succeeded.
            strategy_id: ID of the strategy (for log context).
        """
        state = await self._check_circuit(user_id)

        await self._reset_failure_counter(user_id)

        if state == CircuitState.HALF_OPEN:
            await self._close_circuit(user_id)
            logger.info(
                "execution_guard_probe_succeeded_circuit_closed",
                user_id=user_id,
                strategy_id=strategy_id,
                timestamp_utc=now_utc().isoformat(),
            )
        else:
            logger.debug(
                "execution_guard_success_recorded",
                user_id=user_id,
                strategy_id=strategy_id,
                circuit_state=state.value,
            )

    async def record_failure(
        self,
        user_id: int,
        strategy_id: int,
        error: str,
    ) -> None:
        """
        Record a failed broker order placement.

        Increments the sliding-window failure counter.
        If count reaches failure_threshold → opens circuit.
        If circuit was HALF_OPEN (probe failed) → reopens immediately.

        Args:
            user_id:     ID of the user whose order failed.
            strategy_id: ID of the strategy (for log context).
            error:       Error message from the broker exception.
        """
        state = await self._check_circuit(user_id)

        if state == CircuitState.HALF_OPEN:
            # Probe failed → reopen immediately, no need to check counter
            await self._open_circuit(user_id)
            logger.error(
                "execution_guard_probe_failed_circuit_reopened",
                user_id=user_id,
                strategy_id=strategy_id,
                error=error,
                timestamp_utc=now_utc().isoformat(),
            )
            return

        # CLOSED: increment counter and check threshold
        new_count = await self._increment_failure_counter(user_id)

        logger.warning(
            "execution_guard_failure_recorded",
            user_id=user_id,
            strategy_id=strategy_id,
            failure_count=new_count,
            failure_threshold=self.failure_threshold,
            error=error,
            timestamp_utc=now_utc().isoformat(),
        )

        if new_count >= self.failure_threshold:
            await self._open_circuit(user_id)

    def get_state(self) -> CircuitState:
        """
        Return the in-memory circuit state.

        Note: Production state lives in Redis. This method returns the
        in-memory fallback — use only in tests or for health check display.
        For accurate state, call await _check_circuit(user_id) instead.
        """
        return self._state
"""

---

**State machine walkthrough — confirm this is correct before Step 9:**
```
Normal operation (CLOSED):
  check()          → allowed=True, reason="circuit_closed"
  record_failure() → counter=1,2,3,4 (no transition)
  record_failure() → counter=5 → _open_circuit() → state=OPEN

Circuit open (OPEN, within cooldown window):
  check()          → failure key still exists in Redis
                   → allowed=False, reason="circuit_open"

Cooldown elapsed (OPEN → HALF_OPEN):
  check()          → failure key TTL expired (key gone)
                   → _half_open_circuit() → state=HALF_OPEN
                   → _claim_probe_slot() → SET NX succeeds
                   → allowed=True, reason="half_open_probe"

  Second concurrent request during HALF_OPEN:
  check()          → probe key already exists
                   → allowed=False, reason="half_open_probe_occupied"

Probe succeeds (HALF_OPEN → CLOSED):
  record_success() → state==HALF_OPEN → _close_circuit()
                   → deletes failures key + probe key → state=CLOSED

Probe fails (HALF_OPEN → OPEN):
  record_failure() → state==HALF_OPEN → _open_circuit() immediately
                   → no counter check needed → state=OPEN
"""                   