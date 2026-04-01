# app/engines/execution_engine.py
"""
Execution Engine — PiyushTrade Phase 3, Step 7

Order flow (LOCKED — do not reorder):
  OrderRequest + user + strategy_id
    → load Strategy from DB
    → risk_manager.check()        (module-level fn, takes Strategy ORM object)
    → execution_guard.check()     (returns GuardDecision — checked here)
    → BrokerFactory.get(user)
    → asyncio.to_thread(broker.place_order)   (broker methods are sync)
    → DB write (Order row, status=SENT, idempotency_key persisted)
    → return OrderResult

Rules enforced here:
  - ZERO backtest logic
  - ZERO cross-import with backtest_engine.py
  - AsyncSession for all DB writes
  - Idempotency: Redis SET NX (fast reject) + persisted on Order row (audit trail)
  - Idempotency key timestamp floored to nearest MINUTE (not milliseconds)
  - Broker calls wrapped in asyncio.to_thread() — BaseBroker methods are sync
  - DB fail after broker success → log CRITICAL, return result, let
    reconciliation catch the discrepancy (do NOT attempt broker cancel)
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers import base_broker as bb
from app.brokers.base_broker import (
    BrokerError,
    OrderRequest,
    OrderResult,
    OrderStatus,
)
from app.brokers.factory import BrokerFactory, BrokerConfigError
from app.core.config import settings
from app.core.logging import get_structured_logger
from app.engines import risk_manager
from app.engines.execution_guard import ExecutionGuard, CircuitState
from app.engines.risk_manager import RiskCheckError
from app.models.order import Order, OrderStatus as OrderStatusEnum
from app.models.strategy import Strategy
from app.models.user import User

logger = get_structured_logger(__name__)

# ---------------------------------------------------------------------------
# Idempotency config
# ---------------------------------------------------------------------------

_IDEMPOTENCY_TTL_SECONDS: int = 300          # 5 min — covers any realistic retry window
_IDEMPOTENCY_REDIS_PREFIX: str = "idempotency:order:"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ExecutionError(Exception):
    """Raised when the execution engine cannot place an order for an
    unclassified reason (i.e. not risk, guard, broker, or duplicate)."""


class DuplicateOrderError(ExecutionError):
    """Raised when a Redis idempotency key collision is detected.
    The caller should NOT retry — the original order is in flight."""


class GuardRejectedError(ExecutionError):
    """Raised when ExecutionGuard.check() returns allowed=False.
    Carries the GuardDecision.reason for the API response."""

    def __init__(self, reason: str, circuit_state: CircuitState) -> None:
        self.reason = reason
        self.circuit_state = circuit_state
        super().__init__(f"Execution guard rejected order: {reason} (circuit={circuit_state})")


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------

def _build_idempotency_key(
    user_id: int,
    strategy_id: int,
    order_request: OrderRequest,
) -> str:
    """
    Build a deterministic, collision-resistant idempotency key.

    Timestamp is floored to the nearest MINUTE so retries within the same
    minute map to the same logical order. Millisecond precision would produce
    a new key on every retry — defeating the purpose entirely.

    Key ingredients:
        user_id | strategy_id | trading_symbol | exchange | side |
        order_type | product_code | quantity | minute-floored UTC timestamp
    """
    now_utc = datetime.now(timezone.utc)
    floored_minute = now_utc.replace(second=0, microsecond=0).isoformat()

    raw = (
        f"{user_id}:"
        f"{strategy_id}:"
        f"{order_request.trading_symbol}:"
        f"{order_request.exchange.value}:"
        f"{order_request.side.value}:"
        f"{order_request.order_type.value}:"
        f"{order_request.product_code.value}:"
        f"{order_request.quantity}:"
        f"{floored_minute}"
    )
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"{_IDEMPOTENCY_REDIS_PREFIX}{digest}"


async def _redis_claim_idempotency_key(
    redis_url: str,
    idempotency_key: str,
) -> bool:
    """
    Atomically attempt to claim the idempotency key in Redis via SET NX.

    Returns:
        True  → key was newly set — this is the first attempt, proceed.
        False → key already existed — this is a duplicate, reject.

    SET NX is atomic — there is no TOCTOU race between check and claim.
    """
    redis: aioredis.Redis = aioredis.from_url(redis_url, decode_responses=True)
    try:
        was_set = await redis.set(
            idempotency_key,
            "1",
            nx=True,
            ex=_IDEMPOTENCY_TTL_SECONDS,
        )
        # SET NX: returns True if key was set, None if key already existed
        return was_set is True
    finally:
        await redis.aclose()


# ---------------------------------------------------------------------------
# Strategy loader
# ---------------------------------------------------------------------------

async def _load_strategy(
    db: AsyncSession,
    strategy_id: int,
    user_id: int,
) -> Strategy:
    """
    Load the Strategy ORM object from DB.

    Scoped to user_id — multi-tenancy enforced at the query level.
    Raises ExecutionError if the strategy does not exist or belongs to
    a different user.
    """
    result = await db.execute(
        select(Strategy).where(
            Strategy.id == strategy_id,
            Strategy.user_id == user_id,
        )
    )
    strategy: Optional[Strategy] = result.scalar_one_or_none()

    if strategy is None:
        raise ExecutionError(
            f"Strategy {strategy_id} not found for user {user_id}. "
            "Order rejected."
        )

    return strategy


# ---------------------------------------------------------------------------
# Execution Engine
# ---------------------------------------------------------------------------

class ExecutionEngine:
    """
    Orchestrates the full live order placement pipeline.

    This class contains ZERO backtest logic and must never import from
    app.engines.backtest_engine.

    Broker methods (BaseBroker) are synchronous by contract. They are
    dispatched via asyncio.to_thread() to avoid blocking the event loop.

    Usage:
        engine = ExecutionEngine()
        result = await engine.execute(order_request, user, strategy_id, db)
    """

    def __init__(self) -> None:
        self._guard = ExecutionGuard()

    async def execute(
        self,
        order_request: OrderRequest,
        user: User,
        strategy_id: int,
        db: AsyncSession,
    ) -> OrderResult:
        """
        Place a live order through the full execution pipeline.

        Parameters
        ----------
        order_request:
            Fully-populated OrderRequest DTO. For Nuvama orders,
            streaming_symbol MUST be set before calling this method.
            The idempotency_key field will be populated by this method.
        user:
            Authenticated User ORM instance (carries user.broker).
        strategy_id:
            FK to the Strategy that triggered this order.
        db:
            AsyncSession — caller supplies via get_async_db() dependency.

        Returns
        -------
        OrderResult with broker_order_id and order status.

        Raises
        ------
        ExecutionError       — strategy not found, or unclassified failure.
        DuplicateOrderError  — idempotency key collision (do not retry).
        GuardRejectedError   — execution guard circuit open or guard check failed.
        RiskCheckError       — one of the 3 risk checks failed.
        BrokerConfigError    — unknown broker on user record.
        BrokerError          — broker SDK raised an error during placement.
        """
        log = logger.bind(
            user_id=user.id,
            strategy_id=strategy_id,
            symbol=order_request.trading_symbol,
            exchange=order_request.exchange.value,
            side=order_request.side.value,
            quantity=order_request.quantity,
        )
        log.info("execution_engine_start")

        # ------------------------------------------------------------------
        # Step 0: Load Strategy ORM object
        #   risk_manager.check() requires the full Strategy object
        #   (reads strategy.parameters for thresholds).
        # ------------------------------------------------------------------
        strategy = await _load_strategy(db, strategy_id, user.id)

        # ------------------------------------------------------------------
        # Step 1: Build idempotency key + stamp on OrderRequest
        #   Key is built before broker call so it can gate at Redis
        #   and also be persisted on the Order row for audit.
        # ------------------------------------------------------------------
        idempotency_key = _build_idempotency_key(user.id, strategy_id, order_request)
        order_request.idempotency_key = idempotency_key

        # ------------------------------------------------------------------
        # Step 2: Redis idempotency check — fast reject before any I/O
        #   Atomic SET NX prevents concurrent duplicate submissions.
        #   If the key already exists this is a retry within the 5-min window.
        # ------------------------------------------------------------------
        key_claimed = await _redis_claim_idempotency_key(
            redis_url=settings.REDIS_URL,
            idempotency_key=idempotency_key,
        )
        if not key_claimed:
            log.warning(
                "idempotency_duplicate_rejected",
                idempotency_key=idempotency_key,
            )
            raise DuplicateOrderError(
                f"Duplicate order detected within idempotency window. "
                f"Key: {idempotency_key}. "
                "If this was not a retry, wait 5 minutes and resubmit."
            )

        log.info("idempotency_key_claimed", idempotency_key=idempotency_key)

        # ------------------------------------------------------------------
        # Step 3: Risk manager — three sequential checks
        #   (1) daily loss limit, (2) max open positions, (3) margin.
        #   risk_manager is a module with a module-level check() function.
        #   It is read-only — zero DB writes occur inside.
        # ------------------------------------------------------------------
        try:
            await risk_manager.check(
                db=db,
                user=user,
                strategy=strategy,
                order_request=order_request,
            )
            log.info("risk_check_passed")
        except RiskCheckError:
            log.warning("risk_check_failed", check=str(Exception))
            raise

        # ------------------------------------------------------------------
        # Step 4: Execution guard — circuit breaker + operational checks
        #   Guard is currently a stub (Step 8 implements it fully).
        #   We check GuardDecision.allowed and raise if rejected so that
        #   Step 8's implementation doesn't require changes here.
        # ------------------------------------------------------------------
        guard_decision = await self._guard.check(
            user_id=user.id,
            strategy_id=strategy_id,
            order_payload={
                "trading_symbol": order_request.trading_symbol,
                "exchange": order_request.exchange.value,
                "side": order_request.side.value,
                "quantity": order_request.quantity,
                "idempotency_key": idempotency_key,
            },
        )

        if not guard_decision.allowed:
            log.warning(
                "execution_guard_rejected",
                reason=guard_decision.reason,
                circuit_state=guard_decision.circuit_state.value,
            )
            raise GuardRejectedError(
                reason=guard_decision.reason,
                circuit_state=guard_decision.circuit_state,
            )

        log.info(
            "execution_guard_passed",
            circuit_state=guard_decision.circuit_state.value,
        )

        # ------------------------------------------------------------------
        # Step 5: Resolve broker
        # ------------------------------------------------------------------
        try:
            broker = BrokerFactory.get(user)
        except BrokerConfigError:
            log.error("broker_resolution_failed", broker=str(user.broker))
            raise

        # ------------------------------------------------------------------
        # Step 6: Place order via broker (sync → dispatched to thread pool)
        #   BaseBroker.place_order() is synchronous by contract.
        #   asyncio.to_thread() keeps the event loop unblocked.
        # ------------------------------------------------------------------
        order_result: Optional[OrderResult] = None

        try:
            order_result = await asyncio.to_thread(
                broker.place_order,
                order_request,
            )
            log.info(
                "broker_order_placed",
                broker_order_id=order_result.broker_order_id,
                status=order_result.status.value,
            )
        except BrokerError as exc:
            log.error("broker_place_order_failed", reason=str(exc))
            await self._guard.record_failure(
                user_id=user.id,
                strategy_id=strategy_id,
                error=str(exc),
            )
            raise
        except Exception as exc:
            log.error("broker_unexpected_error", reason=str(exc))
            await self._guard.record_failure(
                user_id=user.id,
                strategy_id=strategy_id,
                error=str(exc),
            )
            raise ExecutionError(f"Unexpected error during broker placement: {exc}") from exc

        # ------------------------------------------------------------------
        # Step 7: Record broker success in execution guard
        #   Resets the failure counter — closes circuit if HALF_OPEN.
        # ------------------------------------------------------------------
        await self._guard.record_success(
            user_id=user.id,
            strategy_id=strategy_id,
        )

        # ------------------------------------------------------------------
        # Step 8: Persist Order row to PostgreSQL (source of truth)
        #   Written AFTER confirmed broker placement.
        #   Includes idempotency_key for durable audit trail.
        #
        #   CRITICAL FAILURE PATH: If the DB write fails after a successful
        #   broker placement, we log at CRITICAL level and return the
        #   OrderResult anyway. The broker holds the order. We do NOT attempt
        #   to cancel — that risks a double-action. Reconciliation will catch
        #   the discrepancy at the next 5-minute run.
        # ------------------------------------------------------------------
        try:
            order_row = Order(
                user_id=user.id,
                strategy_id=strategy_id,
                idempotency_key=idempotency_key,
                status=OrderStatusEnum.SENT,
                instrument=order_request.trading_symbol,
                side=order_request.side,
                qty=order_request.quantity,
                strike=order_request.strike,
                expiry=order_request.expiry,
                broker=user.broker,
                broker_order_id=order_result.broker_order_id,
            )
            db.add(order_row)
            await db.commit()
            await db.refresh(order_row)

            log.info(
                "order_persisted",
                order_id=order_row.id,
                broker_order_id=order_row.broker_order_id,
                status=order_row.status,
            )

        except Exception as exc:
            await db.rollback()
            log.critical(
                "order_db_persist_failed_after_broker_success",
                broker_order_id=order_result.broker_order_id,
                strategy_id=strategy_id,
                symbol=order_request.trading_symbol,
                reason=str(exc),
                action="reconciliation_will_catch",
            )
            # Return the result — broker placed the order successfully.
            # Operator is alerted via the CRITICAL log above.
            return order_result

        return order_result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_engine_instance: Optional[ExecutionEngine] = None


def get_execution_engine() -> ExecutionEngine:
    """Return the shared ExecutionEngine singleton. Thread-safe for reads."""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = ExecutionEngine()
    return _engine_instance