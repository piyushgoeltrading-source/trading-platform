"""
app/engines/risk_manager.py

RiskManager — PiyushTrade
==========================
Pre-trade risk checks that must all pass before an order reaches the
execution engine. Called synchronously inside the execution flow:

    risk_manager.check() → execution_guard.check() → BrokerFactory.get()
    → broker.place_order()

Checks performed (in order):
  1. Daily loss limit      — block if user's realised + unrealised PnL for
                             today has breached max_daily_loss_inr.
  2. Max open positions    — block if strategy already has >= max_positions
                             open orders (PENDING or SENT status today).
  3. Margin availability   — block if broker-reported available cash is below
                             the estimated order margin requirement.

All thresholds are read from strategy.parameters (JSON column). If a key is
absent, the corresponding check uses the system default from this file.
Defaults are intentionally conservative.

strategy.parameters keys consumed here:
  "max_daily_loss_inr"   float  Max loss in INR before trading is halted.
                                Default: 5000.0
  "max_positions"        int    Max concurrent open orders per strategy.
                                Default: 5
  "margin_buffer_pct"    float  Required free margin as % of order value.
                                Default: 20.0  (i.e. 20% buffer above estimated cost)

Database usage:
  - All DB reads use AsyncSession (same pattern as strategy.py).
  - Zero writes — risk_manager never mutates DB state.
  - Broker margin is fetched live via BrokerFactory → broker.get_margins().

Rules:
  - Zero backtest logic. Zero cross-import with backtest_engine.
  - Zero order placement logic. Read-only.
  - get_structured_logger is the only logging import.
  - All DB access via AsyncSession / get_async_db pattern.
  - Raises RiskCheckError (defined here) on any failed check.
    Execution engine catches this and returns HTTP 422 to the client.
"""

from __future__ import annotations

import asyncio

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers.base_broker import OrderRequest
from app.brokers.factory import BrokerFactory
from app.core.logging import get_structured_logger
from app.models.order import Order, OrderStatus
from app.models.strategy import Strategy
from app.models.trade import Trade
from app.models.user import User

logger = get_structured_logger(__name__)

# ---------------------------------------------------------------------------
# System-wide defaults — used when strategy.parameters key is absent
# ---------------------------------------------------------------------------

_DEFAULT_MAX_DAILY_LOSS_INR: float = 5_000.0   # ₹5,000 daily loss ceiling
_DEFAULT_MAX_POSITIONS: int = 5                 # Max open orders per strategy
_DEFAULT_MARGIN_BUFFER_PCT: float = 20.0        # 20% free margin buffer


# ---------------------------------------------------------------------------
# RiskCheckError
# ---------------------------------------------------------------------------

class RiskCheckError(Exception):
    """
    Raised when a pre-trade risk check fails.

    Attributes:
        check:   Short identifier of the check that failed.
                 One of: "daily_loss_limit" | "max_positions" | "margin"
        message: Human-readable reason for rejection.
        context: Dict of diagnostic values (limits, actuals) for logging.
    """

    def __init__(self, check: str, message: str, context: dict | None = None) -> None:
        self.check = check
        self.context = context or {}
        super().__init__(message)


# ---------------------------------------------------------------------------
# Parameter extraction helpers
# ---------------------------------------------------------------------------

def _get_float(params: dict[str, Any], key: str, default: float) -> float:
    """Safely extract a float from strategy.parameters. Returns default on missing/invalid."""
    try:
        return float(params.get(key, default))
    except (TypeError, ValueError):
        return default


def _get_int(params: dict[str, Any], key: str, default: int) -> int:
    """Safely extract an int from strategy.parameters. Returns default on missing/invalid."""
    try:
        return int(params.get(key, default))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Individual checks (async, read-only)
# ---------------------------------------------------------------------------

async def _check_daily_loss_limit(
    db: AsyncSession,
    user: User,
    strategy: Strategy,
) -> None:
    """
    Check 1: Daily loss limit.

    Sums realised_pnl from all trades closed today for this user.
    Also reads any open positions' unrealised PnL via the broker.
    Blocks if the combined loss has breached max_daily_loss_inr.

    Uses DB trades table as the source of truth for realised PnL.
    Unrealised PnL from broker positions is additive on top.

    Args:
        db:       Async DB session.
        user:     Authenticated user ORM instance.
        strategy: Strategy being traded — parameters used for threshold.

    Raises:
        RiskCheckError: Daily loss limit breached.
    """
    params: dict[str, Any] = strategy.parameters or {}
    max_loss = _get_float(params, "max_daily_loss_inr", _DEFAULT_MAX_DAILY_LOSS_INR)

    today = date.today()

    # Sum realised PnL from trades closed today (negative = loss)
    result = await db.execute(
        select(func.coalesce(func.sum(Trade.realised_pnl), 0))
        .join(Order, Trade.order_id == Order.id)
        .where(
            Trade.user_id == user.id,
            Trade.closed_at.isnot(None),
            func.date(Trade.closed_at) == today,
        )
    )
    realised_pnl: float = float(result.scalar() or 0.0)

    # Unrealised PnL from broker positions (best-effort — don't block on failure)
    unrealised_pnl: float = 0.0
    try:
        broker = BrokerFactory.get(user)
        positions = await asyncio.to_thread(broker.get_positions)
        unrealised_pnl = sum(p.pnl for p in positions)
    except Exception as exc:
        logger.warning(
            "risk_daily_loss_broker_positions_failed",
            extra={
                "event": "risk_daily_loss_broker_positions_failed",
                "user_id": user.id,
                "error": str(exc),
            },
        )
        # Proceed with DB-only PnL if broker is unreachable

    total_pnl = realised_pnl + unrealised_pnl

    logger.info(
        "risk_daily_loss_check",
        extra={
            "event": "risk_daily_loss_check",
            "user_id": user.id,
            "strategy_id": strategy.id,
            "realised_pnl": realised_pnl,
            "unrealised_pnl": unrealised_pnl,
            "total_pnl": total_pnl,
            "max_loss_limit": -max_loss,
        },
    )

    if total_pnl <= -max_loss:
        raise RiskCheckError(
            check="daily_loss_limit",
            message=(
                f"Daily loss limit of ₹{max_loss:,.2f} breached. "
                f"Current P&L: ₹{total_pnl:,.2f}. "
                "No further orders will be placed today."
            ),
            context={
                "user_id": user.id,
                "strategy_id": strategy.id,
                "realised_pnl": realised_pnl,
                "unrealised_pnl": unrealised_pnl,
                "total_pnl": total_pnl,
                "max_daily_loss_inr": max_loss,
            },
        )


async def _check_max_positions(
    db: AsyncSession,
    user: User,
    strategy: Strategy,
) -> None:
    """
    Check 2: Max open positions per strategy.

    Counts orders in PENDING or SENT status placed today for this strategy.
    Blocks if the count is >= max_positions.

    "Open" here means orders that have been submitted to the broker but not
    yet filled, cancelled, or failed. FILLED orders are not counted because
    they represent closed legs, not ongoing exposure.

    Args:
        db:       Async DB session.
        user:     Authenticated user ORM instance.
        strategy: Strategy being traded.

    Raises:
        RiskCheckError: Max open positions limit reached.
    """
    params: dict[str, Any] = strategy.parameters or {}
    max_positions = _get_int(params, "max_positions", _DEFAULT_MAX_POSITIONS)

    today = date.today()

    result = await db.execute(
        select(func.count(Order.id)).where(
            Order.user_id == user.id,
            Order.strategy_id == strategy.id,
            Order.status.in_([OrderStatus.PENDING, OrderStatus.SENT]),
            func.date(Order.created_at) == today,
        )
    )
    open_count: int = result.scalar() or 0

    logger.info(
        "risk_max_positions_check",
        extra={
            "event": "risk_max_positions_check",
            "user_id": user.id,
            "strategy_id": strategy.id,
            "open_count": open_count,
            "max_positions": max_positions,
        },
    )

    if open_count >= max_positions:
        raise RiskCheckError(
            check="max_positions",
            message=(
                f"Strategy '{strategy.name}' already has {open_count} open order(s). "
                f"Max allowed: {max_positions}. "
                "Cancel or wait for existing orders to fill before placing new ones."
            ),
            context={
                "user_id": user.id,
                "strategy_id": strategy.id,
                "open_count": open_count,
                "max_positions": max_positions,
            },
        )


async def _check_margin(
    user: User,
    strategy: Strategy,
    order_request: OrderRequest,
) -> None:
    """
    Check 3: Margin availability.

    Fetches live available cash from the broker and compares it against
    an estimated order cost. Blocks if free margin is insufficient.

    Estimated order cost = limit_price (or 0 for MARKET) × quantity.
    Required free margin = estimated_cost × (1 + margin_buffer_pct / 100).

    For MARKET orders with limit_price = 0, the margin check is skipped
    (we cannot estimate cost without an LTP; risk is accepted by design for V1).

    Args:
        user:          Authenticated user ORM instance.
        strategy:      Strategy being traded — parameters used for buffer %.
        order_request: The order about to be placed.

    Raises:
        RiskCheckError: Insufficient margin.
    """
    params: dict[str, Any] = strategy.parameters or {}
    buffer_pct = _get_float(params, "margin_buffer_pct", _DEFAULT_MARGIN_BUFFER_PCT)

    # Estimated cost — use limit_price if set, otherwise skip for MARKET orders
    estimated_price = order_request.limit_price
    if estimated_price <= 0.0:
        logger.info(
            "risk_margin_check_skipped",
            extra={
                "event": "risk_margin_check_skipped",
                "user_id": user.id,
                "strategy_id": strategy.id,
                "reason": "market_order_no_price",
            },
        )
        return

    estimated_cost = estimated_price * order_request.quantity
    required_margin = estimated_cost * (1 + buffer_pct / 100.0)

    # Fetch live margin from broker
    try:
        broker = BrokerFactory.get(user)
        margin = await asyncio.to_thread(broker.get_margins)
        available_cash = margin.available_cash
    except Exception as exc:
        # If margin fetch fails, block the order — safer than allowing blindly
        logger.error(
            "risk_margin_fetch_failed",
            extra={
                "event": "risk_margin_fetch_failed",
                "user_id": user.id,
                "error": str(exc),
            },
        )
        raise RiskCheckError(
            check="margin",
            message=(
                f"Could not verify margin availability: {exc}. "
                "Order blocked until broker margin can be confirmed."
            ),
            context={"user_id": user.id, "error": str(exc)},
        ) from exc

    logger.info(
        "risk_margin_check",
        extra={
            "event": "risk_margin_check",
            "user_id": user.id,
            "strategy_id": strategy.id,
            "estimated_cost": estimated_cost,
            "required_margin": required_margin,
            "available_cash": available_cash,
            "buffer_pct": buffer_pct,
        },
    )

    if available_cash < required_margin:
        raise RiskCheckError(
            check="margin",
            message=(
                f"Insufficient margin. "
                f"Required: ₹{required_margin:,.2f} "
                f"(estimated cost ₹{estimated_cost:,.2f} + {buffer_pct:.0f}% buffer). "
                f"Available: ₹{available_cash:,.2f}."
            ),
            context={
                "user_id": user.id,
                "strategy_id": strategy.id,
                "estimated_cost": estimated_cost,
                "required_margin": required_margin,
                "available_cash": available_cash,
                "margin_buffer_pct": buffer_pct,
            },
        )


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

async def check(
    db: AsyncSession,
    user: User,
    strategy: Strategy,
    order_request: OrderRequest,
) -> None:
    """
    Run all pre-trade risk checks in sequence.

    All three checks must pass for the order to proceed. The first failure
    raises RiskCheckError immediately — subsequent checks are not run.

    Order of checks:
      1. Daily loss limit   (DB query + broker positions)
      2. Max open positions (DB query only)
      3. Margin             (broker live call)

    Args:
        db:            Async DB session (read-only in this function).
        user:          Authenticated user ORM instance (must have .broker set).
        strategy:      Strategy being traded (must have .parameters populated).
        order_request: The order about to be placed.

    Returns:
        None — all checks passed.

    Raises:
        RiskCheckError: One or more checks failed. Execution engine should
                        catch this and return HTTP 422 with the error message.
    """
    logger.info(
        "risk_check_start",
        extra={
            "event": "risk_check_start",
            "user_id": user.id,
            "strategy_id": strategy.id,
            "symbol": order_request.trading_symbol,
            "side": order_request.side.value,
            "qty": order_request.quantity,
        },
    )

    await _check_daily_loss_limit(db, user, strategy)
    await _check_max_positions(db, user, strategy)
    await _check_margin(user, strategy, order_request)

    logger.info(
        "risk_check_passed",
        extra={
            "event": "risk_check_passed",
            "user_id": user.id,
            "strategy_id": strategy.id,
            "symbol": order_request.trading_symbol,
        },
    )
