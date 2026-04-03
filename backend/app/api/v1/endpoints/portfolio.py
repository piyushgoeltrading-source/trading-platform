# app/api/v1/endpoints/portfolio.py
"""
app/api/v1/endpoints/portfolio.py

Portfolio API — PiyushTrade Phase 3, Step 10
=============================================
Endpoints:
  GET /portfolio        — open positions via broker (live or paper mode)
  GET /portfolio/pnl    — realised P&L from DB (source of truth), separated by mode

Rules:
  - All endpoints require JWT authentication.
  - user_id always from JWT — never from request body.
  - /portfolio fetches positions from the resolved broker implementation
    (live broker or PaperBroker; sync → thread pool).
  - /portfolio/pnl reads from PostgreSQL only — never Redis.
  - Standard PiyushTrade error envelope on all errors.
  - Paper trades are identified by broker_order_id prefix "PAPER-".
"""

from __future__ import annotations

import asyncio
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers.base_broker import BrokerError, Position
from app.brokers.factory import BrokerFactory, BrokerConfigError
from app.core.database import get_async_db
from app.core.logging import get_structured_logger
from app.core.security import get_current_user
from app.core.time_utils import now_utc
from app.models.order import Order
from app.models.trade import Trade
from app.models.user import User

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

_PAPER_ORDER_PREFIX = "PAPER-%"


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class PositionResponse(BaseModel):
    trading_symbol: str
    exchange: str
    product_code: str
    quantity: int
    average_price: float
    last_price: float
    pnl: float
    realised_pnl: float


class PortfolioResponse(BaseModel):
    positions: list[PositionResponse]
    total_unrealised_pnl: float
    broker: str
    mode: Literal["live", "paper"]


class PnLSummaryResponse(BaseModel):
    total_realised_pnl: float
    trade_count: int
    open_trade_count: int
    closed_trade_count: int
    mode: Literal["live", "paper"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_mode(mode: str) -> Literal["live", "paper"]:
    """
    Validate and normalise portfolio mode.
    """
    normalised = mode.lower().strip()
    if normalised not in {"live", "paper"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error_code": "INVALID_MODE",
                "message": "mode must be either 'live' or 'paper'",
                "details": {"mode": mode},
            },
        )
    return normalised  # type: ignore[return-value]


def _is_paper_order_filter(mode: Literal["live", "paper"]):
    """
    Return SQLAlchemy filter expression that separates paper vs live trades.

    Paper mode:
      Order.broker_order_id LIKE 'PAPER-%'

    Live mode:
      Order.broker_order_id IS NULL OR NOT LIKE 'PAPER-%'

    Notes:
      - This relies on PaperBroker generating broker_order_id values prefixed
        with "PAPER-".
      - We join through Order because Trade does not carry execution mode.
    """
    if mode == "paper":
        return Order.broker_order_id.like(_PAPER_ORDER_PREFIX)

    return (
        (Order.broker_order_id.is_(None))
        | (~Order.broker_order_id.like(_PAPER_ORDER_PREFIX))
    )


# ---------------------------------------------------------------------------
# GET /portfolio
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=PortfolioResponse,
    summary="Get open positions from broker (live or paper mode)",
)
async def get_portfolio(
    mode: str = Query(
        default="live",
        description="Execution mode: 'live' or 'paper'",
    ),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> PortfolioResponse:
    """
    Fetch open positions from the resolved broker implementation.

    - mode=live  -> fetches positions from the user's configured live broker
    - mode=paper -> fetches positions from PaperBroker

    Source: broker API / broker abstraction (not DB aggregation here).
    Use GET /portfolio/pnl for DB-sourced realised P&L.
    Broker methods are synchronous — dispatched via asyncio.to_thread().
    """
    _ = db  # kept to preserve dependency pattern consistency across endpoints
    resolved_mode = _validate_mode(mode)

    try:
        broker = BrokerFactory.get(current_user, mode=resolved_mode)
    except BrokerConfigError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error_code": "BROKER_CONFIG_ERROR",
                "message": str(exc),
                "details": {"mode": resolved_mode},
            },
        )

    try:
        positions: list[Position] = await asyncio.to_thread(broker.get_positions)
    except BrokerError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error_code": "BROKER_ERROR",
                "message": str(exc),
                "details": {
                    "broker": exc.broker,
                    "mode": resolved_mode,
                },
            },
        )

    # Exclude zero-quantity positions — broker sometimes returns closed ones
    open_positions = [p for p in positions if p.quantity != 0]

    total_unrealised = float(sum(float(p.pnl) for p in open_positions))

    broker_name = (
        "paper"
        if resolved_mode == "paper"
        else str(current_user.broker)
    )

    logger.info(
        "portfolio_fetched",
        extra={
            "event": "portfolio_fetched",
            "user_id": current_user.id,
            "mode": resolved_mode,
            "broker": broker_name,
            "position_count": len(open_positions),
            "total_unrealised_pnl": total_unrealised,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return PortfolioResponse(
        positions=[
            PositionResponse(
                trading_symbol=p.trading_symbol,
                exchange=p.exchange,
                product_code=p.product_code,
                quantity=p.quantity,
                average_price=float(p.average_price),
                last_price=float(p.last_price),
                pnl=float(p.pnl),
                realised_pnl=float(p.realised_pnl),
            )
            for p in open_positions
        ],
        total_unrealised_pnl=total_unrealised,
        broker=broker_name,
        mode=resolved_mode,
    )


# ---------------------------------------------------------------------------
# GET /portfolio/pnl
# ---------------------------------------------------------------------------

@router.get(
    "/pnl",
    response_model=PnLSummaryResponse,
    summary="Get realised P&L summary from database by mode",
)
async def get_pnl(
    mode: str = Query(
        default="live",
        description="Execution mode: 'live' or 'paper'",
    ),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> PnLSummaryResponse:
    """
    Return realised P&L summary sourced from PostgreSQL (source of truth),
    separated by execution mode.

    - total_realised_pnl: sum of all closed trade P&L for this user and mode.
    - trade_count: total trades ever for this user and mode.
    - open_trade_count: trades where closed_at IS NULL for this user and mode.
    - closed_trade_count: trades where closed_at IS NOT NULL for this user and mode.

    Never reads from Redis or live broker — DB only.

    Mode separation is currently inferred from Order.broker_order_id:
      - paper -> broker_order_id LIKE 'PAPER-%'
      - live  -> broker_order_id IS NULL OR NOT LIKE 'PAPER-%'
    """
    resolved_mode = _validate_mode(mode)
    mode_filter = _is_paper_order_filter(resolved_mode)

    # Total realised PnL — closed trades only
    pnl_result = await db.execute(
        select(func.coalesce(func.sum(Trade.realised_pnl), 0))
        .select_from(Trade)
        .join(Order, Order.id == Trade.order_id)
        .where(
            Trade.user_id == current_user.id,
            Trade.closed_at.isnot(None),
            mode_filter,
        )
    )
    total_realised_pnl: float = float(pnl_result.scalar() or 0)

    # Total trade count
    total_result = await db.execute(
        select(func.count(Trade.id))
        .select_from(Trade)
        .join(Order, Order.id == Trade.order_id)
        .where(
            Trade.user_id == current_user.id,
            mode_filter,
        )
    )
    trade_count: int = int(total_result.scalar() or 0)

    # Open trade count (closed_at IS NULL)
    open_result = await db.execute(
        select(func.count(Trade.id))
        .select_from(Trade)
        .join(Order, Order.id == Trade.order_id)
        .where(
            Trade.user_id == current_user.id,
            Trade.closed_at.is_(None),
            mode_filter,
        )
    )
    open_trade_count: int = int(open_result.scalar() or 0)

    logger.info(
        "pnl_fetched",
        extra={
            "event": "pnl_fetched",
            "user_id": current_user.id,
            "mode": resolved_mode,
            "total_realised_pnl": total_realised_pnl,
            "trade_count": trade_count,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return PnLSummaryResponse(
        total_realised_pnl=total_realised_pnl,
        trade_count=trade_count,
        open_trade_count=open_trade_count,
        closed_trade_count=trade_count - open_trade_count,
        mode=resolved_mode,
    )