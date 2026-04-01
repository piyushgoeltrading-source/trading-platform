# app/api/v1/endpoints/portfolio.py
"""
app/api/v1/endpoints/portfolio.py

Portfolio API — PiyushTrade Phase 3, Step 10
=============================================
Endpoints:
  GET /portfolio        — live open positions via broker
  GET /portfolio/pnl    — realised P&L from DB (source of truth)

Rules:
  - All endpoints require JWT authentication.
  - user_id always from JWT — never from request body.
  - /portfolio fetches live from broker (sync → thread pool).
  - /portfolio/pnl reads from PostgreSQL only — never Redis.
  - Standard PiyushTrade error envelope on all errors.
"""

from __future__ import annotations

import asyncio
from typing import Optional

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
from app.models import Order, Trade
from app.models.user import User

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


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


class PnLSummaryResponse(BaseModel):
    total_realised_pnl: float
    trade_count: int
    open_trade_count: int
    closed_trade_count: int


# ---------------------------------------------------------------------------
# GET /portfolio
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=PortfolioResponse,
    summary="Get live open positions from broker",
)
async def get_portfolio(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> PortfolioResponse:
    """
    Fetch live open positions from the user's broker.

    Source: broker API (not DB). Use GET /portfolio/pnl for DB-sourced data.
    Broker methods are synchronous — dispatched via asyncio.to_thread().
    """
    try:
        broker = BrokerFactory.get(current_user)
    except BrokerConfigError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error_code": "BROKER_CONFIG_ERROR",
                "message": str(exc),
                "details": {},
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
                "details": {"broker": exc.broker},
            },
        )

    # Exclude zero-quantity positions — broker sometimes returns closed ones
    open_positions = [p for p in positions if p.quantity != 0]

    total_unrealised = sum(p.pnl for p in open_positions)

    logger.info(
        "portfolio_fetched",
        extra={
            "event": "portfolio_fetched",
            "user_id": current_user.id,
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
                average_price=p.average_price,
                last_price=p.last_price,
                pnl=p.pnl,
                realised_pnl=p.realised_pnl,
            )
            for p in open_positions
        ],
        total_unrealised_pnl=total_unrealised,
        broker=str(current_user.broker),
    )


# ---------------------------------------------------------------------------
# GET /portfolio/pnl
# ---------------------------------------------------------------------------

@router.get(
    "/pnl",
    response_model=PnLSummaryResponse,
    summary="Get realised P&L summary from database",
)
async def get_pnl(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> PnLSummaryResponse:
    """
    Return realised P&L summary sourced from PostgreSQL (source of truth).

    - total_realised_pnl: sum of all closed trade P&L for this user.
    - trade_count: total trades ever.
    - open_trade_count: trades where closed_at IS NULL.
    - closed_trade_count: trades where closed_at IS NOT NULL.

    Never reads from Redis or broker — DB only.
    """
    # Total realised PnL — closed trades only
    pnl_result = await db.execute(
        select(func.coalesce(func.sum(Trade.realised_pnl), 0)).where(
            Trade.user_id == current_user.id,
            Trade.closed_at.isnot(None),
        )
    )
    total_realised_pnl: float = float(pnl_result.scalar())

    # Total trade count
    total_result = await db.execute(
        select(func.count(Trade.id)).where(
            Trade.user_id == current_user.id,
        )
    )
    trade_count: int = total_result.scalar() or 0

    # Open trade count (closed_at IS NULL)
    open_result = await db.execute(
        select(func.count(Trade.id)).where(
            Trade.user_id == current_user.id,
            Trade.closed_at.is_(None),
        )
    )
    open_trade_count: int = open_result.scalar() or 0

    logger.info(
        "pnl_fetched",
        extra={
            "event": "pnl_fetched",
            "user_id": current_user.id,
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
    )