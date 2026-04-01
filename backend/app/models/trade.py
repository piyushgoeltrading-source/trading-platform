# app/api/v1/endpoints/trades.py
"""
app/api/v1/endpoints/trades.py

Trades API — PiyushTrade Phase 3, Step 10
==========================================
Endpoints:
  GET /trades          — list user's trades (paginated, filterable)
  GET /trades/{id}     — get a single trade with its linked order

Rules:
  - All endpoints require JWT authentication.
  - user_id always from JWT — never from request body.
  - Users can only see their own trades.
  - Trades are read-only — never written or deleted via API.
  - Standard PiyushTrade error envelope on all errors.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_db
from app.core.logging import get_structured_logger
from app.core.security import get_current_user
from app.core.time_utils import now_utc
from app.models import Order as OrderModel, Trade
from app.models.user import User

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/trades", tags=["trades"])


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class LinkedOrderResponse(BaseModel):
    """Minimal order detail nested inside a trade response."""
    id: int
    instrument: str
    side: str
    qty: int
    strike: float
    broker: str
    broker_order_id: Optional[str]

    model_config = {"from_attributes": True}


class TradeResponse(BaseModel):
    id: int
    user_id: int
    order_id: int
    fill_price: float
    fill_qty: int
    realised_pnl: Optional[float]
    closed_at: Optional[str]
    exchange_trade_id: Optional[str]
    created_at: str
    updated_at: str
    order: Optional[LinkedOrderResponse] = None

    model_config = {"from_attributes": True}


class TradeListResponse(BaseModel):
    total: int
    items: list[TradeResponse]


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

async def _get_trade_or_404(
    trade_id: int,
    user_id: int,
    db: AsyncSession,
) -> Trade:
    result = await db.execute(
        select(Trade).where(
            Trade.id == trade_id,
            Trade.user_id == user_id,
        )
    )
    trade = result.scalar_one_or_none()
    if trade is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error_code": "TRADE_NOT_FOUND",
                "message": f"Trade {trade_id} not found",
                "details": {"trade_id": trade_id},
            },
        )
    return trade


def _format_trade(trade, order=None) -> TradeResponse:
    """Convert ORM objects to TradeResponse, handling datetime serialisation."""
    return TradeResponse(
        id=trade.id,
        user_id=trade.user_id,
        order_id=trade.order_id,
        fill_price=float(trade.fill_price),
        fill_qty=trade.fill_qty,
        realised_pnl=float(trade.realised_pnl) if trade.realised_pnl is not None else None,
        closed_at=trade.closed_at.isoformat() if trade.closed_at else None,
        exchange_trade_id=trade.exchange_trade_id,
        created_at=trade.created_at.isoformat(),
        updated_at=trade.updated_at.isoformat(),
        order=LinkedOrderResponse(
            id=order.id,
            instrument=order.instrument,
            side=order.side.value if hasattr(order.side, "value") else order.side,
            qty=order.qty,
            strike=float(order.strike),
            broker=order.broker.value if hasattr(order.broker, "value") else str(order.broker),
            broker_order_id=order.broker_order_id,
        ) if order else None,
    )


# ---------------------------------------------------------------------------
# GET /trades
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=TradeListResponse,
    summary="List trades for the authenticated user",
)
async def list_trades(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    order_id: Optional[int] = Query(default=None, description="Filter by order ID"),
    open_only: bool = Query(default=False, description="If true, return only unclosed trades"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> TradeListResponse:
    """
    Return a paginated list of trades for the authenticated user.

    Optionally filter by order_id or open positions only (realised_pnl IS NULL).
    Each trade includes its linked order summary.
    """
    base_query = (
        select(Trade)
        .where(Trade.user_id == current_user.id)
    )

    if order_id is not None:
        base_query = base_query.where(Trade.order_id == order_id)
    if open_only:
        base_query = base_query.where(Trade.closed_at.is_(None))

    count_result = await db.execute(
        select(func.count()).select_from(base_query.subquery())
    )
    total = count_result.scalar_one()

    result = await db.execute(
        base_query.order_by(Trade.created_at.desc()).offset(skip).limit(limit)
    )
    trades = result.scalars().all()

    # Batch-load linked orders to avoid N+1 queries
    order_ids = [t.order_id for t in trades]
    orders_by_id: dict = {}
    if order_ids:
        orders_result = await db.execute(
            select(OrderModel).where(OrderModel.id.in_(order_ids))
        )
        for o in orders_result.scalars().all():
            orders_by_id[o.id] = o

    return TradeListResponse(
        total=total,
        items=[_format_trade(t, orders_by_id.get(t.order_id)) for t in trades],
    )


# ---------------------------------------------------------------------------
# GET /trades/{trade_id}
# ---------------------------------------------------------------------------

@router.get(
    "/{trade_id}",
    response_model=TradeResponse,
    summary="Get a single trade by ID",
)
async def get_trade(
    trade_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> TradeResponse:
    """
    Return a single trade by ID with its linked order detail.
    User must own the trade.
    """
    trade = await _get_trade_or_404(trade_id, current_user.id, db)

    order = None
    if trade.order_id:
        order_result = await db.execute(
            select(OrderModel).where(OrderModel.id == trade.order_id)
        )
        order = order_result.scalar_one_or_none()

    logger.info(
        "get_trade",
        extra={
            "event": "get_trade",
            "user_id": current_user.id,
            "trade_id": trade_id,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return _format_trade(trade, order)