# app/api/v1/endpoints/orders.py
"""
app/api/v1/endpoints/orders.py

Orders API — PiyushTrade Phase 3, Step 10
==========================================
Endpoints:
  POST   /orders           — place a new order via execution_engine
  GET    /orders           — list user's orders (paginated, filterable)
  DELETE /orders/{id}      — cancel an open order via broker

Rules:
  - All endpoints require JWT authentication.
  - user_id always from JWT — never from request body.
  - Users can only see/cancel their own orders.
  - Cancellation goes through the broker — DB status updated after confirmation.
  - Standard PiyushTrade error envelope on all errors.
  - Async throughout — get_async_db + get_current_user.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers.base_broker import (
    Exchange,
    OrderRequest,
    OrderSide,
    OrderType,
    OrderValidity,
    ProductCode,
    BrokerError,
    OrderStatus as BrokerOrderStatus,   # alias to avoid collision
)
from app.brokers.factory import BrokerFactory, BrokerConfigError
from app.core.database import get_async_db
from app.core.logging import get_structured_logger
from app.core.security import get_current_user
from app.core.time_utils import now_utc
from app.engines.execution_engine import (
    ExecutionError,
    DuplicateOrderError,
    GuardRejectedError,
    get_execution_engine,
)
from app.engines.risk_manager import RiskCheckError
from app.models.order import Order
from app.models.order import OrderStatus as DBOrderStatus   # the DB enum — use this as the type
from app.models.user import User

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/orders", tags=["orders"])


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class PlaceOrderRequest(BaseModel):
    """
    Request body for POST /orders.

    All options-specific fields (strike, expiry) are required for this
    platform — PiyushTrade trades options only in V1.
    streaming_symbol is required for Nuvama orders (e.g. '4963_NSE').
    For Zerodha orders it is still required but may be set to trading_symbol
    if Zerodha does not use it — mapper.py handles the conversion.
    """
    trading_symbol: str = Field(..., description="NSE/BSE trading symbol e.g. 'NIFTY24JAN21000CE'")
    streaming_symbol: str = Field(..., description="Exchange token e.g. '4963_NSE' — required for Nuvama")
    exchange: Exchange
    side: OrderSide
    order_type: OrderType
    product_code: ProductCode
    quantity: int = Field(..., gt=0)
    strike: float = Field(..., gt=0, description="Option strike price")
    expiry: date = Field(..., description="Option expiry date (YYYY-MM-DD)")
    strategy_id: int = Field(..., description="Strategy that triggered this order")
    validity: OrderValidity = OrderValidity.DAY
    limit_price: float = Field(default=0.0, ge=0)
    trigger_price: float = Field(default=0.0, ge=0)
    tag: Optional[str] = Field(default=None, max_length=64)


class OrderResponse(BaseModel):
    """Single order record returned from the DB."""
    id: int
    user_id: int
    strategy_id: Optional[int]
    idempotency_key: str
    status: DBOrderStatus
    instrument: str
    side: OrderSide
    qty: int
    strike: float
    expiry: date
    broker: str
    broker_order_id: Optional[str]
    rejection_reason: Optional[str]
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class OrderListResponse(BaseModel):
    total: int
    items: list[OrderResponse]


class CancelOrderResponse(BaseModel):
    order_id: int
    broker_order_id: Optional[str]
    status: str
    message: str


# ---------------------------------------------------------------------------
# Helper: ownership check
# ---------------------------------------------------------------------------

async def _get_order_or_404(
    order_id: int,
    user_id: int,
    db: AsyncSession,
) -> Order:
    result = await db.execute(
        select(Order).where(
            Order.id == order_id,
            Order.user_id == user_id,
        )
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error_code": "ORDER_NOT_FOUND",
                "message": f"Order {order_id} not found",
                "details": {"order_id": order_id},
            },
        )
    return order


# ---------------------------------------------------------------------------
# POST /orders
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Place a new order",
)
async def place_order(
    payload: PlaceOrderRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> OrderResponse:
    """
    Place a live order through the full execution pipeline:
      risk_manager → execution_guard → broker → DB write

    Returns the persisted Order record on success.
    """
    logger.info(
        "place_order_request",
        extra={
            "event": "place_order_request",
            "user_id": current_user.id,
            "strategy_id": payload.strategy_id,
            "symbol": payload.trading_symbol,
            "side": payload.side.value,
            "qty": payload.quantity,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    order_request = OrderRequest(
        trading_symbol=payload.trading_symbol,
        streaming_symbol=payload.streaming_symbol,
        exchange=payload.exchange,
        side=payload.side,
        order_type=payload.order_type,
        product_code=payload.product_code,
        quantity=payload.quantity,
        strike=payload.strike,
        expiry=payload.expiry,
        validity=payload.validity,
        limit_price=payload.limit_price,
        trigger_price=payload.trigger_price,
        tag=payload.tag,
    )

    engine = get_execution_engine()

    try:
        order_result = await engine.execute(
            order_request=order_request,
            user=current_user,
            strategy_id=payload.strategy_id,
            db=db,
        )
    except DuplicateOrderError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error_code": "DUPLICATE_ORDER",
                "message": str(exc),
                "details": {},
            },
        )
    except RiskCheckError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error_code": "RISK_CHECK_FAILED",
                "message": str(exc),
                "details": {"check": exc.check, **exc.context},
            },
        )
    except GuardRejectedError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error_code": "EXECUTION_GUARD_REJECTED",
                "message": str(exc),
                "details": {
                    "reason": exc.reason,
                    "circuit_state": exc.circuit_state.value,
                },
            },
        )
    except BrokerConfigError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error_code": "BROKER_CONFIG_ERROR",
                "message": str(exc),
                "details": {},
            },
        )
    except BrokerError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error_code": "BROKER_ERROR",
                "message": str(exc),
                "details": {"broker": exc.broker},
            },
        )
    except ExecutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error_code": "EXECUTION_ERROR",
                "message": str(exc),
                "details": {},
            },
        )

    # Fetch the persisted Order row to return full DB record
    result = await db.execute(
        select(Order).where(
            Order.broker_order_id == order_result.broker_order_id,
            Order.user_id == current_user.id,
        )
    )
    order_row = result.scalar_one_or_none()

    if order_row is None:
        # DB write failed after broker success (critical path — logged in engine)
        # Return a synthetic response from the OrderResult so caller isn't left hanging
        raise HTTPException(
            status_code=status.HTTP_207_MULTI_STATUS,
            detail={
                "error_code": "ORDER_PLACED_DB_WRITE_FAILED",
                "message": (
                    "Order was placed with the broker but could not be "
                    "confirmed in the database. Reconciliation will resolve this."
                ),
                "details": {
                    "broker_order_id": order_result.broker_order_id,
                    "status": order_result.status.value,
                },
            },
        )

    logger.info(
        "place_order_success",
        extra={
            "event": "place_order_success",
            "user_id": current_user.id,
            "order_id": order_row.id,
            "broker_order_id": order_row.broker_order_id,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return OrderResponse.model_validate(order_row)


# ---------------------------------------------------------------------------
# GET /orders
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=OrderListResponse,
    summary="List orders for the authenticated user",
)
async def list_orders(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    status_filter: Optional[DBOrderStatus] = Query(default=None, alias="status"),
    strategy_id: Optional[int] = Query(default=None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> OrderListResponse:
    """
    Return a paginated list of orders for the authenticated user.
    Filterable by status and strategy_id.
    """
    base_query = select(Order).where(Order.user_id == current_user.id)

    if status_filter is not None:
        base_query = base_query.where(Order.status == status_filter)
    if strategy_id is not None:
        base_query = base_query.where(Order.strategy_id == strategy_id)

    count_result = await db.execute(
        select(func.count()).select_from(base_query.subquery())
    )
    total = count_result.scalar_one()

    result = await db.execute(
        base_query.order_by(Order.created_at.desc()).offset(skip).limit(limit)
    )
    orders = result.scalars().all()

    return OrderListResponse(
        total=total,
        items=[OrderResponse.model_validate(o) for o in orders],
    )


# ---------------------------------------------------------------------------
# DELETE /orders/{order_id}
# ---------------------------------------------------------------------------

@router.delete(
    "/{order_id}",
    response_model=CancelOrderResponse,
    summary="Cancel an open order",
)
async def cancel_order(
    order_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_async_db),
) -> CancelOrderResponse:
    """
    Cancel an open or pending order via the broker.

    Only PENDING or SENT orders can be cancelled.
    DB status is updated to CANCELLED after broker confirmation.
    """
    order = await _get_order_or_404(order_id, current_user.id, db)

    if order.status not in (DBOrderStatus.PENDING, DBOrderStatus.SENT):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error_code": "ORDER_NOT_CANCELLABLE",
                "message": (
                    f"Order {order_id} has status '{order.status.value}' "
                    "and cannot be cancelled."
                ),
                "details": {
                    "order_id": order_id,
                    "current_status": order.status.value,
                    "cancellable_statuses": ["PENDING", "SENT"],
                },
            },
        )

    if not order.broker_order_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error_code": "NO_BROKER_ORDER_ID",
                "message": "Order has no broker_order_id — cannot cancel with broker.",
                "details": {"order_id": order_id},
            },
        )

    # Cancel with broker (sync method → thread pool)
    import asyncio
    try:
        broker = BrokerFactory.get(current_user)
        cancel_result = await asyncio.to_thread(
            broker.cancel_order,
            order.broker_order_id,
        )
    except BrokerConfigError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error_code": "BROKER_CONFIG_ERROR",
                "message": str(exc),
                "details": {},
            },
        )
    except BrokerError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error_code": "BROKER_CANCEL_FAILED",
                "message": str(exc),
                "details": {"broker": exc.broker, "broker_order_id": order.broker_order_id},
            },
        )

    # Update DB status
    order.status = DBOrderStatus.CANCELLED
    await db.commit()

    logger.info(
        "order_cancelled",
        extra={
            "event": "order_cancelled",
            "user_id": current_user.id,
            "order_id": order.id,
            "broker_order_id": order.broker_order_id,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return CancelOrderResponse(
        order_id=order.id,
        broker_order_id=order.broker_order_id,
        status=DBOrderStatus.CANCELLED.value,
        message="Order successfully cancelled with broker.",
    )