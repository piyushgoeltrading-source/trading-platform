"""
app/brokers/paper/client.py

PaperBroker — PiyushTrade
=========================
Implements the BaseBroker contract without making any external broker calls.

Design:
  - No network I/O.
  - No broker SDK dependency.
  - PostgreSQL remains the source of truth for paper state.
  - Paper orders are identified by broker_order_id prefix "PAPER-".
  - Positions are derived from paper trades/orders stored in PostgreSQL.

Rules:
  - This class is synchronous to match BaseBroker.
  - ExecutionEngine remains responsible for persisting Order rows.
  - In paper mode, ExecutionEngine also persists an immediate Trade row.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.brokers.base_broker import (
    BaseBroker,
    BrokerName,
    Exchange,
    Holding,
    LTPResult,
    Margin,
    OrderDetail,
    OrderRequest,
    OrderResult,
    OrderStatus,
    Position,
)
from app.core.database import SessionLocal
from app.core.logging import get_structured_logger
from app.models.order import Order, OrderStatus as DbOrderStatus
from app.models.trade import Trade

logger = get_structured_logger(__name__)

_PAPER_ORDER_PREFIX = "PAPER-"
_DEFAULT_PAPER_CASH = 10_000_000.0


class PaperBroker(BaseBroker):
    """
    In-process broker simulator for safe non-live testing.

    The broker_name attribute reflects the user's configured live broker so
    downstream logs still show the account context, while execution mode is
    controlled by BrokerFactory(mode="paper").
    """

    def __init__(self, user_id: int, broker_name: BrokerName | None = None) -> None:
        self.user_id = user_id
        self.broker_name = broker_name or BrokerName.zerodha

    def _new_order_id(self) -> str:
        return f"{_PAPER_ORDER_PREFIX}{uuid4().hex[:20].upper()}"

    def place_order(self, order_request: OrderRequest) -> OrderResult:
        """
        Simulate an immediate successful fill.

        ExecutionEngine persists the Order row after this returns and uses the
        raw fill_price below to create the corresponding Trade row.
        """
        broker_order_id = self._new_order_id()
        fill_price = float(order_request.limit_price or 0.0)

        logger.info(
            "paper_place_order",
            extra={
                "event": "paper_place_order",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
                "symbol": order_request.trading_symbol,
                "side": order_request.side.value,
                "quantity": order_request.quantity,
                "fill_price": fill_price,
            },
        )

        return OrderResult(
            broker_order_id=broker_order_id,
            status=OrderStatus.COMPLETE,
            message="Paper order filled successfully.",
            raw={
                "paper": True,
                "fill_price": fill_price,
                "simulated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def modify_order(self, broker_order_id: str, order_request: OrderRequest) -> OrderResult:
        logger.info(
            "paper_modify_order",
            extra={
                "event": "paper_modify_order",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
            },
        )
        return OrderResult(
            broker_order_id=broker_order_id,
            status=OrderStatus.COMPLETE,
            message="Paper order modification acknowledged.",
            raw={"paper": True},
        )

    def cancel_order(self, broker_order_id: str) -> OrderResult:
        logger.info(
            "paper_cancel_order",
            extra={
                "event": "paper_cancel_order",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
            },
        )
        return OrderResult(
            broker_order_id=broker_order_id,
            status=OrderStatus.CANCELLED,
            message="Paper order cancelled.",
            raw={"paper": True},
        )

    def get_order_status(self, broker_order_id: str) -> OrderDetail:
        with SessionLocal() as db:
            order = self._get_order(db, broker_order_id)
            trade = self._get_trade(db, order.id)

            avg_price = float(trade.fill_price) if trade else 0.0
            filled_qty = int(trade.fill_qty) if trade else 0

            return OrderDetail(
                broker_order_id=order.broker_order_id or broker_order_id,
                trading_symbol=order.instrument,
                exchange="NFO",
                side=order.side.value if hasattr(order.side, "value") else str(order.side),
                order_type="MARKET",
                product_code="NRML",
                quantity=order.qty,
                filled_quantity=filled_qty,
                average_price=avg_price,
                status=OrderStatus.COMPLETE if trade else OrderStatus.PENDING,
                placed_at=order.created_at,
                updated_at=order.updated_at,
                rejection_reason=order.rejection_reason,
                raw={"paper": True},
            )

    def get_order_history(self, broker_order_id: str) -> list[OrderDetail]:
        return [self.get_order_status(broker_order_id)]

    def get_positions(self) -> list[Position]:
        with SessionLocal() as db:
            signed_qty = func.sum(
                case(
                    (Order.side == "BUY", Trade.fill_qty),
                    else_=-Trade.fill_qty,
                )
            )
            avg_fill = func.avg(Trade.fill_price)

            rows = db.execute(
                select(
                    Order.instrument,
                    signed_qty.label("net_qty"),
                    avg_fill.label("avg_fill"),
                )
                .join(Trade, Trade.order_id == Order.id)
                .where(
                    Order.user_id == self.user_id,
                    Order.broker_order_id.like(f"{_PAPER_ORDER_PREFIX}%"),
                )
                .group_by(Order.instrument)
                .having(signed_qty != 0)
            ).all()

            positions: list[Position] = []
            for row in rows:
                positions.append(
                    Position(
                        trading_symbol=row.instrument,
                        exchange="NFO",
                        product_code="NRML",
                        quantity=int(row.net_qty or 0),
                        average_price=float(row.avg_fill or 0.0),
                        last_price=float(row.avg_fill or 0.0),
                        pnl=0.0,
                        realised_pnl=0.0,
                        raw={"paper": True},
                    )
                )
            return positions

    def get_holdings(self) -> list[Holding]:
        return []

    def get_margins(self) -> Margin:
        return Margin(
            available_cash=_DEFAULT_PAPER_CASH,
            used_margin=0.0,
            total_margin=_DEFAULT_PAPER_CASH,
            span_margin=0.0,
            exposure_margin=0.0,
            raw={"paper": True},
        )

    def get_ltp(self, trading_symbol: str, exchange: Exchange) -> LTPResult:
        return LTPResult(
            trading_symbol=trading_symbol,
            exchange=exchange.value,
            ltp=0.0,
            timestamp=datetime.now(timezone.utc),
        )

    def _get_order(self, db: Session, broker_order_id: str) -> Order:
        order = db.execute(
            select(Order).where(
                Order.user_id == self.user_id,
                Order.broker_order_id == broker_order_id,
            )
        ).scalar_one()
        return order

    def _get_trade(self, db: Session, order_id: int) -> Trade | None:
        return db.execute(
            select(Trade).where(Trade.order_id == order_id)
        ).scalar_one_or_none()
