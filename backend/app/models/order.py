# app/models/order.py
"""
app/models/order.py

ORM model for the orders table — PiyushTrade
=============================================
Defines:
  - OrderStatus  — lifecycle enum (PENDING → SENT → FILLED / CANCELLED / REJECTED)
  - OrderSide    — BUY / SELL
  - Order        — SQLAlchemy ORM model mapped to the `orders` table

Rules:
  - No create_all() here — all schema changes go through Alembic.
  - BrokerName imported from app.models.user to avoid duplicate enum definitions.
  - native_enum=False on all SAEnum columns — stored as VARCHAR for portability.
"""

import enum

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Enum as SAEnum,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.sql import func

from app.core.database import Base
from app.models.user import BrokerName


class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class OrderSide(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id = Column(
        Integer,
        ForeignKey("strategies.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    idempotency_key = Column(String, nullable=False, unique=True, index=True)
    status = Column(
        SAEnum(OrderStatus, native_enum=False),
        nullable=False,
        default=OrderStatus.PENDING,
        server_default="PENDING",
    )
    instrument = Column(String, nullable=False)
    side = Column(SAEnum(OrderSide, native_enum=False), nullable=False)
    qty = Column(Integer, nullable=False)
    strike = Column(Float, nullable=False)
    expiry = Column(Date, nullable=False)
    broker = Column(SAEnum(BrokerName, native_enum=False), nullable=False)
    broker_order_id = Column(String, nullable=True)
    rejection_reason = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<Order id={self.id} user_id={self.user_id} "
            f"instrument={self.instrument} side={self.side} "
            f"status={self.status} broker={self.broker}>"
        )
