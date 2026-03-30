import enum

from sqlalchemy import Column, Integer, String, DateTime, Enum as SAEnum, ForeignKey, Numeric, Date, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FILLED = "FILLED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class OrderSide(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True, index=True)

    # Idempotency — SHA256 of (user_id + strategy_id + strike + expiry + side + qty + floored minute timestamp)
    idempotency_key = Column(String, nullable=False, unique=True)

    # Status machine: PENDING → SENT → FILLED → FAILED / CANCELLED
    status = Column(SAEnum(OrderStatus, native_enum=False), nullable=False, default=OrderStatus.PENDING)

    # Order details
    strike = Column(Numeric(10, 2), nullable=False)
    expiry = Column(Date, nullable=False)
    side = Column(SAEnum(OrderSide, native_enum=False), nullable=False)
    qty = Column(Integer, nullable=False)
    instrument = Column(String, nullable=False)

    # Broker response fields — populated after SENT/FILLED
    broker_order_id = Column(String, nullable=True)
    rejection_reason = Column(String, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    user = relationship("User", backref="orders")
    strategy = relationship("Strategy", backref="orders")

    def __repr__(self) -> str:
        return (
            f"<Order id={self.id} status={self.status} "
            f"strike={self.strike} side={self.side} qty={self.qty} user_id={self.user_id}>"
        )


# Explicit index on idempotency_key for fast duplicate checks before broker calls
Index("ix_orders_idempotency_key", Order.idempotency_key, unique=True)
