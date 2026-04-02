# app/models/trade.py
"""
app/models/trade.py

ORM model for the trades table — PiyushTrade
=============================================
Defines:
  - Trade — SQLAlchemy ORM model mapped to the `trades` table

A Trade is created by the reconciliation service when a broker fill
is confirmed. Trades are read-only via the API — never written or
deleted through endpoints.

Rules:
  - No create_all() here — all schema changes go through Alembic.
  - fill_price and realised_pnl use Numeric for financial precision.
  - native_enum=False on all SAEnum columns.
"""

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    order_id = Column(
        Integer,
        ForeignKey("orders.id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
        index=True,
    )
    fill_price = Column(Numeric(10, 2), nullable=False)
    fill_qty = Column(Integer, nullable=False)
    realised_pnl = Column(Numeric(12, 2), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    exchange_trade_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    order = relationship("Order", backref="trade")

    def __repr__(self) -> str:
        return (
            f"<Trade id={self.id} user_id={self.user_id} "
            f"order_id={self.order_id} fill_price={self.fill_price} "
            f"fill_qty={self.fill_qty}>"
        )
