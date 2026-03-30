from sqlalchemy import Column, Integer, DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="RESTRICT"), nullable=False, unique=True, index=True)

    # Fill details — populated from broker confirmation
    fill_price = Column(Numeric(10, 2), nullable=False)
    fill_qty = Column(Integer, nullable=False)

    # P&L — nullable until position is closed
    realised_pnl = Column(Numeric(12, 2), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)

    # Audit
    exchange_trade_id = Column(String, nullable=True)  # broker's exchange ref, for reconciliation
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    user = relationship("User", backref="trades")
    order = relationship("Order", backref="trade", uselist=False)

    def __repr__(self) -> str:
        return (
            f"<Trade id={self.id} order_id={self.order_id} "
            f"fill_price={self.fill_price} realised_pnl={self.realised_pnl} user_id={self.user_id}>"
        )
