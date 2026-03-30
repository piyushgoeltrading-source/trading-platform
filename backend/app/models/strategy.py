import enum

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Enum as SAEnum, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Instrument(str, enum.Enum):
    NIFTY = "NIFTY"
    BANKNIFTY = "BANKNIFTY"
    SENSEX = "SENSEX"
    BANKEX = "BANKEX"


class StrategyStatus(str, enum.Enum):
    draft = "draft"
    active = "active"
    paused = "paused"
    archived = "archived"


class Strategy(Base):
    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    instrument = Column(SAEnum(Instrument, native_enum=False), nullable=False)
    legs = Column(JSON, nullable=False, default=list)
    is_active = Column(Boolean, default=True, nullable=False)
    status = Column(SAEnum(StrategyStatus, native_enum=False), nullable=False, default=StrategyStatus.draft)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    user = relationship("User", backref="strategies")

    def __repr__(self) -> str:
        return f"<Strategy id={self.id} name={self.name} instrument={self.instrument} user_id={self.user_id}>"
