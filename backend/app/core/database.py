"""
app/core/database.py

SQLAlchemy engine, session factory, and Base for all models.
Moved from: backend/database.py

IMPORTANT:
- create_all() has been REMOVED. All schema changes go through Alembic.
- Import Base into every model file so Alembic can detect them for autogenerate.
- Import get_db (sync) or get_async_db (async) into route files as FastAPI dependencies.

Usage:
    from app.core.database import Base, get_db          # sync routes
    from app.core.database import AsyncSessionLocal, get_async_db  # async routes
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
)
from app.core.config import settings


# SQLAlchemy 2.0-style declarative base
class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Sync engine — used by auth.py, security.py, Alembic env.py
# ---------------------------------------------------------------------------

engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db():
    """
    FastAPI dependency — yields a sync DB session per request and closes it after.
    Used by: auth.py, security.py (sync routes only).

    Usage in routes:
        @router.get("/example")
        def example(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Async engine — used by strategy.py, backtest_tasks.py, all Phase 3 endpoints
# Requires asyncpg: pip install asyncpg
# DATABASE_URL driver is auto-swapped to asyncpg below — no second env var needed.
# ---------------------------------------------------------------------------

_async_url = settings.DATABASE_URL.replace(
    "postgresql+psycopg2://", "postgresql+asyncpg://"
).replace(
    "postgresql://", "postgresql+asyncpg://"
)

async_engine = create_async_engine(
    _async_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_async_db():
    """
    FastAPI dependency — yields an async DB session per request.
    Used by: strategy.py, backtest endpoints, all Phase 3 async routes.

    Usage in routes:
        @router.get("/example")
        async def example(db: AsyncSession = Depends(get_async_db)):
            ...
    """
    async with AsyncSessionLocal() as session:
        yield session
