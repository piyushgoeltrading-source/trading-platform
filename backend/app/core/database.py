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
# Async engine — used by strategy.py, backtest_tasks.py, all Phase 3 endpoints
# Requires asyncpg: pip install asyncpg
# DATABASE_URL should use postgresql+psycopg2:// (or plain postgresql://).
# The async URL is derived here — no second env var needed.
#
# Handles all three input forms defensively:
#   postgresql+psycopg2://  →  async: postgresql+asyncpg://
#   postgresql://            →  async: postgresql+asyncpg://
#   postgresql+asyncpg://   →  async: unchanged (already correct)
# ---------------------------------------------------------------------------

_base_url = settings.DATABASE_URL

# Derive sync URL — normalise to psycopg2 regardless of input form
_sync_url = (
    _base_url
    .replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    .replace("postgresql://", "postgresql+psycopg2://")
)

# Derive async URL — strip psycopg2 if present, ensure asyncpg
_async_url = (
    _base_url
    .replace("postgresql+psycopg2://", "postgresql+asyncpg://")
    .replace("postgresql://", "postgresql+asyncpg://")
)


# ---------------------------------------------------------------------------
# Sync engine — used by auth.py, security.py, Alembic env.py
# ---------------------------------------------------------------------------

engine = create_engine(
    _sync_url,
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