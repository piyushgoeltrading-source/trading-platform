"""
app/core/database.py

SQLAlchemy engine, session factory, and Base for all models.
Moved from: backend/database.py

IMPORTANT:
- create_all() has been REMOVED. All schema changes go through Alembic.
- Import Base into every model file so Alembic can detect them for autogenerate.
- Import get_db into route files as a FastAPI dependency.

Usage:
    from app.core.database import Base, get_db
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from app.core.config import settings


# SQLAlchemy 2.0-style declarative base
class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.DATABASE_URL,
    # Pool settings appropriate for a single EC2 instance (V1)
    pool_pre_ping=True,       # Detect stale connections before using them
    pool_size=10,             # Max persistent connections
    max_overflow=20,          # Allowed burst connections above pool_size
    pool_recycle=3600,        # Recycle connections after 1 hour
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def get_db():
    """
    FastAPI dependency — yields a DB session per request and closes it after.

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
