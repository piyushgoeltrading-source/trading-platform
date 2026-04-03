"""
app/core/config.py

Application Configuration — PiyushTrade
==========================================
All application-wide settings are defined here.

RULES:
  - REDIS_KEY_TTL_SECONDS must ALWAYS be greater than REDIS_STALENESS_THRESHOLD_SECONDS.
    This invariant is validated at startup.
  - All timestamps in Redis use UTC epoch (float). Never IST.
  - NSE holiday calendar is the authoritative source for market hours logic.
  - Never add broker credentials here — those belong in secrets management.
  - bcrypt is pinned to 4.0.1. DO NOT upgrade. passlib is incompatible with 4.1+.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # -------------------------------------------------------------------------
    # Runtime environment
    # -------------------------------------------------------------------------
    ENVIRONMENT: str = "development"

    # -------------------------------------------------------------------------
    # Database
    # -------------------------------------------------------------------------
    DATABASE_URL: str = "postgresql+psycopg2://piyu:%40password@localhost:5432/piyushtrade"
    # Note: @ in password must be encoded as %40 in .env DATABASE_URL
    # Note: Use psycopg2 scheme here — database.py derives the asyncpg URL automatically.

    # -------------------------------------------------------------------------
    # Redis
    # -------------------------------------------------------------------------
    REDIS_URL: str = "redis://localhost:6379/0"

    # CRITICAL: TTL must always be > staleness threshold.
    # This invariant is enforced at startup and in the ingestor before every write.
    REDIS_KEY_TTL_SECONDS: int = 15         # How long a key lives in Redis
    REDIS_STALENESS_THRESHOLD_SECONDS: int = 5  # How old is "too old" during market hours

    # -------------------------------------------------------------------------
    # JWT / Security
    # -------------------------------------------------------------------------
    SECRET_KEY: str = "CHANGE_ME_IN_PRODUCTION_USE_32_BYTES_MIN"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    # -------------------------------------------------------------------------
    # CORS
    # Comma-separated in .env, e.g.
    # CORS_ALLOW_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
    # -------------------------------------------------------------------------
    CORS_ALLOW_ORIGINS: str = "http://localhost:3000"

    # -------------------------------------------------------------------------
    # Celery
    # -------------------------------------------------------------------------
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"

    # -------------------------------------------------------------------------
    # AWS / S3
    # -------------------------------------------------------------------------
    AWS_REGION: str = "ap-south-1"
    S3_BUCKET_BACKTEST: str = "piyushtrade-backtest-results"

    # -------------------------------------------------------------------------
    # NSE Holiday Calendar
    # NSE-declared trading holidays for the current year.
    # Format: "YYYY-MM-DD" strings.
    # Update this list annually. is_market_open() in time_utils.py uses this.
    # Source: https://www.nseindia.com/resources/exchange-communication-holidays
    # -------------------------------------------------------------------------
    NSE_HOLIDAYS: list[str] = [
        # 2025 NSE Holidays
        "2025-01-26",  # Republic Day
        "2025-02-26",  # Mahashivratri
        "2025-03-14",  # Holi
        "2025-03-31",  # Id-Ul-Fitr (Ramzan Id)
        "2025-04-10",  # Shri Mahavir Jayanti
        "2025-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
        "2025-04-18",  # Good Friday
        "2025-05-01",  # Maharashtra Day
        "2025-08-15",  # Independence Day
        "2025-08-27",  # Ganesh Chaturthi
        "2025-10-02",  # Mahatma Gandhi Jayanti
        "2025-10-02",  # Dussehra
        "2025-10-21",  # Diwali - Laxmi Puja
        "2025-10-22",  # Diwali - Balipratipada
        "2025-11-05",  # Prakash Gurpurb Sri Guru Nanak Dev Ji
        "2025-12-25",  # Christmas
        # 2026 NSE Holidays (update when NSE publishes)
        "2026-01-26",  # Republic Day
        "2026-03-20",  # Holi (tentative)
        "2026-04-03",  # Good Friday (tentative)
        "2026-08-15",  # Independence Day
        "2026-10-02",  # Mahatma Gandhi Jayanti
        "2026-12-25",  # Christmas
    ]

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------
    LOG_LEVEL: str = "INFO"

    @property
    def cors_origins(self) -> list[str]:
        """
        Return CORS origins as a clean list.

        Supports a comma-separated env var:
            CORS_ALLOW_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
        """
        return [
            origin.strip()
            for origin in self.CORS_ALLOW_ORIGINS.split(",")
            if origin.strip()
        ]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "ignore"


settings = Settings()


# ---------------------------------------------------------------------------
# Startup invariant: TTL must always be > staleness threshold
# ---------------------------------------------------------------------------
def _validate_redis_config() -> None:
    """
    Validate that Redis TTL and staleness threshold are consistent.
    Called at module import time so misconfiguration is caught immediately.
    """
    if settings.REDIS_KEY_TTL_SECONDS <= settings.REDIS_STALENESS_THRESHOLD_SECONDS:
        raise RuntimeError(
            f"CONFIGURATION ERROR: REDIS_KEY_TTL_SECONDS ({settings.REDIS_KEY_TTL_SECONDS}) "
            f"must be greater than REDIS_STALENESS_THRESHOLD_SECONDS "
            f"({settings.REDIS_STALENESS_THRESHOLD_SECONDS}). "
            "A key must live in Redis longer than the staleness window, otherwise "
            "fresh keys will expire before clients can detect staleness."
        )


def _validate_security_config() -> None:
    """
    Validate security-sensitive configuration.

    In production, SECRET_KEY must never remain at the default placeholder value.
    """
    if settings.ENVIRONMENT.lower() == "production" and (
        settings.SECRET_KEY == "CHANGE_ME_IN_PRODUCTION_USE_32_BYTES_MIN"
    ):
        raise RuntimeError(
            "CONFIGURATION ERROR: SECRET_KEY is still using the default placeholder "
            "value in production. Set a strong SECRET_KEY in your environment."
        )


_validate_redis_config()
_validate_security_config()