"""
app/core/time_utils.py

Time Standardization Module — PiyushTrade
=========================================
RULES:
  - Backend logic ALWAYS uses UTC internally.
  - Redis timestamps are stored as UTC epoch (float seconds).
  - UI layer converts to IST for display only.
  - No timezone logic may be scattered across other modules.

All time operations must flow through this module.
"""

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UTC = timezone.utc
IST = ZoneInfo("Asia/Kolkata")  # UTC+5:30

# NSE market hours in IST
MARKET_OPEN_HOUR_IST = 9
MARKET_OPEN_MINUTE_IST = 15
MARKET_CLOSE_HOUR_IST = 15
MARKET_CLOSE_MINUTE_IST = 30


# ---------------------------------------------------------------------------
# Core UTC helpers
# ---------------------------------------------------------------------------

def now_utc() -> datetime:
    """Return current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def now_utc_epoch() -> float:
    """Return current UTC time as a Unix epoch float (seconds).
    Use this for all Redis timestamp fields.
    """
    return now_utc().timestamp()


def to_utc(dt: datetime) -> datetime:
    """
    Convert any timezone-aware datetime to UTC.
    Raises ValueError if dt is naive (no tzinfo).
    """
    if dt.tzinfo is None:
        raise ValueError(
            f"to_utc() received a naive datetime: {dt!r}. "
            "Always attach timezone info before calling to_utc()."
        )
    return dt.astimezone(UTC)


def from_utc_epoch(epoch: float) -> datetime:
    """Convert a UTC epoch float back to a timezone-aware UTC datetime."""
    return datetime.fromtimestamp(epoch, tz=UTC)


# ---------------------------------------------------------------------------
# IST helpers (UI / display layer only)
# ---------------------------------------------------------------------------

def now_ist() -> datetime:
    """Return current time as a timezone-aware IST datetime.
    Only use this for logging display or UI response formatting.
    """
    return datetime.now(IST)


def to_ist(dt: datetime) -> datetime:
    """
    Convert any timezone-aware datetime to IST for display.
    Raises ValueError if dt is naive.
    """
    if dt.tzinfo is None:
        raise ValueError(
            f"to_ist() received a naive datetime: {dt!r}. "
            "Always attach timezone info before calling to_ist()."
        )
    return dt.astimezone(IST)


def format_ist(dt: datetime) -> str:
    """Format a datetime (any tz) as a human-readable IST string.
    Intended for API response headers and log messages only.
    """
    return to_ist(dt).strftime("%Y-%m-%dT%H:%M:%S IST")


# ---------------------------------------------------------------------------
# Market hours helpers
# ---------------------------------------------------------------------------

def is_market_open(now: datetime | None = None) -> bool:
    """
    Determine whether the NSE market is currently open.

    Logic:
      1. Convert reference time to IST.
      2. Reject weekends.
      3. Reject NSE holidays (loaded from config).
      4. Check if time falls within 09:15–15:30 IST.

    Args:
        now: Optional reference datetime (UTC-aware). Defaults to now_utc().

    Returns:
        True if market is open, False otherwise.
    """
    from app.core.config import settings  # deferred to avoid circular import

    if now is None:
        now = now_utc()

    now_ist_dt = to_ist(now)
    date_today = now_ist_dt.date()

    # Weekend check
    if now_ist_dt.weekday() >= 5:  # Saturday=5, Sunday=6
        return False

    # Holiday check — uses NSE holiday calendar from config
    holiday_set = {
        datetime.strptime(h, "%Y-%m-%d").date()
        for h in settings.NSE_HOLIDAYS
    }
    if date_today in holiday_set:
        return False

    # Time-of-day check
    open_time = now_ist_dt.replace(
        hour=MARKET_OPEN_HOUR_IST,
        minute=MARKET_OPEN_MINUTE_IST,
        second=0,
        microsecond=0,
    )
    close_time = now_ist_dt.replace(
        hour=MARKET_CLOSE_HOUR_IST,
        minute=MARKET_CLOSE_MINUTE_IST,
        second=0,
        microsecond=0,
    )

    return open_time <= now_ist_dt <= close_time


def seconds_since_utc_epoch(epoch: float) -> float:
    """
    Return how many seconds have elapsed since a UTC epoch timestamp.
    Used for staleness checks.
    """
    return now_utc().timestamp() - epoch
