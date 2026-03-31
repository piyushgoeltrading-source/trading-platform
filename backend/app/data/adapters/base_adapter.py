"""
app/data_ingestion/adapters/base_adapter.py

Abstract DataAdapter — PiyushTrade
=====================================
Defines the contract that ALL data source adapters must implement.

Rules:
  - The backtest engine ONLY talks to a DataAdapter — never directly to NSE or any broker.
  - This abstraction allows swapping data sources (NSE EOD, Bloomberg, mock) without
    touching the engine.
  - All returned datetimes must be UTC-aware.
  - Adapters are stateless — no caching, no Redis, no DB writes.
    They fetch and return. Persistence is the caller's responsibility.

Implementations:
  - NSEAdapter   → app/data_ingestion/adapters/nse_adapter.py  (Phase 2)
  - MockAdapter  → tests/adapters/mock_adapter.py              (test use only)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Sequence


# ---------------------------------------------------------------------------
# Data transfer objects
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OHLCVBar:
    """
    A single OHLCV candle for an instrument.

    Fields:
        symbol      NSE trading symbol (e.g. "NIFTY24JAN21000CE")
        timestamp   UTC datetime of the bar open (EOD bars: date at 00:00 UTC)
        open        Opening price
        high        High price
        low         Low price
        close       Closing price
        volume      Total traded volume
        oi          Open interest (options only; 0 for equity)
        expiry      Expiry date for options/futures; None for spot
        strike      Strike price for options; None otherwise
        option_type "CE", "PE", or None for non-option instruments
    """
    symbol: str
    timestamp: datetime       # UTC-aware
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: int = 0
    expiry: date | None = None
    strike: float | None = None
    option_type: str | None = None  # "CE" | "PE" | None
    underlying_price: float | None = None  # Bug 5 fix: backtest_engine.py references bar.underlying_price

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError(
                f"OHLCVBar.timestamp must be UTC-aware, got naive: {self.timestamp}"
            )
        if self.option_type is not None and self.option_type not in ("CE", "PE"):
            raise ValueError(
                f"OHLCVBar.option_type must be 'CE', 'PE', or None. Got: {self.option_type!r}"
            )
        if self.high < self.low:
            raise ValueError(
                f"OHLCVBar.high ({self.high}) cannot be less than low ({self.low})"
            )


@dataclass(frozen=True)
class ChainSnapshot:
    """
    A point-in-time snapshot of an option chain for a single expiry.

    Fields:
        underlying_symbol   e.g. "NIFTY"
        underlying_price    Spot price at snapshot time
        expiry              Expiry date
        timestamp           UTC datetime of snapshot
        strikes             List of OHLCVBar (one per strike × CE/PE)
    """
    underlying_symbol: str
    underlying_price: float
    expiry: date
    timestamp: datetime       # UTC-aware
    strikes: list[OHLCVBar] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError(
                f"ChainSnapshot.timestamp must be UTC-aware, got naive: {self.timestamp}"
            )


@dataclass(frozen=True)
class ExpiryCalendar:
    """
    List of upcoming option expiry dates for an underlying.

    Fields:
        underlying_symbol   e.g. "NIFTY"
        expiries            Sorted list of expiry dates (ascending)
        as_of               UTC datetime this calendar was fetched
    """
    underlying_symbol: str
    expiries: list[date]
    as_of: datetime           # UTC-aware

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None:
            raise ValueError(
                f"ExpiryCalendar.as_of must be UTC-aware, got naive: {self.as_of}"
            )
        if self.expiries != sorted(self.expiries):
            raise ValueError("ExpiryCalendar.expiries must be sorted ascending")


# ---------------------------------------------------------------------------
# Abstract adapter
# ---------------------------------------------------------------------------

class DataAdapter(ABC):
    """
    Abstract base class for all data source adapters.

    Implementations must be stateless — no caching, no side effects.
    The backtest engine is the only intended caller during backtesting.
    """

    @abstractmethod
    async def fetch_ohlcv(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
    ) -> Sequence[OHLCVBar]:
        """
        Fetch OHLCV bars for a symbol over a date range.

        Args:
            symbol:     NSE trading symbol (e.g. "NIFTY", "BANKNIFTY")
            from_date:  Start date (inclusive)
            to_date:    End date (inclusive)

        Returns:
            Sequence of OHLCVBar sorted by timestamp ascending.
            Empty sequence if no data found for the range.

        Raises:
            DataAdapterError: On fetch failure (network, parse, auth).
        """

    @abstractmethod
    async def fetch_chain_snapshot(
        self,
        underlying_symbol: str,
        expiry: date,
        as_of_date: date,
    ) -> ChainSnapshot:
        """
        Fetch the full option chain snapshot for one expiry on a given date.

        Args:
            underlying_symbol:  e.g. "NIFTY"
            expiry:             The expiry date of the chain to fetch
            as_of_date:         The historical date for which to fetch data

        Returns:
            ChainSnapshot with all available strikes.

        Raises:
            DataAdapterError: On fetch failure.
            DataNotAvailableError: If data for as_of_date is not available.
        """

    @abstractmethod
    async def fetch_expiry_calendar(
        self,
        underlying_symbol: str,
        as_of_date: date,
    ) -> ExpiryCalendar:
        """
        Fetch the list of upcoming option expiry dates for an underlying.

        Args:
            underlying_symbol:  e.g. "NIFTY"
            as_of_date:         Reference date (returns expiries on/after this date)

        Returns:
            ExpiryCalendar with sorted expiry dates.

        Raises:
            DataAdapterError: On fetch failure.
        """


# ---------------------------------------------------------------------------
# Adapter-specific exceptions
# ---------------------------------------------------------------------------

class DataAdapterError(Exception):
    """Base exception for all DataAdapter failures."""

    def __init__(self, message: str, adapter: str = "", cause: Exception | None = None) -> None:
        self.adapter = adapter
        self.cause = cause
        super().__init__(f"[{adapter}] {message}" if adapter else message)


class DataNotAvailableError(DataAdapterError):
    """Raised when the requested data does not exist for the given parameters."""

    def __init__(self, symbol: str, date_: date, adapter: str = "") -> None:
        self.symbol = symbol
        self.date_ = date_
        super().__init__(
            message=f"No data available for {symbol} on {date_}",
            adapter=adapter,
        )


class DataAdapterAuthError(DataAdapterError):
    """Raised when the adapter cannot authenticate with the data source."""
