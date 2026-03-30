"""
app/data_ingestion/adapters/nse_adapter.py

NSE EOD Data Adapter — PiyushTrade
=====================================
Fetches End-of-Day (EOD) option chain data from NSE bhav copies.

Source:
  NSE F&O bhav copy: https://www.nseindia.com/api/reports
  File pattern: fo<DDMMMYYYY>bhav.csv.zip
  Example:      fo25JAN2024bhav.csv.zip

Columns used from bhav CSV:
  SYMBOL, EXPIRY_DT, OPTION_TYP, STRIKE_PR,
  OPEN, HIGH, LOW, CLOSE, SETTLE_PR, CONTRACTS,
  VAL_INLAKH, OPEN_INT, CHG_IN_OI, TIMESTAMP

Rules:
  - This adapter is stateless — no Redis, no DB writes.
  - All timestamps returned are UTC-aware (NSE EOD = IST close → converted to UTC).
  - Network errors raise DataAdapterError.
  - Missing data raises DataNotAvailableError.
  - The backtest engine calls this — never called from the live execution path.
"""

from __future__ import annotations

import asyncio
import io
import logging
import zipfile
from datetime import date, datetime, timezone
from typing import Sequence
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from app.core.logging import get_structured_logger
from app.core.time_utils import now_utc
from app.data.adapters.base_adapter import (
    ChainSnapshot,
    DataAdapter,
    DataAdapterError,
    DataNotAvailableError,
    ExpiryCalendar,
    OHLCVBar,
)

logger = get_structured_logger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# NSE market close time — EOD bars are stamped at 15:30 IST converted to UTC
_MARKET_CLOSE_HOUR_IST = 15
_MARKET_CLOSE_MINUTE_IST = 30

# NSE bhav copy base URL
_NSE_BHAV_BASE_URL = (
    "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22"
    "F%26O+-+Bhavcopy%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22"
    "derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date={date_str}&type=equity&mode=single"
)

# Direct bhav copy download URL pattern (more reliable than the reports API)
_NSE_BHAV_DOWNLOAD_URL = (
    "https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/"
    "{month_abbr}/fo{date_str}bhav.csv.zip"
)

# NSE requires browser-like headers to avoid 403
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}

# HTTP timeout for NSE downloads (bhav files can be several MB)
_DOWNLOAD_TIMEOUT_SECONDS = 30


# ---------------------------------------------------------------------------
# Date formatting helpers
# ---------------------------------------------------------------------------

def _bhav_date_str(d: date) -> str:
    """Format date as DDMMMYYYY (e.g. 25JAN2024) for bhav copy filename."""
    return d.strftime("%d%b%Y").upper()


def _nse_to_utc(d: date) -> datetime:
    """
    Convert an NSE EOD date to a UTC-aware datetime.
    NSE EOD bars are stamped at market close: 15:30 IST → UTC.
    """
    ist_close = datetime(
        d.year, d.month, d.day,
        _MARKET_CLOSE_HOUR_IST,
        _MARKET_CLOSE_MINUTE_IST,
        tzinfo=IST,
    )
    return ist_close.astimezone(timezone.utc)


def _parse_nse_expiry(expiry_str: str) -> date:
    """Parse NSE expiry string '25-JAN-2024' or '25JAN2024' to date."""
    for fmt in ("%d-%b-%Y", "%d%b%Y"):
        try:
            return datetime.strptime(expiry_str.strip().upper(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse NSE expiry date: {expiry_str!r}")


# ---------------------------------------------------------------------------
# NSE Adapter
# ---------------------------------------------------------------------------

class NSEAdapter(DataAdapter):
    """
    Fetches NSE F&O EOD data from NSE bhav copy archives.

    Args:
        timeout_seconds: HTTP request timeout. Default 30s.
        session_cookie:  NSE requires a session cookie for some endpoints.
                         Obtain by hitting https://www.nseindia.com first.
                         Optional — direct archive URLs usually work without it.
    """

    def __init__(
        self,
        timeout_seconds: int = _DOWNLOAD_TIMEOUT_SECONDS,
        session_cookie: str | None = None,
    ) -> None:
        self._timeout = timeout_seconds
        self._headers = dict(_NSE_HEADERS)
        if session_cookie:
            self._headers["Cookie"] = session_cookie

    # ------------------------------------------------------------------
    # DataAdapter interface
    # ------------------------------------------------------------------

    async def fetch_ohlcv(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
    ) -> Sequence[OHLCVBar]:
        """
        Fetch daily OHLCV bars for a symbol over a date range.

        Iterates over each trading day in the range, downloads the bhav copy,
        and extracts rows matching the symbol.

        Args:
            symbol:     NSE underlying symbol e.g. "NIFTY", "BANKNIFTY"
            from_date:  Start date (inclusive)
            to_date:    End date (inclusive)

        Returns:
            List of OHLCVBar sorted by timestamp ascending.
        """
        if from_date > to_date:
            raise DataAdapterError(
                message=f"from_date {from_date} must be <= to_date {to_date}",
                adapter="NSEAdapter",
            )

        bars: list[OHLCVBar] = []
        current = from_date

        while current <= to_date:
            # Skip weekends
            if current.weekday() < 5:
                try:
                    day_bars = await self._fetch_day_ohlcv(symbol, current)
                    bars.extend(day_bars)
                except DataNotAvailableError:
                    # Holiday or no data — skip silently, log at debug
                    logger.debug(
                        "No bhav data for date — likely holiday",
                        extra={
                            "event": "bhav_date_skipped",
                            "symbol": symbol,
                            "date": str(current),
                        },
                    )
                except DataAdapterError as exc:
                    logger.warning(
                        "Failed to fetch bhav for date",
                        extra={
                            "event": "bhav_fetch_error",
                            "symbol": symbol,
                            "date": str(current),
                            "error": str(exc),
                        },
                    )

            current = date(current.year, current.month, current.day + 1) if False else \
                date.fromordinal(current.toordinal() + 1)

        bars.sort(key=lambda b: b.timestamp)
        return bars

    async def fetch_chain_snapshot(
        self,
        underlying_symbol: str,
        expiry: date,
        as_of_date: date,
    ) -> ChainSnapshot:
        """
        Fetch all strikes for one expiry on a given EOD date.
        """
        df = await self._download_bhav(as_of_date)
        df = self._filter_symbol_expiry(df, underlying_symbol, expiry)

        if df.empty:
            raise DataNotAvailableError(
                symbol=underlying_symbol,
                date_=as_of_date,
                adapter="NSEAdapter",
            )

        # Derive underlying price from ATM strike (approximation for EOD)
        # In production this should come from the index bhav copy
        underlying_price = float(df["CLOSE"].median())

        strikes = self._rows_to_bars(df, underlying_symbol)
        snapshot_ts = _nse_to_utc(as_of_date)

        return ChainSnapshot(
            underlying_symbol=underlying_symbol,
            underlying_price=underlying_price,
            expiry=expiry,
            timestamp=snapshot_ts,
            strikes=strikes,
        )

    async def fetch_expiry_calendar(
        self,
        underlying_symbol: str,
        as_of_date: date,
    ) -> ExpiryCalendar:
        """
        Derive available expiry dates from the bhav copy on as_of_date.
        """
        df = await self._download_bhav(as_of_date)
        symbol_df = df[df["SYMBOL"].str.upper() == underlying_symbol.upper()]

        if symbol_df.empty:
            raise DataNotAvailableError(
                symbol=underlying_symbol,
                date_=as_of_date,
                adapter="NSEAdapter",
            )

        expiries = sorted(
            {_parse_nse_expiry(str(e)) for e in symbol_df["EXPIRY_DT"].dropna().unique()}
        )

        return ExpiryCalendar(
            underlying_symbol=underlying_symbol,
            expiries=[e for e in expiries if e >= as_of_date],
            as_of=_nse_to_utc(as_of_date),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_day_ohlcv(
        self,
        symbol: str,
        trading_date: date,
    ) -> list[OHLCVBar]:
        """Download bhav for one day and extract rows for the given symbol."""
        df = await self._download_bhav(trading_date)
        symbol_df = df[df["SYMBOL"].str.upper() == symbol.upper()]

        if symbol_df.empty:
            raise DataNotAvailableError(
                symbol=symbol,
                date_=trading_date,
                adapter="NSEAdapter",
            )

        return self._rows_to_bars(symbol_df, symbol)

    async def _download_bhav(self, trading_date: date) -> pd.DataFrame:
        """
        Download and parse the NSE F&O bhav copy CSV for a given date.

        Returns a raw DataFrame with standardized column names.
        Raises DataNotAvailableError if NSE returns 404.
        Raises DataAdapterError on network/parse failures.
        """
        date_str = _bhav_date_str(trading_date)
        year = trading_date.strftime("%Y")
        month_abbr = trading_date.strftime("%b").upper()

        url = _NSE_BHAV_DOWNLOAD_URL.format(
            year=year,
            month_abbr=month_abbr,
            date_str=date_str,
        )

        logger.debug(
            "Downloading NSE bhav copy",
            extra={
                "event": "bhav_download_start",
                "url": url,
                "date": str(trading_date),
                "timestamp_utc": now_utc().isoformat(),
            },
        )

        try:
            async with httpx.AsyncClient(
                headers=self._headers,
                timeout=self._timeout,
                follow_redirects=True,
            ) as client:
                response = await client.get(url)

            if response.status_code == 404:
                raise DataNotAvailableError(
                    symbol="ALL",
                    date_=trading_date,
                    adapter="NSEAdapter",
                )

            if response.status_code != 200:
                raise DataAdapterError(
                    message=f"NSE returned HTTP {response.status_code} for {url}",
                    adapter="NSEAdapter",
                )

        except httpx.TimeoutException as exc:
            raise DataAdapterError(
                message=f"Request timed out after {self._timeout}s: {url}",
                adapter="NSEAdapter",
                cause=exc,
            ) from exc
        except httpx.RequestError as exc:
            raise DataAdapterError(
                message=f"Network error fetching bhav: {exc}",
                adapter="NSEAdapter",
                cause=exc,
            ) from exc

        # Parse zip → CSV → DataFrame
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_name = next(
                    (n for n in zf.namelist() if n.endswith(".csv")), None
                )
                if csv_name is None:
                    raise DataAdapterError(
                        message=f"No CSV found in bhav zip for {trading_date}",
                        adapter="NSEAdapter",
                    )
                with zf.open(csv_name) as csv_file:
                    df = pd.read_csv(csv_file)
        except zipfile.BadZipFile as exc:
            raise DataAdapterError(
                message=f"Downloaded file is not a valid zip for {trading_date}",
                adapter="NSEAdapter",
                cause=exc,
            ) from exc
        except Exception as exc:
            raise DataAdapterError(
                message=f"Failed to parse bhav CSV for {trading_date}: {exc}",
                adapter="NSEAdapter",
                cause=exc,
            ) from exc

        # Normalize column names (NSE occasionally ships with extra whitespace)
        df.columns = [c.strip().upper() for c in df.columns]

        logger.debug(
            "NSE bhav downloaded and parsed",
            extra={
                "event": "bhav_download_complete",
                "date": str(trading_date),
                "rows": len(df),
            },
        )

        return df

    def _filter_symbol_expiry(
        self,
        df: pd.DataFrame,
        symbol: str,
        expiry: date,
    ) -> pd.DataFrame:
        """Filter bhav DataFrame to a specific symbol and expiry."""
        symbol_mask = df["SYMBOL"].str.upper() == symbol.upper()
        # NSE expiry format in CSV: "25-JAN-2024"
        expiry_mask = df["EXPIRY_DT"].apply(
            lambda e: _parse_nse_expiry(str(e)) == expiry
        )
        return df[symbol_mask & expiry_mask].copy()

    def _rows_to_bars(self, df: pd.DataFrame, symbol: str) -> list[OHLCVBar]:
        """Convert filtered bhav DataFrame rows to OHLCVBar objects."""
        bars: list[OHLCVBar] = []

        for _, row in df.iterrows():
            try:
                expiry = _parse_nse_expiry(str(row.get("EXPIRY_DT", "")))
                option_type_raw = str(row.get("OPTION_TYP", "")).strip().upper()
                option_type = option_type_raw if option_type_raw in ("CE", "PE") else None

                strike_raw = row.get("STRIKE_PR", 0)
                strike = float(strike_raw) if strike_raw else None

                # Derive the trading date from TIMESTAMP column or expiry column
                timestamp_raw = row.get("TIMESTAMP", "")
                try:
                    trading_date = datetime.strptime(
                        str(timestamp_raw).strip(), "%d-%b-%Y"
                    ).date()
                except ValueError:
                    # Fallback: use expiry date as a proxy (should not happen with good data)
                    trading_date = expiry

                bar = OHLCVBar(
                    symbol=str(row.get("SYMBOL", symbol)).strip().upper(),
                    timestamp=_nse_to_utc(trading_date),
                    open=float(row.get("OPEN", 0) or 0),
                    high=float(row.get("HIGH", 0) or 0),
                    low=float(row.get("LOW", 0) or 0),
                    close=float(row.get("CLOSE", 0) or row.get("SETTLE_PR", 0) or 0),
                    volume=int(row.get("CONTRACTS", 0) or 0),
                    oi=int(row.get("OPEN_INT", 0) or 0),
                    expiry=expiry,
                    strike=strike,
                    option_type=option_type,
                )
                bars.append(bar)

            except Exception as exc:
                logger.warning(
                    "Skipping malformed bhav row",
                    extra={
                        "event": "bhav_row_parse_error",
                        "error": str(exc),
                        "symbol": symbol,
                    },
                )
                continue

        return bars
