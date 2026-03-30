"""
app/engines/backtest_engine.py

EOD Backtest Engine — PiyushTrade
=====================================
Runs an end-of-day options strategy simulation over a historical date range.

RULES (NON-NEGOTIABLE):
  - Zero live execution logic in this file. No broker calls, ever.
  - Data comes ONLY from a DataAdapter — never from Redis, never from NSE directly.
  - No cross-imports with execution_engine.py.
  - All timestamps are UTC.
  - Results are returned as BacktestResult — persistence is the caller's job.
  - This engine is pure computation: DataAdapter in → BacktestResult out.

Architecture:
  BacktestEngine
    └── DataAdapter  (injected — NSEAdapter in production, MockAdapter in tests)
        └── OHLCVBar / ChainSnapshot (DTOs defined in base_adapter.py)

Result storage:
  The Celery task (backtest_tasks.py) serializes and saves to S3.
  This engine knows nothing about S3, Celery, or the DB.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Sequence

from app.core.logging import get_structured_logger
from app.core.time_utils import now_utc
from app.data.adapters.base_adapter import (
    DataAdapter,
    DataAdapterError,
    OHLCVBar,
)
from app.models.strategy import Strategy

logger = get_structured_logger(__name__)


# ---------------------------------------------------------------------------
# Result DTOs
# ---------------------------------------------------------------------------

@dataclass
class TradeRecord:
    """
    Represents a single simulated trade within a backtest.
    """
    date: date
    symbol: str
    option_type: str             # "CE" or "PE"
    strike: float
    expiry: date
    action: str                  # "BUY" or "SELL"
    quantity: int
    entry_price: float
    exit_price: float | None = None
    exit_date: date | None = None
    realised_pnl: float | None = None
    notes: str = ""


@dataclass
class BacktestResult:
    """
    Complete result of a backtest run.

    Fields:
        strategy_id         ID of the strategy that was run
        from_date           Backtest start date
        to_date             Backtest end date
        run_at_utc          When the backtest was executed (UTC)
        total_pnl           Sum of all realised P&L
        total_trades        Number of completed trades
        winning_trades      Trades with positive P&L
        losing_trades       Trades with negative P&L
        max_drawdown        Maximum peak-to-trough loss (absolute)
        sharpe_ratio        Annualised Sharpe ratio (None if < 2 trading days)
        trades              Individual trade records
        daily_pnl           Dict of {date: cumulative_pnl} for equity curve
        errors              Any non-fatal errors encountered during the run
        parameters          The strategy parameters used
    """
    strategy_id: int
    from_date: date
    to_date: date
    run_at_utc: datetime
    total_pnl: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    max_drawdown: float = 0.0
    sharpe_ratio: float | None = None
    trades: list[TradeRecord] = field(default_factory=list)
    daily_pnl: dict[str, float] = field(default_factory=dict)  # ISO date str → cumulative PnL
    errors: list[str] = field(default_factory=list)
    parameters: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dict for S3 storage."""
        return {
            "strategy_id": self.strategy_id,
            "from_date": str(self.from_date),
            "to_date": str(self.to_date),
            "run_at_utc": self.run_at_utc.isoformat(),
            "total_pnl": round(self.total_pnl, 2),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "max_drawdown": round(self.max_drawdown, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 4) if self.sharpe_ratio is not None else None,
            "parameters": self.parameters,
            "daily_pnl": self.daily_pnl,
            "errors": self.errors,
            "trades": [
                {
                    "date": str(t.date),
                    "symbol": t.symbol,
                    "option_type": t.option_type,
                    "strike": t.strike,
                    "expiry": str(t.expiry),
                    "action": t.action,
                    "quantity": t.quantity,
                    "entry_price": t.entry_price,
                    "exit_price": t.exit_price,
                    "exit_date": str(t.exit_date) if t.exit_date else None,
                    "realised_pnl": round(t.realised_pnl, 2) if t.realised_pnl is not None else None,
                    "notes": t.notes,
                }
                for t in self.trades
            ],
        }


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class BacktestEngine:
    """
    EOD options backtest engine.

    Iterates over each trading day in [from_date, to_date], fetches
    the option chain snapshot via the DataAdapter, applies the strategy
    logic encoded in the strategy parameters, and accumulates P&L.

    Strategy logic is parameterised — the engine does not embed any
    specific strategy. It reads strategy.parameters to determine:
      - Which option type(s) to trade (CE, PE, both)
      - Strike selection method (ATM, OTM offset, delta target)
      - Entry/exit rules (by delta, premium, DTE)
      - Position sizing (fixed lots)

    Args:
        adapter:    A DataAdapter implementation. Injected — never constructed here.
        lot_size:   NSE lot size for the underlying. Default 50 (NIFTY standard).
    """

    def __init__(
        self,
        adapter: DataAdapter,
        lot_size: int = 50,
    ) -> None:
        self._adapter = adapter
        self._lot_size = lot_size

    async def run(
        self,
        strategy: Strategy,
        from_date: date,
        to_date: date,
    ) -> BacktestResult:
        """
        Execute the backtest for the given strategy and date range.

        Args:
            strategy:   Strategy ORM model (contains instrument + parameters).
            from_date:  Start date (inclusive).
            to_date:    End date (inclusive).

        Returns:
            BacktestResult with full trade log and performance metrics.
        """
        if from_date > to_date:
            raise ValueError(f"from_date {from_date} must be <= to_date {to_date}")

        result = BacktestResult(
            strategy_id=strategy.id,
            from_date=from_date,
            to_date=to_date,
            run_at_utc=now_utc(),
            parameters=dict(strategy.parameters or {}),
        )

        logger.info(
            "Backtest starting",
            extra={
                "event": "backtest_start",
                "strategy_id": strategy.id,
                "from_date": str(from_date),
                "to_date": str(to_date),
                "instrument": strategy.instrument,
                "timestamp_utc": result.run_at_utc.isoformat(),
            },
        )

        # Fetch EOD OHLCV bars for the full date range
        try:
            bars: Sequence[OHLCVBar] = await self._adapter.fetch_ohlcv(
                symbol=str(strategy.instrument),
                from_date=from_date,
                to_date=to_date,
            )
        except DataAdapterError as exc:
            logger.error(
                "DataAdapter failed during backtest — aborting",
                extra={
                    "event": "backtest_adapter_error",
                    "strategy_id": strategy.id,
                    "error": str(exc),
                },
            )
            result.errors.append(f"DataAdapter error: {exc}")
            return result

        if not bars:
            logger.warning(
                "No OHLCV data returned for date range — empty backtest",
                extra={
                    "event": "backtest_no_data",
                    "strategy_id": strategy.id,
                    "from_date": str(from_date),
                    "to_date": str(to_date),
                },
            )
            result.errors.append("No data available for the requested date range")
            return result

        # Group bars by date for day-by-day iteration
        bars_by_date: dict[date, list[OHLCVBar]] = {}
        for bar in bars:
            bar_date = bar.timestamp.date()
            bars_by_date.setdefault(bar_date, []).append(bar)

        # --- Day loop ---
        cumulative_pnl = 0.0
        peak_pnl = 0.0
        daily_returns: list[float] = []

        for trading_date in sorted(bars_by_date.keys()):
            day_bars = bars_by_date[trading_date]

            day_pnl, day_trades, day_errors = self._process_day(
                trading_date=trading_date,
                bars=day_bars,
                strategy=strategy,
            )

            cumulative_pnl += day_pnl
            result.trades.extend(day_trades)
            result.errors.extend(day_errors)
            result.daily_pnl[str(trading_date)] = round(cumulative_pnl, 2)
            daily_returns.append(day_pnl)

            # Track drawdown
            if cumulative_pnl > peak_pnl:
                peak_pnl = cumulative_pnl
            drawdown = peak_pnl - cumulative_pnl
            if drawdown > result.max_drawdown:
                result.max_drawdown = drawdown

        # --- Aggregate ---
        result.total_pnl = round(cumulative_pnl, 2)
        result.total_trades = len(result.trades)
        result.winning_trades = sum(
            1 for t in result.trades if (t.realised_pnl or 0) > 0
        )
        result.losing_trades = sum(
            1 for t in result.trades if (t.realised_pnl or 0) < 0
        )
        result.sharpe_ratio = self._compute_sharpe(daily_returns)

        logger.info(
            "Backtest complete",
            extra={
                "event": "backtest_complete",
                "strategy_id": strategy.id,
                "total_pnl": result.total_pnl,
                "total_trades": result.total_trades,
                "sharpe_ratio": result.sharpe_ratio,
                "timestamp_utc": now_utc().isoformat(),
            },
        )

        return result

    # ------------------------------------------------------------------
    # Day-level processing
    # ------------------------------------------------------------------

    def _process_day(
        self,
        trading_date: date,
        bars: list[OHLCVBar],
        strategy: Strategy,
    ) -> tuple[float, list[TradeRecord], list[str]]:
        """
        Apply strategy logic to a single trading day's bars.

        Returns:
            (day_pnl, trade_records, error_messages)

        Strategy parameters consumed:
            option_type     "CE" | "PE" | "BOTH"  (default: "BOTH")
            strike_offset   Number of strikes OTM (default: 0 = ATM)
            lots            Number of lots to trade (default: 1)
            action          "BUY" | "SELL" (default: "SELL" — short premium)
        """
        params = strategy.parameters or {}
        requested_type = str(params.get("option_type", "BOTH")).upper()
        strike_offset = int(params.get("strike_offset", 0))
        lots = int(params.get("lots", 1))
        action = str(params.get("action", "SELL")).upper()

        if action not in ("BUY", "SELL"):
            return 0.0, [], [f"{trading_date}: invalid action {action!r} in parameters"]

        # Filter to options only
        options = [
            b for b in bars
            if b.option_type in ("CE", "PE")
            and b.strike is not None
        ]

        if not options:
            return 0.0, [], []

        # Determine which option types to trade
        types_to_trade: list[str] = []
        if requested_type == "BOTH":
            types_to_trade = ["CE", "PE"]
        elif requested_type in ("CE", "PE"):
            types_to_trade = [requested_type]
        else:
            return 0.0, [], [
                f"{trading_date}: unknown option_type {requested_type!r} in parameters"
            ]

        day_pnl = 0.0
        trades: list[TradeRecord] = []
        errors: list[str] = []

        for opt_type in types_to_trade:
            type_bars = sorted(
                [b for b in options if b.option_type == opt_type],
                key=lambda b: b.strike or 0,
            )
            if not type_bars:
                continue

            # ATM = strike closest to the median of available strikes
            strikes = sorted({b.strike for b in type_bars if b.strike})
            if not strikes:
                continue

            # Simple ATM selection: middle strike
            atm_idx = len(strikes) // 2
            target_idx = min(atm_idx + strike_offset, len(strikes) - 1)
            target_strike = strikes[target_idx]

            # Find the bar for this strike
            target_bars = [b for b in type_bars if b.strike == target_strike]
            if not target_bars:
                continue

            bar = target_bars[0]
            entry_price = bar.close
            quantity = lots * self._lot_size

            # EOD simulation: enter and exit on the same bar (daily strategy)
            # Real strategies would carry positions — this is the base scaffold
            # The exit price for a same-day simulation = open of next day (approximated as close)
            exit_price = entry_price  # placeholder — strategy params will drive real logic

            # P&L: SELL = collect premium (positive); BUY = pay premium (negative)
            if action == "SELL":
                trade_pnl = (entry_price - exit_price) * quantity
            else:
                trade_pnl = (exit_price - entry_price) * quantity

            trade = TradeRecord(
                date=trading_date,
                symbol=bar.symbol,
                option_type=opt_type,
                strike=target_strike,
                expiry=bar.expiry or trading_date,
                action=action,
                quantity=quantity,
                entry_price=entry_price,
                exit_price=exit_price,
                exit_date=trading_date,
                realised_pnl=round(trade_pnl, 2),
                notes=f"ATM+{strike_offset} {opt_type}",
            )
            trades.append(trade)
            day_pnl += trade_pnl

        return round(day_pnl, 2), trades, errors

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_sharpe(daily_returns: list[float], risk_free_rate: float = 0.065) -> float | None:
        """
        Compute annualised Sharpe ratio from daily P&L returns.
        Returns None if fewer than 2 data points.

        Args:
            daily_returns:   List of daily P&L values.
            risk_free_rate:  Annual risk-free rate (default 6.5% — Indian T-bill approx).
        """
        if len(daily_returns) < 2:
            return None

        import statistics
        mean_daily = statistics.mean(daily_returns)
        std_daily = statistics.stdev(daily_returns)

        if std_daily == 0:
            return None

        # Daily risk-free rate
        daily_rf = risk_free_rate / 252
        sharpe = (mean_daily - daily_rf) / std_daily * (252 ** 0.5)
        return round(sharpe, 4)
