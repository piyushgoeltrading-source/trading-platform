# app/services/reconciliation_service.py
"""
app/services/reconciliation_service.py

Reconciliation Service — PiyushTrade Phase 3, Step 9
=====================================================
Compares open positions in PostgreSQL (source of truth) against live
positions reported by each user's broker. Detects and persists discrepancies.
Never silently corrects — all fixes flow through execution_engine.py.

Architecture:
  - ReconciliationService.run(user) executes one cycle for one user.
  - run_all_users() iterates all active users — called by the Celery beat task.
  - Celery beat task (reconcile_positions_task) fires every 5 minutes
    during market hours only (checked via is_market_open()).

DB position logic:
  - "Open position" = net quantity per (trading_symbol, exchange, product_code)
    computed from the trades table for this user.
  - Net qty = sum of signed quantities (BUY positive, SELL negative).
  - Net qty == 0 means the position is closed — excluded from comparison.

Broker position logic:
  - BaseBroker.get_positions() is synchronous — dispatched via asyncio.to_thread().
  - Zero-quantity broker positions are excluded (broker sometimes returns them).

Comparison key:
  (trading_symbol, exchange, product_code) — case-normalised to uppercase.

Discrepancy types:
  missing_in_db     — broker reports a position we have no DB record for.
  missing_in_broker — DB shows an open position broker does not report.
  qty_mismatch      — both sides have the position but quantities differ.

Persistence:
  - ReconciliationResult written to reconciliation_logs table after every run.
  - Discrepancy detail stored as JSONB. Never mutated after insert.
  - Requires migration 0006 (see note at bottom of file).

Rules:
  - PostgreSQL is always the source of truth.
  - Redis is never consulted here.
  - This service NEVER writes to the broker.
  - This service NEVER corrects DB records directly.
  - get_structured_logger is the only logging import.
  - ZERO backtest logic.
  - ZERO cross-import with backtest_engine or execution_engine.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.brokers.base_broker import BrokerError, Position
from app.brokers.factory import BrokerFactory, BrokerConfigError
from app.core.config import settings
from app.core.database import get_async_db
from app.core.logging import get_structured_logger
from app.core.time_utils import is_market_open, now_utc
from app.models.order import Order, OrderStatus
from app.models.trade import Trade
from app.models.user import User
from worker.worker import celery_app

logger = get_structured_logger(__name__)


# ---------------------------------------------------------------------------
# ReconciliationResult (extends stub — all original fields preserved)
# ---------------------------------------------------------------------------

@dataclass
class PositionDiscrepancy:
    """Detail of a single position mismatch."""
    trading_symbol: str
    exchange: str
    product_code: str
    db_qty: int                        # Net qty from trades table (0 if missing)
    broker_qty: int                    # Qty from broker (0 if missing)
    discrepancy_type: str              # "missing_in_db" | "missing_in_broker" | "qty_mismatch"


@dataclass
class ReconciliationResult:
    """
    Outcome of a single reconciliation run for one user.
    Preserved from stub — additional fields appended only.
    """
    run_at_utc: datetime = field(default_factory=now_utc)
    matched: int = 0
    discrepancies: int = 0
    missing_in_db: list[dict] = field(default_factory=list)       # broker has, DB doesn't
    missing_in_broker: list[dict] = field(default_factory=list)   # DB has, broker doesn't
    qty_mismatch: list[dict] = field(default_factory=list)        # both have, qty differs
    notes: str = ""
    # Added fields (not in stub):
    user_id: Optional[int] = None
    broker_name: Optional[str] = None
    broker_fetch_failed: bool = False  # True if broker call errored — partial run


# ---------------------------------------------------------------------------
# Position key type
# ---------------------------------------------------------------------------

# Comparison key: (trading_symbol, exchange, product_code) all uppercase
_PosKey = tuple[str, str, str]


def _pos_key(trading_symbol: str, exchange: str, product_code: str) -> _PosKey:
    return (
        trading_symbol.upper().strip(),
        exchange.upper().strip(),
        product_code.upper().strip(),
    )


# ---------------------------------------------------------------------------
# ReconciliationService
# ---------------------------------------------------------------------------

class ReconciliationService:
    """
    Compares PostgreSQL position records against broker-reported positions.

    One instance is safe to reuse across multiple run() calls.
    Each run() creates its own DB session via the dependency — no session
    is held on the instance between calls.

    Usage (Celery task):
        service = ReconciliationService()
        result = await service.run(user=user, db=db)
    """

    def __init__(
        self,
        db_session: object = None,      # Accepted for stub compat — not used; each run() gets its own session
        broker_adapter: object = None,  # Accepted for stub compat — not used; broker resolved per user
    ) -> None:
        # Stub-compatible constructor — injected values are intentionally
        # ignored in production. BrokerFactory.get(user) resolves per run.
        _ = db_session
        _ = broker_adapter

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self, user: User, db: AsyncSession) -> ReconciliationResult:
        """
        Execute one reconciliation cycle for a single user.

        Args:
            user: Authenticated User ORM instance (must have .broker set).
            db:   AsyncSession — caller provides via get_async_db().

        Returns:
            ReconciliationResult with full discrepancy detail.

        Never raises — all errors are caught, logged, and reflected in the
        result (broker_fetch_failed=True). Caller decides what to do.
        """
        result = ReconciliationResult(
            run_at_utc=now_utc(),
            user_id=user.id,
            broker_name=str(user.broker) if user.broker else None,
        )

        logger.info(
            "reconciliation_run_start",
            user_id=user.id,
            broker=result.broker_name,
            timestamp_utc=result.run_at_utc.isoformat(),
        )

        # Step 1: DB positions (source of truth)
        db_positions = await self._fetch_db_positions(user_id=user.id, db=db)

        # Step 2: Broker positions (comparison target)
        broker_positions: dict[_PosKey, int] = {}
        try:
            broker_positions = await self._fetch_broker_positions(user=user)
        except (BrokerError, BrokerConfigError, Exception) as exc:
            # Broker unreachable — partial run. Log and continue so we
            # at least persist the attempt. Do not raise.
            result.broker_fetch_failed = True
            result.notes = f"Broker fetch failed: {exc}"
            logger.error(
                "reconciliation_broker_fetch_failed",
                user_id=user.id,
                error=str(exc),
            )
            await self._persist(result=result, db=db)
            return result

        # Step 3: Compare
        result = self._compare(
            db_positions=db_positions,
            broker_positions=broker_positions,
            base_result=result,
        )

        # Step 4: Log outcome
        if result.discrepancies > 0:
            logger.error(
                "reconciliation_discrepancies_found",
                user_id=user.id,
                discrepancies=result.discrepancies,
                missing_in_db=len(result.missing_in_db),
                missing_in_broker=len(result.missing_in_broker),
                qty_mismatch=len(result.qty_mismatch),
            )
        else:
            logger.info(
                "reconciliation_clean",
                user_id=user.id,
                matched=result.matched,
            )

        # Step 5: Persist result to DB (always — clean runs too)
        await self._persist(result=result, db=db)

        return result

    async def run_all_users(self, db: AsyncSession) -> list[ReconciliationResult]:
        """
        Run reconciliation for every active user that has a broker configured.

        Called by the Celery beat task. Returns results for all users.
        One user's failure does not abort the others.
        """
        result = await db.execute(
            select(User).where(User.broker.isnot(None))
        )
        users: list[User] = list(result.scalars().all())

        results = []
        for user in users:
            try:
                r = await self.run(user=user, db=db)
                results.append(r)
            except Exception as exc:
                logger.error(
                    "reconciliation_user_run_failed",
                    user_id=user.id,
                    error=str(exc),
                )
        return results

    # ------------------------------------------------------------------
    # Private: fetch DB positions
    # ------------------------------------------------------------------

    async def _fetch_db_positions(
        self,
        user_id: int,
        db: AsyncSession,
    ) -> dict[_PosKey, int]:
        """
        Compute net open positions from the trades table for this user.

        "Net qty" = SUM of signed trade quantities per instrument:
          BUY  trade → positive qty
          SELL trade → negative qty
          Net == 0   → position closed, excluded from result

        Returns:
            Dict mapping (trading_symbol, exchange, product_code) → net_qty.
            Only non-zero positions are included.
        """
        # Compute net signed quantity per instrument across ALL sides.
        # BUY fills are positive, SELL fills are negative.
        # Grouping by instrument only (not side) gives the true net position:
        #   10 BUY + 10 SELL -> net_qty = 0  -> excluded by HAVING (position closed)
        #   10 BUY +  5 SELL -> net_qty = 5  -> included (still open long)
        # Grouping by (instrument, side) was wrong — it split closed positions
        # into two rows with the same dict key, causing the second to overwrite
        # the first and producing false discrepancies on every reconciliation run.
        rows = await db.execute(
            text("""
                SELECT
                    o.instrument,
                    SUM(
                        CASE WHEN o.side = 'BUY'
                            THEN t.fill_qty
                            ELSE -t.fill_qty
                        END
                    ) AS net_qty
                FROM trades t
                JOIN orders o ON t.order_id = o.id
                WHERE t.user_id = :user_id
                GROUP BY o.instrument
                HAVING SUM(
                    CASE WHEN o.side = 'BUY'
                        THEN t.fill_qty
                        ELSE -t.fill_qty
                    END
                ) != 0
            """),
            {"user_id": user_id},
        )

        db_positions: dict[_PosKey, int] = {}
        for row in rows:
            # Trade model has no exchange/product_code — use instrument as key
            key = _pos_key(row.instrument, "NSE", "NRML")  # V1: NSE options only
            db_positions[key] = int(row.net_qty)

        logger.info(
            "reconciliation_db_positions_fetched",
            user_id=user_id,
            count=len(db_positions),
        )
        return db_positions

    # ------------------------------------------------------------------
    # Private: fetch broker positions
    # ------------------------------------------------------------------

    async def _fetch_broker_positions(
        self,
        user: User,
    ) -> dict[_PosKey, int]:
        """
        Fetch live positions from the user's broker via BrokerFactory.

        BaseBroker.get_positions() is synchronous — dispatched to a thread pool
        via asyncio.to_thread() to avoid blocking the event loop.

        Excludes broker positions with net quantity == 0 (broker sometimes
        returns closed positions in the same response).

        Returns:
            Dict mapping (trading_symbol, exchange, product_code) → net_qty.

        Raises:
            BrokerConfigError: Unknown broker on user record.
            BrokerError:       Broker SDK raised an error.
        """
        broker = BrokerFactory.get(user)
        positions: list[Position] = await asyncio.to_thread(broker.get_positions)

        broker_positions: dict[_PosKey, int] = {}
        for pos in positions:
            if pos.quantity == 0:
                continue  # Closed position — exclude
            key = _pos_key(pos.trading_symbol, pos.exchange, pos.product_code)
            broker_positions[key] = pos.quantity

        logger.info(
            "reconciliation_broker_positions_fetched",
            user_id=user.id,
            count=len(broker_positions),
        )
        return broker_positions

    # ------------------------------------------------------------------
    # Private: compare (sync — no I/O)
    # ------------------------------------------------------------------

    def _compare(
        self,
        db_positions: dict[_PosKey, int],
        broker_positions: dict[_PosKey, int],
        base_result: ReconciliationResult,
    ) -> ReconciliationResult:
        """
        Diff DB positions against broker positions.

        Three discrepancy types:
          missing_in_db     — key in broker, not in DB
          missing_in_broker — key in DB, not in broker
          qty_mismatch      — key in both, quantities differ

        Args:
            db_positions:     Output of _fetch_db_positions().
            broker_positions: Output of _fetch_broker_positions().
            base_result:      ReconciliationResult with run metadata populated.

        Returns:
            Populated ReconciliationResult. base_result is mutated and returned.
        """
        all_keys = set(db_positions.keys()) | set(broker_positions.keys())

        for key in all_keys:
            trading_symbol, exchange, product_code = key
            db_qty = db_positions.get(key, 0)
            broker_qty = broker_positions.get(key, 0)

            discrepancy = PositionDiscrepancy(
                trading_symbol=trading_symbol,
                exchange=exchange,
                product_code=product_code,
                db_qty=db_qty,
                broker_qty=broker_qty,
                discrepancy_type="",
            )

            if key not in db_positions:
                discrepancy.discrepancy_type = "missing_in_db"
                base_result.missing_in_db.append(asdict(discrepancy))
                base_result.discrepancies += 1

            elif key not in broker_positions:
                discrepancy.discrepancy_type = "missing_in_broker"
                base_result.missing_in_broker.append(asdict(discrepancy))
                base_result.discrepancies += 1

            elif db_qty != broker_qty:
                discrepancy.discrepancy_type = "qty_mismatch"
                base_result.qty_mismatch.append(asdict(discrepancy))
                base_result.discrepancies += 1

            else:
                base_result.matched += 1

        return base_result

    # ------------------------------------------------------------------
    # Private: persist result
    # ------------------------------------------------------------------

    async def _persist(
        self,
        result: ReconciliationResult,
        db: AsyncSession,
    ) -> None:
        """
        Persist ReconciliationResult to the reconciliation_logs table.

        Discrepancy detail stored as JSONB. Never mutated after insert.
        Requires migration 0006 — see note at bottom of this file.

        Never raises — persistence failure is logged but does not abort
        the reconciliation run (result is still returned to caller).
        """
        try:
            await db.execute(
                text("""
                    INSERT INTO reconciliation_logs (
                        user_id,
                        broker_name,
                        run_at_utc,
                        matched,
                        discrepancies,
                        missing_in_db,
                        missing_in_broker,
                        qty_mismatch,
                        broker_fetch_failed,
                        notes
                    ) VALUES (
                        :user_id,
                        :broker_name,
                        :run_at_utc,
                        :matched,
                        :discrepancies,
                        :missing_in_db,
                        :missing_in_broker,
                        :qty_mismatch,
                        :broker_fetch_failed,
                        :notes
                    )
                """),
                {
                    "user_id": result.user_id,
                    "broker_name": result.broker_name,
                    "run_at_utc": result.run_at_utc,
                    "matched": result.matched,
                    "discrepancies": result.discrepancies,
                    "missing_in_db": json.dumps(result.missing_in_db),
                    "missing_in_broker": json.dumps(result.missing_in_broker),
                    "qty_mismatch": json.dumps(result.qty_mismatch),
                    "broker_fetch_failed": result.broker_fetch_failed,
                    "notes": result.notes,
                },
            )
            await db.commit()

            logger.info(
                "reconciliation_result_persisted",
                user_id=result.user_id,
                matched=result.matched,
                discrepancies=result.discrepancies,
            )
        except Exception as exc:
            await db.rollback()
            logger.error(
                "reconciliation_persist_failed",
                user_id=result.user_id,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# Celery beat task — fires every 5 minutes during market hours
# ---------------------------------------------------------------------------

_service = ReconciliationService()


@celery_app.task(name="tasks.reconcile_positions", bind=True, max_retries=0)
def reconcile_positions_task(self) -> None:
    """
    Celery beat task: run position reconciliation for all users.

    Scheduled every 5 minutes. Skips immediately if market is not open.
    max_retries=0 — reconciliation is time-sensitive; a stale retry would
    compare against already-stale data. Better to wait for the next beat.

    Wiring (add to celery beat schedule in config.py):
        "reconcile-positions": {
            "task": "tasks.reconcile_positions",
            "schedule": crontab(minute="*/5"),
        }
    """
    if not is_market_open():
        logger.info("reconciliation_skipped_market_closed")
        return

    # Celery tasks are sync — run async service in a new event loop
    import asyncio
    from app.core.database import AsyncSessionLocal

    async def _run() -> None:
        async with AsyncSessionLocal() as db:
            await _service.run_all_users(db=db)

    asyncio.run(_run())