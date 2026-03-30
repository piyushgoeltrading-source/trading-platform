"""
app/services/reconciliation_service.py

Reconciliation Service — PiyushTrade (STUB)
=============================================
Phase: STUB — wired but not implemented.

Purpose (Phase 3+):
  - Periodically compare open positions stored in PostgreSQL
    against positions reported by the broker (Zerodha).
  - Detect and flag any discrepancies.
  - Emit alerts for manual review or automated correction.
  - Source of truth is ALWAYS PostgreSQL — broker is the comparison target.

Rules:
  - This service may NEVER write directly to the broker.
  - All corrections flow through execution_engine.py (Phase 3).
  - Discrepancies must be logged and stored — never silently corrected.
  - Redis must not be used as a reference in reconciliation — only PostgreSQL.

This stub exists so:
  1. Import paths are established.
  2. The Celery task (Phase 3) can reference this module immediately.
  3. The class interface is agreed before implementation begins.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.core.logging import get_structured_logger
from app.core.time_utils import now_utc

logger = get_structured_logger(__name__)


@dataclass
class ReconciliationResult:
    """
    Represents the outcome of a single reconciliation run.
    """
    run_at_utc: datetime = field(default_factory=now_utc)
    matched: int = 0               # positions that agree
    discrepancies: int = 0         # positions that differ
    missing_in_db: list[dict] = field(default_factory=list)      # broker has, DB doesn't
    missing_in_broker: list[dict] = field(default_factory=list)  # DB has, broker doesn't
    notes: str = ""


class ReconciliationService:
    """
    Compares PostgreSQL position records against broker-reported positions.

    Phase 3 Implementation Checklist:
    ----------------------------------
    [ ] Inject DB session + broker adapter via __init__.
    [ ] Implement _fetch_db_positions() — query trades table, net by instrument.
    [ ] Implement _fetch_broker_positions() — call Zerodha positions endpoint.
    [ ] Implement _compare() — diff the two sets, populate ReconciliationResult.
    [ ] Emit structured log + persist ReconciliationResult to DB.
    [ ] Wire to a Celery beat task (e.g. every 5 minutes during market hours).
    """

    def __init__(
        self,
        db_session: Any = None,      # SQLAlchemy AsyncSession (Phase 3)
        broker_adapter: Any = None,  # BrokerAdapter base class (Phase 3)
    ) -> None:
        self._db = db_session
        self._broker = broker_adapter

    async def run(self) -> ReconciliationResult:
        """
        Execute one reconciliation cycle.

        Returns:
            ReconciliationResult with matched/discrepancy counts.

        STUB: Currently a no-op. Logs that it was called.
        """
        result = ReconciliationResult()

        logger.info(
            "ReconciliationService.run() called — STUB, no logic yet",
            extra={
                "event": "reconciliation_stub_run",
                "timestamp_utc": result.run_at_utc.isoformat(),
            },
        )

        # TODO (Phase 3): replace stub body with real implementation.
        # Steps:
        #   db_positions = await self._fetch_db_positions()
        #   broker_positions = await self._fetch_broker_positions()
        #   result = self._compare(db_positions, broker_positions)
        #   await self._persist(result)

        return result

    async def _fetch_db_positions(self) -> list[dict]:
        """
        Query PostgreSQL for current open positions per user.
        STUB — Phase 3 implementation.
        """
        raise NotImplementedError("_fetch_db_positions() not yet implemented")

    async def _fetch_broker_positions(self) -> list[dict]:
        """
        Fetch live positions from Zerodha via the broker adapter.
        STUB — Phase 3 implementation.
        """
        raise NotImplementedError("_fetch_broker_positions() not yet implemented")

    def _compare(
        self,
        db_positions: list[dict],
        broker_positions: list[dict],
    ) -> ReconciliationResult:
        """
        Diff DB positions against broker positions.
        STUB — Phase 3 implementation.
        """
        raise NotImplementedError("_compare() not yet implemented")
