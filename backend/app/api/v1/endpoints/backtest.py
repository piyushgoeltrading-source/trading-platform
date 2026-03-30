"""
app/api/backtest.py

Backtest API Router — PiyushTrade
====================================
Endpoints:
  POST /backtest          — dispatch a backtest Celery task
  GET  /backtest/{task_id} — poll task status

Rules:
  - All endpoints require JWT authentication.
  - user_id is extracted from JWT — never from request body.
  - The endpoint validates the strategy belongs to the user before dispatching.
  - Celery task is dispatched asynchronously — client receives task_id immediately.
  - Result polling returns Celery task state — NOT a database query.
  - All errors follow the standard PiyushTrade error envelope.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.logging import get_structured_logger
from app.core.security import get_current_user
from app.core.time_utils import now_utc
from app.models.strategy import Strategy, StrategyStatus
from app.models.user import User
from app.tasks.backtest_tasks import run_backtest

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/backtest", tags=["backtest"])

# Maximum allowed backtest range — prevent runaway downloads
_MAX_BACKTEST_DAYS = 365 * 3  # 3 years


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class BacktestRequest(BaseModel):
    strategy_id: int = Field(..., description="ID of the strategy to backtest")
    from_date: date = Field(..., description="Start date (inclusive), format YYYY-MM-DD")
    to_date: date = Field(..., description="End date (inclusive), format YYYY-MM-DD")

    @model_validator(mode="after")
    def validate_date_range(self) -> "BacktestRequest":
        if self.from_date > self.to_date:
            raise ValueError("from_date must be before or equal to to_date")

        delta = (self.to_date - self.from_date).days
        if delta > _MAX_BACKTEST_DAYS:
            raise ValueError(
                f"Date range exceeds maximum of {_MAX_BACKTEST_DAYS} days "
                f"({delta} days requested)"
            )

        if self.to_date >= date.today():
            raise ValueError(
                "to_date must be in the past — cannot backtest future dates"
            )

        return self


class BacktestDispatchResponse(BaseModel):
    task_id: str
    strategy_id: int
    from_date: date
    to_date: date
    status: Literal["queued"]
    queued_at_utc: str


class BacktestStatusResponse(BaseModel):
    task_id: str
    status: str                     # PENDING | STARTED | SUCCESS | FAILURE | RETRY
    result: dict | None = None      # Populated on SUCCESS
    error: str | None = None        # Populated on FAILURE


# ---------------------------------------------------------------------------
# POST /backtest
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=BacktestDispatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch a backtest task",
)
async def dispatch_backtest(
    payload: BacktestRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> BacktestDispatchResponse:
    """
    Validate the strategy, then dispatch a Celery backtest task.

    Returns task_id immediately — client polls GET /backtest/{task_id} for result.

    Validations:
      - Strategy must exist and belong to the authenticated user.
      - Strategy must not be ARCHIVED (can't backtest an archived strategy).
      - Date range must be valid and in the past.
    """
    # --- Verify strategy ownership ---
    result = await db.execute(
        select(Strategy).where(
            Strategy.id == payload.strategy_id,
            Strategy.user_id == current_user.id,
        )
    )
    strategy = result.scalar_one_or_none()

    if strategy is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error_code": "STRATEGY_NOT_FOUND",
                "message": f"Strategy {payload.strategy_id} not found",
                "details": {"strategy_id": payload.strategy_id},
            },
        )

    if strategy.status == StrategyStatus.ARCHIVED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error_code": "STRATEGY_ARCHIVED",
                "message": "Cannot backtest an archived strategy",
                "details": {"strategy_id": payload.strategy_id},
            },
        )

    # --- Dispatch Celery task ---
    task = run_backtest.delay(
        strategy_id=payload.strategy_id,
        user_id=current_user.id,
        from_date_str=str(payload.from_date),
        to_date_str=str(payload.to_date),
    )

    queued_at = now_utc()

    logger.info(
        "Backtest task dispatched",
        extra={
            "event": "backtest_dispatched",
            "task_id": task.id,
            "strategy_id": payload.strategy_id,
            "user_id": current_user.id,
            "from_date": str(payload.from_date),
            "to_date": str(payload.to_date),
            "timestamp_utc": queued_at.isoformat(),
        },
    )

    return BacktestDispatchResponse(
        task_id=task.id,
        strategy_id=payload.strategy_id,
        from_date=payload.from_date,
        to_date=payload.to_date,
        status="queued",
        queued_at_utc=queued_at.isoformat(),
    )


# ---------------------------------------------------------------------------
# GET /backtest/{task_id}
# ---------------------------------------------------------------------------

@router.get(
    "/{task_id}",
    response_model=BacktestStatusResponse,
    summary="Poll backtest task status",
)
async def get_backtest_status(
    task_id: str,
    current_user: User = Depends(get_current_user),  # noqa: ARG001 — auth required
) -> BacktestStatusResponse:
    """
    Poll the status of a dispatched backtest task.

    Status values (Celery standard):
      PENDING   — task queued, not yet started
      STARTED   — worker has picked it up
      SUCCESS   — completed; result field contains the output
      FAILURE   — failed; error field contains the reason
      RETRY     — being retried after a transient error

    Note: There is no per-user ownership check on task_id here because
    Celery task IDs are UUIDs — unguessable. In Phase 5 we will add
    a task registry table in PostgreSQL for proper ownership enforcement.
    """
    async_result = AsyncResult(task_id)

    response = BacktestStatusResponse(
        task_id=task_id,
        status=async_result.state,
    )

    if async_result.state == "SUCCESS":
        response.result = async_result.result

    elif async_result.state == "FAILURE":
        # Extract error message safely — result may be an exception
        exc = async_result.result
        response.error = str(exc) if exc else "Unknown error"

        logger.warning(
            "Backtest task failed — reported to client",
            extra={
                "event": "backtest_task_failure_polled",
                "task_id": task_id,
                "user_id": current_user.id,
                "error": response.error,
            },
        )

    return response
