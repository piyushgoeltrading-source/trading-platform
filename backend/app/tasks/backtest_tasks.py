"""
app/tasks/backtest_tasks.py

Backtest Celery Task — PiyushTrade
=====================================
Defines the async Celery task that executes a backtest and saves results to S3.

Task: run_backtest
  1. Load strategy from PostgreSQL (by strategy_id + user_id for ownership check).
  2. Instantiate NSEAdapter + BacktestEngine.
  3. Run the backtest.
  4. Serialize BacktestResult to JSON.
  5. Upload to S3 at a deterministic key.
  6. Update the strategy record in DB with last_backtest_at (Phase 3 column).

Rules:
  - task_id is the Celery task ID — clients poll this for status.
  - Redis is NOT used to store backtest results — S3 only.
  - PostgreSQL is NOT written to from this task except for status fields.
  - If the strategy is not found or not owned by the user → task fails immediately.
  - All timestamps are UTC.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from celery import Task

from app.core.config import settings
from app.core.logging import get_structured_logger
from app.core.time_utils import now_utc
from app.data_ingestion.adapters.nse_adapter import NSEAdapter
from app.engines.backtest_engine import BacktestEngine, BacktestResult
from worker.worker import celery_app

logger = get_structured_logger(__name__)


# ---------------------------------------------------------------------------
# S3 key builder
# ---------------------------------------------------------------------------

def _s3_result_key(strategy_id: int, task_id: str, run_at: datetime) -> str:
    """
    Build a deterministic, human-readable S3 key for a backtest result.

    Pattern:
        backtest-results/{strategy_id}/{YYYY}/{MM}/{task_id}.json

    Example:
        backtest-results/42/2025/03/abc123def456.json
    """
    return (
        f"backtest-results/{strategy_id}/"
        f"{run_at.strftime('%Y/%m')}/"
        f"{task_id}.json"
    )


# ---------------------------------------------------------------------------
# Celery task
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="app.tasks.backtest_tasks.run_backtest",
    max_retries=2,
    default_retry_delay=30,
    acks_late=True,       # Only ack after successful completion
    reject_on_worker_lost=True,
)
def run_backtest(
    self: Task,
    strategy_id: int,
    user_id: int,
    from_date_str: str,   # ISO format "YYYY-MM-DD" — dates are not JSON-serializable
    to_date_str: str,
) -> dict:
    """
    Execute a backtest for a strategy and save results to S3.

    Args:
        strategy_id:    ID of the strategy to backtest.
        user_id:        ID of the requesting user (ownership check).
        from_date_str:  Start date as ISO string "YYYY-MM-DD".
        to_date_str:    End date as ISO string "YYYY-MM-DD".

    Returns:
        Dict with keys:
            task_id     Celery task ID
            strategy_id
            s3_key      Where the result JSON was saved
            status      "completed" | "failed"
            run_at_utc  ISO datetime string

    Note:
        This is a synchronous Celery task. Async Celery requires gevent/eventlet.
        The BacktestEngine is called via asyncio.run() to bridge sync→async.
    """
    import asyncio

    task_id = self.request.id
    run_at = now_utc()

    logger.info(
        "Backtest task started",
        extra={
            "event": "backtest_task_start",
            "task_id": task_id,
            "strategy_id": strategy_id,
            "user_id": user_id,
            "from_date": from_date_str,
            "to_date": to_date_str,
            "timestamp_utc": run_at.isoformat(),
        },
    )

    # --- Parse dates ---
    try:
        from_date = date.fromisoformat(from_date_str)
        to_date = date.fromisoformat(to_date_str)
    except ValueError as exc:
        logger.error(
            "Invalid date format in backtest task",
            extra={
                "event": "backtest_task_invalid_dates",
                "task_id": task_id,
                "from_date": from_date_str,
                "to_date": to_date_str,
                "error": str(exc),
            },
        )
        raise ValueError(f"Invalid date format: {exc}") from exc

    if from_date > to_date:
        raise ValueError(f"from_date {from_date} must be <= to_date {to_date}")

    # --- Load strategy from DB (synchronous via asyncio.run) ---
    try:
        strategy = asyncio.run(_load_strategy(strategy_id, user_id))
    except StrategyNotFoundError as exc:
        logger.error(
            "Strategy not found — backtest task aborted",
            extra={
                "event": "backtest_task_strategy_not_found",
                "task_id": task_id,
                "strategy_id": strategy_id,
                "user_id": user_id,
            },
        )
        # Don't retry — strategy not found is a permanent failure
        raise exc

    # --- Run backtest ---
    try:
        adapter = NSEAdapter()
        engine = BacktestEngine(adapter=adapter)
        result: BacktestResult = asyncio.run(
            engine.run(
                strategy=strategy,
                from_date=from_date,
                to_date=to_date,
            )
        )
    except Exception as exc:
        logger.error(
            "Backtest engine raised an exception",
            extra={
                "event": "backtest_engine_error",
                "task_id": task_id,
                "strategy_id": strategy_id,
                "error": str(exc),
            },
        )
        # Retry on transient errors (network issues with NSE download)
        raise self.retry(exc=exc)

    # --- Serialize result ---
    result_dict = result.to_dict()
    result_dict["task_id"] = task_id
    result_json = json.dumps(result_dict, default=str, indent=2)

    # --- Upload to S3 ---
    s3_key = _s3_result_key(strategy_id, task_id, run_at)
    try:
        _upload_to_s3(s3_key, result_json)
    except (BotoCoreError, ClientError) as exc:
        logger.error(
            "S3 upload failed for backtest result",
            extra={
                "event": "backtest_s3_upload_error",
                "task_id": task_id,
                "strategy_id": strategy_id,
                "s3_key": s3_key,
                "error": str(exc),
            },
        )
        raise self.retry(exc=exc)

    logger.info(
        "Backtest task completed and saved to S3",
        extra={
            "event": "backtest_task_complete",
            "task_id": task_id,
            "strategy_id": strategy_id,
            "s3_key": s3_key,
            "total_pnl": result.total_pnl,
            "total_trades": result.total_trades,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return {
        "task_id": task_id,
        "strategy_id": strategy_id,
        "s3_key": s3_key,
        "status": "completed",
        "run_at_utc": run_at.isoformat(),
        "total_pnl": result.total_pnl,
        "total_trades": result.total_trades,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class StrategyNotFoundError(Exception):
    """Raised when a strategy does not exist or does not belong to the user."""


async def _load_strategy(strategy_id: int, user_id: int):
    """
    Load a Strategy from PostgreSQL and verify ownership.
    Raises StrategyNotFoundError if not found or not owned by user_id.
    """
    from sqlalchemy import select
    from app.core.database import AsyncSessionLocal
    from app.models.strategy import Strategy

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Strategy).where(
                Strategy.id == strategy_id,
                Strategy.user_id == user_id,
            )
        )
        strategy = result.scalar_one_or_none()

    if strategy is None:
        raise StrategyNotFoundError(
            f"Strategy {strategy_id} not found for user {user_id}"
        )
    return strategy


def _upload_to_s3(key: str, body: str) -> None:
    """
    Upload a JSON string to S3.

    Raises:
        BotoCoreError / ClientError on AWS failure.
    """
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    s3.put_object(
        Bucket=settings.S3_BUCKET_BACKTEST,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.debug(
        "S3 upload complete",
        extra={
            "event": "s3_upload_complete",
            "bucket": settings.S3_BUCKET_BACKTEST,
            "key": key,
        },
    )
