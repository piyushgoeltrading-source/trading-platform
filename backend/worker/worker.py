"""
worker/worker.py

Celery application for PiyushTrade.
Handles all async jobs: backtesting, data ingestion.

RULE: Never trust Celery task state to determine if an order was placed.
      Order status lives in PostgreSQL only. Celery is for async execution,
      not as a source of truth.

To start the worker (from backend/ directory):
    celery -A worker.worker worker --loglevel=info

To start with concurrency (production):
    celery -A worker.worker worker --loglevel=info --concurrency=4

To monitor tasks:
    celery -A worker.worker flower
"""

from celery import Celery
from app.core.config import settings
from app.core.logging import configure_logging, get_logger

# Configure structured logging for the worker process
configure_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Celery app
# ---------------------------------------------------------------------------
celery_app = Celery(
    "piyushtrade",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    # Serialisation
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",

    # Timezone
    timezone="Asia/Kolkata",
    enable_utc=True,

    # Task behaviour
    task_track_started=True,        # Task moves to STARTED state when picked up
    task_acks_late=True,            # Ack after task completes, not on pickup
                                    # Prevents silent loss if worker crashes mid-task
    worker_prefetch_multiplier=1,   # One task at a time per worker — safe for
                                    # financial operations

    # Result TTL — keep results for 1 hour (for debugging only; order truth is in DB)
    result_expires=3600,

    # Retry policy defaults — individual tasks can override
    task_default_retry_delay=10,    # seconds
    task_max_retries=3,

    # Routing — Phase 2 will add dedicated queues for backtest vs ingestion
    task_default_queue="default",
)

# ---------------------------------------------------------------------------
# Autodiscover tasks
# Celery will find all @celery_app.task decorated functions in these modules.
# Add new task modules here as phases are built.
# ---------------------------------------------------------------------------
celery_app.autodiscover_tasks([
    "app.tasks.backtest_tasks",     # Phase 2
    "app.tasks.ingestion_tasks",    # Phase 1
])

logger.info(
    "Celery worker initialised",
    extra={
        "broker": settings.CELERY_BROKER_URL,
        "backend": settings.CELERY_RESULT_BACKEND,
    },
)


# ---------------------------------------------------------------------------
# Health check task — use this to verify the worker is alive
# celery -A worker.worker inspect ping
# ---------------------------------------------------------------------------
@celery_app.task(name="worker.ping")
def ping() -> str:
    """Simple liveness task. Returns 'pong'."""
    return "pong"
