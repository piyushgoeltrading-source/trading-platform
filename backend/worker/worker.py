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
from celery.schedules import crontab
from kombu import Exchange, Queue

from app.core.config import settings
from app.core.logging import configure_root_logging, get_structured_logger

configure_root_logging()
logger = get_structured_logger(__name__)

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

    # Routing — explicit queues for operational clarity
    task_default_queue="default",
    task_default_exchange="piyushtrade",
    task_default_exchange_type="direct",
    task_default_routing_key="default",

    # Do not let Celery interfere with our structured root logging
    worker_hijack_root_logger=False,

    # Emit task-sent events so monitoring tools can see queue lifecycle more clearly
    task_send_sent_event=True,

    # Connection resilience
    broker_connection_retry_on_startup=True,
)

# ---------------------------------------------------------------------------
# Explicit queues
# Separate workload types so they can be scaled independently later.
# ---------------------------------------------------------------------------
celery_app.conf.task_queues = (
    Queue("default", Exchange("piyushtrade", type="direct"), routing_key="default"),
    Queue("backtest", Exchange("piyushtrade", type="direct"), routing_key="backtest"),
    Queue("ingestion", Exchange("piyushtrade", type="direct"), routing_key="ingestion"),
    Queue("reconciliation", Exchange("piyushtrade", type="direct"), routing_key="reconciliation"),
)

# ---------------------------------------------------------------------------
# Task routes
# Keep heavy or latency-insensitive work off the default queue.
# ---------------------------------------------------------------------------
celery_app.conf.task_routes = {
    "tasks.run_backtest": {"queue": "backtest", "routing_key": "backtest"},
    "tasks.option_chain_ingest": {"queue": "ingestion", "routing_key": "ingestion"},
    "tasks.reconcile_positions": {"queue": "reconciliation", "routing_key": "reconciliation"},
    "worker.ping": {"queue": "default", "routing_key": "default"},
}

# ---------------------------------------------------------------------------
# Autodiscover tasks
# Celery will find all @celery_app.task decorated functions in these modules.
# Add new task modules here as phases are built.
# ---------------------------------------------------------------------------
celery_app.autodiscover_tasks([
    "app.tasks.backtest_tasks",             # Phase 2
    "app.tasks.ingestion_tasks",            # Phase 1
    "app.services.reconciliation_service",  # Phase 3 — position reconciliation
])

# ---------------------------------------------------------------------------
# Beat schedule — periodic tasks
# Run the beat scheduler alongside the worker:
#   celery -A worker.worker beat --loglevel=info
# Or combined (dev only):
#   celery -A worker.worker worker --beat --loglevel=info
# ---------------------------------------------------------------------------
celery_app.conf.beat_schedule = {
    # Reconcile DB positions vs broker every 5 minutes during market hours.
    # The task itself checks is_market_open() and exits immediately if closed.
    "reconcile-positions": {
        "task": "tasks.reconcile_positions",
        "schedule": crontab(minute="*/5"),
        "options": {"queue": "reconciliation", "routing_key": "reconciliation"},
    },
}

logger.info(
    "Celery worker initialised",
    extra={
        "broker": settings.CELERY_BROKER_URL,
        "backend": settings.CELERY_RESULT_BACKEND,
        "default_queue": celery_app.conf.task_default_queue,
        "queues": ["default", "backtest", "ingestion", "reconciliation"],
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