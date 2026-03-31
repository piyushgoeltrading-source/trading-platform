"""
app/tasks/ingestion_tasks.py

Celery tasks for data ingestion.

Phase 1: Option chain ingestor task.
The ingestor runs as a long-lived Celery task — it starts the WebSocket
connection and keeps it alive via WebSocketManager's reconnect logic.
"""

import asyncio
import redis as redis_lib

from worker.worker import celery_app
from app.core.logging import get_structured_logger  # Bug 4 fix: was get_logger (doesn't exist)
from app.core.config import settings

logger = get_structured_logger(__name__)


@celery_app.task(
    name="ingestion.start_option_chain_ingestor",
    bind=True,
    max_retries=None,   # Never give up — WebSocketManager handles reconnects
)
def start_option_chain_ingestor(self):
    """
    Start the option chain ingestor as a long-running Celery task.

    This task starts the WebSocket connection and blocks indefinitely.
    WebSocketManager handles all reconnects internally.

    To start from CLI:
        celery -A worker.worker call ingestion.start_option_chain_ingestor
    """
    from app.data_ingestion.option_chain_ingestor import OptionChainIngestor
    from app.data_ingestion.websocket_manager import WebSocketManager

    logger.info("Starting option chain ingestor task")

    # Bug 4 fix: OptionChainIngestor takes (redis_client, websocket_manager)
    # NOT instrument_tokens — build dependencies and pass them correctly
    redis_client = redis_lib.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=0,
        decode_responses=True,
    )

    # on_tick callback is wired inside OptionChainIngestor.start()
    websocket_manager = WebSocketManager(on_tick=None, heartbeat_timeout=10)

    ingestor = OptionChainIngestor(
        redis_client=redis_client,
        websocket_manager=websocket_manager,
    )

    try:
        asyncio.run(ingestor.start())
    except Exception as e:
        logger.error(
            "Option chain ingestor task crashed",
            extra={"error": str(e)},
        )
        raise
