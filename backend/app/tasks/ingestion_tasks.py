"""
app/tasks/ingestion_tasks.py

Celery tasks for data ingestion.

Phase 1: Option chain ingestor task.
The ingestor runs as a long-lived Celery task — it starts the WebSocket
connection and keeps it alive via WebSocketManager's reconnect logic.
"""

import asyncio
from worker.worker import celery_app
from app.core.logging import get_logger

logger = get_logger(__name__)


@celery_app.task(
    name="ingestion.start_option_chain_ingestor",
    bind=True,
    max_retries=None,   # Never give up — WebSocketManager handles reconnects
)
def start_option_chain_ingestor(self, instrument_tokens: list[int] = None):
    """
    Start the option chain ingestor as a long-running Celery task.

    This task starts the WebSocket connection and blocks indefinitely.
    WebSocketManager handles all reconnects internally.

    Args:
        instrument_tokens: Optional list of NSE instrument tokens to subscribe.
                           Defaults to Nifty + BankNifty index tokens.

    To start from CLI:
        celery -A worker.worker call ingestion.start_option_chain_ingestor
    """
    from app.data_ingestion.option_chain_ingestor import OptionChainIngestor

    logger.info(
        "Starting option chain ingestor task",
        extra={"tokens": instrument_tokens},
    )

    ingestor = OptionChainIngestor(instrument_tokens=instrument_tokens)

    try:
        asyncio.run(ingestor.start())
    except Exception as e:
        logger.error(
            "Option chain ingestor task crashed",
            extra={"error": str(e)},
        )
        raise

