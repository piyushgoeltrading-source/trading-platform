"""
app/api/options.py

Options API Router — PiyushTrade
===================================
Endpoints:
  GET /options/chain/{instrument_token}

Error handling:
  All errors returned as standard PiyushTrade JSON envelope:
  {
      "error_code": "...",
      "message": "...",
      "details": { ... }
  }

HTTP status codes:
  200 — data returned (market open + fresh, or market closed)
  404 — instrument not found in cache
  500 — corrupt payload
  503 — FEED_DEGRADED (market open + stale) or REDIS_UNAVAILABLE
"""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from options_service import (
    ErrorCode,
    FeedDegradedError,
    InvalidPayloadError,
    RedisUnavailableError,
    build_error_response,
    get_option_chain,
)
from app.core.logging import get_structured_logger
from app.core.time_utils import now_utc

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/options", tags=["options"])


# ---------------------------------------------------------------------------
# Dependency: Redis client
# ---------------------------------------------------------------------------

async def get_redis(request: Request):
    """Extract the shared async Redis client from app state."""
    return request.app.state.redis


# ---------------------------------------------------------------------------
# GET /options/chain/{instrument_token}
# ---------------------------------------------------------------------------

@router.get(
    "/chain/{instrument_token}",
    summary="Get live option chain data for an instrument",
    responses={
        200: {"description": "Option chain data returned"},
        404: {"description": "Instrument not found in cache"},
        500: {"description": "Corrupt payload in cache"},
        503: {"description": "Feed degraded or Redis unavailable"},
    },
)
async def get_option_chain_endpoint(
    instrument_token: int,
    redis=Depends(get_redis),
):
    """
    Fetch current option chain data for the given instrument token.

    Behavior:
    - Market OPEN + fresh data  → 200 with data
    - Market OPEN + stale data  → 503 FEED_DEGRADED
    - Market CLOSED             → 200 with data + X-Data-As-Of header
    - Instrument not in cache   → 404
    - Redis unavailable         → 503 REDIS_UNAVAILABLE
    - Corrupt data in cache     → 500 INVALID_PAYLOAD
    """
    try:
        data, extra_headers = await get_option_chain(
            redis_client=redis,
            instrument_token=instrument_token,
        )

        # Build response, attaching optional X-Data-As-Of header
        response = JSONResponse(content=data, status_code=200)
        if extra_headers:
            for key, value in extra_headers.items():
                response.headers[key] = value
        return response

    except FeedDegradedError as exc:
        logger.warning(
            "Returning 503 FEED_DEGRADED to client",
            extra={
                "event": "api_feed_degraded",
                "instrument_token": instrument_token,
                "data_age_seconds": exc.age_seconds,
                "timestamp_utc": now_utc().isoformat(),
            },
        )
        return build_error_response(
            error_code=ErrorCode.FEED_DEGRADED,
            message="Market data is stale. Feed may be degraded.",
            details={
                "data_age_seconds": round(exc.age_seconds, 2),
                "staleness_threshold_seconds": exc.threshold,
            },
            status_code=503,
        )

    except RedisUnavailableError as exc:
        logger.error(
            "Returning 503 REDIS_UNAVAILABLE to client",
            extra={
                "event": "api_redis_unavailable",
                "instrument_token": instrument_token,
                "reason": exc.reason,
                "timestamp_utc": now_utc().isoformat(),
            },
        )
        return build_error_response(
            error_code=ErrorCode.REDIS_UNAVAILABLE,
            message="Cache layer is unavailable. Please retry shortly.",
            details={"reason": exc.reason},
            status_code=503,
        )

    except InvalidPayloadError as exc:
        logger.error(
            "Returning 500 INVALID_PAYLOAD to client",
            extra={
                "event": "api_invalid_payload",
                "instrument_token": instrument_token,
                "field": exc.field,
                "reason": exc.reason,
                "timestamp_utc": now_utc().isoformat(),
            },
        )
        return build_error_response(
            error_code=ErrorCode.INVALID_PAYLOAD,
            message="Stored market data is malformed.",
            details={"field": exc.field, "reason": exc.reason},
            status_code=500,
        )
