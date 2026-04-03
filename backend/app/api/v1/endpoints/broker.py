# app/api/v1/endpoints/broker.py
"""
app/api/v1/endpoints/broker.py

Broker Auth API — PiyushTrade Phase 3, Step 10
===============================================
Endpoints:
  GET  /broker/auth/zerodha/init       — return Zerodha OAuth redirect URL
  GET  /broker/auth/zerodha/callback   — exchange request token for access token
  POST /broker/auth/nuvama/login       — initiate Nuvama OAuth requestId flow
  GET  /broker/status                  — check if user has a valid broker token

Rules:
  - All endpoints require JWT authentication.
  - Tokens stored in Redis only — never PostgreSQL.
  - Redis keys: zerodha:access_token:<user_id> | nuvama:access_token:<user_id>
  - TTL: 24h on all broker tokens (daily rotation required).
  - Nuvama uses OAuth requestId flow — NOT TOTP.
  - Standard PiyushTrade error envelope on all errors.
"""

import asyncio
from typing import Optional

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from app.core.config import settings
from app.core.logging import get_structured_logger
from app.core.security import get_current_user
from app.core.time_utils import now_utc
from app.models.user import User

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/broker", tags=["broker"])

# Token TTL — 24 hours (brokers require daily re-auth)
_TOKEN_TTL_SECONDS: int = 86_400

# Redis key patterns — must match auth.py in each broker package
_ZERODHA_TOKEN_KEY = "zerodha:access_token:{user_id}"
_NUVAMA_TOKEN_KEY = "nuvama:access_token:{user_id}"


# ---------------------------------------------------------------------------
# Redis helper
# ---------------------------------------------------------------------------

async def _get_redis() -> aioredis.Redis:
    return aioredis.from_url(settings.REDIS_URL, decode_responses=True)


async def _token_exists(key: str) -> bool:
    redis = await _get_redis()
    try:
        val = await redis.get(key)
        return val is not None
    finally:
        await redis.aclose()


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class NuvamLoginRequest(BaseModel):
    request_id: str


class BrokerStatusResponse(BaseModel):
    broker: Optional[str]
    token_present: bool
    redis_key: Optional[str]


class ZerodhaInitResponse(BaseModel):
    redirect_url: str


class ZerodhaCallbackResponse(BaseModel):
    message: str
    user_id: int


class NuvamLoginResponse(BaseModel):
    message: str
    user_id: int


# ---------------------------------------------------------------------------
# GET /broker/auth/zerodha/init
# ---------------------------------------------------------------------------

@router.get(
    "/auth/zerodha/init",
    response_model=ZerodhaInitResponse,
    summary="Get Zerodha OAuth redirect URL",
)
async def zerodha_init(
    current_user: User = Depends(get_current_user),
) -> ZerodhaInitResponse:
    """
    Return the Kite Connect OAuth login URL.

    The frontend redirects the user to this URL. After login Zerodha
    redirects back to /broker/auth/zerodha/callback with ?request_token=...
    """
    from app.brokers.zerodha.auth import get_login_url

    try:
        url = await asyncio.to_thread(get_login_url)
    except Exception as exc:
        logger.error(
            "zerodha_init_failed",
            extra={
                "event": "zerodha_init_failed",
                "user_id": current_user.id,
                "error": str(exc),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error_code": "ZERODHA_INIT_FAILED",
                "message": str(exc),
                "details": {},
            },
        )

    logger.info(
        "zerodha_init",
        extra={
            "event": "zerodha_init",
            "user_id": current_user.id,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return ZerodhaInitResponse(redirect_url=url)


# ---------------------------------------------------------------------------
# GET /broker/auth/zerodha/callback
# ---------------------------------------------------------------------------

@router.get(
    "/auth/zerodha/callback",
    response_model=ZerodhaCallbackResponse,
    summary="Exchange Zerodha request token for access token",
)
async def zerodha_callback(
    request_token: str = Query(..., description="Token from Zerodha OAuth redirect"),
    current_user: User = Depends(get_current_user),
) -> ZerodhaCallbackResponse:
    """
    Receive the request_token from Zerodha's OAuth redirect.
    Exchange it for an access token and store in Redis with 24h TTL.

    Zerodha redirects here after successful login:
      /broker/auth/zerodha/callback?request_token=<token>&action=login&status=success
    """
    from app.brokers.zerodha.auth import exchange_request_token

    try:
        await asyncio.to_thread(
            exchange_request_token,
            current_user.id,
            request_token,
        )
    except Exception as exc:
        logger.error(
            "zerodha_callback_failed",
            extra={
                "event": "zerodha_callback_failed",
                "user_id": current_user.id,
                "error": str(exc),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error_code": "ZERODHA_CALLBACK_FAILED",
                "message": str(exc),
                "details": {},
            },
        )

    logger.info(
        "zerodha_token_stored",
        extra={
            "event": "zerodha_token_stored",
            "user_id": current_user.id,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return ZerodhaCallbackResponse(
        message="Zerodha authentication successful. Token valid for 24 hours.",
        user_id=current_user.id,
    )


# ---------------------------------------------------------------------------
# POST /broker/auth/nuvama/login
# ---------------------------------------------------------------------------

@router.post(
    "/auth/nuvama/login",
    response_model=NuvamLoginResponse,
    summary="Initiate Nuvama OAuth requestId login flow",
)
async def nuvama_login(
    payload: NuvamLoginRequest,
    current_user: User = Depends(get_current_user),
) -> NuvamLoginResponse:
    """
    Complete Nuvama authentication using the requestId OAuth flow.

    Flow:
      1. Frontend redirects user to Nuvama login page (URL from config).
      2. After login, Nuvama redirects back with ?requestId=... in URL.
      3. Frontend POSTs that requestId to this endpoint.
      4. This endpoint calls APIConnect SDK to retrieve the session token.
      5. Token stored in Redis with 24h TTL.

    This is OAuth requestId flow — NOT TOTP.
    """
    from app.brokers.nuvama.auth import exchange_request_id

    try:
        await asyncio.to_thread(
            exchange_request_id,
            current_user.id,
            payload.request_id,
        )
    except Exception as exc:
        logger.error(
            "nuvama_login_failed",
            extra={
                "event": "nuvama_login_failed",
                "user_id": current_user.id,
                "error": str(exc),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "error_code": "NUVAMA_LOGIN_FAILED",
                "message": str(exc),
                "details": {},
            },
        )

    logger.info(
        "nuvama_token_stored",
        extra={
            "event": "nuvama_token_stored",
            "user_id": current_user.id,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return NuvamLoginResponse(
        message="Nuvama authentication successful. Token valid for 24 hours.",
        user_id=current_user.id,
    )


# ---------------------------------------------------------------------------
# GET /broker/status
# ---------------------------------------------------------------------------

@router.get(
    "/status",
    response_model=BrokerStatusResponse,
    summary="Check if user has a valid broker token in Redis",
)
async def broker_status(
    current_user: User = Depends(get_current_user),
) -> BrokerStatusResponse:
    """
    Check whether the authenticated user has a live broker token in Redis.

    Does not validate the token with the broker — only checks Redis presence.
    A missing token means the user must re-authenticate before placing orders.
    """
    broker_name = str(current_user.broker) if current_user.broker else None

    if broker_name is None:
        return BrokerStatusResponse(
            broker=None,
            token_present=False,
            redis_key=None,
        )

    # Determine which Redis key to check based on user's broker
    broker_value = current_user.broker.value if hasattr(current_user.broker, "value") else broker_name

    if "zerodha" in broker_value.lower():
        redis_key = _ZERODHA_TOKEN_KEY.format(user_id=current_user.id)
    elif "nuvama" in broker_value.lower():
        redis_key = _NUVAMA_TOKEN_KEY.format(user_id=current_user.id)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error_code": "UNKNOWN_BROKER",
                "message": f"Unknown broker: {broker_value}",
                "details": {"broker": broker_value},
            },
        )

    token_present = await _token_exists(redis_key)

    logger.info(
        "broker_status_check",
        extra={
            "event": "broker_status_check",
            "user_id": current_user.id,
            "broker": broker_value,
            "token_present": token_present,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return BrokerStatusResponse(
        broker=broker_value,
        token_present=token_present,
        redis_key=redis_key,
    )