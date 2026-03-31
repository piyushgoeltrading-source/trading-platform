"""
app/brokers/zerodha/auth.py

Zerodha Kite Connect OAuth — PiyushTrade
==========================================
Handles the two-step Kite Connect OAuth flow:
  Step 1  User is redirected to Kite login URL (generated here).
  Step 2  Kite redirects back with ?request_token=...; we exchange it
          for an access_token via generate_session() and store it in Redis.

Token lifecycle:
  - Access tokens are valid until ~06:00 IST the following day (Zerodha rotates
    them at daily settlement). We store them with a Redis TTL of 24 hours as a
    safety ceiling; in practice the rotation will invalidate them sooner.
  - Token is stored under key: zerodha:access_token:<user_id>
  - get_access_token() is called by ZerodhaBroker before every API call.
    If the key is missing the broker raises BrokerAuthError, which the
    execution engine catches and surfaces as a 401 to the client.

Security:
  - api_key and api_secret come from environment via settings — never hardcoded.
  - request_token is single-use; it is not stored.
  - access_token is stored only in Redis (volatile, never in DB).

Rules:
  - No live-order logic here. Auth only.
  - No database writes. Redis only.
  - get_structured_logger is the only logging import.
"""

from __future__ import annotations

import os
from typing import Optional

import redis

from app.core.config import settings
from app.core.logging import get_structured_logger
from app.brokers.base_broker import BrokerAuthError, BrokerNetworkError

logger = get_structured_logger(__name__)

# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------

_TOKEN_KEY_PREFIX = "zerodha:access_token:"
_TOKEN_TTL_SECONDS = 86_400  # 24-hour ceiling — Kite rotates at ~06:00 IST daily


def _token_key(user_id: int) -> str:
    return f"{_TOKEN_KEY_PREFIX}{user_id}"


# ---------------------------------------------------------------------------
# Settings helpers — api_key / api_secret per user
# ---------------------------------------------------------------------------
# V1 uses a single Zerodha app (one api_key) shared across users.
# In V2 these can be per-user secrets pulled from AWS Secrets Manager.

def _get_api_key() -> str:
    key = os.getenv("ZERODHA_API_KEY", "")
    if not key:
        raise BrokerAuthError(
            "ZERODHA_API_KEY is not set in environment.",
            broker="zerodha",
        )
    return key


def _get_api_secret() -> str:
    secret = os.getenv("ZERODHA_API_SECRET", "")
    if not secret:
        raise BrokerAuthError(
            "ZERODHA_API_SECRET is not set in environment.",
            broker="zerodha",
        )
    return secret


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def get_login_url() -> str:
    """
    Return the Kite Connect OAuth login URL.

    The user must be redirected to this URL to initiate login.
    After successful authentication Kite redirects to the registered
    redirect URL with ?request_token=<token>&action=login&status=success.

    Returns:
        Kite Connect login URL string.

    Raises:
        BrokerAuthError: ZERODHA_API_KEY not configured.
    """
    try:
        from kiteconnect import KiteConnect  # deferred — optional dependency
    except ImportError as exc:
        raise BrokerAuthError(
            "kiteconnect SDK not installed. Run: pip install kiteconnect",
            broker="zerodha",
        ) from exc

    kite = KiteConnect(api_key=_get_api_key())
    url = kite.login_url()
    logger.info("zerodha_login_url_generated", extra={"event": "zerodha_login_url_generated"})
    return url


def exchange_request_token(user_id: int, request_token: str) -> str:
    """
    Exchange a Kite Connect request_token for an access_token and store it in Redis.

    Called from the OAuth callback endpoint (/api/v1/broker/zerodha/callback).

    Args:
        user_id:        PiyushTrade user ID — used to scope the Redis key.
        request_token:  The ?request_token= value from Kite's OAuth redirect.

    Returns:
        The access_token string (also stored in Redis).

    Raises:
        BrokerAuthError:    token exchange failed (invalid/expired request_token).
        BrokerNetworkError: network failure reaching Kite servers.
    """
    try:
        from kiteconnect import KiteConnect
    except ImportError as exc:
        raise BrokerAuthError(
            "kiteconnect SDK not installed. Run: pip install kiteconnect",
            broker="zerodha",
        ) from exc

    kite = KiteConnect(api_key=_get_api_key())

    try:
        session_data = kite.generate_session(
            request_token=request_token,
            api_secret=_get_api_secret(),
        )
    except Exception as exc:
        error_str = str(exc)
        logger.error(
            "zerodha_token_exchange_failed",
            extra={
                "event": "zerodha_token_exchange_failed",
                "user_id": user_id,
                "error": error_str,
            },
        )
        if "network" in error_str.lower() or "timeout" in error_str.lower():
            raise BrokerNetworkError(
                f"Network error during Zerodha token exchange: {error_str}",
                broker="zerodha",
            ) from exc
        raise BrokerAuthError(
            f"Zerodha request_token exchange failed: {error_str}",
            broker="zerodha",
        ) from exc

    access_token: str = session_data["access_token"]

    # Store in Redis with 24-hour TTL ceiling
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_client.setex(
        name=_token_key(user_id),
        time=_TOKEN_TTL_SECONDS,
        value=access_token,
    )

    logger.info(
        "zerodha_access_token_stored",
        extra={
            "event": "zerodha_access_token_stored",
            "user_id": user_id,
            "ttl_seconds": _TOKEN_TTL_SECONDS,
        },
    )
    return access_token


def get_access_token(user_id: int) -> str:
    """
    Retrieve the stored Zerodha access token for a user from Redis.

    Args:
        user_id: PiyushTrade user ID.

    Returns:
        Access token string.

    Raises:
        BrokerAuthError: Token not found in Redis (not logged in, or expired).
    """
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    token: Optional[str] = redis_client.get(_token_key(user_id))

    if not token:
        logger.warning(
            "zerodha_access_token_missing",
            extra={
                "event": "zerodha_access_token_missing",
                "user_id": user_id,
            },
        )
        raise BrokerAuthError(
            f"No Zerodha access token found for user {user_id}. "
            "User must complete OAuth login first.",
            broker="zerodha",
        )

    return token


def revoke_access_token(user_id: int) -> None:
    """
    Delete the stored access token from Redis (logout).

    Args:
        user_id: PiyushTrade user ID.
    """
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_client.delete(_token_key(user_id))
    logger.info(
        "zerodha_access_token_revoked",
        extra={"event": "zerodha_access_token_revoked", "user_id": user_id},
    )
