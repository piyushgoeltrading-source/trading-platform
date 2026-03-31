"""
app/brokers/nuvama/auth.py

Nuvama OAuth — PiyushTrade
===========================
Handles the Nuvama APIConnect OAuth flow (NOT TOTP — confirmed from docs).

Auth flow:
  Step 1  User is redirected to Nuvama login URL (generated here).
  Step 2  Nuvama redirects back with ?requestId=... in the URL.
  Step 3  Caller passes requestId here → we instantiate APIConnect SDK,
          call GetLoginData(), extract the session token, store in Redis.

SDK instantiation (from Nuvama docs):
    api_connect = APIConnect(
        apiKey,
        api_secret,
        requestId,
        downloadContract=True,
        'settings.ini',
    )
    login_data = api_connect.GetLoginData()
    # login_data contains the session token used for all subsequent calls.

Token lifecycle:
  - Valid for 1 day (Nuvama rotates daily). Stored in Redis with 24-hour TTL.
  - Key: nuvama:access_token:<user_id>
  - Key: nuvama:sdk_instance is NOT stored — SDK instances are rebuilt per
    request in client.py using the stored token (SDK is not pickleable).

Rate limit awareness (from handoff):
  - 2,000 orders/day, 10/sec, 3,000 requests/5 min per IP.
  - Auth calls count toward the 86,400 req/day total.
  - No throttle here — enforced at execution engine layer.

Rules:
  - No order logic here. Auth only.
  - No database writes. Redis only.
  - api_key and api_secret from environment only — never hardcoded.
  - get_structured_logger is the only logging import.
"""

from __future__ import annotations

import os
from typing import Optional

import redis

from app.brokers.base_broker import BrokerAuthError, BrokerNetworkError
from app.core.config import settings
from app.core.logging import get_structured_logger

logger = get_structured_logger(__name__)

# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------

_TOKEN_KEY_PREFIX = "nuvama:access_token:"
_TOKEN_TTL_SECONDS = 86_400  # 24-hour ceiling — Nuvama rotates daily


def _token_key(user_id: int) -> str:
    return f"{_TOKEN_KEY_PREFIX}{user_id}"


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    key = os.getenv("NUVAMA_API_KEY", "")
    if not key:
        raise BrokerAuthError(
            "NUVAMA_API_KEY is not set in environment.",
            broker="nuvama",
        )
    return key


def _get_api_secret() -> str:
    secret = os.getenv("NUVAMA_API_SECRET", "")
    if not secret:
        raise BrokerAuthError(
            "NUVAMA_API_SECRET is not set in environment.",
            broker="nuvama",
        )
    return secret


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def get_login_url() -> str:
    """
    Return the Nuvama OAuth login URL.

    The user must be redirected to this URL to initiate login.
    After authentication Nuvama redirects to the registered redirect URL
    with ?requestId=<id> in the query string.

    Returns:
        Nuvama login URL string.

    Raises:
        BrokerAuthError: NUVAMA_API_KEY not configured.
    """
    api_key = _get_api_key()
    url = f"https://www.nuvamawealth.com/api-connect/login?api_key={api_key}"
    logger.info(
        "nuvama_login_url_generated",
        extra={"event": "nuvama_login_url_generated"},
    )
    return url


def exchange_request_id(user_id: int, request_id: str) -> str:
    """
    Exchange a Nuvama requestId for a session token and store it in Redis.

    Called from the OAuth callback endpoint (/api/v1/broker/nuvama/callback).

    Args:
        user_id:    PiyushTrade user ID — scopes the Redis key.
        request_id: The ?requestId= value from Nuvama's OAuth redirect.

    Returns:
        The session token string (also stored in Redis).

    Raises:
        BrokerAuthError:    Login data fetch failed (invalid/expired requestId).
        BrokerNetworkError: Network failure reaching Nuvama servers.
    """
    try:
        from APIConnect import APIConnect  # deferred — optional SDK
    except ImportError as exc:
        raise BrokerAuthError(
            "APIConnect SDK not installed. Run: pip install APIConnect==2.0.0",
            broker="nuvama",
        ) from exc

    api_key = _get_api_key()
    api_secret = _get_api_secret()

    try:
        # Instantiate SDK with requestId — this triggers contract download
        # and session initialisation per Nuvama docs.
        api_connect = APIConnect(
            api_key,
            api_secret,
            request_id,
            downloadContract=True,
            ini_file="settings.ini",
        )
        login_data = api_connect.GetLoginData()
    except Exception as exc:
        error_str = str(exc)
        logger.error(
            "nuvama_token_exchange_failed",
            extra={
                "event": "nuvama_token_exchange_failed",
                "user_id": user_id,
                "error": error_str,
            },
        )
        if "network" in error_str.lower() or "timeout" in error_str.lower() or "connection" in error_str.lower():
            raise BrokerNetworkError(
                f"Network error during Nuvama token exchange: {error_str}",
                broker="nuvama",
            ) from exc
        raise BrokerAuthError(
            f"Nuvama requestId exchange failed: {error_str}",
            broker="nuvama",
        ) from exc

    # Extract session token from login_data.
    # Nuvama returns a dict-like object; token key confirmed as "sessionToken"
    # from SDK docs. Fall back to string cast if SDK returns the token directly.
    if isinstance(login_data, dict):
        session_token: str = str(login_data.get("sessionToken", login_data.get("token", "")))
    else:
        session_token = str(login_data)

    if not session_token:
        raise BrokerAuthError(
            "Nuvama GetLoginData() returned an empty session token.",
            broker="nuvama",
        )

    # Store token in Redis with 24-hour TTL
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_client.setex(
        name=_token_key(user_id),
        time=_TOKEN_TTL_SECONDS,
        value=session_token,
    )

    logger.info(
        "nuvama_access_token_stored",
        extra={
            "event": "nuvama_access_token_stored",
            "user_id": user_id,
            "ttl_seconds": _TOKEN_TTL_SECONDS,
        },
    )
    return session_token


def get_access_token(user_id: int) -> str:
    """
    Retrieve the stored Nuvama session token for a user from Redis.

    Args:
        user_id: PiyushTrade user ID.

    Returns:
        Session token string.

    Raises:
        BrokerAuthError: Token not found in Redis (not logged in, or expired).
    """
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    token: Optional[str] = redis_client.get(_token_key(user_id))

    if not token:
        logger.warning(
            "nuvama_access_token_missing",
            extra={
                "event": "nuvama_access_token_missing",
                "user_id": user_id,
            },
        )
        raise BrokerAuthError(
            f"No Nuvama session token found for user {user_id}. "
            "User must complete OAuth login first.",
            broker="nuvama",
        )

    return token


def revoke_access_token(user_id: int) -> None:
    """
    Delete the stored session token from Redis (logout).

    Args:
        user_id: PiyushTrade user ID.
    """
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_client.delete(_token_key(user_id))
    logger.info(
        "nuvama_access_token_revoked",
        extra={"event": "nuvama_access_token_revoked", "user_id": user_id},
    )
