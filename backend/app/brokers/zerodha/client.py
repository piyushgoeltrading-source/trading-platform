"""
app/brokers/zerodha/client.py

ZerodhaBroker — Kite Connect V3 implementation
================================================
Implements all 9 BaseBroker abstract methods using the kiteconnect SDK.

Architecture:
  - auth.py    manages OAuth and Redis token storage.
  - mapper.py  converts between Kite-native format and PiyushTrade DTOs.
  - This file:  calls auth.get_access_token(), builds KiteConnect instance,
                delegates to SDK, maps response via mapper.

Rules enforced here:
  - Zero backtest logic. Zero live-order logic in the base or backtest engine.
  - All methods are synchronous — kiteconnect SDK is sync.
  - _get_kite() is the single point where KiteConnect is instantiated.
    It always fetches a fresh token from Redis — no in-memory token caching
    (tokens are short-lived and Redis reads are sub-millisecond on ElastiCache).
  - Kite SDK exceptions are caught and re-raised as BrokerError subclasses
    so the execution engine never sees kiteconnect-specific types.
  - Rate limit: Zerodha allows ~3 req/sec on data APIs. Order APIs are
    higher. No internal throttle here — Celery rate-limiting handles it.
  - get_structured_logger is the only logging import.

Broker-specific exception mapping:
  TokenException / InvalidTokenException → BrokerAuthError
  OrderException / InputException        → BrokerOrderError
  NetworkException / DataException       → BrokerNetworkError
  All other KiteException                → BrokerError (base)
"""

from __future__ import annotations

import os
from typing import Any

from app.brokers.base_broker import (
    BaseBroker,
    BrokerAuthError,
    BrokerError,
    BrokerNetworkError,
    BrokerOrderError,
    BrokerRateLimitError,
    Exchange,
    Holding,
    LTPResult,
    Margin,
    OrderDetail,
    OrderRequest,
    OrderResult,
    Position,
)
from app.brokers.zerodha import auth as zerodha_auth
from app.brokers.zerodha import mapper
from app.core.logging import get_structured_logger

logger = get_structured_logger(__name__)


class ZerodhaBroker(BaseBroker):
    """
    Kite Connect V3 broker implementation.

    Args:
        user_id: PiyushTrade user ID. Scopes Redis token key and audit logs.
    """

    def __init__(self, user_id: int) -> None:
        self.user_id = user_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_kite(self):
        """
        Build and return a KiteConnect instance authenticated with the
        current access token for this user.

        Fetches the token from Redis on every call — no in-process caching.

        Returns:
            Authenticated KiteConnect instance.

        Raises:
            BrokerAuthError: Token not in Redis (user must re-authenticate).
        """
        try:
            from kiteconnect import KiteConnect  # deferred — optional SDK
        except ImportError as exc:
            raise BrokerAuthError(
                "kiteconnect SDK not installed. Run: pip install kiteconnect",
                broker="zerodha",
            ) from exc

        api_key = os.getenv("ZERODHA_API_KEY", "")
        if not api_key:
            raise BrokerAuthError(
                "ZERODHA_API_KEY is not set in environment.",
                broker="zerodha",
            )

        access_token = zerodha_auth.get_access_token(self.user_id)  # raises BrokerAuthError if missing
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        return kite

    def _handle_kite_exception(self, exc: Exception, context: str) -> None:
        """
        Map kiteconnect SDK exceptions to PiyushTrade BrokerError subclasses.

        Args:
            exc:     The exception caught from the Kite SDK.
            context: Short string describing the operation (for log events).

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError, BrokerRateLimitError,
            or BrokerError depending on the exception type.
        """
        exc_type = type(exc).__name__
        exc_msg = str(exc)

        logger.error(
            f"zerodha_{context}_error",
            extra={
                "event": f"zerodha_{context}_error",
                "user_id": self.user_id,
                "exc_type": exc_type,
                "error": exc_msg,
            },
        )

        # kiteconnect exception class names (SDK doesn't export a clean hierarchy)
        if exc_type in ("TokenException", "InvalidTokenException"):
            raise BrokerAuthError(exc_msg, broker="zerodha") from exc

        if exc_type in ("OrderException", "InputException", "PermissionException"):
            raise BrokerOrderError(exc_msg, broker="zerodha") from exc

        if exc_type in ("NetworkException", "DataException", "GeneralException"):
            raise BrokerNetworkError(exc_msg, broker="zerodha") from exc

        # Kite returns HTTP 429 as an exception with "Too Many Requests" in message
        if "too many requests" in exc_msg.lower() or "429" in exc_msg:
            raise BrokerRateLimitError(exc_msg, broker="zerodha") from exc

        raise BrokerError(exc_msg, broker="zerodha") from exc

    # ------------------------------------------------------------------
    # Order lifecycle
    # ------------------------------------------------------------------

    def place_order(self, order_request: OrderRequest) -> OrderResult:
        """
        Place a new order via Kite Connect.

        Args:
            order_request: PiyushTrade standard OrderRequest.

        Returns:
            OrderResult with broker_order_id and PENDING status.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        kite = self._get_kite()
        params = mapper.to_kite_order(order_request)

        logger.info(
            "zerodha_place_order",
            extra={
                "event": "zerodha_place_order",
                "user_id": self.user_id,
                "symbol": order_request.trading_symbol,
                "side": order_request.side.value,
                "qty": order_request.quantity,
                "order_type": order_request.order_type.value,
            },
        )

        try:
            response: Any = kite.place_order(**params)
            # Kite returns {"order_id": "..."} — extract the ID string
            broker_order_id: str = str(response) if isinstance(response, str) else str(response.get("order_id", response))
        except Exception as exc:
            self._handle_kite_exception(exc, "place_order")

        logger.info(
            "zerodha_place_order_success",
            extra={
                "event": "zerodha_place_order_success",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
            },
        )
        return mapper.from_kite_place_response(broker_order_id)

    def modify_order(
        self,
        broker_order_id: str,
        order_request: OrderRequest,
    ) -> OrderResult:
        """
        Modify an open order via Kite Connect.

        Args:
            broker_order_id: Kite order ID from place_order().
            order_request:   Updated order parameters.

        Returns:
            OrderResult with PENDING status.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        kite = self._get_kite()
        params = mapper.to_kite_modify(order_request)

        logger.info(
            "zerodha_modify_order",
            extra={
                "event": "zerodha_modify_order",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
            },
        )

        try:
            response: Any = kite.modify_order(
                variety="regular",
                order_id=broker_order_id,
                **params,
            )
            returned_id = str(response) if isinstance(response, str) else str(response.get("order_id", broker_order_id))
        except Exception as exc:
            self._handle_kite_exception(exc, "modify_order")

        return mapper.from_kite_modify_response(returned_id)

    def cancel_order(self, broker_order_id: str) -> OrderResult:
        """
        Cancel an open or pending order via Kite Connect.

        Args:
            broker_order_id: Kite order ID from place_order().

        Returns:
            OrderResult with CANCELLED status.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        kite = self._get_kite()

        logger.info(
            "zerodha_cancel_order",
            extra={
                "event": "zerodha_cancel_order",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
            },
        )

        try:
            kite.cancel_order(variety="regular", order_id=broker_order_id)
        except Exception as exc:
            self._handle_kite_exception(exc, "cancel_order")

        return mapper.from_kite_cancel_response(broker_order_id)

    # ------------------------------------------------------------------
    # Order inspection
    # ------------------------------------------------------------------

    def get_order_status(self, broker_order_id: str) -> OrderDetail:
        """
        Fetch current status of a single order.

        Kite does not have a single-order fetch endpoint — we call orders()
        (all orders for the day) and filter by order_id. The full list is
        typically < 200 items intraday so this is acceptable for V1.

        Args:
            broker_order_id: Kite order ID.

        Returns:
            OrderDetail for the requested order.

        Raises:
            BrokerAuthError, BrokerNetworkError, BrokerOrderError (not found)
        """
        kite = self._get_kite()

        try:
            orders: list[dict] = kite.orders()
        except Exception as exc:
            self._handle_kite_exception(exc, "get_order_status")

        for raw_order in orders:
            if str(raw_order.get("order_id", "")) == broker_order_id:
                return mapper.from_kite_order_dict(raw_order)

        raise BrokerOrderError(
            f"Order {broker_order_id} not found in today's order book.",
            broker="zerodha",
        )

    def get_order_history(self, broker_order_id: str) -> list[OrderDetail]:
        """
        Fetch the full audit trail of state changes for a single order.

        Kite's order_history() endpoint returns a list of status snapshots
        for a given order_id, oldest first.

        Args:
            broker_order_id: Kite order ID.

        Returns:
            List of OrderDetail snapshots, oldest first.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        kite = self._get_kite()

        try:
            history: list[dict] = kite.order_history(order_id=broker_order_id)
        except Exception as exc:
            self._handle_kite_exception(exc, "get_order_history")

        return [mapper.from_kite_order_dict(raw) for raw in history]

    # ------------------------------------------------------------------
    # Portfolio
    # ------------------------------------------------------------------

    def get_positions(self) -> list[Position]:
        """
        Fetch all open positions (net view).

        Returns:
            List of Position objects with non-zero quantity. Empty list if flat.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        kite = self._get_kite()

        try:
            raw: dict = kite.positions()
        except Exception as exc:
            self._handle_kite_exception(exc, "get_positions")

        positions = mapper.from_kite_positions(raw)
        logger.info(
            "zerodha_positions_fetched",
            extra={
                "event": "zerodha_positions_fetched",
                "user_id": self.user_id,
                "count": len(positions),
            },
        )
        return positions

    def get_holdings(self) -> list[Holding]:
        """
        Fetch CNC delivery holdings.

        Returns:
            List of Holding objects. Empty list if no holdings.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        kite = self._get_kite()

        try:
            raw: list[dict] = kite.holdings()
        except Exception as exc:
            self._handle_kite_exception(exc, "get_holdings")

        return mapper.from_kite_holdings(raw)

    def get_margins(self) -> Margin:
        """
        Fetch account margin / funds summary.

        Returns:
            Margin object with available cash and used margin.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        kite = self._get_kite()

        try:
            raw: dict = kite.margins()
        except Exception as exc:
            self._handle_kite_exception(exc, "get_margins")

        return mapper.from_kite_margins(raw)

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_ltp(self, trading_symbol: str, exchange: Exchange) -> LTPResult:
        """
        Fetch the last traded price for a symbol.

        Kite ltp() accepts instruments as "EXCHANGE:SYMBOL" strings.

        Args:
            trading_symbol: NSE/BSE trading symbol (e.g. "NIFTY24JAN21000CE").
            exchange:       Exchange enum.

        Returns:
            LTPResult with ltp field populated.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        kite = self._get_kite()
        kite_exchange = mapper._EXCHANGE_TO_KITE[exchange]
        instrument = f"{kite_exchange}:{trading_symbol}"

        try:
            raw: dict = kite.ltp([instrument])
        except Exception as exc:
            self._handle_kite_exception(exc, "get_ltp")

        return mapper.from_kite_ltp(trading_symbol, exchange, raw)
