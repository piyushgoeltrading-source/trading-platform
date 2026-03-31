"""
app/brokers/nuvama/client.py

NuvamaBroker — APIConnect SDK implementation
=============================================
Implements all 9 BaseBroker abstract methods using the Nuvama APIConnect
Python SDK (pip install APIConnect==2.0.0).

Architecture:
  - auth.py    manages OAuth requestId flow and Redis token storage.
  - mapper.py  converts between Nuvama-native format and PiyushTrade DTOs.
  - This file:  calls auth.get_access_token(), rebuilds the SDK instance per
                call, delegates to SDK methods, maps responses via mapper.

Rules enforced here:
  - Zero backtest logic. Zero cross-import with backtest_engine.
  - All methods are synchronous — APIConnect SDK is sync.
  - _get_sdk() is the single point where APIConnect is instantiated.
    It always fetches a fresh token from Redis — no in-memory caching.
  - Nuvama SDK exceptions are caught and re-raised as BrokerError subclasses
    so the execution engine never sees SDK-specific types.
  - get_structured_logger is the only logging import.

Rate limits (from handoff, Section 4) — enforced at execution engine layer,
but logged here when detected:
  - 2,000 orders/day
  - 10 orders/second
  - 3,000 requests per 5 minutes per IP
  - 86,400 total API requests/day

Nuvama SDK method mapping:
  place_order        → APIConnect.PlaceTrade(**params)
  modify_order       → APIConnect.ModifyTrade(oid, **params)
  cancel_order       → APIConnect.CancelTrade(oid)
  get_order_status   → APIConnect.OrderBook() + filter by oid
  get_order_history  → APIConnect.OrderHistory(oid)
  get_positions      → APIConnect.PositionBook()
  get_holdings       → APIConnect.Holdings()
  get_margins        → APIConnect.RMSSubLimits()
  get_ltp            → APIConnect.PositionBook() ltp field (no dedicated LTP endpoint in V1)

Exception mapping:
  "token" / "session" / "unauthorized" in message → BrokerAuthError
  "order" / "rejected" / "invalid" in message     → BrokerOrderError
  "network" / "timeout" / "connection" in message → BrokerNetworkError
  "rate" / "limit" / "429" in message             → BrokerRateLimitError
  All others                                       → BrokerError (base)
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
from app.brokers.nuvama import auth as nuvama_auth
from app.brokers.nuvama import mapper
from app.core.logging import get_structured_logger

logger = get_structured_logger(__name__)


class NuvamaBroker(BaseBroker):
    """
    Nuvama APIConnect V2.0.0 broker implementation.

    Args:
        user_id: PiyushTrade user ID. Scopes Redis token key and audit logs.
    """

    def __init__(self, user_id: int) -> None:
        self.user_id = user_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_sdk(self):
        """
        Build and return an authenticated APIConnect SDK instance for this user.

        Nuvama's SDK is not picklable and cannot be stored in Redis or as an
        instance attribute safely across Celery tasks. We rebuild it per call
        using the session token stored in Redis by auth.exchange_request_id().

        The SDK is re-initialised with downloadContract=False after the initial
        OAuth exchange (contract file already written to settings.ini by auth.py).

        Returns:
            Authenticated APIConnect instance.

        Raises:
            BrokerAuthError: Token not in Redis or SDK not installed.
        """
        try:
            from APIConnect import APIConnect  # deferred — optional SDK
        except ImportError as exc:
            raise BrokerAuthError(
                "APIConnect SDK not installed. "
                "Run: pip install APIConnect==2.0.0",
                broker="nuvama",
            ) from exc

        api_key = os.getenv("NUVAMA_API_KEY", "")
        api_secret = os.getenv("NUVAMA_API_SECRET", "")

        if not api_key:
            raise BrokerAuthError(
                "NUVAMA_API_KEY is not set in environment.",
                broker="nuvama",
            )
        if not api_secret:
            raise BrokerAuthError(
                "NUVAMA_API_SECRET is not set in environment.",
                broker="nuvama",
            )

        # Raises BrokerAuthError if token is absent from Redis
        session_token = nuvama_auth.get_access_token(self.user_id)

        # Re-instantiate SDK with the stored session token.
        # downloadContract=False — contract already downloaded during OAuth.
        # The SDK accepts session_token as the requestId parameter when
        # re-initialising an existing session.
        try:
            api_connect = APIConnect(
                api_key,
                api_secret,
                session_token,
                downloadContract=False,
                ini_file="settings.ini",
            )
        except Exception as exc:
            raise BrokerAuthError(
                f"Failed to initialise Nuvama SDK: {exc}",
                broker="nuvama",
            ) from exc

        return api_connect

    def _handle_nuvama_exception(self, exc: Exception, context: str) -> None:
        """
        Map Nuvama SDK exceptions to PiyushTrade BrokerError subclasses.

        The APIConnect SDK raises generic Exception subclasses without a clean
        hierarchy — we classify by inspecting the message string.

        Args:
            exc:     Exception caught from the Nuvama SDK.
            context: Short string describing the operation (for log events).

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError,
            BrokerRateLimitError, or BrokerError.
        """
        exc_msg = str(exc).lower()
        exc_type = type(exc).__name__

        logger.error(
            f"nuvama_{context}_error",
            extra={
                "event": f"nuvama_{context}_error",
                "user_id": self.user_id,
                "exc_type": exc_type,
                "error": str(exc),
            },
        )

        if any(kw in exc_msg for kw in ("token", "session", "unauthorized", "unauthenticated", "auth")):
            raise BrokerAuthError(str(exc), broker="nuvama") from exc

        if any(kw in exc_msg for kw in ("rate", "limit", "429", "too many")):
            raise BrokerRateLimitError(str(exc), broker="nuvama") from exc

        if any(kw in exc_msg for kw in ("network", "timeout", "connection", "unreachable", "503", "502")):
            raise BrokerNetworkError(str(exc), broker="nuvama") from exc

        if any(kw in exc_msg for kw in ("order", "rejected", "invalid", "insufficient", "margin")):
            raise BrokerOrderError(str(exc), broker="nuvama") from exc

        raise BrokerError(str(exc), broker="nuvama") from exc

    def _normalise_response(self, response: Any, context: str) -> Any:
        """
        Check a Nuvama SDK response for embedded error signals and raise
        the appropriate BrokerError if found.

        Nuvama SDK returns success responses as dicts with a "status" key.
        Error responses may still return HTTP 200 with status="error" in
        the body rather than raising an exception.

        Args:
            response: Raw SDK response.
            context:  Operation name for log events.

        Returns:
            The response unchanged if it represents success.

        Raises:
            BrokerOrderError:   status == "error" in response body.
            BrokerAuthError:    error message indicates auth failure.
        """
        if not isinstance(response, dict):
            return response

        status = str(response.get("status", "")).lower()
        if status in ("error", "failed", "failure"):
            message = response.get("message", response.get("msg", str(response)))
            msg_lower = str(message).lower()
            logger.error(
                f"nuvama_{context}_api_error",
                extra={
                    "event": f"nuvama_{context}_api_error",
                    "user_id": self.user_id,
                    "message": message,
                },
            )
            if any(kw in msg_lower for kw in ("token", "session", "auth", "unauthorized")):
                raise BrokerAuthError(str(message), broker="nuvama")
            raise BrokerOrderError(str(message), broker="nuvama")

        return response

    # ------------------------------------------------------------------
    # Order lifecycle
    # ------------------------------------------------------------------

    def place_order(self, order_request: OrderRequest) -> OrderResult:
        """
        Place a new order via Nuvama APIConnect.

        Args:
            order_request: PiyushTrade standard OrderRequest.
                           streaming_symbol MUST be populated (e.g. "4963_NSE").

        Returns:
            OrderResult with broker_order_id and PENDING status.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        sdk = self._get_sdk()
        params = mapper.to_nuvama_order(order_request)

        logger.info(
            "nuvama_place_order",
            extra={
                "event": "nuvama_place_order",
                "user_id": self.user_id,
                "symbol": order_request.trading_symbol,
                "streaming_symbol": order_request.streaming_symbol,
                "side": order_request.side.value,
                "qty": order_request.quantity,
                "order_type": order_request.order_type.value,
            },
        )

        try:
            response: Any = sdk.PlaceTrade(**params)
        except Exception as exc:
            self._handle_nuvama_exception(exc, "place_order")

        self._normalise_response(response, "place_order")

        result = mapper.from_nuvama_place_response(response)
        logger.info(
            "nuvama_place_order_success",
            extra={
                "event": "nuvama_place_order_success",
                "user_id": self.user_id,
                "broker_order_id": result.broker_order_id,
            },
        )
        return result

    def modify_order(
        self,
        broker_order_id: str,
        order_request: OrderRequest,
    ) -> OrderResult:
        """
        Modify an open order via Nuvama APIConnect.

        Args:
            broker_order_id: Nuvama oid from place_order().
            order_request:   Updated order parameters.

        Returns:
            OrderResult with PENDING status.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        sdk = self._get_sdk()
        params = mapper.to_nuvama_modify(order_request)

        logger.info(
            "nuvama_modify_order",
            extra={
                "event": "nuvama_modify_order",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
            },
        )

        try:
            response: Any = sdk.ModifyTrade(broker_order_id, **params)
        except Exception as exc:
            self._handle_nuvama_exception(exc, "modify_order")

        self._normalise_response(response, "modify_order")
        return mapper.from_nuvama_modify_response(response, broker_order_id)

    def cancel_order(self, broker_order_id: str) -> OrderResult:
        """
        Cancel an open or pending order via Nuvama APIConnect.

        Args:
            broker_order_id: Nuvama oid from place_order().

        Returns:
            OrderResult with CANCELLED status.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        sdk = self._get_sdk()

        logger.info(
            "nuvama_cancel_order",
            extra={
                "event": "nuvama_cancel_order",
                "user_id": self.user_id,
                "broker_order_id": broker_order_id,
            },
        )

        try:
            response: Any = sdk.CancelTrade(broker_order_id)
        except Exception as exc:
            self._handle_nuvama_exception(exc, "cancel_order")

        self._normalise_response(response, "cancel_order")
        return mapper.from_nuvama_cancel_response(broker_order_id)

    # ------------------------------------------------------------------
    # Order inspection
    # ------------------------------------------------------------------

    def get_order_status(self, broker_order_id: str) -> OrderDetail:
        """
        Fetch current status of a single order.

        Nuvama has no single-order fetch endpoint — we call OrderBook()
        (all orders for the day) and filter by oid. Typically < 200 items
        intraday, acceptable for V1.

        Args:
            broker_order_id: Nuvama oid.

        Returns:
            OrderDetail for the requested order.

        Raises:
            BrokerAuthError, BrokerNetworkError, BrokerOrderError (not found)
        """
        sdk = self._get_sdk()

        try:
            response: Any = sdk.OrderBook()
        except Exception as exc:
            self._handle_nuvama_exception(exc, "get_order_status")

        self._normalise_response(response, "get_order_status")

        # Normalise: OrderBook may return {"data": [...]} or a bare list
        if isinstance(response, dict):
            orders: list[dict] = response.get("data", response.get("orders", []))
        elif isinstance(response, list):
            orders = response
        else:
            orders = []

        for raw_order in orders:
            if str(raw_order.get("oid", raw_order.get("order_id", ""))) == broker_order_id:
                return mapper.from_nuvama_order_dict(raw_order)

        raise BrokerOrderError(
            f"Order {broker_order_id} not found in today's Nuvama order book.",
            broker="nuvama",
        )

    def get_order_history(self, broker_order_id: str) -> list[OrderDetail]:
        """
        Fetch the full audit trail of state changes for a single order.

        Nuvama OrderHistory() returns a list of status snapshots for a given
        oid, oldest first.

        Args:
            broker_order_id: Nuvama oid.

        Returns:
            List of OrderDetail snapshots, oldest first.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        sdk = self._get_sdk()

        try:
            response: Any = sdk.OrderHistory(broker_order_id)
        except Exception as exc:
            self._handle_nuvama_exception(exc, "get_order_history")

        self._normalise_response(response, "get_order_history")

        if isinstance(response, dict):
            history: list[dict] = response.get("data", response.get("history", []))
        elif isinstance(response, list):
            history = response
        else:
            history = []

        return [mapper.from_nuvama_order_dict(raw) for raw in history]

    # ------------------------------------------------------------------
    # Portfolio
    # ------------------------------------------------------------------

    def get_positions(self) -> list[Position]:
        """
        Fetch all open positions via Nuvama PositionBook().

        Returns:
            List of Position objects with non-zero quantity. Empty list if flat.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        sdk = self._get_sdk()

        try:
            response: Any = sdk.PositionBook()
        except Exception as exc:
            self._handle_nuvama_exception(exc, "get_positions")

        self._normalise_response(response, "get_positions")

        positions = mapper.from_nuvama_positions(response)
        logger.info(
            "nuvama_positions_fetched",
            extra={
                "event": "nuvama_positions_fetched",
                "user_id": self.user_id,
                "count": len(positions),
            },
        )
        return positions

    def get_holdings(self) -> list[Holding]:
        """
        Fetch CNC delivery holdings via Nuvama Holdings().

        Returns:
            List of Holding objects. Empty list if no holdings.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        sdk = self._get_sdk()

        try:
            response: Any = sdk.Holdings()
        except Exception as exc:
            self._handle_nuvama_exception(exc, "get_holdings")

        self._normalise_response(response, "get_holdings")
        return mapper.from_nuvama_holdings(response)

    def get_margins(self) -> Margin:
        """
        Fetch account margin / funds summary via Nuvama RMSSubLimits().

        Returns:
            Margin object with available cash and used margin.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        sdk = self._get_sdk()

        try:
            response: Any = sdk.RMSSubLimits()
        except Exception as exc:
            self._handle_nuvama_exception(exc, "get_margins")

        self._normalise_response(response, "get_margins")
        return mapper.from_nuvama_margins(response)

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_ltp(self, trading_symbol: str, exchange: Exchange) -> LTPResult:
        """
        Fetch the last traded price for a symbol.

        Nuvama V1 SDK has no dedicated LTP endpoint. We fetch PositionBook()
        and look for a matching position's ltp field. If the symbol has no
        open position (common case for pre-trade checks), we fall back to
        OrderBook() and read the last known ltp from the most recent order.

        If neither source has the symbol, LTPResult is returned with ltp=0.0
        and the caller (risk_manager) must decide whether to proceed or reject.

        Args:
            trading_symbol: NSE/BSE trading symbol.
            exchange:       Exchange enum.

        Returns:
            LTPResult with ltp field. May be 0.0 if symbol not in book.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        sdk = self._get_sdk()

        # Attempt 1: PositionBook ltp field
        try:
            pos_response: Any = sdk.PositionBook()
            self._normalise_response(pos_response, "get_ltp_positions")

            if isinstance(pos_response, dict):
                pos_list: list[dict] = pos_response.get("data", pos_response.get("positions", []))
            elif isinstance(pos_response, list):
                pos_list = pos_response
            else:
                pos_list = []

            for pos in pos_list:
                sym = pos.get("trdSym", pos.get("Trading_Symbol", ""))
                if sym == trading_symbol:
                    ltp_val = float(pos.get("ltp", pos.get("LTP", 0.0)))
                    if ltp_val > 0.0:
                        logger.info(
                            "nuvama_ltp_from_position",
                            extra={
                                "event": "nuvama_ltp_from_position",
                                "user_id": self.user_id,
                                "symbol": trading_symbol,
                                "ltp": ltp_val,
                            },
                        )
                        return mapper.from_nuvama_ltp(trading_symbol, exchange, ltp_val)

        except BrokerAuthError:
            raise
        except Exception as exc:
            # Non-fatal — fall through to OrderBook attempt
            logger.warning(
                "nuvama_ltp_position_book_failed",
                extra={
                    "event": "nuvama_ltp_position_book_failed",
                    "user_id": self.user_id,
                    "symbol": trading_symbol,
                    "error": str(exc),
                },
            )

        # Attempt 2: OrderBook ltp field (most recent order for this symbol)
        try:
            order_response: Any = sdk.OrderBook()
            self._normalise_response(order_response, "get_ltp_orders")

            if isinstance(order_response, dict):
                order_list: list[dict] = order_response.get("data", order_response.get("orders", []))
            elif isinstance(order_response, list):
                order_list = order_response
            else:
                order_list = []

            for order in reversed(order_list):  # Most recent first
                sym = order.get("trdSym", order.get("Trading_Symbol", ""))
                if sym == trading_symbol:
                    ltp_val = float(order.get("ltp", order.get("LTP", 0.0)))
                    if ltp_val > 0.0:
                        logger.info(
                            "nuvama_ltp_from_order_book",
                            extra={
                                "event": "nuvama_ltp_from_order_book",
                                "user_id": self.user_id,
                                "symbol": trading_symbol,
                                "ltp": ltp_val,
                            },
                        )
                        return mapper.from_nuvama_ltp(trading_symbol, exchange, ltp_val)

        except BrokerAuthError:
            raise
        except Exception as exc:
            self._handle_nuvama_exception(exc, "get_ltp_orders")

        # Neither source had the symbol — return 0.0 with a warning
        logger.warning(
            "nuvama_ltp_not_found",
            extra={
                "event": "nuvama_ltp_not_found",
                "user_id": self.user_id,
                "symbol": trading_symbol,
                "exchange": exchange.value,
            },
        )
        return mapper.from_nuvama_ltp(trading_symbol, exchange, 0.0)
