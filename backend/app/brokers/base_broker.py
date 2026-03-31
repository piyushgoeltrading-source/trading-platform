"""
app/brokers/base_broker.py

Abstract base class for all broker integrations — PiyushTrade
==============================================================
ALL broker implementations (Zerodha, Nuvama, future brokers) must subclass
BaseBroker and implement every abstract method.

Rules:
  - No broker-specific logic lives here — only the contract.
  - Every method receives and returns PiyushTrade standard types (defined below).
  - mapper.py per broker converts between broker-native format and these types.
  - Tokens/sessions are managed by each broker's auth.py, not here.
  - All methods are synchronous — async is handled at the API/task layer.

Standard flow for every order:
    risk_manager → execution_guard → BrokerFactory.get(user)
    → broker.place_order() → mapper → broker API → return OrderResult
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Standard enums — broker-agnostic
# ---------------------------------------------------------------------------

class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_LIMIT = "STOP_LIMIT"
    STOP_MARKET = "STOP_MARKET"


class ProductCode(str, Enum):
    CNC = "CNC"       # Delivery (equity)
    NRML = "NRML"     # Normal (F&O overnight)
    MIS = "MIS"       # Intraday
    MTF = "MTF"       # Margin trading


class OrderValidity(str, Enum):
    DAY = "DAY"
    IOC = "IOC"       # Immediate or cancel


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    COMPLETE = "COMPLETE"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    PARTIAL = "PARTIAL"


class Exchange(str, Enum):
    NSE = "NSE"
    BSE = "BSE"
    NFO = "NFO"       # NSE F&O — options/futures
    BFO = "BFO"       # BSE F&O
    MCX = "MCX"
    CDS = "CDS"       # Currency derivatives


# ---------------------------------------------------------------------------
# Standard data transfer objects
# ---------------------------------------------------------------------------

@dataclass
class OrderRequest:
    """
    PiyushTrade standard order request.
    mapper.py converts this to broker-native format before placing.

    Fields:
        trading_symbol      NSE/BSE trading symbol (e.g. "NIFTY24JAN21000CE")
        streaming_symbol    Exchange token (e.g. "4963_NSE") — required by Nuvama
        exchange            Exchange enum
        side                BUY or SELL
        order_type          MARKET / LIMIT / STOP_LIMIT / STOP_MARKET
        product_code        CNC / NRML / MIS / MTF
        quantity            Number of lots/shares
        validity            DAY or IOC
        limit_price         Required for LIMIT and STOP_LIMIT orders
        trigger_price       Required for STOP_LIMIT and STOP_MARKET orders
        disclosed_quantity  Disclosed qty for iceberg orders (default 0)
        tag                 Optional label for identifying the order (strategy name etc.)
        idempotency_key     Floored-to-minute timestamp string — prevents duplicate orders on retry
    """
    trading_symbol: str
    streaming_symbol: str
    exchange: Exchange
    side: OrderSide
    order_type: OrderType
    product_code: ProductCode
    quantity: int
    validity: OrderValidity = OrderValidity.DAY
    limit_price: float = 0.0
    trigger_price: float = 0.0
    disclosed_quantity: int = 0
    tag: Optional[str] = None
    idempotency_key: Optional[str] = None


@dataclass
class OrderResult:
    """
    PiyushTrade standard result returned after placing or modifying an order.
    mapper.py converts broker-native response to this format.
    """
    broker_order_id: str
    status: OrderStatus
    message: str = ""
    raw: dict = field(default_factory=dict)   # Original broker response — stored for audit


@dataclass
class OrderDetail:
    """Full details of a single order — returned by get_order_status / get_order_history."""
    broker_order_id: str
    trading_symbol: str
    exchange: str
    side: str
    order_type: str
    product_code: str
    quantity: int
    filled_quantity: int
    average_price: float
    status: OrderStatus
    placed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None
    raw: dict = field(default_factory=dict)


@dataclass
class Position:
    """A single open position — returned by get_positions()."""
    trading_symbol: str
    exchange: str
    product_code: str
    quantity: int               # Net quantity (negative = short)
    average_price: float
    last_price: float
    pnl: float                  # Unrealised P&L
    realised_pnl: float = 0.0
    raw: dict = field(default_factory=dict)


@dataclass
class Holding:
    """A single CNC holding — returned by get_holdings()."""
    trading_symbol: str
    exchange: str
    quantity: int
    average_price: float
    last_price: float
    pnl: float
    raw: dict = field(default_factory=dict)


@dataclass
class Margin:
    """Account margin summary — returned by get_margins()."""
    available_cash: float
    used_margin: float
    total_margin: float
    span_margin: float = 0.0
    exposure_margin: float = 0.0
    raw: dict = field(default_factory=dict)


@dataclass
class LTPResult:
    """Last traded price for a symbol — returned by get_ltp()."""
    trading_symbol: str
    exchange: str
    ltp: float
    timestamp: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Abstract broker base class
# ---------------------------------------------------------------------------

class BaseBroker(ABC):
    """
    Abstract base for all broker integrations.

    Subclasses: ZerodhaBroker, NuvamaBroker (V1), KotakBroker (V2)

    All methods are synchronous. The execution engine calls these in a
    thread pool if needed — do not add asyncio here.
    """

    # ------------------------------------------------------------------
    # Order lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def place_order(self, order_request: OrderRequest) -> OrderResult:
        """
        Place a new order.

        Args:
            order_request: PiyushTrade standard OrderRequest.
                           mapper.py converts this to broker format internally.

        Returns:
            OrderResult with broker_order_id and initial status.

        Raises:
            BrokerAuthError:    Session expired or token invalid.
            BrokerOrderError:   Order rejected by broker/exchange.
            BrokerNetworkError: Network failure reaching broker API.
        """
        raise NotImplementedError

    @abstractmethod
    def modify_order(
        self,
        broker_order_id: str,
        order_request: OrderRequest,
    ) -> OrderResult:
        """
        Modify an existing open order.

        Args:
            broker_order_id:  The broker's order ID from place_order().
            order_request:    Updated order parameters.

        Returns:
            OrderResult reflecting the modification result.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> OrderResult:
        """
        Cancel an open or pending order.

        Args:
            broker_order_id:  The broker's order ID from place_order().

        Returns:
            OrderResult with CANCELLED status on success.

        Raises:
            BrokerAuthError, BrokerOrderError, BrokerNetworkError
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Order inspection
    # ------------------------------------------------------------------

    @abstractmethod
    def get_order_status(self, broker_order_id: str) -> OrderDetail:
        """
        Fetch current status of a single order.

        Args:
            broker_order_id: The broker's order ID.

        Returns:
            OrderDetail with current status and fill details.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        raise NotImplementedError

    @abstractmethod
    def get_order_history(self, broker_order_id: str) -> list[OrderDetail]:
        """
        Fetch the full audit trail of state changes for a single order.

        Args:
            broker_order_id: The broker's order ID.

        Returns:
            List of OrderDetail snapshots, oldest first.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Portfolio
    # ------------------------------------------------------------------

    @abstractmethod
    def get_positions(self) -> list[Position]:
        """
        Fetch all open intraday and overnight positions.

        Returns:
            List of Position objects. Empty list if no open positions.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        raise NotImplementedError

    @abstractmethod
    def get_holdings(self) -> list[Holding]:
        """
        Fetch CNC delivery holdings.

        Returns:
            List of Holding objects. Empty list if no holdings.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        raise NotImplementedError

    @abstractmethod
    def get_margins(self) -> Margin:
        """
        Fetch account margin / funds summary.

        Returns:
            Margin object with available cash and used margin.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    @abstractmethod
    def get_ltp(self, trading_symbol: str, exchange: Exchange) -> LTPResult:
        """
        Fetch the last traded price for a symbol.

        Args:
            trading_symbol:  NSE/BSE trading symbol.
            exchange:        Exchange enum.

        Returns:
            LTPResult with ltp and timestamp.

        Raises:
            BrokerAuthError, BrokerNetworkError
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Broker-specific exceptions
# ---------------------------------------------------------------------------

class BrokerError(Exception):
    """Base exception for all broker failures."""

    def __init__(self, message: str, broker: str = "", raw: dict = None) -> None:
        self.broker = broker
        self.raw = raw or {}
        super().__init__(f"[{broker}] {message}" if broker else message)


class BrokerAuthError(BrokerError):
    """Session expired, token invalid, or authentication failed."""


class BrokerOrderError(BrokerError):
    """Order rejected by broker or exchange risk systems."""


class BrokerNetworkError(BrokerError):
    """Network failure or timeout reaching the broker API."""


class BrokerRateLimitError(BrokerError):
    """API rate limit hit — back off before retrying."""
