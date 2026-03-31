"""
app/brokers/zerodha/mapper.py

Zerodha ↔ PiyushTrade format mapper
=====================================
Converts between Kite Connect native request/response formats and
PiyushTrade standard DTOs defined in base_broker.py.

Rules:
  - ONLY this file knows Zerodha-specific field names, constants, and response shapes.
  - ZerodhaBroker.client calls to_kite_order() before placing, and
    from_kite_* after receiving responses.
  - No network calls here — pure data transformation.
  - No live-order logic, no backtest logic.

Kite Connect order constants used here:
  variety:    "regular" | "amo" | "co" | "iceberg" | "auction"
  product:    "CNC" | "NRML" | "MIS" | "MTF"
  order_type: "MARKET" | "LIMIT" | "SL" | "SL-M"
  validity:   "DAY" | "IOC" | "TTL"
  exchange:   "NSE" | "BSE" | "NFO" | "BFO" | "MCX" | "CDS"
  transaction_type: "BUY" | "SELL"

Kite status values → PiyushTrade OrderStatus:
  "OPEN"             → OPEN
  "COMPLETE"         → COMPLETE
  "CANCELLED"        → CANCELLED
  "REJECTED"         → REJECTED
  "OPEN PENDING"     → PENDING
  "MODIFY PENDING"   → PENDING
  "CANCEL PENDING"   → PENDING
  "TRIGGER PENDING"  → OPEN   (SL order waiting for trigger)
  "AMO REQ RECEIVED" → PENDING
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from app.brokers.base_broker import (
    Exchange,
    Holding,
    LTPResult,
    Margin,
    OrderDetail,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderValidity,
    Position,
    ProductCode,
)

# ---------------------------------------------------------------------------
# Internal constant maps
# ---------------------------------------------------------------------------

_PRODUCT_TO_KITE: dict[ProductCode, str] = {
    ProductCode.CNC: "CNC",
    ProductCode.NRML: "NRML",
    ProductCode.MIS: "MIS",
    ProductCode.MTF: "MTF",
}

_ORDER_TYPE_TO_KITE: dict[OrderType, str] = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.STOP_LIMIT: "SL",       # Stop-loss limit
    OrderType.STOP_MARKET: "SL-M",    # Stop-loss market
}

_VALIDITY_TO_KITE: dict[OrderValidity, str] = {
    OrderValidity.DAY: "DAY",
    OrderValidity.IOC: "IOC",
}

_EXCHANGE_TO_KITE: dict[Exchange, str] = {
    Exchange.NSE: "NSE",
    Exchange.BSE: "BSE",
    Exchange.NFO: "NFO",
    Exchange.BFO: "BFO",
    Exchange.MCX: "MCX",
    Exchange.CDS: "CDS",
}

_KITE_STATUS_MAP: dict[str, OrderStatus] = {
    "OPEN": OrderStatus.OPEN,
    "COMPLETE": OrderStatus.COMPLETE,
    "CANCELLED": OrderStatus.CANCELLED,
    "REJECTED": OrderStatus.REJECTED,
    "OPEN PENDING": OrderStatus.PENDING,
    "MODIFY PENDING": OrderStatus.PENDING,
    "CANCEL PENDING": OrderStatus.PENDING,
    "TRIGGER PENDING": OrderStatus.OPEN,   # SL order armed, waiting for trigger
    "AMO REQ RECEIVED": OrderStatus.PENDING,
}


# ---------------------------------------------------------------------------
# OrderRequest → Kite params dict
# ---------------------------------------------------------------------------

def to_kite_order(order_request: OrderRequest) -> dict[str, Any]:
    """
    Convert a PiyushTrade OrderRequest to the kwargs dict expected by
    KiteConnect.place_order().

    Args:
        order_request: PiyushTrade standard order request.

    Returns:
        Dict ready to be unpacked into KiteConnect.place_order(**params).

    Notes:
        - variety is always "regular" in V1 (no AMO, CO, or iceberg support yet).
        - disclosed_quantity is omitted when 0 (Kite default).
        - tag is omitted when None (Kite ignores missing tag).
    """
    params: dict[str, Any] = {
        "variety": "regular",
        "tradingsymbol": order_request.trading_symbol,
        "exchange": _EXCHANGE_TO_KITE[order_request.exchange],
        "transaction_type": order_request.side.value,   # "BUY" or "SELL"
        "order_type": _ORDER_TYPE_TO_KITE[order_request.order_type],
        "product": _PRODUCT_TO_KITE[order_request.product_code],
        "quantity": order_request.quantity,
        "validity": _VALIDITY_TO_KITE[order_request.validity],
    }

    # Price fields — only include when relevant to avoid Kite validation errors
    if order_request.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        params["price"] = order_request.limit_price

    if order_request.order_type in (OrderType.STOP_LIMIT, OrderType.STOP_MARKET):
        params["trigger_price"] = order_request.trigger_price

    if order_request.disclosed_quantity > 0:
        params["disclosed_quantity"] = order_request.disclosed_quantity

    if order_request.tag:
        params["tag"] = order_request.tag

    return params


def to_kite_modify(order_request: OrderRequest) -> dict[str, Any]:
    """
    Convert a PiyushTrade OrderRequest to the kwargs dict expected by
    KiteConnect.modify_order().

    Args:
        order_request: Updated order parameters.

    Returns:
        Dict ready to be unpacked into KiteConnect.modify_order(**params).
    """
    params: dict[str, Any] = {
        "variety": "regular",
        "order_type": _ORDER_TYPE_TO_KITE[order_request.order_type],
        "quantity": order_request.quantity,
        "validity": _VALIDITY_TO_KITE[order_request.validity],
    }

    if order_request.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        params["price"] = order_request.limit_price

    if order_request.order_type in (OrderType.STOP_LIMIT, OrderType.STOP_MARKET):
        params["trigger_price"] = order_request.trigger_price

    if order_request.disclosed_quantity > 0:
        params["disclosed_quantity"] = order_request.disclosed_quantity

    return params


# ---------------------------------------------------------------------------
# Kite responses → PiyushTrade DTOs
# ---------------------------------------------------------------------------

def from_kite_place_response(broker_order_id: str) -> OrderResult:
    """
    Build an OrderResult from Kite's place_order response.

    Kite place_order returns {"order_id": "171229000724687"} on success.
    Initial status is always PENDING — the order hasn't been processed yet.

    Args:
        broker_order_id: The order_id string from Kite's response.

    Returns:
        OrderResult with status PENDING and the broker order ID.
    """
    return OrderResult(
        broker_order_id=broker_order_id,
        status=OrderStatus.PENDING,
        message="Order placed successfully.",
        raw={"order_id": broker_order_id},
    )


def from_kite_modify_response(broker_order_id: str) -> OrderResult:
    """
    Build an OrderResult from Kite's modify_order response.

    Kite modify_order returns {"order_id": "..."} on success.

    Args:
        broker_order_id: The order_id string from Kite's response.

    Returns:
        OrderResult with status PENDING (modification in-flight).
    """
    return OrderResult(
        broker_order_id=broker_order_id,
        status=OrderStatus.PENDING,
        message="Order modification submitted.",
        raw={"order_id": broker_order_id},
    )


def from_kite_cancel_response(broker_order_id: str) -> OrderResult:
    """
    Build an OrderResult from Kite's cancel_order response.

    Args:
        broker_order_id: The order_id string from Kite's response.

    Returns:
        OrderResult with status CANCELLED.
    """
    return OrderResult(
        broker_order_id=broker_order_id,
        status=OrderStatus.CANCELLED,
        message="Order cancellation submitted.",
        raw={"order_id": broker_order_id},
    )


def _parse_kite_status(raw_status: Optional[str]) -> OrderStatus:
    """Map a Kite status string to PiyushTrade OrderStatus. Defaults to PENDING."""
    if not raw_status:
        return OrderStatus.PENDING
    return _KITE_STATUS_MAP.get(raw_status.upper(), OrderStatus.PENDING)


def _parse_kite_datetime(value: Any) -> Optional[datetime]:
    """Parse Kite's datetime strings — may be None, str, or already a datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # Kite returns strings like "2024-01-15 09:15:00"
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def from_kite_order_dict(raw: dict[str, Any]) -> OrderDetail:
    """
    Convert a single Kite order dict (from orders() or order_history()) to
    a PiyushTrade OrderDetail.

    Kite order dict keys used:
        order_id, tradingsymbol, exchange, transaction_type,
        order_type, product, quantity, filled_quantity, average_price,
        status, price, placed_at, exchange_update_timestamp, status_message

    Args:
        raw: Single order dict from KiteConnect.orders() list.

    Returns:
        PiyushTrade OrderDetail.
    """
    return OrderDetail(
        broker_order_id=str(raw.get("order_id", "")),
        trading_symbol=raw.get("tradingsymbol", ""),
        exchange=raw.get("exchange", ""),
        side=raw.get("transaction_type", ""),
        order_type=raw.get("order_type", ""),
        product_code=raw.get("product", ""),
        quantity=int(raw.get("quantity", 0)),
        filled_quantity=int(raw.get("filled_quantity", 0)),
        average_price=float(raw.get("average_price", 0.0)),
        status=_parse_kite_status(raw.get("status")),
        placed_at=_parse_kite_datetime(raw.get("placed_at")),
        updated_at=_parse_kite_datetime(raw.get("exchange_update_timestamp")),
        rejection_reason=raw.get("status_message") or None,
        raw=raw,
    )


def from_kite_positions(raw_positions: dict[str, Any]) -> list[Position]:
    """
    Convert Kite positions() response to a list of PiyushTrade Position objects.

    Kite returns {"net": [...], "day": [...]}.
    We use "net" for the canonical view (combines overnight + day positions).

    Kite position dict keys used:
        tradingsymbol, exchange, product, quantity, average_price,
        last_price, pnl, realised

    Args:
        raw_positions: Full dict returned by KiteConnect.positions().

    Returns:
        List of Position objects. Only non-zero quantity positions are included.
    """
    net: list[dict] = raw_positions.get("net", [])
    result: list[Position] = []

    for pos in net:
        qty = int(pos.get("quantity", 0))
        if qty == 0:
            continue  # Skip flat positions

        result.append(
            Position(
                trading_symbol=pos.get("tradingsymbol", ""),
                exchange=pos.get("exchange", ""),
                product_code=pos.get("product", ""),
                quantity=qty,
                average_price=float(pos.get("average_price", 0.0)),
                last_price=float(pos.get("last_price", 0.0)),
                pnl=float(pos.get("pnl", 0.0)),
                realised_pnl=float(pos.get("realised", 0.0)),
                raw=pos,
            )
        )

    return result


def from_kite_holdings(raw_holdings: list[dict[str, Any]]) -> list[Holding]:
    """
    Convert Kite holdings() response to a list of PiyushTrade Holding objects.

    Kite holding dict keys used:
        tradingsymbol, exchange, quantity, average_price, last_price, pnl

    Args:
        raw_holdings: List returned by KiteConnect.holdings().

    Returns:
        List of Holding objects.
    """
    return [
        Holding(
            trading_symbol=h.get("tradingsymbol", ""),
            exchange=h.get("exchange", ""),
            quantity=int(h.get("quantity", 0)),
            average_price=float(h.get("average_price", 0.0)),
            last_price=float(h.get("last_price", 0.0)),
            pnl=float(h.get("pnl", 0.0)),
            raw=h,
        )
        for h in raw_holdings
    ]


def from_kite_margins(raw_margins: dict[str, Any]) -> Margin:
    """
    Convert Kite margins() response to a PiyushTrade Margin object.

    Kite returns margins per segment. We read the "equity" segment (covers
    NFO F&O as well — Kite groups derivatives under equity margin).

    Kite margins structure:
        {
          "equity": {
            "available": {"cash": ..., "intraday_payin": ..., "live_balance": ...},
            "utilised": {"debits": ..., "span": ..., "exposure": ..., ...},
            "net": ...
          }
        }

    Args:
        raw_margins: Full dict returned by KiteConnect.margins().

    Returns:
        PiyushTrade Margin object.
    """
    equity = raw_margins.get("equity", {})
    available = equity.get("available", {})
    utilised = equity.get("utilised", {})

    available_cash = float(available.get("live_balance", available.get("cash", 0.0)))
    used_margin = float(utilised.get("debits", 0.0))
    span = float(utilised.get("span", 0.0))
    exposure = float(utilised.get("exposure", 0.0))
    total_margin = available_cash + used_margin

    return Margin(
        available_cash=available_cash,
        used_margin=used_margin,
        total_margin=total_margin,
        span_margin=span,
        exposure_margin=exposure,
        raw=raw_margins,
    )


def from_kite_ltp(
    trading_symbol: str,
    exchange: Exchange,
    raw_ltp: dict[str, Any],
) -> LTPResult:
    """
    Convert Kite ltp() response to a PiyushTrade LTPResult.

    Kite ltp() takes "EXCHANGE:SYMBOL" as input and returns:
        {"NSE:NIFTY24JAN21000CE": {"instrument_token": ..., "last_price": ...}}

    Args:
        trading_symbol: The trading symbol requested.
        exchange:       The exchange enum value.
        raw_ltp:        Full dict returned by KiteConnect.ltp().

    Returns:
        PiyushTrade LTPResult.
    """
    kite_key = f"{_EXCHANGE_TO_KITE[exchange]}:{trading_symbol}"
    symbol_data = raw_ltp.get(kite_key, {})
    ltp = float(symbol_data.get("last_price", 0.0))

    return LTPResult(
        trading_symbol=trading_symbol,
        exchange=exchange.value,
        ltp=ltp,
        timestamp=None,  # Kite ltp() does not return a timestamp
    )
