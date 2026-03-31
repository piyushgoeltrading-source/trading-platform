"""
app/brokers/nuvama/mapper.py

Nuvama ↔ PiyushTrade format mapper
=====================================
Converts between Nuvama APIConnect native request/response formats and
PiyushTrade standard DTOs defined in base_broker.py.

Rules:
  - ONLY this file knows Nuvama-specific field names, constants, and response shapes.
  - NuvamaBroker.client calls to_nuvama_order() before placing, and
    from_nuvama_* after receiving responses.
  - No network calls here — pure data transformation.
  - No live-order logic, no backtest logic.

Nuvama field name mapping (from handoff doc, Section 4):
  PiyushTrade field    → Nuvama field (PlaceTrade kwargs)
  -----------------------------------------------------------
  trading_symbol       → Trading_Symbol   (ISIN e.g. "INE090A01021")
  streaming_symbol     → Streaming_Symbol (exchange token e.g. "4963_NSE")
  exchange             → Exchange          ("NSE"/"BSE"/"NFO" etc.)
  side                 → Action            ("BUY"/"SELL")
  order_type           → Order_Type        ("LIMIT"/"MARKET"/"STOP_LIMIT"/"STOP_MARKET")
  product_code         → ProductCode       ("CNC"/"MIS"/"NRML"/"MTF")
  quantity             → Quantity          (integer as string)
  validity             → Duration          ("DAY"/"IOC")
  limit_price          → Limit_Price       (float as string)
  trigger_price        → TriggerPrice      (float as string)
  broker_order_id      ← oid              (from resp["data"]["oid"])

Nuvama order_type constants:
  MARKET      → "MARKET"
  LIMIT       → "LIMIT"
  STOP_LIMIT  → "STOP_LIMIT"    (SL with limit price)
  STOP_MARKET → "STOP_MARKET"   (SL market)

Nuvama status values → PiyushTrade OrderStatus:
  "open"        → OPEN
  "complete"    → COMPLETE
  "cancelled"   → CANCELLED
  "rejected"    → REJECTED
  "pending"     → PENDING
  "modify"      → PENDING   (modification in-flight)
  "trigger"     → OPEN      (SL order waiting for trigger)

Note on streaming_symbol:
  Nuvama requires the exchange token (e.g. "4963_NSE") for order placement,
  not just the trading symbol. This is carried in OrderRequest.streaming_symbol
  and mapped to Streaming_Symbol here. Callers must always populate this field
  for Nuvama orders.
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
    OrderStatus,
    OrderType,
    OrderValidity,
    Position,
    ProductCode,
)

# ---------------------------------------------------------------------------
# Internal constant maps
# ---------------------------------------------------------------------------

_ORDER_TYPE_TO_NUVAMA: dict[OrderType, str] = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.STOP_LIMIT: "STOP_LIMIT",
    OrderType.STOP_MARKET: "STOP_MARKET",
}

_PRODUCT_TO_NUVAMA: dict[ProductCode, str] = {
    ProductCode.CNC: "CNC",
    ProductCode.MIS: "MIS",
    ProductCode.NRML: "NRML",
    ProductCode.MTF: "MTF",
}

_VALIDITY_TO_NUVAMA: dict[OrderValidity, str] = {
    OrderValidity.DAY: "DAY",
    OrderValidity.IOC: "IOC",
}

_EXCHANGE_TO_NUVAMA: dict[Exchange, str] = {
    Exchange.NSE: "NSE",
    Exchange.BSE: "BSE",
    Exchange.NFO: "NFO",
    Exchange.BFO: "BFO",
    Exchange.MCX: "MCX",
    Exchange.CDS: "CDS",
}

_NUVAMA_STATUS_MAP: dict[str, OrderStatus] = {
    "open": OrderStatus.OPEN,
    "complete": OrderStatus.COMPLETE,
    "cancelled": OrderStatus.CANCELLED,
    "rejected": OrderStatus.REJECTED,
    "pending": OrderStatus.PENDING,
    "modify": OrderStatus.PENDING,    # Modification in-flight
    "trigger": OrderStatus.OPEN,      # SL order armed, waiting for trigger
    "transit": OrderStatus.PENDING,   # Order in transit to exchange
}


# ---------------------------------------------------------------------------
# OrderRequest → Nuvama PlaceTrade params dict
# ---------------------------------------------------------------------------

def to_nuvama_order(order_request: OrderRequest) -> dict[str, Any]:
    """
    Convert a PiyushTrade OrderRequest to the kwargs dict expected by
    APIConnect.PlaceTrade().

    Args:
        order_request: PiyushTrade standard OrderRequest.
                       order_request.streaming_symbol MUST be populated
                       (e.g. "4963_NSE") — required by Nuvama.

    Returns:
        Dict ready to be unpacked into APIConnect.PlaceTrade(**params).

    Notes:
        - Nuvama requires both Trading_Symbol (ISIN) and Streaming_Symbol
          (exchange token). Both come from OrderRequest.
        - Price values are passed as strings per Nuvama SDK convention.
        - Limit_Price and TriggerPrice are included only when relevant
          to avoid Nuvama validation errors.
    """
    params: dict[str, Any] = {
        "Trading_Symbol": order_request.trading_symbol,
        "Streaming_Symbol": order_request.streaming_symbol,
        "Exchange": _EXCHANGE_TO_NUVAMA[order_request.exchange],
        "Action": order_request.side.value,          # "BUY" or "SELL"
        "Order_Type": _ORDER_TYPE_TO_NUVAMA[order_request.order_type],
        "ProductCode": _PRODUCT_TO_NUVAMA[order_request.product_code],
        "Quantity": str(order_request.quantity),
        "Duration": _VALIDITY_TO_NUVAMA[order_request.validity],
        "Limit_Price": "0",
        "TriggerPrice": "0",
    }

    # Only include price fields when meaningful — prevents Nuvama rejection
    if order_request.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        params["Limit_Price"] = str(order_request.limit_price)

    if order_request.order_type in (OrderType.STOP_LIMIT, OrderType.STOP_MARKET):
        params["TriggerPrice"] = str(order_request.trigger_price)

    return params


def to_nuvama_modify(order_request: OrderRequest) -> dict[str, Any]:
    """
    Convert a PiyushTrade OrderRequest to the kwargs dict expected by
    APIConnect.ModifyTrade().

    Args:
        order_request: Updated order parameters.

    Returns:
        Dict ready to be unpacked into APIConnect.ModifyTrade(**params).
    """
    params: dict[str, Any] = {
        "Order_Type": _ORDER_TYPE_TO_NUVAMA[order_request.order_type],
        "Quantity": str(order_request.quantity),
        "Duration": _VALIDITY_TO_NUVAMA[order_request.validity],
        "Limit_Price": "0",
        "TriggerPrice": "0",
    }

    if order_request.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        params["Limit_Price"] = str(order_request.limit_price)

    if order_request.order_type in (OrderType.STOP_LIMIT, OrderType.STOP_MARKET):
        params["TriggerPrice"] = str(order_request.trigger_price)

    return params


# ---------------------------------------------------------------------------
# Nuvama responses → PiyushTrade DTOs
# ---------------------------------------------------------------------------

def _extract_order_id(response: Any) -> str:
    """
    Extract the broker order ID from a Nuvama PlaceTrade / ModifyTrade response.

    Nuvama returns {"data": {"oid": "..."}, "status": "success"}.
    Handles dict responses and bare string responses defensively.

    Args:
        response: Raw response from APIConnect.PlaceTrade() or ModifyTrade().

    Returns:
        Order ID string.
    """
    if isinstance(response, dict):
        data = response.get("data", response)
        if isinstance(data, dict):
            return str(data.get("oid", data.get("order_id", "")))
        return str(data)
    return str(response)


def from_nuvama_place_response(response: Any) -> OrderResult:
    """
    Build an OrderResult from Nuvama's PlaceTrade response.

    Nuvama PlaceTrade returns {"data": {"oid": "..."}, "status": "success"}
    on success. Initial status is always PENDING.

    Args:
        response: Raw response from APIConnect.PlaceTrade().

    Returns:
        OrderResult with status PENDING and the broker order ID.
    """
    broker_order_id = _extract_order_id(response)
    return OrderResult(
        broker_order_id=broker_order_id,
        status=OrderStatus.PENDING,
        message="Order placed successfully.",
        raw=response if isinstance(response, dict) else {"raw": str(response)},
    )


def from_nuvama_modify_response(response: Any, fallback_order_id: str) -> OrderResult:
    """
    Build an OrderResult from Nuvama's ModifyTrade response.

    Args:
        response:          Raw response from APIConnect.ModifyTrade().
        fallback_order_id: The original broker_order_id — used if response
                           does not contain an oid.

    Returns:
        OrderResult with status PENDING.
    """
    broker_order_id = _extract_order_id(response) or fallback_order_id
    return OrderResult(
        broker_order_id=broker_order_id,
        status=OrderStatus.PENDING,
        message="Order modification submitted.",
        raw=response if isinstance(response, dict) else {"raw": str(response)},
    )


def from_nuvama_cancel_response(broker_order_id: str) -> OrderResult:
    """
    Build an OrderResult from Nuvama's CancelTrade response.

    CancelTrade returns a status confirmation; we use the known order ID.

    Args:
        broker_order_id: The order ID that was cancelled.

    Returns:
        OrderResult with status CANCELLED.
    """
    return OrderResult(
        broker_order_id=broker_order_id,
        status=OrderStatus.CANCELLED,
        message="Order cancellation submitted.",
        raw={"oid": broker_order_id},
    )


def _parse_nuvama_status(raw_status: Optional[str]) -> OrderStatus:
    """
    Map a Nuvama status string to PiyushTrade OrderStatus.
    Comparison is case-insensitive. Defaults to PENDING for unknown values.
    """
    if not raw_status:
        return OrderStatus.PENDING
    return _NUVAMA_STATUS_MAP.get(raw_status.lower(), OrderStatus.PENDING)


def _parse_nuvama_datetime(value: Any) -> Optional[datetime]:
    """
    Parse Nuvama datetime strings.

    Nuvama returns timestamps in formats like "15-Jan-2024 09:15:00"
    or ISO-style "2024-01-15 09:15:00". Tries both; returns None on failure.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    for fmt in ("%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def from_nuvama_order_dict(raw: dict[str, Any]) -> OrderDetail:
    """
    Convert a single Nuvama order dict (from OrderBook() or OrderHistory())
    to a PiyushTrade OrderDetail.

    Nuvama order dict keys (from SDK docs):
        oid, trdSym, exc, action, ordTyp, prdCode,
        qty, flQty, avgPrc, status, lmPrc, trgPrc,
        ordTm, mdfTm, rejRsn

    Args:
        raw: Single order dict from APIConnect.OrderBook() list.

    Returns:
        PiyushTrade OrderDetail.
    """
    return OrderDetail(
        broker_order_id=str(raw.get("oid", "")),
        trading_symbol=raw.get("trdSym", raw.get("Trading_Symbol", "")),
        exchange=raw.get("exc", raw.get("Exchange", "")),
        side=raw.get("action", raw.get("Action", "")),
        order_type=raw.get("ordTyp", raw.get("Order_Type", "")),
        product_code=raw.get("prdCode", raw.get("ProductCode", "")),
        quantity=int(raw.get("qty", raw.get("Quantity", 0))),
        filled_quantity=int(raw.get("flQty", raw.get("FilledQty", 0))),
        average_price=float(raw.get("avgPrc", raw.get("AvgPrice", 0.0))),
        status=_parse_nuvama_status(raw.get("status", raw.get("Status"))),
        placed_at=_parse_nuvama_datetime(raw.get("ordTm", raw.get("OrderTime"))),
        updated_at=_parse_nuvama_datetime(raw.get("mdfTm", raw.get("ModifyTime"))),
        rejection_reason=raw.get("rejRsn", raw.get("RejReason")) or None,
        raw=raw,
    )


def from_nuvama_positions(raw_positions: Any) -> list[Position]:
    """
    Convert Nuvama PositionBook() response to a list of PiyushTrade Position objects.

    Nuvama returns a list of position dicts directly (not wrapped in net/day).

    Nuvama position dict keys:
        trdSym, exc, prdCode, netQty, avgPrc, ltp, unrealPnl, realPnl

    Args:
        raw_positions: List or dict returned by APIConnect.PositionBook().

    Returns:
        List of Position objects. Only non-zero netQty positions are included.
    """
    # Normalise: Nuvama may return {"data": [...]} or a bare list
    if isinstance(raw_positions, dict):
        position_list: list[dict] = raw_positions.get("data", raw_positions.get("positions", []))
    elif isinstance(raw_positions, list):
        position_list = raw_positions
    else:
        return []

    result: list[Position] = []
    for pos in position_list:
        qty = int(pos.get("netQty", pos.get("qty", 0)))
        if qty == 0:
            continue  # Skip flat positions

        result.append(
            Position(
                trading_symbol=pos.get("trdSym", pos.get("Trading_Symbol", "")),
                exchange=pos.get("exc", pos.get("Exchange", "")),
                product_code=pos.get("prdCode", pos.get("ProductCode", "")),
                quantity=qty,
                average_price=float(pos.get("avgPrc", pos.get("AvgPrice", 0.0))),
                last_price=float(pos.get("ltp", pos.get("LTP", 0.0))),
                pnl=float(pos.get("unrealPnl", pos.get("UnrealPnl", 0.0))),
                realised_pnl=float(pos.get("realPnl", pos.get("RealPnl", 0.0))),
                raw=pos,
            )
        )

    return result


def from_nuvama_holdings(raw_holdings: Any) -> list[Holding]:
    """
    Convert Nuvama Holdings() response to a list of PiyushTrade Holding objects.

    Nuvama holding dict keys:
        trdSym, exc, qty, avgPrc, ltp, pnl

    Args:
        raw_holdings: List or dict returned by APIConnect.Holdings().

    Returns:
        List of Holding objects.
    """
    if isinstance(raw_holdings, dict):
        holding_list: list[dict] = raw_holdings.get("data", raw_holdings.get("holdings", []))
    elif isinstance(raw_holdings, list):
        holding_list = raw_holdings
    else:
        return []

    return [
        Holding(
            trading_symbol=h.get("trdSym", h.get("Trading_Symbol", "")),
            exchange=h.get("exc", h.get("Exchange", "")),
            quantity=int(h.get("qty", h.get("Quantity", 0))),
            average_price=float(h.get("avgPrc", h.get("AvgPrice", 0.0))),
            last_price=float(h.get("ltp", h.get("LTP", 0.0))),
            pnl=float(h.get("pnl", h.get("PnL", 0.0))),
            raw=h,
        )
        for h in holding_list
    ]


def from_nuvama_margins(raw_margins: Any) -> Margin:
    """
    Convert Nuvama RMSSubLimits() response to a PiyushTrade Margin object.

    Nuvama RMSSubLimits returns funds/margin data. Key names confirmed
    from SDK docs as camelCase; PascalCase fallbacks included for safety.

    Nuvama margin dict keys:
        availableCash / AvailableCash
        usedMargin    / UsedMargin
        spanMargin    / SpanMargin
        exposureMargin / ExposureMargin
        netMargin     / NetMargin

    Args:
        raw_margins: Dict or wrapped dict returned by APIConnect.RMSSubLimits().

    Returns:
        PiyushTrade Margin object.
    """
    # Normalise wrapper if present
    if isinstance(raw_margins, dict) and "data" in raw_margins:
        data: dict = raw_margins["data"]
    elif isinstance(raw_margins, dict):
        data = raw_margins
    else:
        data = {}

    available_cash = float(
        data.get("availableCash", data.get("AvailableCash", 0.0))
    )
    used_margin = float(
        data.get("usedMargin", data.get("UsedMargin", 0.0))
    )
    span = float(
        data.get("spanMargin", data.get("SpanMargin", 0.0))
    )
    exposure = float(
        data.get("exposureMargin", data.get("ExposureMargin", 0.0))
    )
    total_margin = float(
        data.get("netMargin", data.get("NetMargin", available_cash + used_margin))
    )

    return Margin(
        available_cash=available_cash,
        used_margin=used_margin,
        total_margin=total_margin,
        span_margin=span,
        exposure_margin=exposure,
        raw=raw_margins if isinstance(raw_margins, dict) else {"raw": str(raw_margins)},
    )


def from_nuvama_ltp(
    trading_symbol: str,
    exchange: Exchange,
    raw_ltp: Any,
) -> LTPResult:
    """
    Convert Nuvama LTP response to a PiyushTrade LTPResult.

    Nuvama does not have a dedicated LTP endpoint in the SDK (V1).
    get_ltp() in client.py uses OrderBook or PositionBook ltp field as
    the fallback. This function handles the normalised ltp value passed
    from client.py after extraction.

    Args:
        trading_symbol: The trading symbol requested.
        exchange:       The exchange enum value.
        raw_ltp:        The raw response dict from the SDK call, or a bare
                        float if the client already extracted it.

    Returns:
        PiyushTrade LTPResult.
    """
    if isinstance(raw_ltp, (int, float)):
        ltp = float(raw_ltp)
        raw_dict: dict = {"ltp": ltp}
    elif isinstance(raw_ltp, dict):
        ltp = float(raw_ltp.get("ltp", raw_ltp.get("LTP", raw_ltp.get("last_price", 0.0))))
        raw_dict = raw_ltp
    else:
        ltp = 0.0
        raw_dict = {"raw": str(raw_ltp)}

    return LTPResult(
        trading_symbol=trading_symbol,
        exchange=exchange.value,
        ltp=ltp,
        timestamp=None,  # Nuvama does not return a timestamp with LTP in V1
    )
