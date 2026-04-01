# app/api/v1/api.py
"""
app/api/v1/api.py

Central API router — PiyushTrade
=================================
Mounts all endpoint routers. Each router owns its own prefix internally.
No prefix is added here — doing so would create double-prefixes.

Phase 3 complete: orders, trades, portfolio, broker routers added.
"""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth,
    backtest,
    broker,
    options,
    orders,
    portfolio,
    strategy,
    trades,
    users,
)

api_router = APIRouter()

# Phase 0-2 routers (unchanged)
api_router.include_router(auth.router,     tags=["auth"])
api_router.include_router(users.router,    prefix="/users",   tags=["users"])
api_router.include_router(options.router,  prefix="/options", tags=["options"])
api_router.include_router(strategy.router, tags=["strategies"])
api_router.include_router(backtest.router, tags=["backtest"])

# Phase 3 routers — each router owns its own prefix
api_router.include_router(orders.router,    tags=["orders"])
api_router.include_router(trades.router,    tags=["trades"])
api_router.include_router(portfolio.router, tags=["portfolio"])
api_router.include_router(broker.router,    tags=["broker"])