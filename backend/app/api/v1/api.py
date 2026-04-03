# app/api/v1/api.py
"""
app/api/v1/api.py

Central API router — PiyushTrade
=================================
Mounts all endpoint routers. Each router owns its own prefix internally.
No prefix is added here — doing so would create double-prefixes.

Phase 3 currently mounted: portfolio, broker.
orders and trades routers should be added here only when their endpoint files exist.
"""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth,
    backtest,
    broker,
    options,
    portfolio,
    strategy,
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
api_router.include_router(portfolio.router, tags=["portfolio"])
api_router.include_router(broker.router,    tags=["broker"])