from fastapi import APIRouter

from app.api.v1.endpoints import auth, backtest, options, strategy, users

api_router = APIRouter()

api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(options.router, prefix="/options", tags=["options"])

# Bug 6 fix: removed prefix="/strategy" and prefix="/backtest" here.
# Each router defines its own prefix internally (/strategies and /backtest).
# Adding prefix here too caused double-prefix: /strategy/strategies/ and /backtest/backtest/
api_router.include_router(strategy.router, tags=["strategies"])
api_router.include_router(backtest.router, tags=["backtest"])
