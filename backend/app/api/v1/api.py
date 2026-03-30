from fastapi import APIRouter

from app.api.v1.endpoints import auth, backtest, options, strategy, users

api_router = APIRouter()

api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(options.router, prefix="/options", tags=["options"])
api_router.include_router(strategy.router, prefix="/strategy", tags=["strategy"])
api_router.include_router(backtest.router, prefix="/backtest", tags=["backtest"])