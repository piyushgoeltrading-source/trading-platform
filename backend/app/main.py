"""
app/main.py

FastAPI Application Entry Point — PiyushTrade
===============================================
Mounts all API routers and manages application lifecycle.

Routers:
  /auth           — login, token refresh (Phase 0)
  /users          — user management (Phase 0)
  /options        — live option chain (Phase 1)
  /strategies     — strategy CRUD (Phase 2)
  /backtest       — backtest dispatch + polling (Phase 2)

Lifecycle:
  startup:  Connect Redis, configure logging
  shutdown: Close Redis connection

Rules:
  - configure_logging() called before anything else.
  - Redis client is stored on app.state.redis — shared across requests.
  - No create_all() here or anywhere — Alembic only for schema changes.
  - reload=True in dev only. Disable in production.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.logging import configure_root_logging, get_structured_logger
from app.core.time_utils import now_utc

# Configure structured logging before ANY other imports that log
configure_root_logging()

logger = get_structured_logger(__name__)


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage shared resources across the application lifetime.

    startup:
      - Connect to Redis and verify connectivity (ping).
      - Store client on app.state.redis.

    shutdown:
      - Close Redis connection cleanly.
    """
    # --- Startup ---
    logger.info(
        "PiyushTrade API starting",
        extra={
            "event": "app_startup",
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    redis_client = aioredis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
    )

    try:
        await redis_client.ping()
        logger.info(
            "Redis connection established",
            extra={"event": "redis_connected", "url": settings.REDIS_URL},
        )
    except Exception as exc:
        logger.error(
            "Redis connection failed at startup",
            extra={
                "event": "redis_startup_failure",
                "error": str(exc),
                "timestamp_utc": now_utc().isoformat(),
            },
        )
        # Do not crash — allow the app to start so health checks can report status.
        # Options API will return REDIS_UNAVAILABLE errors until Redis recovers.

    app.state.redis = redis_client

    yield  # Application runs here

    # --- Shutdown ---
    logger.info(
        "PiyushTrade API shutting down",
        extra={"event": "app_shutdown", "timestamp_utc": now_utc().isoformat()},
    )
    await redis_client.aclose()


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="PiyushTrade API",
        description=(
            "Production-grade options trading platform API. "
            "NSE/BSE options data, strategy management, and backtesting."
        ),
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # --- CORS ---
    # Tighten allowed_origins in production to your actual frontend domain
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],  # Next.js dev server
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # --- Global exception handler — catch-all for unexpected errors ---
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "Unhandled exception",
            extra={
                "event": "unhandled_exception",
                "path": str(request.url.path),
                "method": request.method,
                "error": str(exc),
                "error_type": type(exc).__name__,
                "timestamp_utc": now_utc().isoformat(),
            },
        )
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred",
                "details": {},
            },
        )

    # --- Routers ---
    _mount_routers(app)

    return app


def _mount_routers(app: FastAPI) -> None:
    """Mount all API routers with their version prefix."""
    from app.api.v1.api import api_router

    # Phase 0 routers (auth + users) — import when those files exist
    # from app.api.auth import router as auth_router
    # from app.api.users import router as users_router
    # app.include_router(auth_router, prefix="/api/v1")
    # app.include_router(users_router, prefix="/api/v1")

    app.include_router(api_router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# Health check — outside /api/v1 prefix (load balancer target)
# ---------------------------------------------------------------------------

app = create_app()


@app.get("/health", tags=["health"], include_in_schema=False)
async def health_check(request: Request) -> dict:
    """
    Lightweight health check for load balancer / ECS health target.
    Reports Redis connectivity status without blocking the response.
    """
    redis_ok = False
    try:
        await request.app.state.redis.ping()
        redis_ok = True
    except Exception:
        pass

    return {
        "status": "ok",
        "redis": "connected" if redis_ok else "unavailable",
        "timestamp_utc": now_utc().isoformat(),
    }
