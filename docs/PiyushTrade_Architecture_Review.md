# PiyushTrade Backend — Architecture Review
**Reviewed against:** Handoff v8 · Phase 3 Complete  
**Scope:** Full codebase, all phases (0–3)  
**Date:** April 2026

---

## Executive Summary

The overall architecture is **well-structured and largely sound**. The layering is clean, the execution pipeline is correctly ordered and locked, multi-tenancy is consistently enforced, the broker abstraction is solid, and the async/sync boundary is handled correctly throughout. The codebase is readable and well-documented.

That said, the review uncovered **4 hard bugs that will cause runtime failures**, several **moderate risks** that could cause subtle production issues, and a set of **hardening items** to address before going live. These are catalogued below in priority order.

---

## Section 1 — Critical Bugs (Will Break at Runtime)

### BUG-1 · `app/models/order.py` and `app/models/trade.py` contain wrong file content

**Severity:** 🔴 Fatal  
**What happened:** Both `order.py` and `trade.py` in `app/models/` contain the content of the endpoint files (`orders.py` and `trades.py`) rather than the ORM model definitions. The file header comments confirm this — `order.py` starts with `# app/api/v1/endpoints/orders.py`.

**Impact:** The entire application will fail to import. `app/models/__init__.py` imports `Order`, `OrderStatus`, `OrderSide`, and `Trade` from these files, but those classes do not exist in the current file content. Every module that touches orders or trades will crash at startup with an `ImportError`.

**Fix:** Restore the correct ORM model definitions to `app/models/order.py` and `app/models/trade.py`. Based on the rest of the codebase (execution engine, endpoint schemas, migration references), these models should define:

```python
# app/models/order.py
class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=True)
    idempotency_key = Column(String, nullable=False, unique=True)
    status = Column(SAEnum(OrderStatus, native_enum=False), nullable=False)
    instrument = Column(String, nullable=False)
    side = Column(SAEnum(OrderSide, native_enum=False), nullable=False)
    qty = Column(Integer, nullable=False)
    strike = Column(Float, nullable=False)
    expiry = Column(Date, nullable=False)
    broker = Column(SAEnum(BrokerName, native_enum=False), nullable=False)
    broker_order_id = Column(String, nullable=True)
    rejection_reason = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
```

---

### BUG-2 · Zerodha callback argument order is swapped

**Severity:** 🔴 Fatal  
**Location:** `app/api/v1/endpoints/broker.py` lines ~171–174  

**Signature in `zerodha/auth.py`:**
```python
def exchange_request_token(user_id: int, request_token: str) -> str:
```

**Call in `broker.py`:**
```python
access_token = await asyncio.to_thread(
    exchange_request_token,
    request_token,       # ← passed as user_id (wrong)
    current_user.id,     # ← passed as request_token (wrong)
)
```

**Impact:** Every Zerodha OAuth callback will call `exchange_request_token` with the token string as `user_id` and the integer user ID as the token string. The Redis key will be malformed, `generate_session()` will receive an integer instead of a string, and authentication will fail for every Zerodha user.

**Fix:**
```python
access_token = await asyncio.to_thread(
    exchange_request_token,
    current_user.id,     # user_id first
    request_token,       # request_token second
)
```

---

### BUG-3 · Nuvama login endpoint calls non-existent function

**Severity:** 🔴 Fatal  
**Location:** `app/api/v1/endpoints/broker.py` line ~244  

**Called:**
```python
from app.brokers.nuvama.auth import authenticate_with_request_id
```

**Actual function name in `nuvama/auth.py`:**
```python
def exchange_request_id(user_id: int, request_id: str) -> str:
```

**Impact:** Every Nuvama login attempt will immediately raise `ImportError: cannot import name 'authenticate_with_request_id'`.

**Fix:** Change the import and the call:
```python
from app.brokers.nuvama.auth import exchange_request_id

access_token = await asyncio.to_thread(
    exchange_request_id,
    current_user.id,
    payload.request_id,
)
```

---

### BUG-4 · Sync SQLAlchemy engine receives an `asyncpg://` URL

**Severity:** 🔴 Fatal  
**Location:** `app/core/database.py` and `app/core/config.py`

The default `DATABASE_URL` in `config.py` is:
```
postgresql+asyncpg://piyu:%40password@localhost:5432/piyushtrade
```

This URL is passed directly to the **sync** `create_engine()`. The sync engine requires `psycopg2`, not `asyncpg`. The URL swap logic in `database.py` only handles `postgresql+psycopg2://` and `postgresql://` — it does not handle the case where the URL already contains `asyncpg`.

**Impact:** `auth.py` and `security.py` use the sync engine via `get_db()`. On startup, `create_engine()` will attempt to import `asyncpg` as a synchronous dialect driver (it isn't one) and fail. Login and registration will be completely broken.

**Fix:** Set the default DATABASE_URL to a psycopg2 URL:
```python
DATABASE_URL: str = "postgresql+psycopg2://piyu:%40password@localhost:5432/piyushtrade"
```
Or update the swap logic to strip the `asyncpg` prefix:
```python
_async_url = settings.DATABASE_URL
if "asyncpg" not in _async_url:
    _async_url = _async_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")
    _async_url = _async_url.replace("postgresql://", "postgresql+asyncpg://")

_sync_url = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
engine = create_engine(_sync_url, ...)
```
Also note that `asyncpg` is not in `requirements.txt` — it must be added.

---

## Section 2 — Moderate Issues (Incorrect Behaviour, Not Immediate Crashes)

### ISSUE-1 · Reconciliation SQL `GROUP BY` is incorrect

**Location:** `app/services/reconciliation_service.py` — `_fetch_db_positions()`

The query groups by `(o.instrument, o.side)` and computes `SUM(fill_qty * sign)` per group. This means a position that is both bought and sold (fully closed) produces **two separate rows** — one BUY row with a positive sum, one SELL row with a negative sum — rather than a single net-zero row that would be excluded. When these two rows are written to the `db_positions` dict using the same `_pos_key(row.instrument, "NSE", "NRML")`, the second row silently overwrites the first.

**Impact:** Reconciliation will report false discrepancies for any position that has both buy and sell fills. Flat (closed) positions will incorrectly appear as open.

**Fix:** Remove `o.side` from the `GROUP BY` and the `SELECT` — it should not be part of the key. The signed sum across all sides gives the net quantity naturally:

```sql
SELECT
    o.instrument,
    SUM(
        CASE WHEN o.side = 'BUY' THEN t.fill_qty ELSE -t.fill_qty END
    ) AS net_qty
FROM trades t
JOIN orders o ON t.order_id = o.id
WHERE t.user_id = :user_id
GROUP BY o.instrument
HAVING SUM(
    CASE WHEN o.side = 'BUY' THEN t.fill_qty ELSE -t.fill_qty END
) != 0
```

---

### ISSUE-2 · `logger.bind()` is called on a stdlib `logging.Logger`

**Location:** `app/engines/execution_engine.py` line ~244

```python
log = logger.bind(
    user_id=user.id,
    strategy_id=strategy_id,
    ...
)
log.info("execution_engine_start")
```

`logger` is created by `get_structured_logger(__name__)`, which returns a standard `logging.Logger`. The stdlib `Logger` does not have a `.bind()` method — that is a `structlog` API. This will raise `AttributeError: 'Logger' object has no attribute 'bind'` the first time `engine.execute()` is called.

**Fix:** Replace the `logger.bind()` pattern with `extra={}` calls consistent with the rest of the codebase:

```python
log_ctx = {
    "user_id": user.id,
    "strategy_id": strategy_id,
    "symbol": order_request.trading_symbol,
    "exchange": order_request.exchange.value,
    "side": order_request.side.value,
    "quantity": order_request.quantity,
}
logger.info("execution_engine_start", extra={"event": "execution_engine_start", **log_ctx})
```

---

### ISSUE-3 · Zerodha callback writes token to Redis twice

**Location:** `app/api/v1/endpoints/broker.py` — `zerodha_callback()`

After calling `exchange_request_token()` (which already stores the token in Redis via `zerodha/auth.py`), the endpoint does a second `redis.set()` to the same key. This is redundant but not harmful in isolation. The real risk is that `broker.py` uses `_ZERODHA_TOKEN_KEY = "zerodha:access_token:{user_id}"` (formatted string) while `auth.py` uses `f"zerodha:access_token:{user_id}"` (f-string) — the formats happen to match in this case, but having two places write to the same key with separate TTL logic is fragile.

**Fix:** Remove the duplicate Redis write from `broker.py`. The auth module is the single owner of token storage. The endpoint's sole job is calling `exchange_request_token` and returning the response.

---

### ISSUE-4 · `risk_manager` calls broker methods synchronously inside an async context

**Location:** `app/engines/risk_manager.py` — `_check_daily_loss_limit()` and `_check_margin()`

```python
broker = BrokerFactory.get(user)
positions = broker.get_positions()       # SYNC — blocks the event loop
margin = broker.get_margins()            # SYNC — blocks the event loop
```

These are sync SDK calls made directly inside `async def` functions with no `asyncio.to_thread()` wrapper. The execution engine correctly wraps `broker.place_order()` in `to_thread()`, but the risk manager does not apply the same pattern to its two broker calls.

**Impact:** Under load, these blocking calls (which may take 200–500ms each on Zerodha/Nuvama) will block the entire event loop, degrading all concurrent requests.

**Fix:**
```python
positions = await asyncio.to_thread(broker.get_positions)
margin = await asyncio.to_thread(broker.get_margins)
```

---

### ISSUE-5 · Celery beat schedule for reconciliation is missing from `config.py`

**Location:** `app/core/config.py`  

The handoff doc (Section 2) explicitly flags this as a known pending item. The `reconcile_positions_task` task exists and is correctly decorated, but there is no `beat_schedule` entry in `config.py` to fire it. The task will never run automatically.

**Fix:** Add to `Settings` or to the Celery app configuration in `worker.py`:
```python
from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    "reconcile-positions": {
        "task": "tasks.reconcile_positions",
        "schedule": crontab(minute="*/5"),
    },
}
```

---

### ISSUE-6 · Reconciliation task not registered in Celery autodiscover

**Location:** `worker/worker.py`

```python
celery_app.autodiscover_tasks([
    "app.tasks.backtest_tasks",
    "app.tasks.ingestion_tasks",
    # reconciliation_service is missing
])
```

The `reconcile_positions_task` lives in `app.services.reconciliation_service`, but that module is not in the autodiscover list. Celery will not find the task.

**Fix:** Add it:
```python
celery_app.autodiscover_tasks([
    "app.tasks.backtest_tasks",
    "app.tasks.ingestion_tasks",
    "app.services.reconciliation_service",
])
```

---

### ISSUE-7 · `from __future__ import annotations` present in `portfolio.py` and `broker.py`

**Location:** `app/api/v1/endpoints/portfolio.py` and `broker.py`

The handoff doc (Section 6) explicitly lists this as a rule violation — `from __future__ import annotations` breaks Pylance type resolution in endpoint files. These two endpoint files have it; `orders.py` and `trades.py` (correctly) do not. Note that `broker.py` also imports `Optional` from `typing` which is no longer needed with this import, making it doubly inconsistent.

**Fix:** Remove the `from __future__ import annotations` line from both endpoint files.

---

## Section 3 — Architecture Review

### Execution Pipeline

The locked execution order (`risk_manager → execution_guard → BrokerFactory → mapper → broker → DB`) is correctly implemented in `execution_engine.py` with no deviations. The pipeline cannot be bypassed — there is no alternative code path that skips any step. This is one of the strongest aspects of the codebase.

The critical-path failure handling (broker success + DB failure → log CRITICAL + return result, let reconciliation catch it) is correct and well-reasoned. The decision not to attempt a broker cancel on DB failure is sound — a cancel attempt that itself fails would leave the position in an unknown state.

**One concern:** The idempotency key is built using `datetime.now(timezone.utc)` inside `_build_idempotency_key()`, but the `order_request.idempotency_key` field is set before the Redis check. If the Redis SET NX fails (network timeout), the key is already stamped on the request but the claim wasn't made. A retry within the same minute will produce the same key and correctly be allowed through — this is the intended behaviour. However, if the Redis network is down entirely, `_redis_claim_idempotency_key()` will raise an unhandled exception, bubbling up as an `ExecutionError`. This is acceptable but worth documenting explicitly.

### Multi-Tenancy

`user_id` FK is present on all financial tables. Every query in every endpoint and service scopes to `current_user.id` — there are no cross-user data leaks. The strategy loader in the execution engine enforces user scoping at the query level, not just at the API layer. This is correct.

### Redis Discipline

The codebase correctly treats Redis as cache/operational state only. Financial truth lives exclusively in PostgreSQL. Circuit breaker state, idempotency keys, and broker tokens are all appropriate Redis uses. No financial data (PnL, fills, positions) is ever sourced from Redis. The `options_service.py` staleness logic correctly degrades to a 503 rather than serving stale data past the threshold. All of this is architecturally correct.

### Broker Abstraction

The `BaseBroker` contract is clean. All broker-specific knowledge (field names, SDK exceptions, response shapes) is confined to the `mapper.py` files. The `_handle_*_exception` pattern in both clients correctly maps SDK exceptions to the PiyushTrade hierarchy so the execution engine never sees broker-specific types. The Nuvama `_normalise_response()` correctly handles the HTTP 200 + `status="error"` pattern. The deferred import pattern in `factory.py` and both clients correctly prevents optional SDK `ImportError` at startup.

One note: `get_order_status()` on both brokers fetches the full order book and filters in Python. This is documented as acceptable for V1 (< 200 orders/day). For V2 with higher volume, a broker-side filter or a direct order fetch endpoint should be evaluated.

### Security

**JWT:** Uses `python-jose` with HS256 and a configurable secret. The `SECRET_KEY` default is `"CHANGE_ME_IN_PRODUCTION_USE_32_BYTES_MIN"` — this will be used in production if `.env` is not set. There is no startup assertion that validates `SECRET_KEY != default`. This must be caught by deployment checklist.

**Password hashing:** `bcrypt` pinned at 4.0.1. The `passlib` incompatibility note is documented everywhere it matters. Do not change this.

**`get_current_user` uses sync DB:** The auth dependency uses the sync engine (`get_db`). This means async Phase 3 endpoints that depend on `get_current_user` will open a sync connection for the auth check and an async connection for data. This creates two connections per request. It is not incorrect but is slightly inefficient. For V1 with low concurrency, this is fine. For V2, consider a fully async auth path.

**CORS:** `allow_origins=["http://localhost:3000"]` is hardcoded. This must be parameterised via an environment variable before production deployment. A wildcard CORS policy with `allow_credentials=True` would be a security vulnerability.

### Logging

The structured JSON logger is well-designed. UTC timestamps, consistent event keys, and contextual IDs (user_id, strategy_id) are present throughout. The `configure_root_logging()` call in `main.py` at import time (before other imports) is the correct pattern.

One issue: `execution_engine.py` uses `logger.bind()` (structlog API) while every other file uses `extra={}` (stdlib API). This inconsistency will cause a runtime crash as noted in ISSUE-2.

### Alembic / Schema Management

No `create_all()` calls exist anywhere in the codebase — all schema changes go through Alembic. This rule is correctly enforced. All models import from `app.core.database.Base`. The `alembic/env.py` imports all models for autogenerate to work.

Two pending items from the handoff doc remain unaddressed:
1. Migration `0006_add_reconciliation_logs` has not been run.
2. `ReconciliationLog` ORM model is not imported in `app/models/__init__.py` (it likely does not exist yet as a file).

### Celery Worker

The worker configuration is correct: `task_acks_late=True`, `worker_prefetch_multiplier=1`, JSON serialisation. The `result_expires=3600` is appropriate — task results are for debugging only, not financial truth.

The `timezone="Asia/Kolkata"` with `enable_utc=True` is correct — Celery schedules in IST but stores timestamps in UTC.

### Data Layer

`pool_pre_ping=True` on both engines prevents stale connection errors after periods of inactivity. `pool_recycle=3600` correctly rotates connections every hour. `expire_on_commit=False` on the async session is correct for FastAPI's request-scoped session pattern.

---

## Section 4 — Handoff TODO Status

Cross-referencing against Section 2 of the handoff doc:

| Item | Status |
|------|--------|
| Migration `0006_add_reconciliation_logs` | ❌ Not run, no ORM model file exists |
| `base_broker.py` — add `strike`/`expiry` to `OrderRequest` | ❌ Not applied (fields absent from dataclass) |
| `execution_engine.py` — fix Order() constructor field names | ✅ Correctly uses `instrument`, `side`, `qty`, `strike`, `expiry` |
| `reconciliation_service.py` — fix worker import | ✅ Uses `from worker.worker import celery_app` (correct) |
| `config.py` — add Celery beat schedule for reconciliation | ❌ Not added (ISSUE-5 above) |
| `models/__init__.py` — import ReconciliationLog | ❌ Model file doesn't exist yet |

---

## Section 5 — Pre-Launch Checklist

Before connecting live broker credentials and placing real orders, the following must be complete:

1. **Restore `app/models/order.py` and `app/models/trade.py`** with correct ORM content (BUG-1)
2. **Fix Zerodha callback argument order** (BUG-2)
3. **Fix Nuvama login function name** (BUG-3)
4. **Fix sync engine URL** and add `asyncpg` to `requirements.txt` (BUG-4)
5. **Fix `logger.bind()` in execution_engine.py** (ISSUE-2)
6. **Fix reconciliation SQL GROUP BY** (ISSUE-1)
7. **Add Celery beat schedule** for reconciliation (ISSUE-5)
8. **Add reconciliation_service to Celery autodiscover** (ISSUE-6)
9. **Wrap risk_manager broker calls in `asyncio.to_thread()`** (ISSUE-4)
10. **Add `strike` and `expiry` fields to `OrderRequest` dataclass** in `base_broker.py`
11. **Create `ReconciliationLog` ORM model** and migration `0006`
12. **Run `alembic upgrade head`**
13. **Parameterise `CORS allow_origins`** via environment variable
14. **Assert `SECRET_KEY != default`** at startup in `config.py`
15. **Add broker env vars** to `.env`: `ZERODHA_API_KEY`, `ZERODHA_API_SECRET`, `NUVAMA_API_KEY`, `NUVAMA_API_SECRET`
16. **Create empty `settings.ini`** at project root for Nuvama SDK

---

## Section 6 — Open Architecture Questions

These are not bugs but are worth deciding before V2:

**Q1: `get_current_user` is fully sync.** All async Phase 3 endpoints use it, creating a mixed sync/async DB pattern per request. For V1 concurrency this is fine. If you expect > 50 concurrent users placing orders simultaneously, consider an async `get_current_user` using `get_async_db`.

**Q2: Zerodha daily token refresh.** The handoff doc lists this as Open Question #1. With a 24h Redis TTL and Zerodha rotating tokens at ~06:00 IST, the window between rotation and user re-auth could leave orders blocked for up to several hours. Consider a background job that detects expired tokens and notifies users rather than having them discover it at order time.

**Q3: `OrderRequest.strike` and `expiry` are missing from the dataclass.** The execution engine correctly sets these on the `Order()` row, but `OrderRequest` has no `strike` or `expiry` fields — meaning the engine reads them from `order_request.strike` which doesn't exist. This will raise `AttributeError` at the DB write step. The `base_broker.py` update (handoff Section 2) must happen before any order can be persisted.

**Q4: Reconciliation position key hardcodes `"NSE"` and `"NRML"`.** The `_fetch_db_positions()` key is `_pos_key(row.instrument, "NSE", "NRML")` for all rows. If you ever trade on BSE, BFO, or with MIS product code, these positions will never match broker positions (which use the actual exchange/product values). Add `exchange` and `product_code` to the `Order` model and join them into this query.

---

*PiyushTrade Architecture Review — Confidential*
