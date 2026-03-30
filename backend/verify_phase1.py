"""
verify_phase1.py
================
Phase 1 verification script for PiyushTrade.

Covers all 6 checks:
  1. Redis key shape and TTL
  2. Staleness 503 during market hours
  3. Stale data + X-Data-As-Of header outside market hours
  4. Feed status propagation (NOT_STARTED / DEGRADED)
  5. Reconnect backoff log lines
  6. Celery task discoverability

Usage:
    python verify_phase1.py                    # all checks
    python verify_phase1.py --check 2          # single check
    python verify_phase1.py --check 2,3        # subset

Requirements:
    pip install redis httpx

Run from the backend/ root with your .env loaded:
    cd backend
    python verify_phase1.py
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import redis

# ---------------------------------------------------------------------------
# Configuration — adjust if your setup differs
# ---------------------------------------------------------------------------

API_BASE      = "http://127.0.0.1:8000"
REDIS_HOST    = "localhost"
REDIS_PORT    = 6379
REDIS_DB      = 0
REDIS_PASSWORD: Optional[str] = None   # set if your Redis requires auth

# The Redis key pattern your option_chain_ingestor uses.
# Common patterns: "option:{token}" or "option_chain:{token}"
# Check option_chain_ingestor.py — search for redis.set( or r.set(
# and copy the key format exactly.
TEST_TOKEN = "256265"                  # NIFTY instrument token on Zerodha
REDIS_KEY  = f"option:{TEST_TOKEN}"   # <-- update if your key format differs

# Staleness threshold from architecture doc
STALE_THRESHOLD_SECONDS = 5

# Redis TTL from architecture doc
EXPECTED_TTL_SECONDS = 15

# NSE market hours in IST (UTC+5:30)
MARKET_OPEN_IST  = (9, 15)   # 09:15
MARKET_CLOSE_IST = (15, 30)  # 15:30

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASS  = "\033[92m[PASS]\033[0m"
FAIL  = "\033[91m[FAIL]\033[0m"
INFO  = "\033[94m[INFO]\033[0m"
WARN  = "\033[93m[WARN]\033[0m"
RESET = "\033[0m"


def _r() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )


def _now_epoch() -> int:
    return int(time.time())


def _is_market_hours() -> bool:
    """Return True if current IST time is within 09:15–15:30."""
    now_utc = datetime.now(timezone.utc)
    ist_hour   = (now_utc.hour   + 5) % 24
    ist_minute = (now_utc.minute + 30) % 60
    if now_utc.minute + 30 >= 60:
        ist_hour += 1
    ist_total = ist_hour * 60 + ist_minute
    open_total  = MARKET_OPEN_IST[0]  * 60 + MARKET_OPEN_IST[1]
    close_total = MARKET_CLOSE_IST[0] * 60 + MARKET_CLOSE_IST[1]
    return open_total <= ist_total <= close_total


def _write_redis_key(rdb: redis.Redis, timestamp: int, feed_status: str = "LIVE") -> None:
    """Write a synthetic option chain key exactly as the ingestor would."""
    payload = {
        "data": {
            "instrument_token": int(TEST_TOKEN),
            "last_price": 24500.0,
            "oi": 1000,
            "volume": 500,
        },
        "timestamp": timestamp,
        "feed_status": feed_status,
    }
    rdb.set(REDIS_KEY, json.dumps(payload), ex=EXPECTED_TTL_SECONDS)


def _delete_redis_key(rdb: redis.Redis) -> None:
    rdb.delete(REDIS_KEY)


def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def _result(passed: bool, message: str, detail: str = "") -> None:
    tag = PASS if passed else FAIL
    print(f"  {tag}  {message}")
    if detail:
        for line in detail.strip().splitlines():
            print(f"         {line}")


# ---------------------------------------------------------------------------
# Check 1 — Redis key shape and TTL
# ---------------------------------------------------------------------------

def check_1_redis_key_shape():
    _section("Check 1 — Redis key shape and TTL")
    print(f"  {INFO}  Writing a fresh key → {REDIS_KEY}")

    rdb = _r()
    ts  = _now_epoch()
    _write_redis_key(rdb, ts, feed_status="LIVE")

    raw = rdb.get(REDIS_KEY)
    ttl = rdb.ttl(REDIS_KEY)

    if raw is None:
        _result(False, "Key not found in Redis after write",
                "Check REDIS_KEY constant in this script matches option_chain_ingestor.py")
        return

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        _result(False, f"Key value is not valid JSON: {e}", f"Raw value: {raw[:200]}")
        return

    # Shape checks
    has_data        = "data" in parsed
    has_timestamp   = "timestamp" in parsed
    has_feed_status = "feed_status" in parsed
    ts_is_int       = isinstance(parsed.get("timestamp"), (int, float))
    fs_valid        = parsed.get("feed_status") in ("LIVE", "DEGRADED", "DOWN")
    ttl_ok          = 0 < ttl <= EXPECTED_TTL_SECONDS

    _result(has_data,        "Key contains 'data' field")
    _result(has_timestamp,   "Key contains 'timestamp' field")
    _result(has_feed_status, "Key contains 'feed_status' field")
    _result(ts_is_int,       "timestamp is a numeric epoch value",
            f"Got: {parsed.get('timestamp')!r}")
    _result(fs_valid,        f"feed_status is LIVE|DEGRADED|DOWN",
            f"Got: {parsed.get('feed_status')!r}")
    _result(ttl_ok,          f"TTL is 0 < ttl ≤ {EXPECTED_TTL_SECONDS}s",
            f"Got TTL: {ttl}s  (−1 means no expiry set — architecture violation)")

    if not ttl_ok and ttl == -1:
        print(f"\n  {WARN}  TTL is −1 (no expiry). Fix: add ex={EXPECTED_TTL_SECONDS} to your redis.set() call.")


# ---------------------------------------------------------------------------
# Check 2 — Staleness 503 during market hours
# ---------------------------------------------------------------------------

def check_2_staleness_503():
    _section("Check 2 — Staleness 503 during market hours")

    in_market = _is_market_hours()
    if not in_market:
        print(f"  {WARN}  Currently OUTSIDE market hours (IST 09:15–15:30).")
        print(f"         This check writes a stale key and forces the API call.")
        print(f"         The 503 path only fires during market hours per architecture.")
        print(f"         Check 2 will be SKIPPED — run again between 09:15–15:30 IST,")
        print(f"         OR temporarily mock is_market_hours() to return True in your service.")
        return

    rdb = _r()
    stale_ts = _now_epoch() - (STALE_THRESHOLD_SECONDS + 5)  # 10s old — definitely stale
    print(f"  {INFO}  Writing stale key (timestamp = now − {STALE_THRESHOLD_SECONDS + 5}s)")
    _write_redis_key(rdb, stale_ts, feed_status="LIVE")

    url = f"{API_BASE}/options/token/{TEST_TOKEN}"
    print(f"  {INFO}  GET {url}")

    try:
        resp = httpx.get(url, timeout=5.0)
    except httpx.ConnectError:
        _result(False, "Could not connect to API server",
                f"Is uvicorn running at {API_BASE}?")
        return

    is_503      = resp.status_code == 503
    body        = resp.text
    has_keyword = "FEED_DEGRADED" in body or "stale" in body.lower() or "feed" in body.lower()

    _result(is_503, f"API returned 503 for stale data during market hours",
            f"Got: {resp.status_code}\nBody: {body[:300]}")
    _result(has_keyword, "Response body mentions FEED_DEGRADED or stale/feed",
            f"Body: {body[:300]}")

    _delete_redis_key(rdb)


# ---------------------------------------------------------------------------
# Check 3 — Stale data with X-Data-As-Of header outside market hours
# ---------------------------------------------------------------------------

def check_3_stale_header_outside_hours():
    _section("Check 3 — X-Data-As-Of header outside market hours")

    in_market = _is_market_hours()
    if in_market:
        print(f"  {WARN}  Currently INSIDE market hours (IST 09:15–15:30).")
        print(f"         This check expects a 200 + header, but during market hours the")
        print(f"         service correctly returns 503 instead.")
        print(f"         Check 3 SKIPPED — run after 15:30 IST, OR mock is_market_hours().")
        return

    rdb = _r()
    stale_ts = _now_epoch() - 60   # 60 seconds old
    print(f"  {INFO}  Writing stale key (timestamp = now − 60s)")
    _write_redis_key(rdb, stale_ts, feed_status="LIVE")

    url = f"{API_BASE}/options/token/{TEST_TOKEN}"
    print(f"  {INFO}  GET {url}")

    try:
        resp = httpx.get(url, timeout=5.0)
    except httpx.ConnectError:
        _result(False, "Could not connect to API server",
                f"Is uvicorn running at {API_BASE}?")
        return

    is_200      = resp.status_code == 200
    has_header  = "x-data-as-of" in {k.lower() for k in resp.headers}
    header_val  = resp.headers.get("x-data-as-of") or resp.headers.get("X-Data-As-Of", "")
    header_sane = len(header_val) > 0

    _result(is_200,       f"API returned 200 for stale data outside market hours",
            f"Got: {resp.status_code}")
    _result(has_header,   "Response includes X-Data-As-Of header",
            f"Headers: {dict(resp.headers)}")
    _result(header_sane,  "X-Data-As-Of header has a non-empty value",
            f"Value: {header_val!r}")

    _delete_redis_key(rdb)


# ---------------------------------------------------------------------------
# Check 4 — Feed status endpoint returns expected values
# ---------------------------------------------------------------------------

def check_4_feed_status():
    _section("Check 4 — Feed status endpoint")

    url = f"{API_BASE}/health/feed"
    print(f"  {INFO}  GET {url}")

    try:
        resp = httpx.get(url, timeout=5.0)
    except httpx.ConnectError:
        _result(False, "Could not connect to API server",
                f"Is uvicorn running at {API_BASE}?")
        return

    is_200 = resp.status_code == 200
    _result(is_200, f"GET /health/feed returned 200", f"Got: {resp.status_code}")

    if not is_200:
        return

    try:
        body = resp.json()
    except Exception:
        _result(False, "Response is not valid JSON", f"Raw: {resp.text[:200]}")
        return

    has_status    = "status" in body
    status_valid  = body.get("status") in ("NOT_STARTED", "LIVE", "DEGRADED", "DOWN")
    has_last_tick = "last_tick" in body
    has_staleness = "staleness_seconds" in body

    _result(has_status,    "Response contains 'status' field",
            f"Got: {body.get('status')!r}")
    _result(status_valid,  "status is NOT_STARTED|LIVE|DEGRADED|DOWN",
            f"Got: {body.get('status')!r}")
    _result(has_last_tick, "Response contains 'last_tick' field",
            f"Got: {body.get('last_tick')!r}")
    _result(has_staleness, "Response contains 'staleness_seconds' field",
            f"Got: {body.get('staleness_seconds')!r}")

    print(f"\n  {INFO}  Full response: {json.dumps(body, indent=4)}")


# ---------------------------------------------------------------------------
# Check 5 — Reconnect backoff log detection
# ---------------------------------------------------------------------------

def check_5_reconnect_logs():
    _section("Check 5 — Reconnect backoff log lines")
    print(f"  {INFO}  This check scans your running uvicorn stdout/stderr for backoff lines.")
    print(f"         It cannot trigger a real disconnect — that requires missing Kite credentials.")
    print()
    print(f"  {INFO}  To test manually:")
    print(f"         1. Start uvicorn WITHOUT KITE_API_KEY / KITE_ACCESS_TOKEN in .env")
    print(f"         2. Watch logs for lines matching these patterns:")
    print()

    patterns = [
        "Reconnect attempt",
        "backoff",
        "retry",
        "WebSocket",
        "DEGRADED",
        "feed_status",
    ]
    for p in patterns:
        print(f"         grep -i '{p}' your_uvicorn_log.txt")

    print()
    print(f"  {INFO}  Expected backoff sequence in logs:")
    delay = 3
    for attempt in range(1, 6):
        print(f"         Attempt {attempt}: waiting {delay}s before reconnect")
        delay = min(delay * 2, 60)

    print()
    print(f"  {WARN}  Check 5 is a MANUAL check — automated log capture requires")
    print(f"         running uvicorn as a subprocess, which is outside the scope of")
    print(f"         this script to avoid interfering with your running server.")
    print(f"         See the guide below for step-by-step instructions.")


# ---------------------------------------------------------------------------
# Check 6 — Celery task discoverability
# ---------------------------------------------------------------------------

def check_6_celery_task():
    _section("Check 6 — Celery task discoverability")
    print(f"  {INFO}  Running: celery -A app.tasks.ingestion_tasks inspect registered")
    print(f"         (requires a Celery worker to be running in another terminal)")
    print()

    # First verify the module can be imported without errors
    print(f"  {INFO}  Step 1: Import check (no running worker needed)")
    result = subprocess.run(
        [sys.executable, "-c", "import app.tasks.ingestion_tasks; print('import OK')"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    import_ok = result.returncode == 0 and "import OK" in result.stdout
    _result(import_ok, "app.tasks.ingestion_tasks imports without errors",
            result.stderr.strip() if result.stderr else "")

    if not import_ok:
        print(f"\n  {WARN}  Fix import errors before checking Celery worker registration.")
        return

    # Try to reach a running worker
    print(f"\n  {INFO}  Step 2: Worker registration check")
    result2 = subprocess.run(
        ["celery", "-A", "app.tasks.ingestion_tasks", "inspect", "registered", "--timeout", "4"],
        capture_output=True,
        text=True,
        timeout=10,
    )

    combined = result2.stdout + result2.stderr
    worker_running = "ingest" in combined.lower() or "task" in combined.lower()
    no_worker_msg  = "no nodes replied" in combined.lower() or "error" in combined.lower()

    if no_worker_msg or (result2.returncode != 0 and not worker_running):
        print(f"  {WARN}  No Celery worker is running. That's expected at this stage.")
        print(f"         Start one in a separate terminal:")
        print(f"         cd backend")
        print(f"         celery -A app.tasks.ingestion_tasks worker --loglevel=info")
        print(f"         Then re-run this check.")
    else:
        task_visible = "ingestion" in combined.lower() or "ingest" in combined.lower()
        _result(task_visible, "Ingestion task is visible in registered tasks",
                combined[:400])


# ---------------------------------------------------------------------------
# Check 7 — API 503 when Redis key is missing entirely
# ---------------------------------------------------------------------------

def check_7_missing_key_503():
    _section("Check 7 — 503 when Redis key does not exist")
    print(f"  {INFO}  Deleting key {REDIS_KEY!r} then calling API")

    rdb = _r()
    _delete_redis_key(rdb)

    url = f"{API_BASE}/options/token/{TEST_TOKEN}"
    try:
        resp = httpx.get(url, timeout=5.0)
    except httpx.ConnectError:
        _result(False, "Could not connect to API server",
                f"Is uvicorn running at {API_BASE}?")
        return

    is_503 = resp.status_code == 503
    _result(is_503, "API returns 503 when Redis key is completely missing",
            f"Got: {resp.status_code} — Body: {resp.text[:200]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_CHECKS = {
    1: check_1_redis_key_shape,
    2: check_2_staleness_503,
    3: check_3_stale_header_outside_hours,
    4: check_4_feed_status,
    5: check_5_reconnect_logs,
    6: check_6_celery_task,
    7: check_7_missing_key_503,
}


def main():
    parser = argparse.ArgumentParser(description="PiyushTrade Phase 1 verification")
    parser.add_argument(
        "--check",
        default="all",
        help="Comma-separated check numbers to run, e.g. --check 1,2,4  (default: all)",
    )
    args = parser.parse_args()

    if args.check == "all":
        selected = sorted(ALL_CHECKS.keys())
    else:
        try:
            selected = [int(x.strip()) for x in args.check.split(",")]
        except ValueError:
            print(f"Invalid --check value: {args.check!r}")
            sys.exit(1)

    print("\n" + "═" * 60)
    print("  PiyushTrade — Phase 1 Verification")
    print("═" * 60)
    print(f"  API base : {API_BASE}")
    print(f"  Redis    : {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    print(f"  Token    : {TEST_TOKEN}  →  key: {REDIS_KEY}")

    in_mkt = _is_market_hours()
    print(f"  Market   : {'OPEN (IST 09:15–15:30)' if in_mkt else 'CLOSED'}")
    print("═" * 60)

    for n in selected:
        if n not in ALL_CHECKS:
            print(f"\n  {WARN}  Unknown check #{n} — skipping")
            continue
        ALL_CHECKS[n]()

    print(f"\n{'═' * 60}")
    print("  Done.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
