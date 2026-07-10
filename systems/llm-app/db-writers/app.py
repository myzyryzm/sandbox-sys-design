"""db-writers — persistence readers for llm-worker's generations (sandbox-llm-persistence).

A claim loop XREADGROUPs the worker's `runs:started` announcements on the shared
llm-worker-stream redis (group READER_GROUP, consumer SERVICE_ID — unique per member,
so the group divides runs across members, one reader per run). For each claimed
run_id it accumulates the run's typed token stream `tokens:<run_id>` (entries
{type: token|done|error, text} — dispatch keys off type, never the text), then writes
the joined output into chat-db as the run's ASSISTANT message row:

  run_id == user_message.id == the assistant message id, so the row is
  INSERT INTO message (id=run_id, chat_id=<from the user_message row>,
  content=<accumulated text>, role='assistant') ON CONFLICT (id) DO NOTHING —
  idempotent per run (a crash after persist but before XACK redelivers the run).

Status: done entry -> complete; error entry -> failed; ~30s without a new entry ->
partial (covers dropped runs and already-expired token streams). Per the registry
description, partial/failed outputs carry a " [partial]" / " [failed]" suffix. The
announcement is XACKed only AFTER persisting.

The Prometheus instrumentation is hand-written (no auto-instrumentor), plus the
persistence metrics contract the manifest's cards read:
persistence_runs_total{status} and persistence_active_runs.
"""

import os
import threading
import time

import psycopg
import redis
from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

SERVICE_ID = os.environ.get("SERVICE_ID", "db-writers")
REDIS_HOST = os.environ.get("REDIS_HOST", "llm-worker-stream")
ANNOUNCE = os.environ.get("ANNOUNCE_STREAM", "runs:started")
GROUP = os.environ.get("READER_GROUP", "db-writers")
DB_NODE = os.environ.get("DB_NODE", "chat-db")
DB_DSN = f"postgresql://sandbox:sandbox@{DB_NODE}:5432/{DB_NODE.replace('-', '_')}"

BLOCK_MS = 5000  # per XREADGROUP block; also the idle-probe cadence
IDLE_LIMIT = 6  # ~30s without a new token entry -> persist partial
TOKENS_TTL_S = 600  # re-arm the token stream's expiry after reading (mkstream leak guard)

# ---------------------------------------------------------------------------
# Metric definitions — the generic HTTP set (manifest req/s, p95, in-flight,
# errors cards) plus the persistence contract (persisted / active cards).
# ---------------------------------------------------------------------------

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests processed",
    ["method", "endpoint", "status"],
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)
http_requests_in_flight = Gauge(
    "http_requests_in_flight",
    "Number of HTTP requests currently in flight",
)
EXCLUDED_PATHS = {"/metrics"}

persistence_runs_total = Counter(
    "persistence_runs_total",
    "Runs persisted to the database, by outcome",
    ["status"],  # complete | partial | failed
)
persistence_active_runs = Gauge(
    "persistence_active_runs",
    "Runs currently being accumulated from their token stream",
)

_redis = redis.Redis(host=REDIS_HOST, port=6379)

# Live counters for GET /reader/state (the Readers tab polls this through the lb).
_state_lock = threading.Lock()
_counts = {"complete": 0, "partial": 0, "failed": 0}
_active = 0
_last_run = None


def _ensure_group(stream):
    """Create the consumer group on `stream` if it doesn't exist (mkstream=True:
    an expired/missing key is recreated empty rather than erroring)."""
    try:
        _redis.xgroup_create(stream, GROUP, id="0", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _persist(run_id, text, status):
    """Write the accumulated output as the run's assistant message row. Idempotent
    per run_id (the row id IS the run id) so announcement redelivery is safe."""
    global _last_run
    suffix = "" if status == "complete" else f" [{status}]"
    try:
        with psycopg.connect(DB_DSN, connect_timeout=5) as conn:
            row = conn.execute(
                "SELECT chat_id FROM user_message WHERE id = %s", (int(run_id),)
            ).fetchone()
            if row is None:
                # The run's user_message row is gone — nothing to attach the reply to.
                status = "failed"
            else:
                conn.execute(
                    "INSERT INTO message (id, chat_id, content, role)"
                    " VALUES (%s, %s, %s, 'assistant')"
                    " ON CONFLICT (id) DO NOTHING",
                    (int(run_id), row[0], text + suffix),
                )
            conn.commit()
    except Exception as exc:
        print(f"[{SERVICE_ID}] persist failed for run {run_id}: {exc}", flush=True)
        raise  # leave the announcement un-acked: it stays claimable for a retry
    persistence_runs_total.labels(status=status).inc()
    with _state_lock:
        _counts[status] += 1
        _last_run = {"run_id": run_id, "status": status, "chars": len(text)}
    print(f"[{SERVICE_ID}] persisted run {run_id}: {status}, {len(text)} chars", flush=True)


def _read_full_message(run_id):
    """Accumulate tokens:<run_id> through the group until its done/error entry (or
    ~30s of silence), then persist. Always re-arms the key's expiry on the way out —
    _ensure_group's mkstream may have recreated an already-expired stream."""
    global _active
    stream = f"tokens:{run_id}"
    _ensure_group(stream)
    buffer = []
    idle = 0
    persistence_active_runs.inc()
    with _state_lock:
        _active += 1
    try:
        while True:
            resp = _redis.xreadgroup(GROUP, SERVICE_ID, {stream: ">"}, count=10, block=BLOCK_MS)
            entries = resp[0][1] if resp else []
            if not entries:
                idle += 1
                if idle >= IDLE_LIMIT:  # ~30s with no progress: dropped or expired run
                    _persist(run_id, "".join(buffer), "partial")
                    return
                continue
            idle = 0
            for entry_id, fields in entries:
                etype = fields.get(b"type", b"").decode()
                text = fields.get(b"text", b"").decode()
                _redis.xack(stream, GROUP, entry_id)
                if etype == "token":
                    buffer.append(text)
                elif etype == "done":
                    _persist(run_id, "".join(buffer), "complete")
                    return
                elif etype == "error":
                    _persist(run_id, "".join(buffer), "failed")
                    return
                # unknown-shape entries: skip defensively
    finally:
        persistence_active_runs.dec()
        with _state_lock:
            _active -= 1
        try:
            _redis.expire(stream, TOKENS_TTL_S)
        except Exception:
            pass


def _handle_announcements(entries):
    for entry_id, fields in entries:
        run_id = fields.get(b"run_id", b"").decode()
        if run_id:
            _read_full_message(run_id)
        # Ack only AFTER persisting — a crash mid-run leaves the announcement
        # pending (re-read from the PEL on restart), so no run is silently lost.
        _redis.xack(ANNOUNCE, GROUP, entry_id)


def _claim_loop():
    while True:
        try:
            _ensure_group(ANNOUNCE)
            # First drain THIS consumer's pending entries (redeliveries from a
            # previous crash — id "0" reads the PEL instead of new entries).
            resp = _redis.xreadgroup(GROUP, SERVICE_ID, {ANNOUNCE: "0"}, count=10)
            if resp and resp[0][1]:
                _handle_announcements(resp[0][1])
            while True:
                resp = _redis.xreadgroup(GROUP, SERVICE_ID, {ANNOUNCE: ">"}, count=1, block=BLOCK_MS)
                if resp and resp[0][1]:
                    _handle_announcements(resp[0][1])
        except Exception as exc:
            print(f"[{SERVICE_ID}] claim loop error: {exc}", flush=True)
            time.sleep(2)  # redis/db down: keep last state, reconnect


threading.Thread(target=_claim_loop, daemon=True, name="claim").start()

app = FastAPI(title="sandbox service")


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Time each request and record all three metrics.

    Read this top-to-bottom to see the full lifecycle of a measured request.
    """
    path = request.url.path
    method = request.method

    if path in EXCLUDED_PATHS:
        return await call_next(request)

    # In-flight: up on entry, guaranteed back down on exit (even on error).
    http_requests_in_flight.inc()
    start = time.perf_counter()
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        # If the handler blew up, record it as a 500 before re-raising so the
        # metric still reflects what the client would have seen.
        status = 500
        raise
    finally:
        duration = time.perf_counter() - start
        http_request_duration_seconds.labels(method=method, endpoint=path).observe(
            duration
        )
        http_requests_total.labels(
            method=method, endpoint=path, status=str(status)
        ).inc()
        http_requests_in_flight.dec()

    return response


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/reader/state")
async def reader_state():
    """Control-plane introspection for the Readers tab / aggregate state route."""
    with _state_lock:
        counts = dict(_counts)
        active = _active
        last_run = _last_run
    return {
        "group": GROUP,
        "consumer": SERVICE_ID,
        "active": active,
        "persisted": sum(counts.values()),
        "counts": counts,
        "last_run": last_run,
    }


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
