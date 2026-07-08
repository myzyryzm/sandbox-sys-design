"""
FastAPI backend for the `hello-lb` sandbox system.

This is a learning tool: the Prometheus instrumentation is written by hand with
`prometheus_client` (no black-box auto-instrumentor) so you can read exactly how
each metric is produced. The interesting part is `metrics_middleware` below.

Endpoints:
  GET /health   -> {"status": "ok"}
  GET /metrics  -> Prometheus exposition format (the three metrics defined below)
"""

import os
import threading
import time

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from psycopg.rows import dict_row
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Metric definitions
#
# These three metrics are what the manifest's PromQL queries refer to. If you
# rename anything here, update systems/hello-lb/manifest.json to match.
# ---------------------------------------------------------------------------

# Total number of HTTP requests, broken down so we can compute rates and error
# ratios in PromQL (e.g. sum(rate(http_requests_total[1m]))).
http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests processed",
    ["method", "endpoint", "status"],
)

# Per-request latency. Default buckets are fine for Phase 1; histogram_quantile()
# in PromQL turns the *_bucket series into p50/p95/etc.
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
)

# Number of requests currently being handled (incremented on entry, decremented
# on exit). A Gauge because it goes up and down.
http_requests_in_flight = Gauge(
    "http_requests_in_flight",
    "Number of HTTP requests currently in flight",
)

# We do NOT instrument the /metrics endpoint itself. Prometheus scrapes it every
# few seconds, and counting those scrapes would pollute req/s and latency with
# traffic that isn't "real" application load. Excluding it keeps the manifest's
# req/s query reflecting actual traffic through the system.
EXCLUDED_PATHS = {"/metrics"}

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


# ---------------------------------------------------------------------------
# Chat creation (POST /chats)
#
# Talks to two downstream databases: user-db (to confirm the caller's user row
# exists) and chat-db (to insert the new chat). The caller identifies itself via
# the X-User-Id header rather than a request body — CreateChatRequest is empty.
#
# The handler is a sync `def` so FastAPI runs it in a thread pool, keeping the
# blocking psycopg calls off the event loop while the metrics middleware above
# still measures it like any other request.
# ---------------------------------------------------------------------------

USER_DB_DSN = os.environ.get(
    "USER_DB_DSN", "postgresql://sandbox:sandbox@user-db:5432/user_db"
)
CHAT_DB_DSN = os.environ.get(
    "CHAT_DB_DSN", "postgresql://sandbox:sandbox@chat-db:5432/chat_db"
)

DEFAULT_CHAT_TITLE = "new chat"


class CreateChatRequest(BaseModel):
    pass


class CreateChatResponse(BaseModel):
    id: int


# --- Snowflake id generation ------------------------------------------------
# Chat / User ids are snowflake ids (63-bit bigints): time component in the high
# bits, a machine id, then a per-millisecond sequence. Enough to mint sortable,
# collision-resistant ids for the chat rows without an external service.
_SNOWFLAKE_EPOCH_MS = 1_700_000_000_000  # custom epoch (~Nov 2023)
_MACHINE_ID = os.getpid() & 0x3FF  # 10 bits
_snowflake_lock = threading.Lock()
_snowflake_last_ms = 0
_snowflake_seq = 0


def next_snowflake() -> int:
    global _snowflake_last_ms, _snowflake_seq
    with _snowflake_lock:
        now = int(time.time() * 1000)
        if now == _snowflake_last_ms:
            _snowflake_seq = (_snowflake_seq + 1) & 0xFFF  # 12-bit sequence
            if _snowflake_seq == 0:
                # Sequence exhausted this ms: spin to the next millisecond.
                while now <= _snowflake_last_ms:
                    now = int(time.time() * 1000)
        else:
            _snowflake_seq = 0
        _snowflake_last_ms = now
        return (
            ((now - _SNOWFLAKE_EPOCH_MS) << 22)
            | (_MACHINE_ID << 12)
            | _snowflake_seq
        )


@app.post("/chats", response_model=CreateChatResponse)
def create_chat(
    body: CreateChatRequest,
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
):
    """Create a chat for the caller identified by the X-User-Id header.

    The user_id is taken from the header (not the body). We confirm that user
    exists in user-db, then insert a new chat row into chat-db with the default
    title. Returns the new chat's snowflake id.
    """
    if not x_user_id:
        raise HTTPException(status_code=400, detail="missing X-User-Id header")
    try:
        user_id = int(x_user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid X-User-Id header")

    # 1. Confirm the user exists (user-db).
    try:
        with psycopg.connect(
            USER_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            user = conn.execute(
                'SELECT id FROM "user" WHERE id = %s', (user_id,)
            ).fetchone()
    except psycopg.Error:
        raise HTTPException(status_code=503, detail="user-db unavailable")
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")

    # 2. Create the chat (chat-db). chat.user_id is text, so store it as a string.
    chat_id = next_snowflake()
    try:
        with psycopg.connect(
            CHAT_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            conn.execute(
                "INSERT INTO chat (id, user_id, title) VALUES (%s, %s, %s)",
                (chat_id, str(user_id), DEFAULT_CHAT_TITLE),
            )
    except psycopg.Error:
        raise HTTPException(status_code=503, detail="chat-db unavailable")

    return CreateChatResponse(id=chat_id)


# ---------------------------------------------------------------------------
# List a user's chats (GET /chats)
#
# Reads chat-db only. The caller identifies itself via the X-User-Id header (no
# request body); pagination comes in as query params: `cursor` is the start
# offset (default 0) and `maxChats` is the page size (default 10). Chats are
# returned newest-updated first, so callers can walk them with cursor += page.
#
# GetChatsResponse types `chats` as a single Chat, but the described behavior
# (a user's chats, ordered, paginated by cursor/maxChats) is a page of rows —
# we return `chats` as a list of Chat objects.
# ---------------------------------------------------------------------------

DEFAULT_CURSOR = 0
DEFAULT_MAX_CHATS = 10
MAX_MAX_CHATS = 100  # cap the page size so one request can't scan the table


class ChatItem(BaseModel):
    id: int
    user_id: int
    title: str
    updated_at: str
    created_at: str


class GetChatsResponse(BaseModel):
    chats: list[ChatItem]


@app.get("/chats", response_model=GetChatsResponse)
def get_chats(
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
    cursor: int = Query(default=DEFAULT_CURSOR, ge=0),
    max_chats: int = Query(default=DEFAULT_MAX_CHATS, ge=1, alias="maxChats"),
):
    """List the caller's chats, newest-updated first, one page at a time.

    user_id comes from the X-User-Id header. `cursor` is the start offset
    (default 0) and `maxChats` is how many rows to return (default 10, capped
    at 100). Rows are ordered by updated_at descending (id descending to break
    ties for a stable page).
    """
    if not x_user_id:
        raise HTTPException(status_code=400, detail="missing X-User-Id header")
    try:
        user_id = int(x_user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid X-User-Id header")

    limit = min(max_chats, MAX_MAX_CHATS)

    try:
        with psycopg.connect(
            CHAT_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            rows = conn.execute(
                """
                SELECT id, user_id, title, updated_at, created_at
                FROM chat
                WHERE user_id = %s
                ORDER BY updated_at DESC, id DESC
                OFFSET %s
                LIMIT %s
                """,
                (user_id, cursor, limit),
            ).fetchall()
    except psycopg.Error:
        raise HTTPException(status_code=503, detail="chat-db unavailable")

    return GetChatsResponse(
        chats=[
            ChatItem(
                id=row["id"],
                user_id=row["user_id"],
                title=row["title"],
                updated_at=row["updated_at"].isoformat(),
                created_at=row["created_at"].isoformat(),
            )
            for row in rows
        ]
    )


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# etcd worker registration (sandbox-etcd skill)
#
# Each worker keeps a leased key alive under /services/chat-service/ so
# listeners discover the live worker set. The lease TTL comes from the mounted
# /etcd/etcd.json (re-read by mtime, so a UI TTL change applies live); on ANY
# error the loop reconnects (possibly to another member), re-grants and re-puts
# — so it survives cluster recreation and quorum loss.
# ---------------------------------------------------------------------------

import json
import random
import socket

import etcd3

ETCD_SERVICE = "chat-service"
ETCD_WORKER_ID = os.environ.get("ETCD_WORKER_ID", socket.gethostname())
ETCD_ENDPOINTS = os.environ.get("ETCD_ENDPOINTS", "etcd-1:2379").split(",")

_ETCD_CFG = "/etcd/etcd.json"
_ttl_cache = {"mtime": 0.0, "ttl": 15}


def _lease_ttl():
    try:
        m = os.stat(_ETCD_CFG).st_mtime
        if m != _ttl_cache["mtime"]:
            with open(_ETCD_CFG) as f:
                ttl = int(json.load(f)["cluster"]["leaseTtlSeconds"])
            _ttl_cache.update(mtime=m, ttl=ttl)
    except Exception:
        pass  # keep last good value
    return _ttl_cache["ttl"]


def _register_worker():
    key = f"/services/{ETCD_SERVICE}/{ETCD_WORKER_ID}"
    value = f"{ETCD_SERVICE}:8000"  # plain service: its own compose DNS name
    while True:
        try:
            host, port = random.choice(ETCD_ENDPOINTS).split(":")
            client = etcd3.client(host=host, port=int(port), timeout=5)
            ttl = _lease_ttl()
            lease = client.lease(ttl)
            client.put(key, value, lease=lease)
            while True:
                time.sleep(max(1, ttl / 3))
                if _lease_ttl() != ttl:
                    break  # TTL changed in the UI -> re-grant with the new TTL
                # A vanished lease (cluster recreated) is NOT an error: gRPC reconnects
                # to the new cluster and refresh() returns TTL 0 instead of raising.
                r = lease.refresh()
                if not r or getattr(r[0], "TTL", 0) <= 0:
                    break  # lease vanished -> re-grant + re-put
        except Exception:
            time.sleep(2)  # quorum lost / member down / connect failure
        # fall through: reconnect, re-grant, re-put


threading.Thread(target=_register_worker, daemon=True).start()
