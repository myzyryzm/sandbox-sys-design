"""LLM Worker service — simulated LLM inference with continuous batching.

Roles in one process:
  - Worker gRPC SERVER : AddPrompt admits a user message into the batch;
                         GetStatus reports capacity. (:50051, intra-network)
  - worker loop thread : steps every active sequence (prefill, then one decode
                         per tick), XADDs each generated token to the linked
                         redis stream tokens:<user_message_id>, END token 26 last.
  - reaper loop thread : every 2s evicts prefix-cache entries older than the TTL
                         and fires the user-authored on_cache_evict hook.

The prefix cache is keyed by chat id: a finished sequence's KV caches are kept so
a follow-up prompt in the same chat prefills only its new tokens. On a cache
miss with a chat id, prior messages are pulled from the configured chat-history
postgres and prefilled as history.

Live config (no rebuild): /config/worker.json is bind-mounted read-only and
mtime-polled — ttl_seconds (0-60; 0 disables caching), chat_db (postgres node
name or null), max_active. The on_cache_evict hook lives in the bind-mounted
hooks.py (authored via the node's Edit tab; restart-only to apply).
"""

import json
import os
import sys
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

sys.path.insert(0, "/app/grpc_pkg")
sys.path.insert(0, "/app")

import grpc  # noqa: E402
import psycopg  # noqa: E402
import redis  # noqa: E402

import Worker_pb2_grpc as worker_grpc  # noqa: E402
from Worker_servicer import WorkerServicer  # noqa: E402

from model import END_TOKEN, Sequence, detokenize, tokenize  # noqa: E402

SERVICE_ID = os.environ.get("SERVICE_ID", "llm-worker")
REDIS_HOST = os.environ.get("REDIS_HOST", f"{SERVICE_ID}-stream")
GRPC_PORT = 50051
STEP_INTERVAL = 0.25  # seconds per batch step — a 100-token output streams over ~25s
REAP_INTERVAL = 2.0
STREAM_MAXLEN = 256  # per-stream cap (approximate trim)
STREAM_TTL_S = 600  # stream key expiry once END is written

# --- live config (mtime-polled bind mount; edits apply with no rebuild) -----
_CFG_PATH = "/config/worker.json"
_DEFAULTS = {"ttl_seconds": 30, "chat_db": None, "max_active": 5}
_cfg = {"mtime": 0, "value": dict(_DEFAULTS)}


def cfg():
    try:
        m = os.stat(_CFG_PATH).st_mtime
    except OSError:
        return _cfg["value"]
    if m != _cfg["mtime"]:
        try:
            with open(_CFG_PATH) as fh:
                _cfg["value"] = {**_DEFAULTS, **json.load(fh)}
            _cfg["mtime"] = m
        except Exception:  # mid-write file: keep last-good
            pass
    return _cfg["value"]


# --- user-authored eviction hook (bind-mounted hooks.py; restart to apply) ---
try:
    import hooks as _hooks  # noqa: E402

    _on_cache_evict = getattr(_hooks, "on_cache_evict", None)
except Exception as exc:  # a broken hook must never take the worker down
    print(f"[{SERVICE_ID}] hooks.py failed to import: {exc}", flush=True)
    _on_cache_evict = None


# ---------------------------------------------------------------------------
# Prometheus instrumentation (same HTTP shape as the generic service template)
# ---------------------------------------------------------------------------
http_requests_total = Counter(
    "http_requests_total", "Total HTTP requests processed", ["method", "endpoint", "status"]
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "HTTP request duration in seconds", ["method", "endpoint"]
)
http_requests_in_flight = Gauge(
    "http_requests_in_flight", "Number of HTTP requests currently in flight"
)
EXCLUDED_PATHS = {"/metrics"}

llm_tokens_streamed_total = Counter(
    "llm_tokens_streamed_total", "Tokens XADDed to the linked redis stream (incl. END)"
)
llm_prompts_total = Counter(
    "llm_prompts_total", "AddPrompt outcomes", ["result"]  # accepted | rejected
)
llm_cache_hits_total = Counter(
    "llm_cache_hits_total", "AddPrompt admissions that reused a cached prefix"
)
llm_cache_evictions_total = Counter(
    "llm_cache_evictions_total", "Prefix-cache entries evicted by the reaper"
)
llm_active_sequences = Gauge("llm_active_sequences", "Sequences currently in the batch")
llm_cached_prefixes = Gauge("llm_cached_prefixes", "Chats currently held in the prefix cache")

_redis = redis.Redis(host=REDIS_HOST, port=6379)


class InferenceEngine:
    """Continuous batching + prefix cache. One lock guards active/cached/counter;
    numpy work happens outside it (a sequence in `active` is only ever stepped by
    the worker thread; one in `cached` is only ever touched by add_prompt)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.active = []  # list[Sequence]
        self.cached = {}  # chat id -> (Sequence, cached_at)
        self.counter = 0

    # --- gRPC-facing (called via asyncio.to_thread from the servicer) -------
    def add_prompt(self, um_id, content, chat, message):
        prompt_tokens = tokenize(content)

        with self.lock:
            if len(self.active) >= int(cfg()["max_active"]):
                llm_prompts_total.labels(result="rejected").inc()
                return {"accepted": False, "seq_id": 0, "reason": "worker full", "cache_hit": False}
            entry = self.cached.pop(chat, None) if chat is not None else None
            if entry is not None:
                prev, cached_at = entry
                if not prompt_tokens:  # nothing new to feed on top of the prefix
                    self.cached[chat] = entry  # put back untouched
                    llm_prompts_total.labels(result="rejected").inc()
                    return {"accepted": False, "seq_id": 0, "reason": "empty prompt", "cache_hit": False}
                seq_id = self.counter
                self.counter += 1
                self.active.append(prev.continue_with(seq_id, um_id, prompt_tokens))
                llm_prompts_total.labels(result="accepted").inc()
                llm_cache_hits_total.inc()
                return {"accepted": True, "seq_id": seq_id, "reason": "", "cache_hit": True}

        # Cache miss: maybe pull the chat's prior messages (outside the lock —
        # the query can be slow and must not stall the worker loop).
        history_tokens = []
        chat_db = cfg().get("chat_db")
        if chat is not None and chat_db:
            history_tokens = self._history_tokens(chat_db, chat, message)
        if not prompt_tokens and not history_tokens:
            llm_prompts_total.labels(result="rejected").inc()
            return {"accepted": False, "seq_id": 0, "reason": "empty prompt", "cache_hit": False}

        with self.lock:
            if len(self.active) >= int(cfg()["max_active"]):  # re-check: lock was released
                llm_prompts_total.labels(result="rejected").inc()
                return {"accepted": False, "seq_id": 0, "reason": "worker full", "cache_hit": False}
            seq_id = self.counter
            self.counter += 1
            self.active.append(Sequence(seq_id, um_id, chat, history_tokens + prompt_tokens))
        llm_prompts_total.labels(result="accepted").inc()
        return {"accepted": True, "seq_id": seq_id, "reason": "", "cache_hit": False}

    def status(self):
        with self.lock:
            max_active = int(cfg()["max_active"])
            return {
                "has_space": len(self.active) < max_active,
                "active_count": len(self.active),
                "cached_count": len(self.cached),
                "max_active": max_active,
            }

    def state(self):
        """Full view for /llm/state (the Edit tab + diagram poll this)."""
        now = time.time()
        with self.lock:
            active = [
                {
                    "seq_id": s.seq_id,
                    "user_message_id": s.user_message_id,
                    "chat": s.chat,
                    "generated": len(s.generated),
                    "target_len": s.target_len,
                    "prefilled": s.prefilled,
                }
                for s in self.active
            ]
            cached = [
                {"chat": chat, "age_s": round(now - ts, 1)}
                for chat, (_seq, ts) in self.cached.items()
            ]
            counter = self.counter
        c = cfg()
        return {
            "ok": True,
            "id": SERVICE_ID,
            "active": active,
            "active_count": len(active),
            "cached": cached,
            "cached_count": len(cached),
            "counter": counter,
            "config": {
                "ttl_seconds": int(c["ttl_seconds"]),
                "chat_db": c.get("chat_db"),
                "max_active": int(c["max_active"]),
            },
        }

    # --- internals -----------------------------------------------------------
    def _history_tokens(self, chat_db, chat, message):
        """All prior messages in the chat, oldest first, excluding this prompt's
        own message row. Any failure (bad node name, schema mismatch, db down)
        logs and falls back to prompt-only — the admit path never fails on it."""
        dsn = f"postgresql://sandbox:sandbox@{chat_db}:5432/{chat_db.replace('-', '_')}"
        try:
            with psycopg.connect(dsn, connect_timeout=3) as conn:
                rows = conn.execute(
                    "SELECT content FROM message"
                    " WHERE chat_id = %s AND id <> COALESCE(%s, -1)"
                    " ORDER BY created_at",
                    (chat, message),
                ).fetchall()
            tokens = []
            for (content,) in rows:
                tokens.extend(tokenize(content))
            return tokens
        except Exception as exc:
            print(f"[{SERVICE_ID}] chat history query failed ({chat_db}): {exc}", flush=True)
            return []

    def _stream_token(self, um_id, token):
        try:
            key = f"tokens:{um_id}"
            _redis.xadd(key, {"t": int(token)}, maxlen=STREAM_MAXLEN, approximate=True)
            llm_tokens_streamed_total.inc()
            if token == END_TOKEN:
                _redis.expire(key, STREAM_TTL_S)
        except Exception as exc:
            print(f"[{SERVICE_ID}] redis xadd failed: {exc}", flush=True)

    # --- background loops (daemon threads) ------------------------------------
    def worker_loop(self):
        while True:
            with self.lock:
                batch = list(self.active)
            for seq in batch:
                try:
                    if not seq.prefilled:
                        seq.prefill()
                    else:
                        self._stream_token(seq.user_message_id, seq.decode_step())
                except Exception as exc:
                    # A poisoned sequence must not stall the batch: finish it.
                    print(f"[{SERVICE_ID}] step failed for seq {seq.seq_id}: {exc}", flush=True)
                    seq.done = True

            finished = []
            with self.lock:
                still = []
                keep_cache = int(cfg()["ttl_seconds"]) > 0
                for s in self.active:
                    if not s.done:
                        still.append(s)
                        continue
                    finished.append(s)
                    if s.chat is not None and keep_cache:
                        self.cached[s.chat] = (s, time.time())
                self.active[:] = still
                llm_active_sequences.set(len(self.active))
                llm_cached_prefixes.set(len(self.cached))
            for s in finished:
                self._stream_token(s.user_message_id, END_TOKEN)
            time.sleep(STEP_INTERVAL)

    def reaper_loop(self):
        while True:
            time.sleep(REAP_INTERVAL)
            ttl = int(cfg()["ttl_seconds"])
            now = time.time()
            evicted = []
            with self.lock:
                if ttl <= 0:
                    # Caching disabled: flush silently (this is an eviction-policy
                    # hook, not a completion hook — nothing fires here).
                    self.cached.clear()
                else:
                    for chat in [c for c, (_s, ts) in self.cached.items() if now - ts > ttl]:
                        seq, ts = self.cached.pop(chat)
                        evicted.append((chat, seq, ts))
                llm_cached_prefixes.set(len(self.cached))
            for chat, seq, ts in evicted:
                llm_cache_evictions_total.inc()
                entry = {
                    "chat": chat,
                    "seq_id": seq.seq_id,
                    "user_message_id": seq.user_message_id,
                    "prompt_tokens": list(seq.tokens[: len(seq.tokens) - len(seq.generated)]),
                    "generated_tokens": list(seq.generated),
                    "text": detokenize(seq.tokens),
                    "cached_at": ts,
                    "evicted_at": now,
                }
                if callable(_on_cache_evict):
                    try:
                        _on_cache_evict(entry)
                    except Exception as exc:
                        print(f"[{SERVICE_ID}] on_cache_evict failed: {exc}", flush=True)
                else:
                    print(f"[{SERVICE_ID}] cache evict: chat={chat} seq={seq.seq_id}", flush=True)


engine = InferenceEngine()


@asynccontextmanager
async def lifespan(app):
    server = grpc.aio.server()
    worker_grpc.add_WorkerServicer_to_server(WorkerServicer(engine), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await server.start()

    # The two background loops from the spec: worker (inference) + reaper (cache TTL).
    threading.Thread(target=engine.worker_loop, daemon=True, name="worker").start()
    threading.Thread(target=engine.reaper_loop, daemon=True, name="reaper").start()
    try:
        yield
    finally:
        await server.stop(grace=2)


app = FastAPI(title="llm-worker", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    path = request.url.path
    method = request.method
    if path in EXCLUDED_PATHS:
        return await call_next(request)
    http_requests_in_flight.inc()
    start = time.perf_counter()
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        status = 500
        raise
    finally:
        http_request_duration_seconds.labels(method=method, endpoint=path).observe(
            time.perf_counter() - start
        )
        http_requests_total.labels(method=method, endpoint=path, status=str(status)).inc()
        http_requests_in_flight.dec()
    return response


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/llm/state")
async def llm_state():
    return engine.state()


# ---------------------------------------------------------------------------
# etcd worker registration (sandbox-etcd skill)
#
# Each worker keeps a leased key alive under /services/llm-worker/ (value
# host:port) so listeners discover the live worker set. The lease TTL comes from
# the mounted /etcd/etcd.json (re-read by mtime, so a UI TTL change applies
# live); on ANY error the loop reconnects (possibly to another member), re-grants
# and re-puts — so it survives cluster recreation and quorum loss.
#
# Transport note: the etcd3 python client pins protobuf<4, which is incompatible
# with this worker's own protobuf 5.x gRPC (Worker_pb2 requires the 5.28 runtime,
# and both can't load in one process). We therefore talk to etcd through its
# stock v3 JSON gRPC-gateway over httpx — the skill's documented fallback. The
# gateway is real etcd, so the lease keepalive below is a genuine lease refresh
# (a vanished lease returns TTL 0, exactly like the native refresh()).
# ---------------------------------------------------------------------------

import base64
import random
import socket

import httpx

ETCD_SERVICE = "llm-worker"
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


def _b64(s):
    return base64.b64encode(s.encode()).decode()


def _register_worker():
    key = f"/services/{ETCD_SERVICE}/{ETCD_WORKER_ID}"
    value = f"{SERVICE_ID}:8000"  # this container's OWN compose DNS name (base=llm-worker, instance=llm-worker-N)
    while True:
        try:
            host, port = random.choice(ETCD_ENDPOINTS).split(":")
            base = f"http://{host}:{port}"
            ttl = _lease_ttl()
            with httpx.Client(base_url=base, timeout=5) as c:
                grant = c.post("/v3/lease/grant", json={"TTL": str(ttl)})
                grant.raise_for_status()
                lease_id = grant.json()["ID"]
                put = c.post(
                    "/v3/kv/put",
                    json={"key": _b64(key), "value": _b64(value), "lease": lease_id},
                )
                put.raise_for_status()
                while True:
                    time.sleep(max(1, ttl / 3))
                    if _lease_ttl() != ttl:
                        break  # TTL changed in the UI -> re-grant with the new TTL
                    # A vanished lease (cluster recreated) is NOT an error: gRPC
                    # transparently reconnects to the new cluster and keepalive
                    # returns TTL 0 instead of failing.
                    ka = c.post("/v3/lease/keepalive", json={"ID": lease_id})
                    ka.raise_for_status()
                    new_ttl = int(ka.json().get("result", {}).get("TTL", 0) or 0)
                    if new_ttl <= 0:
                        break  # lease vanished -> re-grant + re-put
        except Exception:
            time.sleep(2)  # quorum lost / member down / connect failure
        # fall through: reconnect, re-grant, re-put


threading.Thread(target=_register_worker, daemon=True).start()
