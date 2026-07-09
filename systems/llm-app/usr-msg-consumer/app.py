"""
FastAPI backend for the `hello-lb` sandbox system.

This is a learning tool: the Prometheus instrumentation is written by hand with
`prometheus_client` (no black-box auto-instrumentor) so you can read exactly how
each metric is produced. The interesting part is `metrics_middleware` below.

Endpoints:
  GET /health   -> {"status": "ok"}
  GET /metrics  -> Prometheus exposition format (the three metrics defined below)
"""

import time

from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

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


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# Kafka consumer function "processUserMessage" (sandbox-event-stream skill)
#
# Consumes chat-db CDC INSERT events for the user_message table from topic
# "user-message" on user-messages-stream, picks an available llm-worker —
# sticky chat→worker routing so a chat's prompts reuse that worker's prefix
# cache — and admits the prompt via the Worker gRPC contract (AddPrompt).
# Offsets are committed only after a worker accepts the prompt, so a message
# that finds no available worker is retried next cycle.
# ---------------------------------------------------------------------------

import base64
import json
import os
import random
import re
import threading

import grpc
import httpx
from kafka import KafkaConsumer

import Worker_pb2
import Worker_pb2_grpc

# --- live consumersPaused flag (mounted read-only; re-read on mtime change,
# --- keep last-good on a mid-write read) -----------------------------------
_PAUSE = "/streams/user-messages-stream.json"
_pc = {"mtime": 0, "paused": False}


def _consumers_paused():
    try:
        m = os.stat(_PAUSE).st_mtime
    except OSError:
        return _pc["paused"]
    if m != _pc["mtime"]:
        try:
            _pc["paused"] = bool(json.load(open(_PAUSE)).get("consumersPaused"))
            _pc["mtime"] = m
        except Exception:  # mid-write file: keep last-good
            pass
    return _pc["paused"]


# --- llm-worker discovery + gRPC client -------------------------------------
# Each llm-worker keeps a leased key under /services/llm-worker/ in etcd
# (value "<host>:8000", the container's compose DNS name); its Worker gRPC
# server listens on :50051. The etcd3 python client pins protobuf<4, which
# conflicts with the protobuf 5.x the Worker stubs need, so we read the
# keyspace through etcd's v3 JSON gRPC-gateway (same fallback llm-worker uses).

ETCD_ENDPOINTS = os.environ.get(
    "ETCD_ENDPOINTS", "etcd-1:2379,etcd-2:2379,etcd-3:2379"
).split(",")
_WORKER_PREFIX = "/services/llm-worker/"
GRPC_PORT = 50051


def _b64(s):
    return base64.b64encode(s.encode()).decode()


class Worker:
    """One live llm-worker instance (the gRPC channel persists across refreshes)."""

    def __init__(self, host):
        self.host = host
        self._stub = Worker_pb2_grpc.WorkerStub(
            grpc.insecure_channel(f"{host}:{GRPC_PORT}")
        )

    def get_status(self):
        """True when the worker has space for another prompt (False on any error)."""
        try:
            reply = self._stub.GetStatus(Worker_pb2.StatusRequest(), timeout=2)
            return bool(reply.has_space)
        except Exception:
            return False

    def add_prompt(self, user_message):
        return self._stub.AddPrompt(
            Worker_pb2.AddPromptRequest(
                id=user_message["id"],
                content=user_message["content"],
                chat=user_message["chat"],
                message=user_message["message"],
            ),
            timeout=5,
        )


workers = []  # all live llm-worker instances (refreshed from etcd)
chat_worker_dict = {}  # chat id -> Worker: sticky routing enables prefix caching
_worker_cache = {}  # host -> Worker, so channels are reused across refreshes


def _refresh_workers():
    """Range-read /services/llm-worker/ and rebuild `workers` (last-good on failure)."""
    global workers
    end = _WORKER_PREFIX[:-1] + chr(ord(_WORKER_PREFIX[-1]) + 1)
    for ep in random.sample(ETCD_ENDPOINTS, len(ETCD_ENDPOINTS)):
        try:
            r = httpx.post(
                f"http://{ep}/v3/kv/range",
                json={"key": _b64(_WORKER_PREFIX), "range_end": _b64(end)},
                timeout=3,
            )
            r.raise_for_status()
            hosts = set()
            for kv in r.json().get("kvs", []):
                value = base64.b64decode(kv["value"]).decode()  # "<host>:8000"
                hosts.add(value.rsplit(":", 1)[0])
            for h in hosts:
                if h not in _worker_cache:
                    _worker_cache[h] = Worker(h)
            workers = [_worker_cache[h] for h in sorted(hosts)]
            # Drop chat affinities to vanished workers so those chats fail over
            # instead of waiting forever on a dead worker.
            for chat in [c for c, w in chat_worker_dict.items() if w.host not in hosts]:
                del chat_worker_dict[chat]
            return
        except Exception:
            continue  # try another member; keep the last-good list if all fail


def find_available_worker(user_message):
    """The chat's sticky worker if it has space; else the first free worker."""
    chat = user_message["chat"]
    if chat in chat_worker_dict:
        worker = chat_worker_dict[chat]
        if worker.get_status() is True:
            return worker
    else:
        for worker in workers:
            if worker.get_status() is True:
                return worker
    return None


# test_decoding column syntax: name[type]:value, text values quoted with '' escapes
_FIELD_RE = re.compile(r"(\w+)\[[^\]]*\]:('(?:[^']|'')*'|\S+)")


def _parse_user_message(value_bytes):
    """CDC event bytes -> {id, content, chat, message}, or None for anything else.

    Events look like {"table":"user_message","op":"INSERT","raw":"table
    public.user_message: INSERT: id[bigint]:1 content[text]:'hi'
    message_id[bigint]:2 chat_id[bigint]:3"} (postgres test_decoding via
    chat-db-cdc).
    """
    try:
        event = json.loads(value_bytes)
        if event.get("table") != "user_message" or event.get("op") != "INSERT":
            return None
        row = {}
        for name, val in _FIELD_RE.findall(event["raw"].split(":", 2)[2]):
            row[name] = val[1:-1].replace("''", "'") if val.startswith("'") else val
        return {
            "id": int(row["id"]),
            "content": row["content"],
            "chat": int(row["chat_id"]),
            "message": int(row["message_id"]),
        }
    except Exception as exc:
        print(f"[processUserMessage] unparseable event skipped: {exc}", flush=True)
        return None


def _consume_processUserMessage():
    consumer = KafkaConsumer(
        bootstrap_servers="user-messages-stream:9092",
        group_id="processUserMessage-group",
        client_id=os.environ["SERVICE_ID"],  # container id: maps member -> partitions
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit only after a worker accepts the prompt
        max_poll_records=1,
        value_deserializer=lambda b: b,
    )
    consumer.subscribe(["user-message"])
    print("[processUserMessage] consuming user-message", flush=True)
    while True:
        if _consumers_paused():
            if consumer.assignment():
                consumer.pause(*consumer.assignment())  # stop fetching; lag holds
            time.sleep(1)
            continue
        if consumer.paused():
            consumer.resume(*consumer.paused())  # resume from committed position
        polled = consumer.poll(timeout_ms=1000)
        if not polled:
            continue  # no message this cycle
        _refresh_workers()
        for tp, records in polled.items():
            for msg in records:
                user_message = _parse_user_message(msg.value)
                if user_message is None:  # not a user_message INSERT: skip it
                    consumer.commit()
                    continue
                worker = find_available_worker(user_message)
                if worker is None:
                    # No capacity (or the chat's sticky worker is busy): rewind
                    # so the next cycle retries this same message.
                    consumer.seek(tp, msg.offset)
                    time.sleep(1)
                    continue
                try:
                    reply = worker.add_prompt(user_message)
                except Exception as exc:
                    print(f"[processUserMessage] AddPrompt to {worker.host} failed: {exc}", flush=True)
                    reply = None
                if reply is not None and reply.accepted:
                    chat_worker_dict[user_message["chat"]] = worker
                    consumer.commit()
                    print(
                        f"[processUserMessage] user_message {user_message['id']} "
                        f"(chat {user_message['chat']}) -> {worker.host} "
                        f"seq {reply.seq_id} cache_hit={reply.cache_hit}",
                        flush=True,
                    )
                else:  # raced out of space / worker died: retry next cycle
                    consumer.seek(tp, msg.offset)
                    time.sleep(1)


threading.Thread(target=_consume_processUserMessage, daemon=True).start()
