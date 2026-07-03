"""
FastAPI backend for the `refund-service` service.

Besides the by-hand Prometheus instrumentation (see `metrics_middleware`), this
service runs a background Kafka consumer in a daemon thread: the `processRefund`
poll loop that drains the `refunds` topic of the `refund-stream` cluster and,
for each refund, tells payments-api how to unwind the payment. The loop is the
interesting part — read it below `metrics()`.

Endpoints:
  GET /health   -> {"status": "ok"}
  GET /metrics  -> Prometheus exposition format (the three metrics defined below)
"""

import json
import os
import re
import threading
import time
import traceback
from typing import Optional

import httpx
import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from psycopg.rows import dict_row
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

# ---------------------------------------------------------------------------
# Metric definitions
#
# These three metrics are what the manifest's PromQL queries refer to. If you
# rename anything here, update systems/payment-service/manifest.json to match.
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


# ===========================================================================
# Kafka consumer function: processRefund
#
# Background daemon thread that consumes the `refunds` topic of the
# `refund-stream` cluster (group id refund-service-processRefund, polling every
# 1s) and, for each refund, decides how payments-api should unwind the payment:
#
#   1. Read all Transactions for the message's payment_order_id from ledger-db.
#   2. If any is a `step2a_payin-complete` Transaction, the payin was captured,
#      so ask payments-api to REFUND it:  POST /refund-payment
#      (idempotency_key = payment_order_id).
#   3. Otherwise the payment was never captured, so ask payments-api to CANCEL
#      it instead:  POST /cancel-payment (idempotency_key = payment_order_id).
#
# The API calls are idempotent on payment_order_id. A connection-level failure
# (can't reach ledger-db or payments-api) propagates to the loop, which rewinds
# the partition so the message is retried rather than dropped; a business error
# *response* (e.g. 400 "not found") is terminal — we still commit and move on.
#
# The loop is pause-aware: the Topics tab's "Pause consumers" toggle flips a
# top-level `consumersPaused` flag in the mounted streams.json, which we honor
# live (no rebuild) by pausing/resuming our assigned partitions.
# ===========================================================================

LEDGER_DB_DSN = os.environ.get(
    "LEDGER_DB_DSN", "postgresql://sandbox:sandbox@ledger-db:5432/ledger_db"
)
PAYMENTS_API_BASE = os.environ.get("PAYMENTS_API_BASE", "http://payments-api:8000")
KAFKA_BOOTSTRAP = os.environ.get("REFUND_STREAM_BOOTSTRAP", "refund-stream:9092")

GROUP_ID = "refund-service-processRefund"
TOPIC = "refunds"
POLL_MS = 1000

# CDC (test_decoding) renders a text column as  name[text]:'value'  (embedded
# single quotes are doubled). Pull payment_order_id out of that raw line.
_PMT_RE = re.compile(r"payment_order_id\[[^\]]*\]:'((?:[^']|'')*)'")


def _payment_order_id(request) -> Optional[str]:
    """Extract payment_order_id from a consumed message.

    The topic is fed by ledger-db-cdc, whose envelope is {table, op, raw} with the
    Refund row encoded in `raw` (a test_decoding line); tolerate a plain Refund
    payload too.
    """
    if isinstance(request, dict):
        if request.get("payment_order_id"):
            return str(request["payment_order_id"])
        raw = request.get("raw", "")
        if isinstance(raw, str):
            m = _PMT_RE.search(raw)
            if m:
                return m.group(1).replace("''", "'")
    return None


def _offset(next_offset: int) -> OffsetAndMetadata:
    """Build an OffsetAndMetadata across kafka-python versions.

    kafka-python 2.x is (offset, metadata); 3.x adds a trailing leader_epoch.
    """
    try:
        return OffsetAndMetadata(next_offset, "", -1)
    except TypeError:
        return OffsetAndMetadata(next_offset, "")


def _commit(consumer: KafkaConsumer, msg) -> None:
    """Commit just this message's offset (next position = offset + 1)."""
    consumer.commit(
        {TopicPartition(msg.topic, msg.partition): _offset(msg.offset + 1)}
    )


def _process_refund(msg, consumer: KafkaConsumer) -> None:
    """Process one refund message; see the module-level docstring for the logic.

    Unexpected/infra errors (can't reach ledger-db / payments-api) propagate to
    the caller, which rewinds so the message is retried rather than silently
    dropped.
    """
    request = msg.value

    pmt_id = _payment_order_id(request)
    if pmt_id is None:
        # Malformed message — nothing to act on; skip it.
        _commit(consumer, msg)
        return

    # 1. Find this payment order's Transactions.
    with psycopg.connect(LEDGER_DB_DSN, row_factory=dict_row, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT step FROM transaction WHERE payment_order_id = %s",
            (pmt_id,),
        ).fetchall()
    steps = {r["step"] for r in rows}

    # 2/3. If the payin was captured (a step2a_payin-complete Transaction exists)
    #      refund it; otherwise it was never captured, so cancel it. Both API
    #      calls are idempotent on payment_order_id.
    path = "/refund-payment" if "step2a_payin-complete" in steps else "/cancel-payment"
    httpx.post(
        f"{PAYMENTS_API_BASE}{path}",
        json={"idempotency_key": pmt_id},
        timeout=10.0,
    )

    # Handled (a business-error response is terminal — don't reprocess).
    _commit(consumer, msg)


# --- pause-aware flag (live, no rebuild) -----------------------------------
_PAUSE = "/streams/refund-stream.json"
_pc = {"mtime": 0, "paused": False}


def _consumers_paused() -> bool:
    try:
        m = os.stat(_PAUSE).st_mtime
    except OSError:
        return _pc["paused"]
    if m != _pc["mtime"]:
        try:
            with open(_PAUSE) as fh:
                _pc["paused"] = bool(json.load(fh).get("consumersPaused"))
            _pc["mtime"] = m
        except Exception:  # mid-write file: keep last-good
            pass
    return _pc["paused"]


def _deserialize(b: bytes):
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return {"raw": b.decode("utf-8", "replace")}


def _make_consumer() -> KafkaConsumer:
    """Build the consumer, retrying until the broker is reachable."""
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                enable_auto_commit=False,  # we commit explicitly per the logic
                auto_offset_reset="earliest",
                value_deserializer=_deserialize,
            )
            consumer.subscribe([TOPIC])
            return consumer
        except Exception:
            traceback.print_exc()
            time.sleep(2)


def _consume_processRefund() -> None:
    consumer = _make_consumer()
    while True:
        if _consumers_paused():
            if consumer.assignment():
                consumer.pause(*consumer.assignment())  # stop fetching; offsets hold
            time.sleep(1)
            continue
        if consumer.paused():
            consumer.resume(*consumer.paused())  # resume where we left off
        try:
            batches = consumer.poll(timeout_ms=POLL_MS)
        except Exception:
            traceback.print_exc()
            time.sleep(2)
            continue
        for tp, records in batches.items():
            for msg in records:
                try:
                    _process_refund(msg, consumer)
                except Exception:
                    # Infra error (e.g. ledger-db / payments-api unreachable):
                    # don't commit; rewind this partition so we retry, re-poll.
                    traceback.print_exc()
                    consumer.seek(tp, msg.offset)
                    break


threading.Thread(target=_consume_processRefund, daemon=True).start()
