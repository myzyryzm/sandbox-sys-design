"""
FastAPI backend for the `payout-consumer` service.

Besides the by-hand Prometheus instrumentation (see `metrics_middleware`), this
service runs a background Kafka consumer in a daemon thread: the `consumePayout`
poll loop that drains the `payout` topic of the `payout-stream` cluster and
drives each payment order's payout. The loop is the interesting part — read it
below `metrics()`.

Endpoints:
  GET /health   -> {"status": "ok"}
  GET /metrics  -> Prometheus exposition format (the three metrics defined below)
"""

import json
import os
import random
import re
import threading
import time
import traceback
from typing import Optional

import httpx
import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from psycopg.rows import dict_row
from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition

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
# Kafka consumer function: consumePayout
#
# Background daemon thread that consumes the `payout` topic of the
# `payout-stream` cluster (group id payout-consumer-consumePayout, polling every
# 10s) and, for each payout signal, completes the payment order's payout:
#
#   1. Read all Transactions for the message's payment_order_id. They must be
#      EXACTLY {step1_payin, step2a_payin-complete, step2b_fufilled}. If not,
#      commitError().
#   2. Grab the seller's credit LedgerEntry on the step2b_fufilled Transaction
#      (direction=credit, account owner_id != 'platform') -> amount/currency and
#      the seller account.
#   3. Call payout-api POST /payout (idempotency_key=payment_order_id, that
#      amount/currency, recipient=the seller account's owner_id). On any error,
#      commitError().
#   4. Atomically post the step3_payout Transaction plus its double-entry pair
#      (debit the seller account, credit the payout account). If that fails,
#      commitError(). Otherwise commit the offset and move on.
#
# commitError() republishes the consumed message to the payout-error topic,
# commits the offset, and returns (the message is done — not retried).
#
# The loop is pause-aware: the Topics tab's "Pause consumers" toggle flips a
# top-level `consumersPaused` flag in the mounted streams.json, which we honor
# live (no rebuild) by pausing/resuming our assigned partitions.
# ===========================================================================

LEDGER_DB_DSN = os.environ.get(
    "LEDGER_DB_DSN", "postgresql://sandbox:sandbox@ledger-db:5432/ledger_db"
)
PAYOUT_API_BASE = os.environ.get("PAYOUT_API_BASE", "http://payout-api:8000")
KAFKA_BOOTSTRAP = os.environ.get("PAYOUT_STREAM_BOOTSTRAP", "payout-stream:9092")

GROUP_ID = "payout-consumer-consumePayout"
TOPIC = "payout"
ERROR_TOPIC = "payout-error"
POLL_MS = 10000

# A completed payment order has exactly these three Transactions before payout.
REQUIRED_STEPS = {"step1_payin", "step2a_payin-complete", "step2b_fufilled"}

# CDC (test_decoding) renders a text column as  name[text]:'value'  (embedded
# single quotes are doubled). Pull payment_order_id out of that raw line.
_PMT_RE = re.compile(r"payment_order_id\[[^\]]*\]:'((?:[^']|'')*)'")


def _snowflake_id() -> int:
    """A roughly time-ordered 63-bit id (fits a Postgres bigint).

    ledger-db rows (Transaction / LedgerEntry) carry app-assigned snowflake ids;
    this packs epoch-millis with random low bits so same-millisecond inserts
    don't collide.
    """
    return (int(time.time() * 1000) << 20) | random.getrandbits(20)


def _account_id(conn, owner_id: str, name: Optional[str]) -> int:
    """Resolve an Account by its (owner_id, name) identity.

    Accounts are delineated by (owner_id, name) and are expected to already exist
    in the chart of accounts; name may be NULL (e.g. the payout account), so the
    lookup uses IS NOT DISTINCT FROM. A missing account is a server-side invariant
    violation, so raise rather than invent one.
    """
    found = conn.execute(
        "SELECT id FROM account WHERE owner_id = %s AND name IS NOT DISTINCT FROM %s",
        (owner_id, name),
    ).fetchone()
    if found is None:
        raise RuntimeError(f"account ({owner_id}, {name}) not found")
    return found["id"]


def _payment_order_id(request) -> Optional[str]:
    """Extract payment_order_id from a consumed message.

    The topic is fed by ledger-db-cdc, whose envelope is {table, op, raw} with the
    Payout row encoded in `raw`; tolerate a plain Payout payload too.
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


# One shared producer for the payout-error topic (same cluster we consume).
_producer = None


def _error_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _producer


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


def _process_payout(msg, consumer: KafkaConsumer) -> None:
    """Process one payout message; see the module-level docstring for the logic.

    Unexpected/infra errors (can't reach ledger-db while reading) propagate to the
    caller, which rewinds so the message is retried rather than silently dropped.
    The three described failure branches each commitError().
    """
    request = msg.value

    def commit_error() -> None:
        # Park the message on the error topic, then commit so we don't reprocess.
        producer = _error_producer()
        producer.send(ERROR_TOPIC, request)
        producer.flush()
        _commit(consumer, msg)

    pmt_id = _payment_order_id(request)
    if pmt_id is None:
        # Malformed message — nothing to act on; park it.
        commit_error()
        return

    with psycopg.connect(LEDGER_DB_DSN, row_factory=dict_row, autocommit=True) as conn:
        # 1. Must be exactly the three pre-payout Transactions.
        rows = conn.execute(
            "SELECT step FROM transaction WHERE payment_order_id = %s",
            (pmt_id,),
        ).fetchall()
        if {r["step"] for r in rows} != REQUIRED_STEPS:
            commit_error()
            return

        # 2. Seller's credit line on step2b_fufilled (owner_id != platform): this
        #    is the 90% seller payout, and its account is who we pay / debit.
        seller = conn.execute(
            "SELECT le.amount, le.currency, le.account_id, a.owner_id "
            "FROM ledger_entry le "
            "JOIN transaction t ON le.transaction_id = t.id "
            "JOIN account a ON le.account_id = a.id "
            "WHERE t.payment_order_id = %s AND t.step = 'step2b_fufilled' "
            "AND le.direction = 'credit' AND a.owner_id <> 'platform'",
            (pmt_id,),
        ).fetchone()
        if seller is None:
            commit_error()
            return
        amount = seller["amount"]
        currency = seller["currency"]
        seller_account_id = seller["account_id"]
        recipient = seller["owner_id"]  # the seller account's owner_id

        # 3. Ask payout-api to make the payout (idempotent on payment_order_id).
        try:
            resp = httpx.post(
                f"{PAYOUT_API_BASE}/payout",
                json={
                    "idempotency_key": pmt_id,
                    "amount": amount,
                    "currency": currency,
                    "recipient": recipient,
                },
                timeout=10.0,
            )
            resp.raise_for_status()
        except Exception:
            commit_error()
            return

        # 4. Post the step3_payout Transaction + its double-entry pair atomically:
        #    debit the seller account, credit the payout account.
        try:
            with conn.transaction():
                txn_id = _snowflake_id()
                conn.execute(
                    "INSERT INTO transaction (id, payment_order_id, step) "
                    "VALUES (%s, %s, 'step3_payout')",
                    (txn_id, pmt_id),
                )
                payout_account_id = _account_id(conn, "payout", None)
                conn.execute(
                    "INSERT INTO ledger_entry "
                    "(id, transaction_id, account_id, direction, amount, currency) "
                    "VALUES (%s, %s, %s, 'debit', %s, %s)",
                    (_snowflake_id(), txn_id, seller_account_id, amount, currency),
                )
                conn.execute(
                    "INSERT INTO ledger_entry "
                    "(id, transaction_id, account_id, direction, amount, currency) "
                    "VALUES (%s, %s, %s, 'credit', %s, %s)",
                    (_snowflake_id(), txn_id, payout_account_id, amount, currency),
                )
        except Exception:
            commit_error()
            return

        # Success: this payout is fully posted.
        _commit(consumer, msg)


# --- pause-aware flag (live, no rebuild) -----------------------------------
_PAUSE = "/streams/payout-stream.json"
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


def _deserialize(b: bytes):
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return {"raw": b.decode("utf-8", "replace")}


def _consume_consumePayout() -> None:
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
                    _process_payout(msg, consumer)
                except Exception:
                    # Infra error (e.g. ledger-db unreachable mid-read): don't
                    # commit; rewind this partition so we retry, then re-poll.
                    traceback.print_exc()
                    consumer.seek(tp, msg.offset)
                    break


threading.Thread(target=_consume_consumePayout, daemon=True).start()
