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
import random
import time
from typing import Optional

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
# Payout receipt
#
# POST /payout confirms that a posted payout has landed at the payout service.
# It keys off the request's idempotency_key (which is the payment_order_id in
# ledger-db): it requires a prior step3_payout Transaction for that id — it reads
# that payout's credit LedgerEntry (the one crediting the payout account) to size
# the amount/currency, otherwise the request is a 400 — is idempotent on the
# step4_payout-received Transaction (returns 200 if one already exists), and
# otherwise posts that Transaction plus its double-entry pair in ledger-db: debit
# the payout account, credit platform/cash for the same amount. It touches one
# downstream node, ledger-db (Transaction / LedgerEntry / Account); a DB failure
# surfaces to the caller as a 400.
# ---------------------------------------------------------------------------

LEDGER_DB_DSN = os.environ.get(
    "LEDGER_DB_DSN", "postgresql://sandbox:sandbox@ledger-db:5432/ledger_db"
)


class MakePayoutRequest(BaseModel):
    idempotency_key: str


def _snowflake_id() -> int:
    """A roughly time-ordered 63-bit id (fits a Postgres bigint).

    ledger-db rows (Transaction / LedgerEntry) carry app-assigned snowflake ids;
    this packs the epoch-millis timestamp with random low bits so inserts in the
    same millisecond don't collide.
    """
    return (int(time.time() * 1000) << 20) | random.getrandbits(20)


def _account_id(conn, owner_id: str, name: Optional[str]) -> int:
    """Resolve an Account by its (owner_id, name) identity.

    Accounts are delineated by (owner_id, name) and are expected to already exist
    in the chart of accounts; name may be NULL (e.g. the payout account), so the
    lookup uses IS NOT DISTINCT FROM. A missing account is a server-side invariant
    violation, so raise (500) rather than invent one.
    """
    found = conn.execute(
        "SELECT id FROM account WHERE owner_id = %s AND name IS NOT DISTINCT FROM %s",
        (owner_id, name),
    ).fetchone()
    if found is None:
        raise HTTPException(
            status_code=500, detail=f"account ({owner_id}, {name}) not found"
        )
    return found["id"]


@app.post("/payout")
def process_payout(req: MakePayoutRequest):
    """Confirm a payout was received: move the funds from payout into platform cash."""
    try:
        with psycopg.connect(
            LEDGER_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            # Idempotent: if this payout receipt is already posted, do nothing more.
            existing = conn.execute(
                "SELECT id FROM transaction "
                "WHERE payment_order_id = %s AND step = 'step4_payout-received'",
                (req.idempotency_key,),
            ).fetchone()
            if existing is not None:
                return {"status": "step4_payout-received"}

            # Require the step3 payout to have been posted; size this receipt from
            # its credit LedgerEntry (the one crediting the payout account).
            payout_credit = conn.execute(
                "SELECT le.amount, le.currency FROM ledger_entry le "
                "JOIN transaction t ON le.transaction_id = t.id "
                "WHERE t.payment_order_id = %s AND t.step = 'step3_payout' "
                "AND le.direction = 'credit'",
                (req.idempotency_key,),
            ).fetchone()
            if payout_credit is None:
                raise HTTPException(
                    status_code=400,
                    detail="no step3_payout credit ledger entry for idempotency_key",
                )
            amount = payout_credit["amount"]
            currency = payout_credit["currency"]

            # Post the receipt Transaction plus its debit/credit pair.
            try:
                with conn.transaction():
                    txn_id = _snowflake_id()
                    conn.execute(
                        "INSERT INTO transaction (id, payment_order_id, step) "
                        "VALUES (%s, %s, 'step4_payout-received')",
                        (txn_id, req.idempotency_key),
                    )
                    payout_id = _account_id(conn, "payout", None)
                    cash_id = _account_id(conn, "platform", "cash")
                    # Debit the payout account (the payable is settled)...
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'debit', %s, %s)",
                        (_snowflake_id(), txn_id, payout_id, amount, currency),
                    )
                    # ...credit platform/cash (the money landed back).
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'credit', %s, %s)",
                        (_snowflake_id(), txn_id, cash_id, amount, currency),
                    )
            except psycopg.errors.UniqueViolation:
                # A concurrent caller already posted step4_payout-received; the
                # double-entry insert is rolled back. Treat it as already done.
                return {"status": "step4_payout-received"}
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="ledger database error")

    return {"status": "step4_payout-received"}


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
