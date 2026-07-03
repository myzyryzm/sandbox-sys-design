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
from datetime import datetime, timezone
from typing import Literal, Optional

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
# Shared database configuration
#
# The payment-flow webhook handlers below read and write two downstream nodes:
# order-db (OrderPayment) and ledger-db (Transaction / LedgerEntry / Account /
# PayoutReadiness / Payout / Refund). Any DB failure surfaces to the caller as a
# 400 (bad request). Each handler is a sync `def`, so FastAPI runs it in a thread
# pool — keeping the blocking psycopg calls off the event loop while the metrics
# middleware above still measures it like any other request.
# ---------------------------------------------------------------------------

DB_DSN = os.environ.get(
    "ORDER_DB_DSN", "postgresql://sandbox:sandbox@order-db:5432/order_db"
)
LEDGER_DB_DSN = os.environ.get(
    "LEDGER_DB_DSN", "postgresql://sandbox:sandbox@ledger-db:5432/ledger_db"
)


# ---------------------------------------------------------------------------
# Payment provider webhook
#
# POST /payment-flow/1 settles a payment attempt the provider has finished
# processing. On 'fail' we flip the OrderPayment attempt to payment_fail in
# order-db. On 'success' we post the payin into ledger-db as one double-entry
# Transaction — debit the psp's cash, credit platform/escrow for the same amount
# — alongside a PayoutReadiness row for the order (payin_complete / fufilled both
# false), then advance the OrderPayment to payment_success. It touches two
# downstream nodes: ledger-db (Transaction / LedgerEntry / Account /
# PayoutReadiness) and order-db (OrderPayment); a DB failure surfaces to the
# caller as a 400.
# ---------------------------------------------------------------------------


class PaymentWebhookRequest(BaseModel):
    status: Literal["success", "fail"]
    currency: str
    amount: int
    payment_order_id: str
    seller_id: str


def _snowflake_id() -> int:
    """A roughly time-ordered 63-bit id (fits a Postgres bigint).

    ledger-db rows (Transaction / LedgerEntry / Account) carry app-assigned
    snowflake ids; this packs the epoch-millis timestamp with random low bits so
    inserts in the same millisecond don't collide.
    """
    return (int(time.time() * 1000) << 20) | random.getrandbits(20)


def _account_id(conn, owner_id: str, name: Optional[str]) -> int:
    """Resolve an Account by its (owner_id, name) identity.

    Accounts are delineated by (owner_id, name) and are expected to already exist
    in the chart of accounts; name may be NULL (e.g. a psp's cash account), so the
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


@app.post("/payment-flow/1")
def payment_webhook(req: PaymentWebhookRequest):
    """Settle a payment attempt from the provider's webhook callback."""
    if req.status == "fail":
        # order-db: mark this attempt failed; the caller can retry on a new one.
        try:
            with psycopg.connect(DB_DSN, autocommit=True) as conn:
                updated = conn.execute(
                    "UPDATE order_payment SET status = 'payment_fail' WHERE id = %s",
                    (req.payment_order_id,),
                )
        except psycopg.Error:
            raise HTTPException(status_code=400, detail="database error")
        if updated.rowcount == 0:
            raise HTTPException(status_code=404, detail="order payment not found")
        return {"status": "payment_fail"}

    # status == 'success': post the payin double-entry, then mark the order paid.
    try:
        with psycopg.connect(
            LEDGER_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            with conn.transaction():
                txn_id = _snowflake_id()
                conn.execute(
                    "INSERT INTO transaction (id, payment_order_id, step) "
                    "VALUES (%s, %s, 'step1_payin')",
                    (txn_id, req.payment_order_id),
                )
                psp_id = _account_id(conn, "psp", None)
                escrow_id = _account_id(conn, "platform", "escrow")
                # Debit the psp (money received), credit platform/escrow (held).
                conn.execute(
                    "INSERT INTO ledger_entry "
                    "(id, transaction_id, account_id, direction, amount, currency) "
                    "VALUES (%s, %s, %s, 'debit', %s, %s)",
                    (_snowflake_id(), txn_id, psp_id, req.amount, req.currency),
                )
                conn.execute(
                    "INSERT INTO ledger_entry "
                    "(id, transaction_id, account_id, direction, amount, currency) "
                    "VALUES (%s, %s, %s, 'credit', %s, %s)",
                    (_snowflake_id(), txn_id, escrow_id, req.amount, req.currency),
                )
                # Seed this order's payout-readiness signal: payin posted but not
                # yet completed or fulfilled. Atomic with the payin double-entry.
                conn.execute(
                    "INSERT INTO payout_readiness "
                    "(id, payment_order_id, payin_complete, fufilled, created_at) "
                    "VALUES (%s, %s, false, false, %s)",
                    (
                        _snowflake_id(),
                        req.payment_order_id,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
    except psycopg.errors.UniqueViolation:
        # The ledger is append-only: a Transaction for this (payment_order_id,
        # step) already exists (e.g. a duplicate webhook delivery). Reject rather
        # than post a second payin — the whole double-entry insert is rolled back.
        raise HTTPException(
            status_code=400,
            detail="transaction already exists for (payment_order_id, step1_payin)",
        )
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="ledger database error")

    # order-db: advance the attempt to payment_success.
    try:
        with psycopg.connect(DB_DSN, autocommit=True) as conn:
            updated = conn.execute(
                "UPDATE order_payment SET status = 'payment_success' WHERE id = %s",
                (req.payment_order_id,),
            )
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="database error")
    if updated.rowcount == 0:
        raise HTTPException(status_code=404, detail="order payment not found")
    return {"status": "payment_success"}


# ---------------------------------------------------------------------------
# Payin completion webhook (step 2a)
#
# POST /payment-flow/2a settles a payin that has already been posted: it
# moves the funds from the psp into the platform's cash. It requires a prior
# step1_payin Transaction for the payment_order_id (otherwise the request is a
# 400), is idempotent on the step2a_payin-complete Transaction (returns 200 if
# one already exists), and otherwise posts that Transaction plus its double-entry
# pair in ledger-db — debit platform/cash, credit the psp for the same amount.
# In the same atomic transaction it also flips this order's PayoutReadiness
# payin_complete to true, and if that leaves both readiness flags (payin_complete
# and fufilled) true it posts the order's Payout. It touches one downstream node,
# ledger-db (Transaction / LedgerEntry / Account / PayoutReadiness / Payout); a
# DB failure surfaces to the caller as a 400.
# ---------------------------------------------------------------------------


class PaymentRegistrationRequest(BaseModel):
    idempotency_key: str
    amount: int
    currency: str
    return_url: str
    payment_order_id: str
    seller_id: str


@app.post("/payment-flow/2a")
def step2a_payment_webhook(req: PaymentRegistrationRequest):
    """Complete a payin: settle the held funds from the psp into platform cash."""
    try:
        with psycopg.connect(
            LEDGER_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            # Require the step1 payin to have been posted before completing it.
            payin = conn.execute(
                "SELECT id FROM transaction "
                "WHERE payment_order_id = %s AND step = 'step1_payin'",
                (req.payment_order_id,),
            ).fetchone()
            if payin is None:
                raise HTTPException(
                    status_code=400,
                    detail="no step1_payin transaction for payment_order_id",
                )

            # Idempotent: if this payin is already completed, do nothing more.
            existing = conn.execute(
                "SELECT id FROM transaction "
                "WHERE payment_order_id = %s AND step = 'step2a_payin-complete'",
                (req.payment_order_id,),
            ).fetchone()
            if existing is not None:
                return {"status": "step2a_payin-complete"}

            # Post the completion Transaction plus its debit/credit pair.
            try:
                with conn.transaction():
                    txn_id = _snowflake_id()
                    conn.execute(
                        "INSERT INTO transaction (id, payment_order_id, step) "
                        "VALUES (%s, %s, 'step2a_payin-complete')",
                        (txn_id, req.payment_order_id),
                    )
                    cash_id = _account_id(conn, "platform", "cash")
                    psp_id = _account_id(conn, "psp", None)
                    # Debit platform/cash (money landed), credit the psp (settled).
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'debit', %s, %s)",
                        (_snowflake_id(), txn_id, cash_id, req.amount, req.currency),
                    )
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'credit', %s, %s)",
                        (_snowflake_id(), txn_id, psp_id, req.amount, req.currency),
                    )
                    # Mark this order's payin complete on its readiness signal,
                    # atomic with the completion double-entry. If the order is
                    # also already fulfilled, both readiness flags are now true,
                    # so post its Payout in the same transaction.
                    readiness = conn.execute(
                        "UPDATE payout_readiness SET payin_complete = true "
                        "WHERE payment_order_id = %s "
                        "RETURNING payin_complete, fufilled",
                        (req.payment_order_id,),
                    ).fetchone()
                    if (
                        readiness is not None
                        and readiness["payin_complete"]
                        and readiness["fufilled"]
                    ):
                        conn.execute(
                            "INSERT INTO payout (id, payment_order_id) "
                            "VALUES (%s, %s)",
                            (_snowflake_id(), req.payment_order_id),
                        )
            except psycopg.errors.UniqueViolation:
                # A concurrent caller already posted step2a_payin-complete; the
                # double-entry insert is rolled back. Treat it as already done.
                return {"status": "step2a_payin-complete"}
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="ledger database error")

    return {"status": "step2a_payin-complete"}


# ---------------------------------------------------------------------------
# Order fulfilment webhook (step 2b)
#
# POST /payment-flow/2b releases the escrowed funds for a paid order: it
# finds the order's payment_success OrderPayment in order-db, then in ledger-db
# splits the held amount out of escrow into the seller's payout (90%) and the
# platform's income (10%). It requires a prior step1_payin Transaction for that
# OrderPayment (otherwise the request is a 400), is idempotent/terminal on the
# step2b_fufilled (or refund) Transaction (returns 200 if one already exists),
# and otherwise posts the step2b_fufilled Transaction plus its debit/credit
# triple in ledger-db. In the same atomic transaction it also flips this order's
# PayoutReadiness fufilled to true, and if that leaves both readiness flags
# (payin_complete and fufilled) true it posts the order's Payout. The
# amount/currency come from the OrderPayment row (the request only carries
# order_id). It touches two downstream nodes: order-db (OrderPayment) and
# ledger-db (Transaction / LedgerEntry / Account / PayoutReadiness / Payout); a
# DB failure surfaces to the caller as a 400.
# ---------------------------------------------------------------------------


class PaymentFulfillmentRequest(BaseModel):
    order_id: str


@app.post("/payment-flow/2b")
def step2b_payment_webhook(req: PaymentFulfillmentRequest):
    """Fulfil a paid order: release escrow to the seller (90%) and platform income (10%)."""
    # order-db: the order must have a payment_success OrderPayment to fulfil.
    try:
        with psycopg.connect(DB_DSN, row_factory=dict_row, autocommit=True) as conn:
            order_payment = conn.execute(
                "SELECT id, seller_id, amount, currency FROM order_payment "
                "WHERE order_id = %s AND status = 'payment_success'",
                (req.order_id,),
            ).fetchone()
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="database error")
    if order_payment is None:
        raise HTTPException(
            status_code=400, detail="no payment_success order payment for order"
        )

    payment_order_id = order_payment["id"]
    seller_id = order_payment["seller_id"]
    amount = order_payment["amount"]
    currency = order_payment["currency"]

    # ledger-db: post the fulfilment, gating on the step1 payin and idempotency.
    try:
        with psycopg.connect(
            LEDGER_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            steps = {
                row["step"]
                for row in conn.execute(
                    "SELECT step FROM transaction WHERE payment_order_id = %s",
                    (payment_order_id,),
                ).fetchall()
            }
            # Require the step1 payin to have been posted before fulfilling.
            if "step1_payin" not in steps:
                raise HTTPException(
                    status_code=400,
                    detail="no step1_payin transaction for payment_order_id",
                )
            # Terminal / idempotent: already fulfilled or refunded -> nothing to post.
            if "refund" in steps:
                return {"status": "refund"}
            if "step2b_fufilled" in steps:
                return {"status": "step2b_fufilled"}

            # Split the held amount: seller gets 90% (rounded up), platform income
            # gets 10% (rounded down). Integer math keeps both exact, and the two
            # credits always sum back to the full escrow debit (double-entry stays
            # balanced) because ceil(0.9*a) + floor(0.1*a) == a for integer a.
            seller_amount = (amount * 9 + 9) // 10  # ceil(amount * 0.9)
            income_amount = amount // 10  # floor(amount * 0.1)

            # Post the fulfilment Transaction plus its debit/credit triple.
            try:
                with conn.transaction():
                    txn_id = _snowflake_id()
                    conn.execute(
                        "INSERT INTO transaction (id, payment_order_id, step) "
                        "VALUES (%s, %s, 'step2b_fufilled')",
                        (txn_id, payment_order_id),
                    )
                    escrow_id = _account_id(conn, "platform", "escrow")
                    seller_account_id = _account_id(conn, seller_id, None)
                    income_id = _account_id(conn, "platform", "income")
                    # Debit escrow for the full held amount...
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'debit', %s, %s)",
                        (_snowflake_id(), txn_id, escrow_id, amount, currency),
                    )
                    # ...credit the seller's payout (90%)...
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'credit', %s, %s)",
                        (
                            _snowflake_id(),
                            txn_id,
                            seller_account_id,
                            seller_amount,
                            currency,
                        ),
                    )
                    # ...and credit the platform's income (10%).
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'credit', %s, %s)",
                        (_snowflake_id(), txn_id, income_id, income_amount, currency),
                    )
                    # Mark this order fulfilled on its readiness signal, atomic with
                    # the fulfilment triple. If the payin is also already complete,
                    # both readiness flags are now true, so post its Payout in the
                    # same transaction.
                    readiness = conn.execute(
                        "UPDATE payout_readiness SET fufilled = true "
                        "WHERE payment_order_id = %s "
                        "RETURNING payin_complete, fufilled",
                        (payment_order_id,),
                    ).fetchone()
                    if (
                        readiness is not None
                        and readiness["payin_complete"]
                        and readiness["fufilled"]
                    ):
                        conn.execute(
                            "INSERT INTO payout (id, payment_order_id) "
                            "VALUES (%s, %s)",
                            (_snowflake_id(), payment_order_id),
                        )
            except psycopg.errors.UniqueViolation:
                # A concurrent caller already posted step2b_fufilled; the triple is
                # rolled back. Treat it as already done.
                return {"status": "step2b_fufilled"}
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="ledger database error")

    return {"status": "step2b_fufilled"}


# ---------------------------------------------------------------------------
# Payment-flow refund (refund variant on the payment-flow path)
#
# POST /payment-flow/refund reverses a paid order's payin before it has been
# paid out — the same claw-back as POST /orders/refund, addressed at the
# payment-flow path. It finds the order's payment_success OrderPayment in
# order-db, then in ledger-db is terminal once the order is already refunded
# (200, "refund initiated") or fulfilled (200, "order complete cannot refund"),
# and requires a prior step1_payin Transaction (otherwise a 400). Otherwise it
# posts a refund Transaction plus its double-entry pair and a Refund row, all in
# one atomic transaction. Where the held escrow goes back depends on whether the
# payin was completed (step2a): if so the funds already sit in platform/cash, so
# we debit escrow / credit platform/cash; if not, the psp still holds the money,
# so we debit escrow / credit the psp. The amount/currency come from the
# OrderPayment row (the request only carries order_id). It touches two
# downstream nodes: order-db (OrderPayment) and ledger-db (Transaction /
# LedgerEntry / Account / Refund); a DB failure surfaces to the caller as a 400.
# ---------------------------------------------------------------------------


class PaymentFlowRefundRequest(BaseModel):
    order_id: str


@app.post("/payment-flow/refund")
def payment_flow_refund(req: PaymentFlowRefundRequest):
    """Refund a paid-but-unpaid-out order: claw the held escrow back."""
    # order-db: the order must have a payment_success OrderPayment to refund.
    try:
        with psycopg.connect(DB_DSN, row_factory=dict_row, autocommit=True) as conn:
            order_payment = conn.execute(
                "SELECT id, amount, currency FROM order_payment "
                "WHERE order_id = %s AND status = 'payment_success'",
                (req.order_id,),
            ).fetchone()
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="database error")
    if order_payment is None:
        raise HTTPException(
            status_code=400, detail="no payment_success order payment for order"
        )

    payment_order_id = order_payment["id"]
    amount = order_payment["amount"]
    currency = order_payment["currency"]

    # ledger-db: post the refund, gating on the step1 payin and terminal states.
    try:
        with psycopg.connect(
            LEDGER_DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            steps = {
                row["step"]
                for row in conn.execute(
                    "SELECT step FROM transaction WHERE payment_order_id = %s",
                    (payment_order_id,),
                ).fetchall()
            }
            # Already refunded -> nothing to post; the refund is in flight.
            if "refund" in steps:
                return {"status": "refund", "detail": "refund initiated"}
            # Already fulfilled (payout done) -> can't claw it back.
            if "step2b_fufilled" in steps:
                return {
                    "status": "step2b_fufilled",
                    "detail": "order complete cannot refund",
                }
            # Require the step1 payin to have been posted before refunding.
            if "step1_payin" not in steps:
                raise HTTPException(
                    status_code=400,
                    detail="no step1_payin transaction for payment_order_id",
                )

            # Whether the payin was completed (step2a) decides where escrow goes:
            # if completed the money already sits in platform/cash (credit it);
            # otherwise the psp still holds it (credit the psp). Either way we
            # record a Refund for the money to return to the customer.
            payin_completed = "step2a_payin-complete" in steps

            # Post the refund Transaction plus its debit/credit pair and Refund,
            # all in one atomic transaction.
            try:
                with conn.transaction():
                    txn_id = _snowflake_id()
                    conn.execute(
                        "INSERT INTO transaction (id, payment_order_id, step) "
                        "VALUES (%s, %s, 'refund')",
                        (txn_id, payment_order_id),
                    )
                    escrow_id = _account_id(conn, "platform", "escrow")
                    # Debit escrow for the full held amount...
                    conn.execute(
                        "INSERT INTO ledger_entry "
                        "(id, transaction_id, account_id, direction, amount, currency) "
                        "VALUES (%s, %s, %s, 'debit', %s, %s)",
                        (_snowflake_id(), txn_id, escrow_id, amount, currency),
                    )
                    if payin_completed:
                        # ...credit platform/cash (the completed payin landed here).
                        cash_id = _account_id(conn, "platform", "cash")
                        conn.execute(
                            "INSERT INTO ledger_entry "
                            "(id, transaction_id, account_id, direction, amount, currency) "
                            "VALUES (%s, %s, %s, 'credit', %s, %s)",
                            (_snowflake_id(), txn_id, cash_id, amount, currency),
                        )
                    else:
                        # ...credit the psp (it still holds the un-completed payin).
                        psp_id = _account_id(conn, "psp", None)
                        conn.execute(
                            "INSERT INTO ledger_entry "
                            "(id, transaction_id, account_id, direction, amount, currency) "
                            "VALUES (%s, %s, %s, 'credit', %s, %s)",
                            (_snowflake_id(), txn_id, psp_id, amount, currency),
                        )
                    # Record a Refund to be paid back out to the customer (posted
                    # in both branches, atomic with the double-entry above).
                    conn.execute(
                        "INSERT INTO refund "
                        "(id, transaction_id, payment_order_id, amount, currency) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (
                            _snowflake_id(),
                            txn_id,
                            payment_order_id,
                            amount,
                            currency,
                        ),
                    )
            except psycopg.errors.UniqueViolation:
                # A concurrent caller already posted the refund; rolled back.
                return {"status": "refund", "detail": "refund initiated"}
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="ledger database error")

    return {"status": "refund", "detail": "refund initiated"}


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
