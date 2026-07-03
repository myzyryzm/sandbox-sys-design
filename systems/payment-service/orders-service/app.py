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
import time
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
# Shared checkout config + models
#
# DB_DSN / PAYMENTS_API_BASE and the OrderCheckout request/response models below
# are used by the POST /checkout handler. Any database transaction that fails
# surfaces to the caller as a 400 (bad request). That handler is a sync `def` so
# FastAPI runs it in a thread pool — that keeps the blocking psycopg / httpx
# calls off the event loop while the metrics middleware above still measures it
# like any other request.
# ---------------------------------------------------------------------------

DB_DSN = os.environ.get(
    "ORDER_DB_DSN", "postgresql://sandbox:sandbox@order-db:5432/order_db"
)
PAYMENTS_API_BASE = os.environ.get("PAYMENTS_API_BASE", "http://payments-api:8000")


class OrderCheckoutRequest(BaseModel):
    order_id: str


class OrderCheckoutResponse(BaseModel):
    token: Optional[str] = None
    status: str


# ---------------------------------------------------------------------------
# Checkout (POST /checkout)
#
# Starts or resumes the payment for an order. When the payments-api registration
# call itself fails, the OrderPayment is flipped to `register_fail` and the
# caller gets a 400 (bad request) — the failure surfaces immediately. It talks
# to two downstream nodes: order-db (Order / OrderPayment / OrderItem / Item)
# and payments-api (POST /payment).
# ---------------------------------------------------------------------------


def _checkout_call_payment_api(
    conn, payment_id: str, amount: int, currency: str, seller_id: str
) -> OrderCheckoutResponse:
    """Register a pending OrderPayment with payments-api and settle its status.

    The OrderPayment id doubles as the idempotency_key and payment_order_id, so
    re-registering the same attempt replays the same token rather than
    re-charging. If the registration call fails, the row is marked `register_fail`
    and the request is rejected with a 400. On success we read the row's current
    status and, only while it is still `pending`, advance it to `register_success`
    with the returned token (a read-then-write guard); otherwise 400.
    """
    try:
        resp = httpx.post(
            f"{PAYMENTS_API_BASE}/payment",
            json={
                "idempotency_key": payment_id,
                "amount": amount,
                "currency": currency,
                "return_url": "",
                "payment_order_id": payment_id,
                "seller_id": seller_id,
            },
            timeout=10.0,
        )
        resp.raise_for_status()
        token = resp.json()["token"]
    except Exception:
        # Registration failed: mark the attempt failed and reject the request.
        conn.execute(
            "UPDATE order_payment SET status = 'register_fail' WHERE id = %s",
            (payment_id,),
        )
        raise HTTPException(status_code=400, detail="payment registration failed")

    # Read the current status, then advance to register_success only while the
    # attempt is still pending; anything else is a bad request.
    current = conn.execute(
        "SELECT status FROM order_payment WHERE id = %s", (payment_id,)
    ).fetchone()
    if current is None or current["status"] != "pending":
        raise HTTPException(
            status_code=400, detail="order payment is no longer pending"
        )
    conn.execute(
        "UPDATE order_payment SET status = 'register_success', token = %s "
        "WHERE id = %s AND status = 'pending'",
        (token, payment_id),
    )
    return OrderCheckoutResponse(status="register_success", token=token)


def _checkout_create_order_payment(
    conn, attempt_idx: int, order_id: str
) -> OrderCheckoutResponse:
    """Create a fresh pending OrderPayment attempt and register it.

    Sums the order's Item prices (seller_id and currency assumed uniform across
    the order's items), writes a `pending` OrderPayment (id =
    `<order_id>:<attempt_idx>`), then registers it via
    `_checkout_call_payment_api`.
    """
    items = conn.execute(
        "SELECT i.price, i.currency, i.seller_id "
        "FROM order_item oi JOIN item i ON oi.item_id = i.id "
        "WHERE oi.order_id = %s",
        (order_id,),
    ).fetchall()
    amount = sum(int(i["price"]) for i in items)
    currency = items[0]["currency"] if items else ""
    seller_id = items[0]["seller_id"] if items else ""

    payment_id = f"{order_id}:{attempt_idx}"

    conn.execute(
        "INSERT INTO order_payment "
        "(id, order_id, seller_id, amount, currency, attempt_idx, status) "
        "VALUES (%s, %s, %s, %s, %s, %s, 'pending')",
        (payment_id, order_id, seller_id, amount, currency, attempt_idx),
    )

    return _checkout_call_payment_api(conn, payment_id, amount, currency, seller_id)


@app.post(
    "/checkout",
    response_model=OrderCheckoutResponse,
    response_model_exclude_none=True,
)
def checkout(req: OrderCheckoutRequest):
    """Start or resume the checkout for an order.

    Looks up the order, then inspects its most recent payment attempt and either
    replays that attempt's state or starts the next attempt. Any DB failure is
    surfaced as a 400, and a failed payments-api registration is a 400 too.
    """
    try:
        with psycopg.connect(
            DB_DSN, row_factory=dict_row, autocommit=True
        ) as conn:
            order = conn.execute(
                'SELECT id FROM "order" WHERE id = %s', (req.order_id,)
            ).fetchone()
            if order is None:
                raise HTTPException(status_code=400, detail="order not found")

            # All attempts for this order, oldest first; the last row is current.
            payments = conn.execute(
                "SELECT id, attempt_idx, status, token, seller_id, amount, currency "
                "FROM order_payment "
                "WHERE order_id = %s ORDER BY created_at ASC, attempt_idx ASC",
                (req.order_id,),
            ).fetchall()

            if not payments:
                return _checkout_create_order_payment(conn, 1, req.order_id)

            last = payments[-1]
            status = last["status"]
            if status == "payment_success":
                return OrderCheckoutResponse(status=status)
            if status == "register_success":
                return OrderCheckoutResponse(status=status, token=last["token"])
            if status == "pending":
                # Resume the still-pending attempt: re-register with payments-api
                # using its saved fields (its id is the idempotency key, so this
                # replays rather than re-charges), then settle it.
                return _checkout_call_payment_api(
                    conn, last["id"], last["amount"], last["currency"], last["seller_id"]
                )
            # register_fail / payment_fail -> start the next attempt.
            return _checkout_create_order_payment(
                conn, int(last["attempt_idx"]) + 1, req.order_id
            )
    except psycopg.Error:
        raise HTTPException(status_code=400, detail="database error")


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
