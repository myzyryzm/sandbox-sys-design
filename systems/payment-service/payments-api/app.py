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
import secrets
import time
from typing import Dict, List, Literal

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
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
# Payment registration
#
# Mirrors the shared TypeScript models PaymentRegistrationRequest /
# PaymentRegistrationResponse from the system's models bank.
# ---------------------------------------------------------------------------


class PaymentRegistrationRequest(BaseModel):
    idempotency_key: str
    amount: float
    currency: str
    return_url: str
    payment_order_id: str
    seller_id: str


class PaymentRegistrationResponse(BaseModel):
    token: str
    currency: str
    amount: float
    payment_order_id: str
    seller_id: str


# In-memory idempotency storage. Two maps keep the lookup O(1) in both
# directions: the idempotency key tells us whether we've seen this request
# before, and the token lets us rebuild the response from the stored request.
_idempotency_to_token: Dict[str, str] = {}
_token_to_request: Dict[str, PaymentRegistrationRequest] = {}


def _response_for(token: str, req: PaymentRegistrationRequest) -> PaymentRegistrationResponse:
    return PaymentRegistrationResponse(
        token=token,
        currency=req.currency,
        amount=req.amount,
        payment_order_id=req.payment_order_id,
        seller_id=req.seller_id,
    )


@app.post("/payment", response_model=PaymentRegistrationResponse)
async def create_payment(req: PaymentRegistrationRequest):
    """Register a payment and hand back a token.

    Idempotent on `idempotency_key`: a repeat key replays the original token and
    its stored details instead of minting a new one.
    """
    existing_token = _idempotency_to_token.get(req.idempotency_key)
    if existing_token is not None:
        return _response_for(existing_token, _token_to_request[existing_token])

    token = secrets.token_urlsafe(24)
    _idempotency_to_token[req.idempotency_key] = token
    _token_to_request[token] = req
    return _response_for(token, req)


# ---------------------------------------------------------------------------
# Payment completion
#
# POST /complete-payment is the customer-facing step: they submit their card for
# a previously registered payment (identified by its token). We look the token up
# in the in-memory store from /payment, notify the merchant's webhook on
# service-1 (POST /payment/webhook, alias step1_paymentWebhook) of the outcome,
# and hand back a MakePaymentResponse telling the client where to redirect.
#
# The webhook notification is best-effort: this endpoint's own status/return_url
# is decided solely by whether the token exists (per the spec), so a webhook that
# errors or 404s — e.g. the failure path posts an empty payment_order_id that
# service-1 can't match — must not change what we return to the customer.
# ---------------------------------------------------------------------------

SERVICE_1_BASE = os.environ.get("SERVICE_1_BASE", "http://service-1:8000")


class MakePaymentRequest(BaseModel):
    token: str
    card_number: str


class MakePaymentResponse(BaseModel):
    status: Literal["success", "fail"]
    return_url: str


# In-memory log of every completion attempt's request body, in arrival order.
# Each incoming MakePaymentRequest is appended here regardless of outcome.
_processing_payments: List[MakePaymentRequest] = []


def _notify_webhook(
    status: str,
    currency: str,
    amount: int,
    payment_order_id: str,
    seller_id: str,
) -> None:
    """Fire service-1's step1_paymentWebhook with the attempt outcome.

    Best-effort: the customer-facing response does not depend on the webhook
    succeeding, so any transport/HTTP error is swallowed rather than surfaced.
    """
    try:
        httpx.post(
            f"{SERVICE_1_BASE}/payment-flow/1",
            json={
                "status": status,
                "currency": currency,
                "amount": amount,
                "payment_order_id": payment_order_id,
                "seller_id": seller_id,
            },
            timeout=10.0,
        )
    except Exception:
        # Webhook delivery is not part of this endpoint's contract; ignore it.
        pass


@app.post("/complete-payment", response_model=MakePaymentResponse)
def complete_payment(req: MakePaymentRequest):
    """Complete a registered payment by its token.

    A known token replays the stored PaymentRegistrationRequest into a 'success'
    webhook and redirects the customer to its return_url; an unknown token fires a
    'fail' webhook with empty fields and returns an empty redirect.
    """
    # Record the incoming completion request in arrival order.
    _processing_payments.append(req)

    pr = _token_to_request.get(req.token)
    if pr is None:
        _notify_webhook(
            status="fail",
            currency="",
            amount=0,
            payment_order_id="",
            seller_id="",
        )
        return MakePaymentResponse(status="fail", return_url="")

    _notify_webhook(
        status="success",
        currency=pr.currency,
        amount=int(pr.amount),
        payment_order_id=pr.payment_order_id,
        seller_id=pr.seller_id,
    )
    return MakePaymentResponse(status="success", return_url=pr.return_url)


# ---------------------------------------------------------------------------
# Payment cancellation
#
# POST /cancel-payment pulls a queued completion attempt out of the in-memory
# _processing_payments log by its idempotency key. Each entry there is a
# MakePaymentRequest keyed by token; the idempotency key lives on the registered
# PaymentRegistrationRequest, so we resolve each attempt's token through
# _token_to_request to find the one whose idempotency_key matches. A match is
# removed from _processing_payments and filed under _canceled_payments; if no
# queued attempt maps to the given key, we 404. Purely in-memory, no downstream.
# ---------------------------------------------------------------------------


class CancelPaymentRequest(BaseModel):
    idempotency_key: str


# In-memory log of completion attempts that have been canceled before settlement.
_canceled_payments: List[MakePaymentRequest] = []


@app.post("/cancel-payment")
def cancel_payment(req: CancelPaymentRequest):
    """Cancel a queued completion attempt by its idempotency key.

    Scans _processing_payments for the attempt whose token resolves to a
    registered payment carrying req.idempotency_key. The first match is removed
    from the processing queue and appended to _canceled_payments (returning
    success); if none is found, the key isn't queued and we return a 404 error.
    """
    for attempt in _processing_payments:
        pr = _token_to_request.get(attempt.token)
        if pr is not None and pr.idempotency_key == req.idempotency_key:
            _processing_payments.remove(attempt)
            _canceled_payments.append(attempt)
            return {"status": "success"}

    raise HTTPException(
        status_code=404,
        detail=f"No processing payment found for idempotency_key {req.idempotency_key}",
    )


# ---------------------------------------------------------------------------
# Batch payin completion
#
# POST /process-payment sweeps the in-memory _processing_payments log (every
# completion attempt recorded by /complete-payment) and, for each entry whose
# token still resolves to a registered PaymentRegistrationRequest, asks service-1
# to settle the payin via paymentFlowStep2a (POST /payment-flow/2a). A 200 means
# the payin completed, so that PaymentRegistrationRequest is filed under
# _processed_payments; a transport error or any non-200 files it under
# _failed_payments. Entries whose token no longer maps to a registered request
# have nothing to settle and are skipped.
# ---------------------------------------------------------------------------

# In-memory outcomes accumulated across process-payment sweeps.
_processed_payments: List[PaymentRegistrationRequest] = []
_failed_payments: List[PaymentRegistrationRequest] = []


@app.post("/process-payment")
def process_payment():
    """Settle every queued completion attempt through service-1's step2a webhook.

    For each MakePaymentRequest recorded in _processing_payments we look up its
    registered PaymentRegistrationRequest by token and POST it to service-1's
    paymentFlowStep2a (/payment-flow/2a). A 200 files the PaymentRegistrationRequest
    under _processed_payments; a transport error or any non-200 files it under
    _failed_payments.

    The queue is drained up front: we snapshot the current attempts and clear the
    list so each attempt is processed exactly once, while anything /complete-payment
    appends during this sweep stays queued for the next call.
    """
    attempts = list(_processing_payments)
    _processing_payments.clear()

    for attempt in attempts:
        pr = _token_to_request.get(attempt.token)
        if pr is None:
            # Unknown token: no registered payment to complete, so skip it.
            continue
        try:
            resp = httpx.post(
                f"{SERVICE_1_BASE}/payment-flow/2a",
                json={
                    "idempotency_key": pr.idempotency_key,
                    "amount": int(pr.amount),
                    "currency": pr.currency,
                    "return_url": pr.return_url,
                    "payment_order_id": pr.payment_order_id,
                    "seller_id": pr.seller_id,
                },
                timeout=10.0,
            )
            succeeded = resp.status_code == 200
        except Exception:
            # Treat any transport failure as a failed payin for this attempt.
            succeeded = False

        if succeeded:
            _processed_payments.append(pr)
        else:
            _failed_payments.append(pr)

    return {
        "processed": len(_processed_payments),
        "failed": len(_failed_payments),
    }


# ---------------------------------------------------------------------------
# Payin refund
#
# POST /refund-payment scans the in-memory _processed_payments log (payins that
# /process-payment successfully settled) for the PaymentRegistrationRequest
# carrying req.idempotency_key. A match is filed under _refunded_payments and we
# return success; if no processed payment carries that key, we 404. Purely
# in-memory, no downstream.
# ---------------------------------------------------------------------------


class RefundPaymentRequest(BaseModel):
    idempotency_key: str


# In-memory log of processed payins that have since been refunded.
_refunded_payments: List[PaymentRegistrationRequest] = []


@app.post("/refund-payment")
def refund_payment(req: RefundPaymentRequest):
    """Refund a settled payin by its idempotency key.

    Scans _processed_payments for the PaymentRegistrationRequest whose
    idempotency_key matches req.idempotency_key. The first match is appended to
    _refunded_payments (returning success); if none is found, that key was never
    processed and we return a 404 error.
    """
    for pr in _processed_payments:
        if pr.idempotency_key == req.idempotency_key:
            _refunded_payments.append(pr)
            return {"status": "success"}

    raise HTTPException(
        status_code=404,
        detail=f"No processed payment found for idempotency_key {req.idempotency_key}",
    )


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
