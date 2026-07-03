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
from typing import Dict, List

import httpx
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
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


# ---------------------------------------------------------------------------
# Make payout
#
# Mirrors the shared TypeScript model MakePayoutRequest from the system's
# models bank. The handler simply records each incoming request in an in-memory
# map keyed by its idempotency_key so a repeat key overwrites the stored copy.
# ---------------------------------------------------------------------------


class MakePayoutRequest(BaseModel):
    idempotency_key: str
    amount: float
    currency: str
    recipient: str


# In-memory store of payout requests, keyed by idempotency_key.
processing_payouts: Dict[str, MakePayoutRequest] = {}

# Results of the last /process-payouts run, accumulated across runs. A payout
# lands in exactly one of these depending on whether payout-service-2 accepted it.
processed_payments: List[MakePayoutRequest] = []
failed_payments: List[MakePayoutRequest] = []

# payout-service-2 serves processPayout at POST /payout (reached service-to-service,
# not through the lb). Override with the env var to point elsewhere in tests.
PAYOUT_SERVICE_2_BASE = os.environ.get(
    "PAYOUT_SERVICE_2_BASE", "http://payout-service-2:8000"
)


@app.post("/payout")
async def payout(req: MakePayoutRequest):
    """Save the incoming payout request, keyed by its idempotency_key.

    Responds with just a status code and a standard message: 200 + "payout
    accepted" on success, 500 + "payout failed" if the save did not complete.
    """
    try:
        processing_payouts[req.idempotency_key] = req
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "payout failed"},
        )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "payout accepted"},
    )


# ---------------------------------------------------------------------------
# Process payouts
#
# Drains the in-memory `processing_payouts` queue: for each buffered
# MakePayoutRequest it calls payout-service-2.processPayout (POST /payout) with
# just the request's idempotency_key. A 2xx moves that payout into
# `processed_payments`; any error (non-2xx, timeout, connection failure) moves it
# into `failed_payments`. Either way the queue is cleared once every buffered
# payout has been attempted. Touches one downstream node, payout-service-2.
# ---------------------------------------------------------------------------


@app.post("/process-payouts")
async def process_payouts():
    """Submit every buffered payout to payout-service-2, then clear the queue."""
    # Snapshot the buffered payouts so the dict can be cleared at the end without
    # racing the iteration.
    pending = list(processing_payouts.values())

    for payout_req in pending:
        try:
            resp = httpx.post(
                f"{PAYOUT_SERVICE_2_BASE}/payout",
                json={"idempotency_key": payout_req.idempotency_key},
                timeout=10.0,
            )
            resp.raise_for_status()
        except Exception:
            failed_payments.append(payout_req)
        else:
            processed_payments.append(payout_req)

    # Every buffered payout has been attempted; clear the queue.
    processing_payouts.clear()

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "processed": len(processed_payments),
            "failed": len(failed_payments),
        },
    )


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
