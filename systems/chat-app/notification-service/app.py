"""
FastAPI backend for the `hello-lb` sandbox system.

This is a learning tool: the Prometheus instrumentation is written by hand with
`prometheus_client` (no black-box auto-instrumentor) so you can read exactly how
each metric is produced. The interesting part is `metrics_middleware` below.

Endpoints:
  GET /health         -> {"status": "ok"}
  GET /metrics        -> Prometheus exposition format (the three metrics defined below)
  GET /notifications  -> all notifications addressed to a user (Notification.to)
"""

import os
import time

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pymongo import MongoClient
from pymongo.errors import PyMongoError

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


# ---------------------------------------------------------------------------
# notification-db (mongodb) source
#
# notification-db has no host port — it is reached over the docker network as
# `notification-db:27017` (db `notification_db`, collection `Notification`, all
# fields required + typed string by its validator). One lazily-established,
# pooled MongoClient is reused across requests; MongoClient is lazy, so an
# unreachable db surfaces as a PyMongoError on the first query (-> 503), not here.
# ---------------------------------------------------------------------------
NOTIFICATION_DB_URL = os.environ.get(
    "NOTIFICATION_DB_URL", "mongodb://notification-db:27017"
)
_mongo_client = None


def notification_collection():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(NOTIFICATION_DB_URL, serverSelectionTimeoutMS=5000)
    return _mongo_client["notification_db"]["Notification"]


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


# GetNotificationsRequest is `{ id: string }`; the sandbox `lb.get()` helper can't
# carry a GET body, so the single field arrives as the `id` query parameter. `id`
# identifies the user, matched against Notification.to. Response shape is
# GetNotifcationsResponse `{ notifications: Notification[] }`. Sync `def` so
# FastAPI runs the blocking pymongo call in its threadpool.
@app.get("/notifications")
def get_notifications(
    id: str = Query(..., description="user id; matched against Notification.to"),
):
    """Return every notification addressed to `id` (the Notification.to field)."""
    try:
        col = notification_collection()
        docs = col.find({"to": id}, {"_id": 0})
        notifications = [
            {
                "id": str(d.get("id", "")),
                "to": str(d.get("to", "")),
                "from": str(d.get("from", "")),
                "message": str(d.get("message", "")),
                "sentAt": str(d.get("sentAt", "")),
            }
            for d in docs
        ]
    except PyMongoError:
        return JSONResponse(
            status_code=503, content={"detail": "notification-db unavailable"}
        )
    return {"notifications": notifications}
