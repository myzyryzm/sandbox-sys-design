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
# etcd discovery — shared client setup (sandbox-etcd skill)
# ---------------------------------------------------------------------------

import os
import random
import threading

import etcd3

ETCD_ENDPOINTS = os.environ.get("ETCD_ENDPOINTS", "etcd-1:2379").split(",")


# ---------------------------------------------------------------------------
# etcd listener on /services/llm-worker/ (sandbox-etcd skill)
#
# Keeps a live in-memory map of llm-worker workers (worker id -> host:port).
# etcd PUSHES every change over the watch stream — a PUT adds/updates a worker,
# a DELETE (explicit or lease expiry when a worker dies) removes it. Never
# polls. On any watch error: reconnect and resync from a fresh get_prefix.
# ---------------------------------------------------------------------------

_LLM_WORKER_PREFIX = "/services/llm-worker/"
LLM_WORKERS: dict = {}  # worker id -> host:port — the live view other code reads


def _watch_llm_workers():
    while True:
        client = None
        try:
            host, port = random.choice(ETCD_ENDPOINTS).split(":")
            client = etcd3.client(host=host, port=int(port), timeout=5)

            def _apply(resp):  # etcd PUSHES each change here — the hot path
                for ev in resp.events:
                    wid = ev.key.decode()[len(_LLM_WORKER_PREFIX):]
                    if isinstance(ev, etcd3.events.PutEvent):
                        LLM_WORKERS[wid] = ev.value.decode()
                    else:  # DeleteEvent: explicit delete OR lease expiry
                        LLM_WORKERS.pop(wid, None)

            client.add_watch_prefix_callback(_LLM_WORKER_PREFIX, _apply)
            # baseline AFTER the watch is armed, so no change can fall in the gap
            fresh = {
                m.key.decode()[len(_LLM_WORKER_PREFIX):]: v.decode()
                for v, m in client.get_prefix(_LLM_WORKER_PREFIX)
            }
            LLM_WORKERS.clear()
            LLM_WORKERS.update(fresh)
            # Slow anti-entropy sweep: after a cluster recreate the re-armed watch can
            # silently wait on a "future revision" and deliver nothing — the sweep
            # detects any silent staleness and forces a full resync. Updates still
            # arrive pushed; this is a correctness backstop, not the delivery path.
            while True:
                time.sleep(30)
                fresh = {
                    m.key.decode()[len(_LLM_WORKER_PREFIX):]: v.decode()
                    for v, m in client.get_prefix(_LLM_WORKER_PREFIX)
                }
                if fresh != LLM_WORKERS:
                    raise RuntimeError("stale watch — full resync")
        except Exception:
            time.sleep(2)  # reconnect + full resync from scratch
        finally:
            try:
                if client is not None:
                    client.close()  # tears down the watch stream + its threads
            except Exception:
                pass


threading.Thread(target=_watch_llm_workers, daemon=True).start()


@app.get("/discovery/llm-worker")
async def discovery_llm_worker():
    """The live llm-worker worker set, as maintained by the etcd watch."""
    return {"keyspace": _LLM_WORKER_PREFIX, "workers": dict(LLM_WORKERS)}
