"""
FastAPI backend for the `hello-lb` sandbox system.

This is a learning tool: the Prometheus instrumentation is written by hand with
`prometheus_client` (no black-box auto-instrumentor) so you can read exactly how
each metric is produced. The interesting part is `metrics_middleware` below.

Endpoints:
  GET /health                       -> {"status": "ok"}
  GET /metrics                      -> Prometheus exposition format (the three metrics defined below)
  GET /discovery/llm-worker         -> live etcd-discovered map of llm-worker workers
  GET /discovery/usr-msg-consumer   -> live etcd-discovered map of usr-msg-consumer workers
  GET /config/app-settings          -> live etcd-watched config map (key -> value)
"""

import os
import random
import threading
import time

import etcd3
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


# ---------------------------------------------------------------------------
# etcd watch listeners — discovery keyspaces (/services/llm-worker/,
# /services/usr-msg-consumer/) and a config keyspace (/config/app-settings/).
#
# etcd PUSHES every change over the watch stream (a PUT adds/updates a key; a
# DELETE removes it). We never poll for updates; the 30s get_prefix sweep below
# is only an anti-entropy backstop that tears down a watch gone silently stale
# after a cluster recreate (the re-armed watch would wait on a future revision
# and deliver nothing, with no error raised) — it's also what picks the app's
# replayed config values back up after a recreate.
#
# For a discovery keyspace a DELETE is an explicit delete OR a lease expiry (a
# worker died); for the config keyspace the keys are persistent (no lease), so a
# DELETE only ever means the web app explicitly removed that key.
# ---------------------------------------------------------------------------

ETCD_ENDPOINTS = os.environ.get("ETCD_ENDPOINTS", "etcd-1:2379").split(",")

LLM_WORKERS: dict[str, str] = {}  # worker id -> host:port — the live view
USR_MSG_WORKERS: dict[str, str] = {}  # worker id -> host:port — the live view
CONFIG: dict[str, str] = {}  # key -> value — the live settings other code reads


def _watch_prefix(prefix: str, store: dict[str, str]):
    while True:
        client = None
        try:
            host, port = random.choice(ETCD_ENDPOINTS).split(":")
            client = etcd3.client(host=host, port=int(port), timeout=5)

            def _apply(resp):  # etcd pushes each change here — the hot path
                for ev in resp.events:
                    k = ev.key.decode()[len(prefix):]
                    if isinstance(ev, etcd3.events.PutEvent):
                        store[k] = ev.value.decode()
                    else:  # DeleteEvent: explicit delete OR (discovery) lease expiry
                        store.pop(k, None)

            client.add_watch_prefix_callback(prefix, _apply)
            # baseline AFTER the watch is armed, so no change falls in the gap
            fresh = {m.key.decode()[len(prefix):]: v.decode()
                     for v, m in client.get_prefix(prefix)}
            store.clear()
            store.update(fresh)
            while True:
                time.sleep(30)
                fresh = {m.key.decode()[len(prefix):]: v.decode()
                         for v, m in client.get_prefix(prefix)}
                if fresh != store:
                    raise RuntimeError("stale watch — full resync")
        except Exception:
            time.sleep(2)  # quorum loss / member down -> keep last-known view
        finally:
            try:
                if client is not None:
                    client.close()  # tears down the watch stream + its threads
            except Exception:
                pass


threading.Thread(
    target=_watch_prefix, args=("/services/llm-worker/", LLM_WORKERS), daemon=True
).start()
threading.Thread(
    target=_watch_prefix,
    args=("/services/usr-msg-consumer/", USR_MSG_WORKERS),
    daemon=True,
).start()
threading.Thread(
    target=_watch_prefix, args=("/config/app-settings/", CONFIG), daemon=True
).start()


@app.get("/discovery/llm-worker")
async def discovery_llm_worker():
    return {"keyspace": "/services/llm-worker/", "workers": dict(LLM_WORKERS)}


@app.get("/discovery/usr-msg-consumer")
async def discovery_usr_msg_consumer():
    return {"keyspace": "/services/usr-msg-consumer/", "workers": dict(USR_MSG_WORKERS)}


@app.get("/config/app-settings")
async def config_app_settings():
    return {"keyspace": "/config/app-settings/", "config": dict(CONFIG)}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
