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
  GET /assignments                  -> current llm-worker -> usr-msg-consumer rebalance mapping
"""

import os
import random
import threading
import time
import traceback

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


# ---------------------------------------------------------------------------
# Worker<->consumer rebalancing (the on_llm_worker per-event handler)
#
# The coordinator keeps three live etcd views: LLM_WORKERS, USR_MSG_WORKERS and
# CONFIG (above). On any pushed change to any of them it recomputes a mapping of
# usr-msg-consumer -> {llm-worker...}, capped at WORKER_RATIO llm-workers per
# consumer (from /config/app-settings), and pushes it out over gRPC:
#   - each llm-worker is told its assigned consumer via Worker.UpdateConsumer
#   - each usr-msg-consumer is told its worker set via Consumer.UpdateWorkers
# A worker stays on its current consumer when possible; a worker with no valid
# current consumer is placed on the least-loaded consumer (least-connections).
#
# The gRPC pushes BLOCK (network I/O with timeouts), and a watch's per-event
# handler runs on the watch callback thread which must never block — so the
# handlers only WAKE a dedicated rebalance thread (below) that does the work.
# The coordinator is a gRPC CLIENT of both contracts; the stubs are vendored
# copies of the shared grpc/ ones, downgraded to protobuf 3.20.3 (the version
# the etcd3 client pins) — see the NOTE header in each *_pb2.py.
# ---------------------------------------------------------------------------

import grpc

import Consumer_pb2
import Consumer_pb2_grpc
import Worker_pb2
import Worker_pb2_grpc

GRPC_PORT = 50051  # Worker and Consumer both serve gRPC on this intra-net port

ASSIGNMENTS: dict[str, str] = {}  # llm-worker id -> usr-msg-consumer id (last pushed)
_assign_lock = threading.Lock()
_rebalance_wake = threading.Event()


def _worker_ratio() -> int:
    """Max llm-workers per usr-msg-consumer, from CONFIG['WORKER_RATIO'] (>=1)."""
    try:
        return max(1, int(CONFIG.get("WORKER_RATIO")))
    except (TypeError, ValueError):
        return 1  # config key absent/garbage: fall back to one worker per consumer


def _compute_assignment(worker_ids, consumer_ids, ratio, prev):
    """Bipartite worker->consumer assignment: sticky first, then least-connections.

    - Keep a worker on its previous consumer when that consumer still exists and
      has spare capacity (< ratio) — "stay connected if possible".
    - Any remaining worker goes to the consumer with the fewest assigned workers
      (ties broken by id for determinism), never exceeding `ratio`.
    Returns worker_id -> consumer_id (workers past total capacity are omitted).
    """
    consumers = set(consumer_ids)
    load = {c: 0 for c in consumer_ids}
    assignment = {}
    leftover = []
    for w in sorted(worker_ids):
        c = prev.get(w)
        if c in consumers and load[c] < ratio:
            assignment[w] = c  # sticky: keep it where it already was
            load[c] += 1
        else:
            leftover.append(w)
    for w in leftover:
        free = [c for c in consumer_ids if load[c] < ratio]
        if not free:
            continue  # every consumer at capacity: this worker stays unassigned
        c = min(free, key=lambda c: (load[c], c))  # least-connections, id tiebreak
        assignment[w] = c
        load[c] += 1
    return assignment


def _grpc_host(value: str) -> str:
    """Registry values are '<dns>:8000' (the HTTP port); gRPC is on GRPC_PORT."""
    return value.rsplit(":", 1)[0]


def _push_update_consumer(worker_value: str, consumer_value: str):
    """Tell one llm-worker which consumer it is connected to (Worker.UpdateConsumer)."""
    whost = _grpc_host(worker_value)
    chost = _grpc_host(consumer_value)
    try:
        with grpc.insecure_channel(f"{whost}:{GRPC_PORT}") as ch:
            Worker_pb2_grpc.WorkerStub(ch).UpdateConsumer(
                Worker_pb2.UpdateConsumerRequest(host=chost, port=GRPC_PORT), timeout=5
            )
    except Exception as exc:
        print(f"[rebalance] UpdateConsumer {whost} -> {chost} failed: {exc}", flush=True)


def _push_update_workers(consumer_value: str, worker_values):
    """Replace one consumer's full worker set (Consumer.UpdateWorkers)."""
    chost = _grpc_host(consumer_value)
    endpoints = [
        Consumer_pb2.WorkerEndpoint(host=_grpc_host(v), port=GRPC_PORT)
        for v in worker_values
    ]
    try:
        with grpc.insecure_channel(f"{chost}:{GRPC_PORT}") as ch:
            Consumer_pb2_grpc.ConsumerStub(ch).UpdateWorkers(
                Consumer_pb2.UpdateWorkersRequest(workers=endpoints), timeout=5
            )
    except Exception as exc:
        print(f"[rebalance] UpdateWorkers {chost} ({len(endpoints)}) failed: {exc}", flush=True)


def _do_rebalance():
    """Recompute the worker<->consumer mapping from the live views and push it."""
    with _assign_lock:
        workers = dict(LLM_WORKERS)  # snapshot: worker id -> "<dns>:8000"
        consumers = dict(USR_MSG_WORKERS)  # snapshot: consumer id -> "<dns>:8000"
        ratio = _worker_ratio()
        assignment = _compute_assignment(
            workers.keys(), consumers.keys(), ratio, ASSIGNMENTS
        )
        ASSIGNMENTS.clear()
        ASSIGNMENTS.update(assignment)
        # every live consumer (incl. those now holding none) so UpdateWorkers also
        # clears consumers that just lost their workers
        by_consumer = {c: [] for c in consumers}
        for w, c in assignment.items():
            by_consumer[c].append(w)

    # push OUTSIDE the lock — gRPC blocks; _rebalance_loop already serializes calls
    print(
        f"[rebalance] {len(workers)} workers, {len(consumers)} consumers, ratio "
        f"{ratio} -> " + (", ".join(f"{c}:{len(ws)}" for c, ws in by_consumer.items()) or "(none)"),
        flush=True,
    )
    for w, c in assignment.items():
        _push_update_consumer(workers[w], consumers[c])
    for c, ws in by_consumer.items():
        _push_update_workers(consumers[c], [workers[w] for w in ws])


def _rebalance_loop():
    """Serialize + coalesce rebalances off the watch callback threads.

    Woken by any handler (below); also re-asserts the mapping every 30s so the
    system converges after a coordinator restart (baseline load fires no events)
    or a transient push failure. Idempotent, so re-pushing is harmless.
    """
    while True:
        triggered = _rebalance_wake.wait(timeout=30)
        _rebalance_wake.clear()
        if triggered:
            time.sleep(0.3)  # debounce a burst of events into one rebalance
            _rebalance_wake.clear()
        try:
            _do_rebalance()
        except Exception:
            traceback.print_exc()


# --- per-event handlers (fired by the watches, once per pushed change) -------
# Each only WAKES the rebalance thread — they run on the watch callback thread,
# which must never block, so the actual gRPC work happens off-thread.


def on_llm_worker(event_type, key, value, workers):
    """llm-worker set changed (put/delete): rebalance the worker<->consumer map."""
    _rebalance_wake.set()


def on_usr_msg_consumer(event_type, key, value, consumers):
    """usr-msg-consumer set changed: the assignment target set moved, rebalance."""
    _rebalance_wake.set()


def on_app_settings(event_type, key, value, config):
    """Config changed (e.g. WORKER_RATIO): the cap moved, rebalance."""
    _rebalance_wake.set()


def _fire(handler, event_type, key, value, current_map):
    """Run an authored per-event handler in isolation — it must never stall or
    kill the watch (that would trip the 30s sweep into a resync storm)."""
    try:
        handler(event_type, key, value, current_map)
    except Exception:
        traceback.print_exc()


def _watch_prefix(prefix: str, store: dict[str, str], on_event=None):
    while True:
        client = None
        try:
            host, port = random.choice(ETCD_ENDPOINTS).split(":")
            client = etcd3.client(host=host, port=int(port), timeout=5)

            def _apply(resp):  # etcd pushes each change here — the hot path
                for ev in resp.events:
                    k = ev.key.decode()[len(prefix):]
                    if isinstance(ev, etcd3.events.PutEvent):
                        val = ev.value.decode()
                        store[k] = val
                        if on_event is not None:  # AFTER the map update; pushed only
                            _fire(on_event, "put", k, val, store)
                    else:  # DeleteEvent: explicit delete OR (discovery) lease expiry
                        store.pop(k, None)
                        if on_event is not None:
                            _fire(on_event, "delete", k, None, store)

            client.add_watch_prefix_callback(prefix, _apply)
            # baseline AFTER the watch is armed, so no change falls in the gap
            # (do NOT fire on_event for these keys — only for pushed events above)
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
    target=_watch_prefix,
    args=("/services/llm-worker/", LLM_WORKERS, on_llm_worker),
    daemon=True,
).start()
threading.Thread(
    target=_watch_prefix,
    args=("/services/usr-msg-consumer/", USR_MSG_WORKERS, on_usr_msg_consumer),
    daemon=True,
).start()
threading.Thread(
    target=_watch_prefix,
    args=("/config/app-settings/", CONFIG, on_app_settings),
    daemon=True,
).start()
threading.Thread(target=_rebalance_loop, daemon=True).start()


@app.get("/discovery/llm-worker")
async def discovery_llm_worker():
    return {"keyspace": "/services/llm-worker/", "workers": dict(LLM_WORKERS)}


@app.get("/discovery/usr-msg-consumer")
async def discovery_usr_msg_consumer():
    return {"keyspace": "/services/usr-msg-consumer/", "workers": dict(USR_MSG_WORKERS)}


@app.get("/config/app-settings")
async def config_app_settings():
    return {"keyspace": "/config/app-settings/", "config": dict(CONFIG)}


@app.get("/assignments")
async def assignments():
    """The current llm-worker -> usr-msg-consumer rebalance mapping (last pushed)."""
    with _assign_lock:
        mapping = dict(ASSIGNMENTS)
    by_consumer: dict[str, list[str]] = {}
    for w, c in mapping.items():
        by_consumer.setdefault(c, []).append(w)
    return {
        "ratio": _worker_ratio(),
        "worker_to_consumer": mapping,
        "consumer_to_workers": by_consumer,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    """Serve the current metric values in Prometheus exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
