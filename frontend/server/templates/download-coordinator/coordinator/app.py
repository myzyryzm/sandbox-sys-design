"""Download Coordinator service.

Roles in one process:
  - origin SEED      : holds the whole file (every chunk), served via ChunkTransfer.
  - ORCHESTRATOR     : Coordination gRPC server — registers workers, hands out
                       assignments (load-balanced across all current holders), tracks
                       liveness, records completion.
  - control API      : POST /dc/distribute, GET /dc/sources, GET /dc/state (driven by
                       the coordinator's custom Edit tab + the diagram poll).

All orchestration state lives in the `Orchestrator` object — deliberately separable so
a future hot standby need only mirror it (standby itself is out of scope for v1).

Prometheus instrumentation is hand-written (same shape as the generic service template)
so the manifest's req/s, p95, in-flight and error queries work for this node too.
"""

import asyncio
import collections
import os
import random
import shutil
import sys
import threading
import time
import urllib.request
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# The shared gRPC package + chunk store are mounted read-only (see docker-compose):
#   /app/grpc_pkg  -> systems/<id>/grpc      (generated _pb2 + the shared servicers)
#   /app/dc_common -> systems/<id>/dc_common (the shared ChunkStore)
sys.path.insert(0, "/app/grpc_pkg")
sys.path.insert(0, "/app")

import grpc  # noqa: E402

import ChunkTransfer_pb2_grpc as ct_grpc  # noqa: E402
import Coordination_pb2_grpc as co_grpc  # noqa: E402
from ChunkTransfer_servicer import ChunkTransferServicer  # noqa: E402
from Coordination_servicer import CoordinationServicer  # noqa: E402
from dc_common.chunkstore import ChunkStore, chunk_file, sha256_hex  # noqa: E402

SERVICE_ID = os.environ.get("SERVICE_ID", "coordinator")
DATA_DIR = "/data"
SOURCE_DIR = os.path.join(DATA_DIR, "source")
GRPC_PORT = 50051
DEFAULT_CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(64 * 1024 * 1024)))

# ---------------------------------------------------------------------------
# Prometheus instrumentation (identical shape to templates/service/app.py)
# ---------------------------------------------------------------------------
http_requests_total = Counter(
    "http_requests_total", "Total HTTP requests processed", ["method", "endpoint", "status"]
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "HTTP request duration in seconds", ["method", "endpoint"]
)
http_requests_in_flight = Gauge(
    "http_requests_in_flight", "Number of HTTP requests currently in flight"
)
EXCLUDED_PATHS = {"/metrics"}


# ---------------------------------------------------------------------------
# Orchestrator — the separable distribution state (the hot-standby seam)
# ---------------------------------------------------------------------------
class Orchestrator:
    ALIVE_WINDOW = 15.0  # a worker silent longer than this is not used as a source

    def __init__(self, coordinator_id, store):
        self.id = coordinator_id
        self.store = store
        self._lock = threading.Lock()
        self.phase = "idle"  # idle | downloading | chunking | distributing | error
        self.error = None
        self.ready = False
        self.chunk_count = 0
        self.chunk_size = 0
        self.file_size = 0
        self.full_hash = ""
        self.checksums = []
        self.workers = {}  # id -> {bitmap, status, last_heartbeat, complete, full_hash_ok}
        self.recent = collections.deque(maxlen=120)  # recent transfers, for the diagram
        self._rr = 0  # round-robin cursor → spreads sources across holders

    def set_phase(self, phase, error=None):
        with self._lock:
            self.phase = phase
            self.error = error

    def activate(self, chunk_count, chunk_size, file_size, full_hash, checksums):
        with self._lock:
            self.chunk_count = chunk_count
            self.chunk_size = chunk_size
            self.file_size = file_size
            self.full_hash = full_hash
            self.checksums = list(checksums)
            self.ready = True
            self.phase = "distributing"
            self.error = None

    def _manifest_locked(self):
        return {
            "ready": self.ready,
            "chunk_count": self.chunk_count,
            "chunk_size": self.chunk_size,
            "file_size": self.file_size,
            "full_hash": self.full_hash,
            "chunk_checksums": list(self.checksums),
        }

    def register(self, worker_id):
        now = time.time()
        with self._lock:
            # A Register always means a (re)starting worker, so reset its server-side
            # view: a worker that restarted having lost chunks must not show stale
            # complete/24-held. It re-syncs immediately via the bitmap it carries on its
            # next heartbeat + RequestAssignment (and re-sends ReportComplete if done).
            self.workers[worker_id] = {
                "bitmap": b"", "status": "registered", "last_heartbeat": now,
                "complete": False, "full_hash_ok": False,
            }
            return self._manifest_locked()

    def heartbeat(self, worker_id, bitmap, status):
        now = time.time()
        with self._lock:
            w = self.workers.setdefault(worker_id, {"complete": False, "full_hash_ok": False})
            w["bitmap"] = bytes(bitmap or b"")
            w["status"] = status or "downloading"
            w["last_heartbeat"] = now

    def report_complete(self, worker_id, ok):
        with self._lock:
            w = self.workers.setdefault(worker_id, {})
            w["complete"] = True
            w["full_hash_ok"] = bool(ok)
            w["status"] = "complete" if ok else "hash-failed"
            w["last_heartbeat"] = time.time()

    def _alive_holders_locked(self, chunk_id, now):
        holders = []
        if self.store.has(chunk_id):  # the coordinator always holds every chunk once ready
            holders.append(self.id)
        for wid, w in self.workers.items():
            bm = w.get("bitmap", b"")
            alive = (now - w.get("last_heartbeat", 0)) < self.ALIVE_WINDOW
            if alive and chunk_id < len(bm) and bm[chunk_id] == 1:
                holders.append(wid)
        return holders

    def request_assignment(self, worker_id, bitmap):
        now = time.time()
        with self._lock:
            w = self.workers.setdefault(worker_id, {"complete": False, "full_hash_ok": False})
            w["bitmap"] = bytes(bitmap or b"")
            w["last_heartbeat"] = now
            w["status"] = "downloading"
            if not self.ready:
                return {"kind": "WAIT"}
            wb = w["bitmap"]
            missing = [i for i in range(self.chunk_count) if i >= len(wb) or wb[i] == 0]
            if not missing:
                return {"kind": "DONE"}
            # Random missing chunk → out-of-order arrival (visible on the bitmap grid).
            random.shuffle(missing)
            for cid in missing:
                holders = self._alive_holders_locked(cid, now)
                if holders:
                    src = holders[self._rr % len(holders)]
                    self._rr += 1
                    self.recent.append({"from": src, "to": worker_id, "chunk": cid, "ts": now})
                    return {"kind": "ASSIGN", "chunk_id": cid, "source_addr": f"{src}:{GRPC_PORT}"}
            return {"kind": "WAIT"}

    def state(self):
        now = time.time()
        with self._lock:
            workers = {}
            held_workers = 0
            for wid, w in self.workers.items():
                bm = w.get("bitmap", b"")
                held = sum(1 for b in bm if b == 1)
                held_workers += held
                workers[wid] = {
                    "bitmap": list(bm),
                    "held": held,
                    "status": w.get("status", "?"),
                    "complete": bool(w.get("complete")),
                    "full_hash_ok": bool(w.get("full_hash_ok")),
                    "alive": (now - w.get("last_heartbeat", 0)) < self.ALIVE_WINDOW,
                }
            coord_bm = self.store.bitmap_bytes()
            coord_held = sum(1 for b in coord_bm if b == 1)
            denom = self.chunk_count * (len(self.workers) + 1)
            progress = (coord_held + held_workers) / denom if denom else 0.0
            return {
                "phase": self.phase,
                "error": self.error,
                "ready": self.ready,
                "chunk_count": self.chunk_count,
                "chunk_size": self.chunk_size,
                "file_size": self.file_size,
                "full_hash": self.full_hash,
                "coordinator": {"id": self.id, "bitmap": list(coord_bm), "held": coord_held},
                "workers": workers,
                "recent": list(self.recent),
                "progress": progress,
            }


store = ChunkStore(DATA_DIR)
orch = Orchestrator(SERVICE_ID, store)


# ---------------------------------------------------------------------------
# Distribution (staging + chunking + seeding) — blocking work, run off the loop
# ---------------------------------------------------------------------------
def _download(url, path):
    req = urllib.request.Request(url, headers={"User-Agent": "download-coordinator/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r, open(path, "wb") as f:
        shutil.copyfileobj(r, f, length=1 << 20)


def _do_distribute(source, chunk_size):
    os.makedirs(SOURCE_DIR, exist_ok=True)
    src_type = source.get("type")
    val = source.get("value", "")
    if src_type == "local":
        if not val or "/" in val or val in (".", ".."):
            raise ValueError("invalid local file name")
        path = os.path.join(SOURCE_DIR, val)
        if not os.path.isfile(path):
            raise ValueError(f"no pre-staged file '{val}' under source/")
    elif src_type == "url":
        orch.set_phase("downloading")
        name = os.path.basename(urlparse(val).path) or "download.bin"
        if "/" in name:
            name = "download.bin"
        path = os.path.join(SOURCE_DIR, name)
        _download(val, path)
    else:
        raise ValueError("source.type must be 'url' or 'local'")

    orch.set_phase("chunking")
    checksums = []
    file_size = 0
    for i, data in chunk_file(path, chunk_size):
        cs = sha256_hex(data)
        store.write_chunk(i, data, checksum=cs)  # coordinator holds every chunk
        checksums.append(cs)
        file_size += len(data)
    if not checksums:
        raise ValueError("source file is empty")
    chunk_count = len(checksums)
    full = store.full_hash(chunk_count)
    store.set_manifest(chunk_count, checksums)
    orch.activate(chunk_count, chunk_size, file_size, full, checksums)


async def _run_distribute(source, chunk_size):
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, _do_distribute, source, chunk_size)
    except Exception as e:  # surface the failure in /dc/state
        orch.set_phase("error", str(e))


# ---------------------------------------------------------------------------
# gRPC server (Coordination + ChunkTransfer) alongside the HTTP app
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app):
    server = grpc.aio.server()
    ct_grpc.add_ChunkTransferServicer_to_server(ChunkTransferServicer(store), server)
    co_grpc.add_CoordinationServicer_to_server(CoordinationServicer(orch), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await server.start()
    app.state.grpc = server
    try:
        yield
    finally:
        await server.stop(grace=2)


app = FastAPI(title="download-coordinator", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    path = request.url.path
    method = request.method
    if path in EXCLUDED_PATHS:
        return await call_next(request)
    http_requests_in_flight.inc()
    start = time.perf_counter()
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        status = 500
        raise
    finally:
        http_request_duration_seconds.labels(method=method, endpoint=path).observe(
            time.perf_counter() - start
        )
        http_requests_total.labels(method=method, endpoint=path, status=str(status)).inc()
        http_requests_in_flight.dec()
    return response


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/dc/sources")
async def dc_sources():
    """Pre-staged local files the user can distribute (drop files into the node's
    data/source/ folder, or download one via /dc/distribute with a url source)."""
    try:
        files = sorted(
            f for f in os.listdir(SOURCE_DIR)
            if os.path.isfile(os.path.join(SOURCE_DIR, f))
        )
    except FileNotFoundError:
        files = []
    return {"ok": True, "sources": files}


@app.post("/dc/distribute")
async def dc_distribute(req: Request):
    if orch.phase in ("downloading", "chunking"):
        return JSONResponse({"ok": False, "error": "a distribution is already in progress"}, status_code=409)
    body = await req.json()
    source = body.get("source") or {}
    try:
        chunk_size = int(body.get("chunk_size") or DEFAULT_CHUNK_SIZE)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "chunk_size must be an integer"}, status_code=400)
    if chunk_size <= 0:
        return JSONResponse({"ok": False, "error": "chunk_size must be positive"}, status_code=400)
    orch.set_phase("downloading" if source.get("type") == "url" else "chunking")
    asyncio.create_task(_run_distribute(source, chunk_size))
    return {"ok": True, "started": True}


@app.get("/dc/state")
async def dc_state():
    return {"ok": True, **orch.state()}
