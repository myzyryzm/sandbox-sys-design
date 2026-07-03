"""Download Worker service.

A generic worker — every worker runs THIS exact code and differs only by config
(SERVICE_ID, COORDINATOR). Roles in one process:
  - ChunkTransfer SERVER : serves chunks it already holds to any peer (so the moment a
                           worker finishes chunk N it becomes a valid source for N).
  - Coordination CLIENT  : registers, pulls assignments, heartbeats, reports completion.
  - download loop        : pull → verify → write+flip bitmap (persisted BEFORE asking for
                           the next assignment) → repeat, until it holds everything and
                           the full-file hash verifies.

Durable: chunks + bitmap live on a bind-mounted /data volume, so a restarted worker
re-registers and resumes from its on-disk bitmap with no re-fetch.
"""

import asyncio
import os
import sys
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

sys.path.insert(0, "/app/grpc_pkg")
sys.path.insert(0, "/app")

import grpc  # noqa: E402

import ChunkTransfer_pb2 as ct  # noqa: E402
import ChunkTransfer_pb2_grpc as ct_grpc  # noqa: E402
import Coordination_pb2 as co  # noqa: E402
import Coordination_pb2_grpc as co_grpc  # noqa: E402
from ChunkTransfer_servicer import ChunkTransferServicer  # noqa: E402
from dc_common.chunkstore import ChunkStore  # noqa: E402

SERVICE_ID = os.environ.get("SERVICE_ID", "worker")
COORDINATOR = os.environ.get("COORDINATOR", "coordinator")
DATA_DIR = "/data"
GRPC_PORT = 50051

store = ChunkStore(DATA_DIR)
state = {"status": "registering"}  # liveness/status reported in heartbeats

# ---------------------------------------------------------------------------
# Prometheus instrumentation (same shape as the generic service template)
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


async def fetch_chunk(source_addr, chunk_id):
    """Pull one chunk from a peer (or the coordinator) via ChunkTransfer. Returns
    (data, checksum) — the receiver verifies the checksum before writing."""
    async with grpc.aio.insecure_channel(source_addr) as ch:
        stub = ct_grpc.ChunkTransferStub(ch)
        buf = bytearray()
        checksum = None
        async for frame in stub.GetChunk(ct.ChunkRequest(chunk_id=chunk_id, requester_id=SERVICE_ID)):
            which = frame.WhichOneof("frame")
            if which == "header":
                checksum = frame.header.checksum
            elif which == "data":
                buf += frame.data
        return bytes(buf), checksum


async def heartbeat_loop(coord):
    while True:
        try:
            await coord.Heartbeat(
                co.HeartbeatRequest(worker_id=SERVICE_ID, bitmap=store.bitmap_bytes(), status=state["status"])
            )
        except Exception:
            pass
        await asyncio.sleep(3)


async def download_loop(coord):
    # 1. Register and wait for the coordinator to be ready (a distribution started).
    chunk_count = 0
    full_hash = ""
    while True:
        try:
            m = await coord.Register(co.RegisterRequest(worker_id=SERVICE_ID))
        except Exception:
            state["status"] = "waiting-for-coordinator"
            await asyncio.sleep(1)
            continue
        if m.ready and m.chunk_count > 0:
            # set_manifest sizes the bitmap AND marks chunks already on disk as held —
            # this is the resume path after a restart.
            store.set_manifest(m.chunk_count, list(m.chunk_checksums))
            chunk_count = m.chunk_count
            full_hash = m.full_hash
            break
        state["status"] = "waiting-for-distribution"
        await asyncio.sleep(1)

    # 2. Pull until complete. The updated bitmap on the NEXT RequestAssignment is the
    #    only "chunk complete" signal — there is no separate RPC for it.
    state["status"] = "downloading"
    while True:
        a = await coord.RequestAssignment(
            co.AssignmentRequest(worker_id=SERVICE_ID, bitmap=store.bitmap_bytes())
        )
        if a.kind == co.Assignment.Kind.DONE:
            ok = store.full_hash(chunk_count) == full_hash
            try:
                await coord.ReportComplete(co.CompleteRequest(worker_id=SERVICE_ID, full_hash_ok=ok))
            except Exception:
                pass
            state["status"] = "complete" if ok else "hash-failed"
            return  # keep serving peers (ChunkTransfer server stays up) + heartbeating
        if a.kind == co.Assignment.Kind.WAIT:
            await asyncio.sleep(0.5)
            continue
        # ASSIGN: fetch from the source the coordinator chose, write + flip + persist.
        try:
            data, checksum = await fetch_chunk(a.source_addr, a.chunk_id)
            store.write_chunk(a.chunk_id, data, checksum=checksum)
        except Exception:
            # Source unavailable / bad chunk → brief backoff; the next RequestAssignment
            # will be handed a different source (the coordinator always has every chunk).
            await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app):
    # ChunkTransfer server so peers can pull whatever this worker holds.
    server = grpc.aio.server()
    ct_grpc.add_ChunkTransferServicer_to_server(ChunkTransferServicer(store), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    await server.start()

    coord_channel = grpc.aio.insecure_channel(f"{COORDINATOR}:{GRPC_PORT}")
    coord = co_grpc.CoordinationStub(coord_channel)
    tasks = [asyncio.create_task(download_loop(coord)), asyncio.create_task(heartbeat_loop(coord))]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await coord_channel.close()
        await server.stop(grace=2)


app = FastAPI(title="download-worker", lifespan=lifespan)


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


@app.get("/dc/worker")
async def dc_worker():
    """This worker's own view (debugging; the diagram reads worker state from the
    coordinator's aggregate /dc/state)."""
    bm = list(store.bitmap_bytes())
    return {"ok": True, "id": SERVICE_ID, "status": state["status"], "bitmap": bm, "held": sum(bm)}
