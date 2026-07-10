"""
LLM-worker SCALER for a "Distributed Systems Sandbox" LLM worker group.

One scaler runs per LLM worker group. It watches the group's REAL batch
occupancy — each worker's /llm/state reports its active sequences and its
max_active capacity — and computes a desired worker count from the mounted
scaling policy. It never touches docker itself: the app's control plane polls
GET /state and applies `desired` through its own scale reconciler, so this
container needs no privileges beyond HTTP to its workers.

Env (set by the app when the worker is created):
  BASE        — the base worker's compose service name (members resolve from it)
  SERVICE_ID  — this scaler's own id (<base>-scaler)
  SYSTEM_ID   — the owning system (the control plane checks it against a
                same-named scaler in another stack holding the shared ports)

Mounts (read-only, re-read live by mtime — edits apply with NO rebuild):
  /config/scaler.json  — { enabled, min, max, scale_up_util, scale_down_util,
                           up_stable_seconds, down_stable_seconds,
                           cooldown_seconds }
  /manifest.json       — the system manifest: the member set is discovered here
                         (BASE + every node carrying instanceOf: BASE), so a
                         manual scale is picked up within one poll.

Decision loop (every POLL_SECONDS):
  utilization = sum(active sequences) / sum(max_active) over REACHABLE workers
  util > scale_up_util   continuously for up_stable_seconds   -> desired += 1
  util < scale_down_util continuously for down_stable_seconds -> desired -= 1
  both clamped to [min, max] and gated by cooldown_seconds since the last
  change; enabled:false freezes desired at the live worker count; a tick with
  NO reachable worker makes no decision at all (no data is not low load).
"""

import json
import os
import re
import threading
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

BASE = os.environ.get("BASE", "llm-worker")
SERVICE_ID = os.environ.get("SERVICE_ID", "llm-scaler")
SYSTEM_ID = os.environ.get("SYSTEM_ID", "")
WORKER_PORT = int(os.environ.get("WORKER_PORT", "8000"))
POLICY_PATH = "/config/scaler.json"
MANIFEST_PATH = "/manifest.json"
POLL_SECONDS = 3.0

POLICY_DEFAULTS = {
    "enabled": True,
    "min": 1,
    "max": 8,
    "scale_up_util": 0.8,
    "scale_down_util": 0.3,
    "up_stable_seconds": 15,
    "down_stable_seconds": 60,
    "cooldown_seconds": 90,
}

# ---------------------------------------------------------------------------
# Metrics — the standard hand-written HTTP set (what the manifest's generic
# service queries read) plus the scaler-specific gauges its node cards show.
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

llm_worker_utilization = Gauge(
    "llm_worker_utilization", "Batch utilization: active sequences / total max_active, 0..1"
)
llm_worker_desired_replicas = Gauge(
    "llm_worker_desired_replicas", "Worker count the scaling policy currently wants"
)
llm_worker_members = Gauge(
    "llm_worker_members", "Reachable workers in the group right now"
)

EXCLUDED_PATHS = {"/metrics"}

app = FastAPI(title=f"llm scaler {SERVICE_ID}")


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
        duration = time.perf_counter() - start
        http_request_duration_seconds.labels(method=method, endpoint=path).observe(duration)
        http_requests_total.labels(method=method, endpoint=path, status=str(status)).inc()
        http_requests_in_flight.dec()
    return response


# ---------------------------------------------------------------------------
# Live-mounted config (mtime-polled; keep last-good on a mid-write parse error)
# ---------------------------------------------------------------------------

class _MtimeFile:
    def __init__(self, path, fallback):
        self.path = path
        self.fallback = fallback
        self.value = dict(fallback)
        self.mtime = None

    def read(self):
        try:
            m = os.stat(self.path).st_mtime
        except OSError:
            return self.value
        if m == self.mtime:
            return self.value
        try:
            with open(self.path) as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                merged = dict(self.fallback)
                merged.update(raw)
                self.value = merged
                self.mtime = m
        except (OSError, ValueError):
            pass  # mid-write / garbled — keep last-good
        return self.value


_policy_file = _MtimeFile(POLICY_PATH, POLICY_DEFAULTS)
_manifest_file = _MtimeFile(MANIFEST_PATH, {"nodes": []})


def _policy():
    p = dict(_policy_file.read())
    # Clamp so a hand-edited file can't wedge the loop.
    p["min"] = max(1, int(p.get("min", 1) or 1))
    p["max"] = max(p["min"], int(p.get("max", 8) or 8))
    return p


def _ordinal(node_id):
    m = re.search(r"-(\d+)$", node_id)
    return int(m.group(1)) if m else 0


def _member_ids():
    """The group's worker ids from the live-mounted manifest: BASE first, then
    its instances in ordinal order — a manual scale shows up within one poll."""
    nodes = _manifest_file.read().get("nodes") or []
    members = [
        n["id"]
        for n in nodes
        if isinstance(n, dict)
        and n.get("service_type") == "llm_worker"
        and (n.get("id") == BASE or n.get("instanceOf") == BASE)
    ]
    return sorted(members, key=lambda i: (i != BASE, _ordinal(i)))


# ---------------------------------------------------------------------------
# Worker polling
# ---------------------------------------------------------------------------

_state_lock = threading.Lock()
_state = {
    "ok": False,
    "base": BASE,
    "system": SYSTEM_ID,
    "utilization": None,
    "active": 0,
    "capacity": 0,
    "members": [],
    "current": 0,
    "desired": None,
    "enabled": True,
    "policy": dict(POLICY_DEFAULTS),
    "lastDecision": None,
    "error": "starting up",
}


def _poll_once(decider, client):
    policy = _policy()

    members = []
    active_total = 0
    capacity_total = 0
    for mid in _member_ids():
        entry = {"id": mid, "reachable": False, "active": 0, "max_active": 0}
        try:
            r = client.get(f"http://{mid}:{WORKER_PORT}/llm/state")
            r.raise_for_status()
            body = r.json()
            entry["reachable"] = True
            entry["active"] = int(body.get("active_count") or 0)
            entry["max_active"] = int((body.get("config") or {}).get("max_active") or 0)
            active_total += entry["active"]
            capacity_total += entry["max_active"]
        except Exception:
            pass  # booting / mid-recreate / gone — counts as unreachable
        members.append(entry)

    live = [m for m in members if m["reachable"]]
    utilization = (active_total / capacity_total) if (live and capacity_total > 0) else None

    decision = decider.decide(
        policy=policy,
        utilization=utilization,
        live_members=len(live),
    )

    if utilization is not None:
        llm_worker_utilization.set(utilization)
    llm_worker_members.set(len(live))
    if decision["desired"] is not None:
        llm_worker_desired_replicas.set(decision["desired"])

    with _state_lock:
        _state.update(
            ok=bool(live),
            utilization=round(utilization, 4) if utilization is not None else None,
            active=active_total,
            capacity=capacity_total,
            members=members,
            current=len(live),
            desired=decision["desired"],
            enabled=policy.get("enabled") is not False,
            policy=policy,
            lastDecision=decision["last"],
            error=None if live else "no reachable workers",
        )


class Decider:
    """The policy state machine: sustained-signal + cooldown stepping of `desired`."""

    def __init__(self):
        self.desired = None
        self.over_since = None
        self.under_since = None
        self.last_change = 0.0
        self.last = None

    def decide(self, policy, utilization, live_members):
        now = time.time()
        lo = int(policy["min"])
        hi = max(lo, int(policy["max"]))
        enabled = policy.get("enabled") is not False

        # No reachable worker → no data. Keep desired where it is (never treat a
        # blackout as "idle, scale down"); baseline once workers answer.
        if utilization is None:
            if self.desired is not None:
                self.desired = min(max(self.desired, lo), hi)
            self.over_since = self.under_since = None
            return {"desired": self.desired, "last": self.last}

        # First successful poll (or scaler restart): baseline on what actually runs,
        # so a fresh scaler never yanks an existing group up/down without a signal.
        if self.desired is None:
            self.desired = min(max(live_members or lo, lo), hi)

        if not enabled:
            # Frozen: track reality so re-enabling starts from the true count.
            self.desired = min(max(live_members or self.desired, lo), hi)
            self.over_since = self.under_since = None
            return {"desired": self.desired, "last": self.last}

        over = utilization > float(policy["scale_up_util"])
        under = utilization < float(policy["scale_down_util"])
        self.over_since = (self.over_since or now) if over else None
        self.under_since = (self.under_since or now) if under else None

        cooled = (now - self.last_change) >= int(policy["cooldown_seconds"])
        want = self.desired

        if (
            over
            and self.over_since is not None
            and (now - self.over_since) >= int(policy["up_stable_seconds"])
            and cooled
        ):
            want = self.desired + 1
        elif (
            under
            and self.under_since is not None
            and (now - self.under_since) >= int(policy["down_stable_seconds"])
            and cooled
        ):
            want = self.desired - 1

        want = min(max(want, lo), hi)
        if want != self.desired:
            reason = (
                f"util {utilization:.2f} > {policy['scale_up_util']} for {policy['up_stable_seconds']}s"
                if want > self.desired
                else f"util {utilization:.2f} < {policy['scale_down_util']} for {policy['down_stable_seconds']}s"
            )
            self.last = {
                "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                "from": self.desired,
                "to": want,
                "reason": reason,
            }
            self.desired = want
            self.last_change = now
            self.over_since = self.under_since = None

        return {"desired": self.desired, "last": self.last}


def _watch_loop():
    decider = Decider()
    client = httpx.Client(timeout=2.0)
    while True:
        try:
            _poll_once(decider, client)
        except Exception as exc:  # manifest mid-write / DNS — retry forever
            with _state_lock:
                _state.update(ok=False, error=str(exc) or exc.__class__.__name__)
        time.sleep(POLL_SECONDS)


threading.Thread(target=_watch_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/state")
async def state():
    with _state_lock:
        return dict(_state)


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
