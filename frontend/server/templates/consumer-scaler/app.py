"""
Consumer-group SCALER for a "Distributed Systems Sandbox" Kafka consumer group.

One scaler runs per consumer-group service. It watches the group's REAL state on
the broker — per-partition lag, live members and their partition assignments —
and computes a desired replica count from the mounted scaling policy. It never
touches docker itself: the app's control plane polls GET /state and applies
`desired` through its own scale reconciler, so this container needs no privileges
beyond talking to its broker.

Env (set by the app when the group is created):
  CLUSTER     — the Kafka compose service name (bootstrap = CLUSTER:9092)
  SERVICE_ID  — this scaler's own id (<base>-scaler)

Mounts (read-only, re-read live by mtime — edits apply with NO rebuild):
  /config/scaler.json      — { groupId, enabled, min, max, scale_up_lag,
                               scale_down_lag, up_stable_seconds,
                               down_stable_seconds, cooldown_seconds }
  /streams/<CLUSTER>.json  — the cluster's topic registry: the group's topic is
                             discovered here (follows a topic move live), plus the
                             cluster-level consumersPaused flag (scale-up is
                             suppressed while paused — lag grows BY DESIGN then).

Decision loop (every POLL_SECONDS):
  effective_max = min(policy.max, partition count)   # extra members would idle
  lag > scale_up_lag   continuously for up_stable_seconds   -> desired += 1
  lag < scale_down_lag continuously for down_stable_seconds -> desired -= 1
  both clamped to [min, effective_max] and gated by cooldown_seconds since the
  last change; enabled:false freezes desired at the live member count.
"""

import json
import os
import threading
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

CLUSTER = os.environ.get("CLUSTER", "kafka")
SERVICE_ID = os.environ.get("SERVICE_ID", "consumer-scaler")
BOOTSTRAP = f"{CLUSTER}:9092"
POLICY_PATH = "/config/scaler.json"
STREAMS_PATH = f"/streams/{CLUSTER}.json"
POLL_SECONDS = 3.0

POLICY_DEFAULTS = {
    "groupId": "",
    "enabled": True,
    "min": 1,
    "max": 8,
    "scale_up_lag": 1000,
    "scale_down_lag": 100,
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

consumer_group_lag_total = Gauge(
    "consumer_group_lag_total", "Total lag (end offset - committed) across the group's partitions"
)
consumer_group_desired_replicas = Gauge(
    "consumer_group_desired_replicas", "Replica count the scaling policy currently wants"
)
consumer_group_members = Gauge(
    "consumer_group_members", "Live members in the consumer group right now"
)

EXCLUDED_PATHS = {"/metrics"}

app = FastAPI(title=f"consumer scaler {SERVICE_ID}")


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
_streams_file = _MtimeFile(STREAMS_PATH, {"topics": [], "consumersPaused": False})


def _policy():
    p = dict(_policy_file.read())
    # Clamp so a hand-edited file can't wedge the loop.
    p["min"] = max(1, int(p.get("min", 1) or 1))
    p["max"] = max(p["min"], int(p.get("max", 8) or 8))
    return p


def _group_topic(group_id):
    """The topic this group is registered on (streams.json is authoritative and
    live-mounted, so a topic move is picked up within one poll)."""
    data = _streams_file.read()
    for t in data.get("topics") or []:
        for g in (t or {}).get("consumers") or []:
            if g and g.get("groupId") == group_id:
                return t.get("id")
    return None


def _paused():
    return _streams_file.read().get("consumersPaused") is True


# ---------------------------------------------------------------------------
# Broker polling
# ---------------------------------------------------------------------------

_state_lock = threading.Lock()
_state = {
    "ok": False,
    "group": "",
    "topic": None,
    "partitions": 0,
    "lag": None,
    "lagPerPartition": {},
    "paused": False,
    "enabled": True,
    "members": [],
    "current": 0,
    "desired": None,
    "policy": dict(POLICY_DEFAULTS),
    "lastDecision": None,
    "error": "starting up",
}

_admin = None
_probe = None


def _clients():
    """Lazily create (and re-create after failures) the admin + groupless probe
    consumer used for end offsets."""
    global _admin, _probe
    from kafka import KafkaAdminClient, KafkaConsumer

    if _admin is None:
        _admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id=SERVICE_ID)
    if _probe is None:
        _probe = KafkaConsumer(bootstrap_servers=BOOTSTRAP, client_id=f"{SERVICE_ID}-probe")
    return _admin, _probe


def _reset_clients():
    global _admin, _probe
    for c in (_admin, _probe):
        try:
            if c is not None:
                c.close()
        except Exception:
            pass
    _admin = None
    _probe = None


def _describe_members(admin, group_id):
    """Live members with their partition assignments. kafka-python >= 3 returns
    pre-decoded dicts (describe_groups); 2.x returns namedtuples whose assignment
    bytes decode via the consumer protocol. On any hiccup fall back to whatever
    decoded so /state never breaks."""
    members = []
    try:
        if hasattr(admin, "describe_groups"):  # kafka-python >= 3.x
            desc = (admin.describe_groups([group_id]) or {}).get(group_id) or {}
            for m in desc.get("members") or []:
                parts = []
                for t in (m.get("member_assignment") or {}).get("assigned_partitions") or []:
                    parts.extend(int(p) for p in t.get("partitions") or [])
                members.append({
                    "clientId": m.get("client_id", ""),
                    "host": m.get("client_host", ""),
                    "partitions": sorted(parts),
                })
        else:  # kafka-python 2.x
            desc = admin.describe_consumer_groups([group_id])[0]
            for m in getattr(desc, "members", []) or []:
                entry = {"clientId": getattr(m, "client_id", ""), "host": getattr(m, "client_host", ""), "partitions": []}
                try:
                    from kafka.coordinator.protocol import ConsumerProtocolMemberAssignment

                    raw = getattr(m, "member_assignment", b"") or b""
                    if raw:
                        assignment = ConsumerProtocolMemberAssignment.decode(raw)
                        for _topic, parts in assignment.assignment:
                            entry["partitions"].extend(int(p) for p in parts)
                        entry["partitions"].sort()
                except Exception:
                    pass
                members.append(entry)
    except Exception:
        return members
    members.sort(key=lambda e: e["clientId"])
    return members


def _poll_once(decider):
    from kafka.structs import TopicPartition

    policy = _policy()
    group_id = str(policy.get("groupId") or "")
    paused = _paused()
    topic = _group_topic(group_id) if group_id else None

    if not group_id or not topic:
        with _state_lock:
            _state.update(
                ok=False,
                group=group_id,
                topic=topic,
                paused=paused,
                enabled=policy.get("enabled") is not False,
                policy=policy,
                error="no consumer group registered on this cluster yet" if group_id else "no groupId configured",
            )
        return

    admin, probe = _clients()

    parts = probe.partitions_for_topic(topic) or set()
    tps = [TopicPartition(topic, p) for p in sorted(parts)]
    end = probe.end_offsets(tps) if tps else {}
    beginning = probe.beginning_offsets(tps) if tps else {}

    committed = {}
    try:
        if hasattr(admin, "list_group_offsets"):  # kafka-python >= 3.x
            committed = (admin.list_group_offsets(group_id) or {}).get(group_id) or {}
        else:  # kafka-python 2.x
            committed = admin.list_consumer_group_offsets(group_id) or {}
    except Exception:
        committed = {}

    lag_per = {}
    total = 0
    for tp in tps:
        end_off = end.get(tp, 0)
        c = committed.get(tp)
        base = c.offset if c is not None and c.offset >= 0 else beginning.get(tp, 0)
        lag = max(0, end_off - base)
        lag_per[str(tp.partition)] = lag
        total += lag

    members = _describe_members(admin, group_id)
    decision = decider.decide(
        policy=policy,
        lag=total,
        partitions=len(tps),
        live_members=len(members),
        paused=paused,
    )

    consumer_group_lag_total.set(total)
    consumer_group_members.set(len(members))
    if decision["desired"] is not None:
        consumer_group_desired_replicas.set(decision["desired"])

    with _state_lock:
        _state.update(
            ok=True,
            group=group_id,
            topic=topic,
            partitions=len(tps),
            lag=total,
            lagPerPartition=lag_per,
            paused=paused,
            enabled=policy.get("enabled") is not False,
            members=members,
            current=len(members),
            desired=decision["desired"],
            policy=policy,
            lastDecision=decision["last"],
            error=None,
        )


class Decider:
    """The policy state machine: sustained-signal + cooldown stepping of `desired`."""

    def __init__(self):
        self.desired = None
        self.over_since = None
        self.under_since = None
        self.last_change = 0.0
        self.last = None

    def decide(self, policy, lag, partitions, live_members, paused):
        now = time.time()
        lo = int(policy["min"])
        hi = max(lo, min(int(policy["max"]), partitions or int(policy["max"])))
        enabled = policy.get("enabled") is not False

        # First successful poll (or scaler restart): baseline on what actually runs,
        # so a fresh scaler never yanks an existing group up/down without a signal.
        if self.desired is None:
            self.desired = min(max(live_members or lo, lo), hi)

        if not enabled:
            # Frozen: track reality so re-enabling starts from the true count.
            self.desired = min(max(live_members or self.desired, lo), hi)
            self.over_since = self.under_since = None
            return {"desired": self.desired, "last": self.last}

        over = lag > int(policy["scale_up_lag"])
        under = lag < int(policy["scale_down_lag"])
        self.over_since = (self.over_since or now) if over else None
        self.under_since = (self.under_since or now) if under else None

        cooled = (now - self.last_change) >= int(policy["cooldown_seconds"])
        want = self.desired

        if (
            over
            and not paused  # paused consumers lag by design — never scale up on it
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
                f"lag {lag} > {policy['scale_up_lag']} for {policy['up_stable_seconds']}s"
                if want > self.desired
                else f"lag {lag} < {policy['scale_down_lag']} for {policy['down_stable_seconds']}s"
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
    while True:
        try:
            _poll_once(decider)
        except Exception as exc:  # broker down / mid-rebalance / DNS — retry forever
            _reset_clients()
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
