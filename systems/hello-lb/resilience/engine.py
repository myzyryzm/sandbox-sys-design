"""
Shared circuit-breaker + retry wrapper for the sandbox system.

ONE shared implementation, imported by every wired service — per-connection behavior
comes only from the manifest policy (data), never from divergent code. Self-contained
so it could later be lifted into a sidecar.

Semantics (do NOT invert): "open" means broken/blocking.
  CLOSED    = healthy; calls flow, failures are counted.
  OPEN      = tripped; calls are blocked and fail fast (or serve a fallback) WITHOUT
              touching the downstream. Entered after `failure_threshold` consecutive
              failures.
  HALF_OPEN = testing recovery; after `pause_duration_seconds`, allow
              `half_open_trial_calls` trial calls. All succeed -> CLOSED. Any fail ->
              OPEN, restart the pause.

Composition: the breaker is the OUTER gate, retry the INNER loop. Per logical call we
check the breaker first (OPEN -> fast-fail/fallback, no retries); otherwise we attempt
the call under the retry policy, and only a fully-exhausted retry counts as ONE failure
toward the breaker threshold.
"""
import asyncio
import json
import os
import random
import threading
import time

from prometheus_client import Counter, Gauge

# Metrics live in the default registry, so they appear on the service's existing
# /metrics with no new Prometheus scrape job. Labeled by connection "<from>-><to>".
_STATE = Gauge("circuit_breaker_state", "Breaker state: 0=closed, 1=open, 2=half_open", ["connection"])
_FAILURES = Counter("circuit_breaker_failures_total", "Logical-call failures counted by the breaker", ["connection"])
_TRIPS = Counter("circuit_breaker_trips_total", "Times the breaker tripped to OPEN", ["connection"])
_RETRY_ATTEMPTS = Counter("retry_attempts_total", "Retry attempts made (the re-tries, excluding the first try)", ["connection"])
_RETRY_EXHAUSTED = Counter("retry_exhausted_total", "Retry sequences that exhausted every attempt", ["connection"])
_RETRY_BACKOFF = Gauge("retry_current_backoff_seconds", "Current/last computed retry backoff delay", ["connection"])

CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"
_STATE_CODE = {CLOSED: 0, OPEN: 1, HALF_OPEN: 2}


class CircuitOpenError(Exception):
    """Raised when a call is short-circuited by an OPEN breaker under `fail_fast`."""


async def _invoke(fn):
    """Run the wrapped call. An async function is awaited; a plain sync function (e.g.
    a blocking psycopg call) is run in a worker thread so it never blocks the loop."""
    if asyncio.iscoroutinefunction(fn):
        return await fn()
    return await asyncio.to_thread(fn)


class _Conn:
    """Runtime state for one connection (from->to). Policy values are passed in per
    call, since the manifest policy can change at runtime; only state lives here."""

    def __init__(self, from_id, to):
        self.to = to
        self.key = f"{from_id}->{to}"
        self.lock = threading.Lock()
        # breaker state
        self.state = CLOSED
        self.failures = 0
        self.opened_at = 0.0
        self.half_open_successes = 0
        self.half_open_inflight = 0
        # display fields (last-seen policy), for /resilience/state
        self.open_behavior = "fail_fast"
        self.failure_threshold = 0
        self.half_open_required = 1
        # live retry view
        self.retry_active = False
        self.retry_attempt = 0
        self.retry_max = 0
        self.retry_next_backoff = 0.0
        self.retry_exhausted = False
        _STATE.labels(self.key).set(0)

    def _set_state(self, s):
        self.state = s
        _STATE.labels(self.key).set(_STATE_CODE[s])

    def _trip(self):
        if self.state != OPEN:
            _TRIPS.labels(self.key).inc()
        self._set_state(OPEN)
        self.opened_at = time.monotonic()
        self.failures = 0  # decisions are pause-based once OPEN

    # --- breaker gate (call under self.lock) ---
    def allow(self, cb):
        """True if the call may proceed (CLOSED, or a granted HALF-OPEN trial); False
        if it must be blocked (OPEN, or HALF-OPEN with no trial slots left)."""
        if self.state == OPEN:
            if time.monotonic() - self.opened_at >= cb["pause_duration_seconds"]:
                self._set_state(HALF_OPEN)
                self.half_open_successes = 0
                self.half_open_inflight = 0
            else:
                return False
        if self.state == HALF_OPEN:
            if self.half_open_successes + self.half_open_inflight >= cb["half_open_trial_calls"]:
                return False
            self.half_open_inflight += 1
            return True
        return True  # CLOSED

    def on_success(self, cb):
        with self.lock:
            if self.state == HALF_OPEN:
                self.half_open_inflight = max(0, self.half_open_inflight - 1)
                self.half_open_successes += 1
                if self.half_open_successes >= cb["half_open_trial_calls"]:
                    self._set_state(CLOSED)
                    self.failures = 0
            else:
                self.failures = 0  # a success resets the consecutive-failure run

    def on_failure(self, cb):
        with self.lock:
            _FAILURES.labels(self.key).inc()
            if self.state == HALF_OPEN:
                self.half_open_inflight = max(0, self.half_open_inflight - 1)
                self._trip()  # a trial failed -> reopen, restart the pause
            else:
                self.failures += 1
                if self.failures >= cb["failure_threshold"]:
                    self._trip()

    def _backoff(self, rt, attempt):
        """Delay before retry #attempt (1-based over the re-tries). Exponential growth
        from base, capped at max; full jitter randomizes [0, computed] to desynchronize
        clients (the thundering-herd fix)."""
        planned = min(rt["max_delay_seconds"], rt["base_delay_seconds"] * (2 ** (attempt - 1)))
        if rt.get("strategy") == "exponential_backoff_jitter":
            return random.uniform(0, planned)
        return planned

    async def run_with_retry(self, fn, rt):
        """Inner retry loop. Returns the result, or raises the last error once attempts
        are exhausted (which the caller counts as one breaker failure)."""
        attempts = int(rt["max_attempts"]) if rt.get("enabled") else 1
        attempts = max(1, attempts)
        last_exc = None
        for i in range(1, attempts + 1):
            try:
                result = await _invoke(fn)
                with self.lock:
                    self.retry_active = False
                    self.retry_exhausted = False
                    self.retry_next_backoff = 0.0
                    _RETRY_BACKOFF.labels(self.key).set(0)
                return result
            except Exception as exc:  # noqa: BLE001 - any downstream error is a failed try
                last_exc = exc
                if i < attempts:
                    _RETRY_ATTEMPTS.labels(self.key).inc()
                    delay = self._backoff(rt, i)
                    with self.lock:
                        self.retry_active = True
                        self.retry_attempt = i + 1
                        self.retry_max = attempts
                        self.retry_next_backoff = delay
                        _RETRY_BACKOFF.labels(self.key).set(delay)
                    await asyncio.sleep(delay)
                else:
                    with self.lock:
                        self.retry_active = False
                        self.retry_exhausted = True
                        self.retry_next_backoff = 0.0
                        _RETRY_EXHAUSTED.labels(self.key).inc()
                        _RETRY_BACKOFF.labels(self.key).set(0)
        raise last_exc

    def snapshot(self):
        with self.lock:
            cb = {"state": self.state, "open_behavior": self.open_behavior, "failures": self.failures}
            if self.state == HALF_OPEN:
                cb["trial"] = {"done": self.half_open_successes, "required": self.half_open_required}
            retry = {
                "active": self.retry_active,
                "attempt": self.retry_attempt,
                "max": self.retry_max,
                "next_backoff_seconds": round(self.retry_next_backoff, 3),
                "exhausted": self.retry_exhausted,
            }
            return {"to": self.to, "circuit_breaker": cb, "retry": retry}


class ResilienceRegistry:
    """Per-service entry point. Reads this service's outbound policies from the mounted
    manifest at runtime (so threshold edits need no rebuild) and guards each call."""

    def __init__(self, service_id=None, manifest_path="/manifest.json"):
        self.service_id = service_id or os.environ.get("SERVICE_ID", "")
        self.manifest_path = manifest_path
        self._conns = {}
        self._conns_lock = threading.Lock()
        self._policy_cache = {}
        self._policy_mtime = None
        self._policy_lock = threading.Lock()

    def _load_edges(self):
        """{ to: resilience } for edges from this service. Cached by manifest mtime so
        concurrent calls don't re-parse constantly, while edits are picked up promptly."""
        try:
            mtime = os.stat(self.manifest_path).st_mtime
        except OSError:
            return self._policy_cache
        with self._policy_lock:
            if mtime != self._policy_mtime:
                try:
                    with open(self.manifest_path) as fh:
                        manifest = json.load(fh)
                    edges = {
                        e["to"]: e["resilience"]
                        for e in manifest.get("edges", [])
                        if e.get("from") == self.service_id and e.get("resilience")
                    }
                    self._policy_cache = edges
                    self._policy_mtime = mtime
                except Exception:  # noqa: BLE001 - a mid-write manifest: keep last good
                    pass
            return self._policy_cache

    def policy_for(self, to):
        return self._load_edges().get(to)

    def _conn(self, to):
        with self._conns_lock:
            conn = self._conns.get(to)
            if conn is None:
                conn = _Conn(self.service_id, to)
                self._conns[to] = conn
            return conn

    async def call(self, to, fn):
        """Guard `fn` (an async or sync callable) as the outbound call to `to`. Returns
        the result (or the configured fallback while OPEN); raises CircuitOpenError when
        short-circuited under fail_fast, or the underlying error when a CLOSED/HALF-OPEN
        attempt fully fails."""
        policy = self.policy_for(to)
        if not policy:
            return await _invoke(fn)  # no policy on this connection -> pass through

        cb = policy.get("circuit_breaker") or {}
        rt = policy.get("retry") or {}
        conn = self._conn(to)
        if cb.get("enabled"):
            conn.open_behavior = cb.get("open_behavior", "fail_fast")
            conn.failure_threshold = int(cb.get("failure_threshold", 0))
            conn.half_open_required = int(cb.get("half_open_trial_calls", 1))
            with conn.lock:
                allowed = conn.allow(cb)
            if not allowed:
                if conn.open_behavior == "fallback":
                    return cb.get("fallback_response")
                raise CircuitOpenError(f"{conn.key} circuit is OPEN")

        try:
            result = await conn.run_with_retry(fn, rt)
        except Exception:
            if cb.get("enabled"):
                conn.on_failure(cb)
            raise
        if cb.get("enabled"):
            conn.on_success(cb)
        return result

    def state(self):
        with self._conns_lock:
            conns = list(self._conns.values())
        return {"connections": [c.snapshot() for c in conns]}
