---
name: sandbox-resilience
description: >-
  Attach a circuit-breaker + retry resilience policy to a connection (a source service ->
  target node outbound call) in a "Distributed Systems Sandbox" system (systems/<id>/). Use
  whenever the task is to make a service's outbound call resilient — wrap it in the shared
  breaker/retry wrapper, emit the per-connection metrics, expose the fast in-memory state — or to
  change/remove such a policy. It covers the CLOSED/OPEN/HALF-OPEN state machine, the retry/breaker
  composition order, the per-connection manifest `resilience` block (read at runtime), the shared
  wrapper placement + per-service wiring + single-service rebuild, and the diagram conventions.
---

# Attaching a sandbox resilience policy to a connection

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; the frontend runs under `npm run dev` and reads these files live, so **never run
`./start.sh`**. Rebuild a changed service with `docker compose` directly (command below).

A **connection** is a `(fromService → toNode)` outbound call — a service reading a database
(psycopg), or calling another service (gRPC/HTTP). A **resilience policy** wraps that call with a
**circuit breaker** and/or **retry**. The web app's connection modal drives this
(`POST /api/connection-resilience` writes the policy to the manifest, then launches this session);
by hand, reproduce the same shape. This is **wiring**, like [[sandbox-grpc-attach]] — the policy
(the data) already exists in the manifest before you run; your job is the shared wrapper + the
per-service code that reads it.

**A load-balanced target is ONE cluster breaker.** If `to` is a load-balanced service
(`type:"service-lb"` — a per-service haproxy sidecar fronting N instances; see
[[sandbox-service-lb]]), the policy stays a single breaker keyed `<from>-><to>`, exactly like any
other node: the caller already reaches every instance transparently through the sidecar (calling
`http://<to>:8000/…` as before), and haproxy's own health checks eject a failing instance
underneath. Do **not** fan the policy out into per-instance breakers or point it at an instance id —
resilience on a load-balanced service is uniform across the cluster.

## Critical semantics — get the breaker states right

**"open" means broken/blocking, not "open for business."** Never invert this:

- **CLOSED** = healthy. Calls flow; the breaker counts consecutive failures. (Closed circuit =
  current flows.)
- **OPEN** = tripped. Calls are blocked and fail fast (or return a fallback) **without touching the
  downstream**. Entered after `failure_threshold` consecutive failures.
- **HALF-OPEN** = testing recovery. After `pause_duration_seconds`, allow `half_open_trial_calls`
  trial calls. All succeed → CLOSED. Any fail → OPEN, restart the pause.

```
CLOSED ──(failure_threshold consecutive failures)──> OPEN
OPEN ──(pause_duration_seconds elapses)──> HALF-OPEN
HALF-OPEN ──(half_open_trial_calls succeed)──> CLOSED
HALF-OPEN ──(a trial call fails)──> OPEN  (restart pause)
```

## Composition — retry is the inner loop, breaker the outer gate

Order matters. Per logical call: **check the breaker first.** If OPEN → fast-fail or serve the
fallback per `open_behavior`, **no retries**. If CLOSED/HALF-OPEN → attempt the call under the retry
policy; only a **fully-exhausted** retry sequence counts as **one** failure toward the breaker
threshold (a call that succeeds on retry #2 is a success, not a failure). `exponential_backoff`
doubles the delay from `base_delay_seconds`, capped at `max_delay_seconds`;
`exponential_backoff_jitter` adds randomized jitter (this is the thundering-herd fix — without it,
many clients retrying a recovered service synchronize and re-overwhelm it).

## The places a policy lives (working dir is the repo root)

1. `systems/<id>/manifest.json` — the connection's `resilience` block on the matching `edges[]`
   entry (the web app writes this before launching you; it is the single source of truth, **read at
   runtime — do not bake it into code**):
   ```json
   "edges": [{ "from": "service-1", "to": "catalog-db", "resilience": {
     "circuit_breaker": { "enabled": true, "failure_threshold": 5, "pause_duration_seconds": 10,
                          "half_open_trial_calls": 1, "open_behavior": "fail_fast", "fallback_response": null },
     "retry": { "enabled": true, "max_attempts": 3, "strategy": "exponential_backoff_jitter",
                "base_delay_seconds": 0.5, "max_delay_seconds": 8 } }}]
   ```
   - `open_behavior` is `fail_fast` (raise/return an error while OPEN) or `fallback` (return
     `fallback_response` while OPEN).
2. `systems/<id>/resilience/` — the **single shared wrapper** package (the breaker/retry engine),
   imported by every wired service. Same anti-drift rule as a gRPC contract's single servicer
   ([[sandbox-grpc-attach]]): one implementation for the whole system; per-service differences
   come only from the per-connection policy in the manifest, never from divergent code.
3. `systems/<id>/<service>/` — the service's `app.py` / `Dockerfile` / `requirements.txt`, wired to
   route its outbound call through the wrapper.

## The shared wrapper — `systems/<id>/resilience/`

A small, self-contained package (so it could later be lifted into a sidecar). It must:

- Implement the breaker state machine + retry exactly as above, **per connection** (keyed by
  `"<from>-><to>"`), composition order respected.
- **Read the policy at runtime** from the mounted manifest (mount `./manifest.json:/manifest.json:ro`
  and set `SERVICE_ID` in compose — same pattern as [[sandbox-grpc-attach]]). Look up
  `edges[] where from==SERVICE_ID and to==<target>` per logical call (cheap; the file is small), so
  a threshold edit in the modal takes effect with **no rebuild**.
- Keep **in-memory current state** per connection (state, consecutive failures, half-open trial
  progress, whether a retry is in flight + its next backoff) for the fast read.
- Emit Prometheus metrics into the default registry (so they appear on the service's existing
  `/metrics` — **no new scrape job**), all labeled `connection="<from>-><to>"`:
  `circuit_breaker_state` (0=closed,1=open,2=half_open), `circuit_breaker_failures_total`,
  `circuit_breaker_trips_total`, `retry_attempts_total`, `retry_exhausted_total`,
  `retry_current_backoff_seconds`.

Expose the wrapper so a call site reads naturally, e.g. `await guard("catalog-db", do_call)` where
`guard` checks the breaker, runs the retry loop, records metrics + state, and raises/falls-back per
policy. For a **sync** downstream (psycopg) run the call in a thread; for an async one (gRPC) await
it directly.

## Per-service wiring + fast-state endpoint

In the `from` service's `app.py` (leave the metrics middleware + other routes intact):

- Route the **specific outbound call** to `<to>` through the wrapper (e.g. wrap the
  `psycopg.connect(...).execute(...)` in the `/items` handler for `catalog-db`). When OPEN with
  `fail_fast`, surface a clear error (e.g. HTTP 503); with `fallback`, return `fallback_response`.
- Add `GET /resilience/state` returning the wrapper's in-memory state for this service's
  connections, e.g. `{ "connections": [ { "to": "catalog-db", "circuit_breaker": { "state":
  "open", "open_behavior": "fail_fast", "trial": {"done":0,"required":1} }, "retry": { "active":
  false, "attempt": 0, "max": 3, "next_backoff_seconds": 0, "exhausted": false } } ] }`. The web app
  aggregates this through the LB (`GET /api/resilience-state`) and the diagram polls it ~750ms to
  show the breaker tripping live, faster than the Prometheus scrape.
- `Dockerfile`: `COPY` the system's `resilience/` package into the image. `requirements.txt`:
  nothing new is required beyond `prometheus_client` (already present); add libs only if your call
  site needs them.
- `docker-compose.yml`: mount `./manifest.json:/manifest.json:ro` and set `SERVICE_ID: <service>`
  on that service (idempotent — it may already be mounted for gRPC).

## First attach vs config edit

- **First policy on a service** (`firstAttach` from the API) → do the full wiring above + rebuild
  **only** that service: `docker compose -f systems/<id>/docker-compose.yml up -d --build <service>`.
- **Editing thresholds** on an already-wired connection → manifest-only; the wrapper re-reads at
  runtime, so **no rebuild** (at most `docker compose ... restart <service>` if you must reset
  in-memory state). Re-pointing or adding a *new* call site that isn't yet wrapped is a code change
  → rebuild.

## Diagram conventions (already implemented in the frontend)

A connection with `circuit_breaker.enabled` draws a **mid-line circle**: **filled = CLOSED =
healthy**, **hollow = OPEN = blocked**, **half-filled = HALF-OPEN = testing** — driven by the fast
`/resilience/state` read. Do not invert. Live overlay text near the line (`breaker OPEN —
fast-failing` / `serving fallback`, `breaker HALF-OPEN — testing (1/1 trial)`, `retrying — attempt
2/3 in 1.4s`, `retries exhausted`) is derived from the same reported state, so keep the
`/resilience/state` fields accurate (and the backoff value the real computed one, incl. jitter).

## Verify

1. `docker compose -f systems/<id>/docker-compose.yml up -d --build <service>` — service healthy,
   `/resilience/state` returns the connection, `/metrics` shows the six `circuit_breaker_*` /
   `retry_*` series labeled `connection="<from>-><to>"`.
2. **Breaker**: stop the downstream (`docker compose ... stop <to>`), drive the call → consecutive
   failures reach `failure_threshold` → circle goes filled→hollow, calls fast-fail/fallback; restart
   `<to>` → pause elapses → half-filled → trial success → filled (CLOSED).
3. **Retry**: induce intermittent failure → attempt count + live backoff countdown show; jitter
   visibly randomizes delays.
4. **Composition**: while OPEN, no retries fire (immediate fast-fail/fallback); a fully-exhausted
   retry counts as one breaker failure.
5. **Config edit, no rebuild**: change a threshold in the modal → the running wrapper picks it up.

## Remove / change

Delete the `resilience` block from the edge (the web app's modal/DELETE does this), unwrap the call
site in `app.py` if no policy remains on that connection, and rebuild that service. Reference
[[sandbox-grpc-attach]] for the manifest-mount / `SERVICE_ID` / single-service rebuild mechanics
this shares.
