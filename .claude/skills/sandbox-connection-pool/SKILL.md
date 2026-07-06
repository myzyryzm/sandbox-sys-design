---
name: sandbox-connection-pool
description: >-
  Attach or edit a connection pool on an INTERNAL connection (a source service -> internal target
  node outbound call) in a "Distributed Systems Sandbox" system (systems/<id>/). Use whenever the
  task is to give a service's outbound call a real, sized connection pool — replace its per-request
  connect with a module-level shared pool (psycopg_pool for postgres, MongoClient pool params for
  mongo, a shared httpx.Client + Limits for service->service), emit the per-connection pool metrics,
  expose GET /pool/state — or to change/remove such a pool. Covers the four pool params
  (max_connections / min_idle / idle_timeout_seconds / max_lifetime_seconds), the per-connection
  manifest `connection_pool` block (read at STARTUP), the per-service wiring + single-service
  rebuild, and the diagram conventions. Pool sizes are construction-time, so any change needs a
  service rebuild/restart (unlike [[sandbox-resilience]] thresholds).
---

# Attaching a sandbox connection pool to a connection

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; the frontend runs under `npm run dev` and reads these files live, so **never run
`./start.sh`**. Rebuild a changed service with `docker compose` directly (command below).

A **connection** is a `(fromService → toNode)` outbound call — a service reading a database
(psycopg / pymongo), or calling another **internal** service (httpx). A **connection pool** replaces
the default *open-a-connection-per-request* pattern with a **module-level shared pool** so
concurrent requests reuse a bounded set of live connections. The web app's connection modal drives
this (`POST /api/connection-pool` writes the config to the manifest, then launches this session); by
hand, reproduce the same shape. This is **wiring**, like [[sandbox-resilience]] and
[[sandbox-grpc-attach]] — the config (the data) already exists in the manifest before you run; your
job is the per-service code that reads it and constructs the pool.

**Internal connections only.** External services / clients sit outside the system boundary and are
rejected by the backend and hidden in the modal — never pool an outbound call to a `type:"client"`
or `external:true` node.

## The four parameters

| Param | Meaning | postgres (`psycopg_pool`) | mongodb (`pymongo`) | httpx (service→service) |
| --- | --- | --- | --- | --- |
| `max_connections` | hard cap on live connections | `max_size` | `maxPoolSize` | `Limits(max_connections=…)` |
| `min_idle` | connections kept warm when idle | `min_size` | `minPoolSize` | `Limits(max_keepalive_connections=…)` *(best effort)* |
| `idle_timeout_seconds` | reap an idle connection after this | `max_idle` | `maxIdleTimeMS` (×1000) | `Limits(keepalive_expiry=…)` |
| `max_lifetime_seconds` | recycle any connection after this | `max_lifetime` | **no equivalent** — document it | **not supported by httpx** — document it |

**Honesty rule:** postgres honors all four cleanly. For mongo, `max_lifetime` has no equivalent; for
httpx, `min_idle` maps only approximately (`max_keepalive_connections`) and `max_lifetime` is not
supported at all. When a param can't be honored, add a short code comment and mention it in your
summary — **do not fake it** with hand-rolled reaping.

## The places the config lives (working dir is the repo root)

1. `systems/<id>/manifest.json` — the connection's `connection_pool` block on the matching `edges[]`
   entry (the web app writes this before launching you; single source of truth, **read at STARTUP —
   do not hard-code the numbers**). It sits alongside any `resilience` block on the same edge:
   ```json
   "edges": [{ "from": "orders-service", "to": "order-db", "connection_pool": {
     "enabled": true, "max_connections": 10, "min_idle": 2,
     "idle_timeout_seconds": 30, "max_lifetime_seconds": 1800 } }]
   ```
2. `systems/<id>/<service>/` — the service's `app.py` / `Dockerfile` / `requirements.txt`, wired to
   build the pool at import and route its outbound call through it.

Optionally factor a tiny `systems/<id>/connection_pool/` helper (a `pool_config(to)` reader over the
mounted manifest keyed by `SERVICE_ID`, plus per-target factory functions) if more than one service
pools — same anti-drift spirit as the shared resilience/gRPC packages. Keep it light; pool
construction genuinely differs per target type.

## Reading the config — at STARTUP, keyed by SERVICE_ID

Mount the manifest read-only and read the block **once at import** (not per request):

```python
import json, os
SERVICE_ID = os.environ.get("SERVICE_ID", "")
def pool_config(to):
    edges = json.load(open("/manifest.json")).get("edges", [])
    for e in edges:
        if e.get("from") == SERVICE_ID and e.get("to") == to and e.get("connection_pool", {}).get("enabled"):
            return e["connection_pool"]
    return None
```

**Key divergence from [[sandbox-resilience]]:** pool sizes are fixed when the pool object is
*constructed*, so there is **no live re-read** — even a pure size edit requires a rebuild/restart of
the source service. Say so; don't imply a threshold-style hot edit.

## Per-target construction

Replace the per-request connect (verify the real call site first — e.g.
`with psycopg.connect(DSN, …)` inside a handler, or a one-shot `httpx.post(url, …)`).

- **postgres** — add `psycopg_pool` to `requirements.txt`; build once at module load:
  ```python
  from psycopg_pool import ConnectionPool
  _cfg = pool_config("order-db")
  pool = ConnectionPool(DB_DSN, open=True,
      min_size=_cfg["min_idle"], max_size=_cfg["max_connections"],
      max_idle=_cfg["idle_timeout_seconds"], max_lifetime=_cfg["max_lifetime_seconds"])
  # handler: with pool.connection() as conn: ...   (replaces psycopg.connect(...))
  ```
- **mongodb** — pass pool params to the existing single `MongoClient`:
  `MongoClient(URL, maxPoolSize=max_connections, minPoolSize=min_idle,
  maxIdleTimeMS=idle_timeout_seconds*1000, waitQueueTimeoutMS=<checkout timeout ms>)`. No
  `max_lifetime` equivalent — note it.
- **service→service (httpx)** — one module-level shared client, reused across requests:
  ```python
  import httpx
  _cfg = pool_config("payments-api")
  client = httpx.Client(timeout=10.0, limits=httpx.Limits(
      max_connections=_cfg["max_connections"],
      max_keepalive_connections=_cfg["min_idle"],
      keepalive_expiry=_cfg["idle_timeout_seconds"]))
  # replace httpx.post(url, ...) with client.post(url, ...)
  ```
  `max_lifetime` is not supported by httpx — leave a comment, don't emulate it.
- **redis / kafka targets** — no Python client pool exists in these systems today (redis is used only
  by the Node ws-servers; kafka via producers). If asked to pool one, **no-op with a clear note**
  rather than inventing a pool.

## Metrics + fast-state endpoint

Emit gauges into the default registry (they appear on the service's existing `/metrics` — **no new
scrape job**), labeled `connection="<from>-><to>"`: `connection_pool_max`, `connection_pool_active`,
`connection_pool_idle`. For psycopg, read `pool.get_stats()` (`pool_size`, `pool_available`, …) in a
gauge callback; for httpx/mongo, report `max` from config and best-effort active/idle (0 if the
client doesn't expose them).

Add `GET /pool/state` returning the live snapshot the diagram polls:
```json
{ "connections": [ { "to": "order-db", "max": 10, "active": 3, "idle": 2 } ] }
```
The web app aggregates this through the LB (`GET /api/connection-pool-state`) and the diagram polls it
~750ms to show a live `pool <active>/<max> · <idle> idle` badge on the line.

## Compose / Dockerfile

In the `from` service's `docker-compose.yml`: mount `./manifest.json:/manifest.json:ro` and set
`SERVICE_ID: <service>` (idempotent — may already be set for resilience/gRPC). `requirements.txt`
gains `psycopg_pool` for postgres targets (nothing new for mongo/httpx — those libs are already
present). No shared-package `COPY` is needed unless you factored a `connection_pool/` helper.

## First attach vs edit — always rebuild/restart

- **First pool on a service** (`firstAttach` from the API) → full wiring above + rebuild **only**
  that service: `docker compose -f systems/<id>/docker-compose.yml up -d --build <service>`.
- **Editing sizes** on an already-wired pool → the numbers are construction-time, so this is **not**
  a no-op: keep the wiring, then rebuild/restart that one service so the pool is reconstructed:
  `docker compose -f systems/<id>/docker-compose.yml up -d --build <service>` (a plain `restart`
  suffices if no code changed). This is the opposite of the resilience "threshold edit needs no
  rebuild" rule — do not carry that assumption over.

## Diagram conventions (already implemented in the frontend)

Inter-service/db lines are drawn **border-to-border with an arrowhead pointing from the source to the
target** (request → response). service→db is one line into the db; when service A→B **and** B→A both
exist, they render as **two parallel arrowed lines**. A connection with a live pool shows a small
`pool <active>/<max> · <idle> idle` badge just below the line, driven by the fast `/pool/state` read —
keep those fields accurate.

## Verify

1. `docker compose -f systems/<id>/docker-compose.yml up -d --build <service>` — service healthy;
   `/pool/state` returns the connection; `/metrics` shows `connection_pool_max/active/idle` labeled
   `connection="<from>-><to>"`.
2. **Cap**: drive more concurrent requests than `max_connections`; active plateaus at
   `max_connections` and extra requests wait (up to the checkout timeout) — they don't open unbounded
   connections. On postgres, `SELECT count(*) FROM pg_stat_activity` stays ≤ `max_connections`.
3. **Warm idle**: after load drains, idle settles toward `min_idle` (not 0).
4. **Reap**: leave it idle past `idle_timeout_seconds`; idle above `min_idle` is reaped.
5. **Sizes are construction-time**: change a param in the modal → confirm it takes effect only after
   the service is rebuilt/restarted (no live pickup) — and that the live badge reflects the new max.
6. **Unsupported params**: confirm `max_lifetime` (mongo/httpx) and `min_idle` (httpx) limitations are
   commented in code and stated in your summary, not silently dropped.

## Remove / change

Delete the `connection_pool` block from the edge (the web app's modal/DELETE does this), restore the
service's original connect (per-request `psycopg.connect` / one-shot `httpx.post`) if no pool remains
on that connection, drop the now-unused metrics + `/pool/state` entry, and rebuild that service.
Reference [[sandbox-resilience]] / [[sandbox-grpc-attach]] for the manifest-mount / `SERVICE_ID` /
single-service rebuild mechanics this shares.
