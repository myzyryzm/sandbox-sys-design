# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A "Distributed Systems Sandbox": you grow small but **real** Docker systems from a browser and
watch live Prometheus metrics on a generic React diagram. There is no mock layer — every node
(service, database, Kafka, gRPC server, …) is a real container.

Two cooperating processes:

1. **A per-system Docker stack** — `systems/<id>/docker-compose.yml`, one container per node
   (FastAPI services, databases + exporters, the nginx `lb`, `prometheus`). Only one system holds
   the shared host ports (8080 lb / 8000 / 9090 prometheus) at a time.
2. **The shared frontend = the backend control plane.** `frontend/` is a generic Vite/React app,
   and its "backend" is a set of **Vite dev-server plugins** under `frontend/server/*.js` (wired in
   `frontend/vite.config.js`). They expose `/api/*` routes that mutate `systems/<id>/` on disk and
   run `docker compose`. There is no separate API server — `npm run dev` IS the backend. The browser
   reaches Prometheus only through the Vite proxy (`/api/prometheus/*` → `:9090`), and loads system
   files through a tiny middleware that serves `../systems/` at `/systems/*` (same-origin, no CORS).

## CRITICAL operational rules

- **NEVER run `./start.sh` / `./stop.sh` / `./create_new.sh` when working inside an attached session.**
  Those scripts tear down and restart the Vite dev server you (and the user's browser terminal) are
  running inside. They are for a human starting the app from a fresh shell only.
- **Rebuild via `docker compose -f systems/<id>/docker-compose.yml …`, never the root scripts.** The
  canonical single-service rebuild after editing a service's `app.py` / `Dockerfile` / `requirements.txt`:
  ```
  docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
  ```
  After an nginx route change: `… exec -T lb nginx -s reload`. After a scrape-job change:
  `… restart prometheus`. (The backend's `rebuild()` in `frontend/server/scaffold.js` does exactly
  build → up -d → nginx reload → prometheus restart.)
- **Reach a service only through the lb:** `http://localhost:8080/<service><path>`; live OpenAPI at
  `http://localhost:8080/<service>/openapi.json`. DB/exporter containers are not published to host ports.
- **localhost-only security posture.** The dev server lets the browser drive real `docker` and a real
  `claude` PTY on the host. Backend plugins invoke docker with `execFile` + arg arrays (never shell
  strings), validate every `?system=` / node name against strict whitelists, and only write generated
  files. Keep the dev port private; preserve these invariants in any backend edit.
- Use the session scratchpad (see environment) for temp files, not the repo.

## Commands

```bash
cd frontend
npm install            # first time; postinstall fixes node-pty's spawn-helper exec bit on macOS
npm run dev            # http://localhost:5173 — the app AND the backend control plane
npm run build          # production build (use to type/compile-check after frontend edits)
```

There is no test runner or linter configured. "Verifying" a change means: rebuild the affected
container with `docker compose`, then `curl` it through the lb / check the Prometheus target / watch
the node on the diagram. The skills' `## Verify` sections give the exact commands per feature.

Point the frontend at a different system by editing `VITE_SYSTEM_ID` in `frontend/.env` (a single env
var today; no in-app selector yet).

## The manifest is the core abstraction

`systems/<id>/manifest.json` declares **both** topology and the metric queries; the React app is a
generic renderer of it — **to change what the user sees, edit the manifest, not the React code.**
- `nodes[]`: `{ id, label, type, position{x,y}, metrics:[{label, query, unit, scale?}],
  health?:{ query, rules:[{color, when}] } }`. `when` is a tiny safe `value <op> number` expression
  (e.g. `value < 1`); first matching rule wins; no value yet → gray.
- `edges[]`: `{ from, to }` node ids.
- `prometheus_base` is `/api/prometheus` (matches the Vite proxy); `poll_interval_ms` (default 4000).
- A node's `query` PromQL and the metric it reads must stay consistent with what the service's
  `app.py` actually exports. Service metrics are **hand-written** with `prometheus_client` in a
  `@app.middleware("http")` block (`http_requests_total`, `http_request_duration_seconds`,
  `http_requests_in_flight`) — keep that middleware explicit; never swap in an auto-instrumentor.

## The extensibility contract

Every feature is the same shape: **a manifest node + a compose service (+ exporter) + a Prometheus
scrape job (+ nginx route for HTTP)**, scaffolded by one dev-server plugin, so new capabilities slot
in without touching the generic frontend. Shared scaffolding primitives live in
`frontend/server/scaffold.js` (`cloneTemplate`, `addComposeService`, `addScrapeJob`, `addNginxRoute`,
`addManifestNode`, `serviceMetrics`, `serviceHealth`, `rebuild`) and `frontend/server/systems.js`
(`repoRoot`, `systemDir`, `isValidSystem`, position helpers). Compose/prometheus YAML edits go through
a **comment-preserving** `yaml` parser; nginx edits splice at `# === end upstreams/locations ===`
markers — generated files keep reading like the hand-authored ones. Don't fork these; compose them.

**Node names are permanent ids.** A name doubles as compose service name, nginx route, on-disk folder,
Prometheus job, and manifest node id: lowercase `^[a-z][a-z0-9-]*$` (`NAME_RE` in `scaffold.js`,
mirrored client-side by `frontend/src/nodeName.js`), unique per system, **no rename** (delete + re-add).

### Per-system registries (JSON the frontend & launched Claude sessions read)

Beyond the manifest, a system carries plain-JSON registries that are **edited live with no docker
rebuild** (the frontend re-reads them on a timer):
- `endpoints.json` — `service id → [{ method, path, protocol, downstream:[nodeIds], alias, request,
  response, requestModel, responseModel, description, conversationId, history }]`. `downstream` is
  what the diagram's lifecycle trace draws — keep it accurate. `GET /api/endpoints` merges this onto
  live OpenAPI discovery (through the lb).
- `models.json` — `{ models:[{ name, ts, description, … }] }`, the per-system "model bank" of reusable
  **TypeScript** interfaces. A model "references" another when that name appears as a field *type*
  (`\b<Name>\b`). Endpoints reference these via `requestModel`/`responseModel`; the database flow turns
  selected models into tables/collections (model→model reference = FK). Helpers in
  `frontend/src/modelBank.js`. **`//` comments in a model's `ts` are authoritative schema directives**
  (PK/FK/unique/index/length/…) that the DB-authoring prompt honors and that override generic defaults;
  reference detection scans a comment-stripped copy so a model named only in a comment isn't a phantom FK.
- `scenarios.json` — `{ functions:[{ client, name, args, description, steps, … }] }`, the multi-step
  call sequences external **clients** run through the lb. Each function is **owned by one client**
  (identity `(client, name)`; no shared bank, no attach — external services don't have functions);
  the diagram groups this file by `client` to draw a client's function rows + lifecycle traces.
- `streams.json` (Kafka topics + producers/consumers), `grpc/_registry.json` (gRPC contract bank +
  provenance).

### Mutations are done by launched Claude sessions + skills, not by the backend

The dev-server plugins handle the mechanical scaffold (compose/nginx/prometheus/manifest splice +
rebuild). Anything requiring **judgment** — writing a FastAPI route, authoring a DB schema, generating
a `.proto` + servicer, wiring resilience — is delegated to a spawned `claude` session whose prompt is
built from the persisted metadata. The terminal (`frontend/server/terminal.js`) runs `claude` in a
real PTY over a `/term` WebSocket, seeded with an `--append-system-prompt` that inlines the live
`manifest.json` and names the right skill; endpoint/feature flows spawn `claude --session-id <uuid>`
(or `--resume`) with an enriched task prompt. **When you ARE that launched session, follow the matching
skill in `.claude/skills/` — it has the canonical procedure and `Verify` steps:**

| Task | Skill |
| --- | --- |
| Add/edit/delete an HTTP route on a service | `sandbox-endpoint` |
| Add/update/delete a datastore (postgres/mongo/redis/MinIO) **or a read replica** | `sandbox-database` |
| Build a database's CDC worker (capture changes → Kafka) | `sandbox-database-cdc` |
| Add/update/delete a Kafka cluster, topics, producers/consumers | `sandbox-event-stream` |
| Define a gRPC contract (`.proto` + protoc + shared servicer) | `sandbox-grpc-contract` |
| Attach a contract to a service (server/client roles, targets) | `sandbox-grpc-attach` |
| Circuit-breaker + retry on a connection (edge) | `sandbox-resilience` |
| Register a new custom service type | `sandbox-custom-service-type` |
| Work on the Download Coordinator (peer-to-peer chunk distribution) | `sandbox-download-coordinator` |

`frontend/server/skills.js` serves these at `GET /api/skills`; because sessions spawn with cwd = repo
root, they auto-load as project skills — adding a `SKILL.md` makes it available with no code change.

## Layout pointers

- `systems/<id>/` — one self-contained system. `hello-lb` is the seed (lb → `service-1`); a service
  template lives at `frontend/server/templates/service/` ("Add service" clones it). `deltest` is a
  scratch system.
- `frontend/server/<feature>.js` — one plugin per `/api/<feature>` route (databases, services,
  endpoints, models, eventstreams, grpc, resilience, replicas, outage, clients, remove, dbschema,
  simulate, …); `customTypes/` holds backend recipes for custom service types (frontend renderers in
  `frontend/src/customTypes/`). `remove.js` also **blocks deleting a node another still depends on**
  (`findDependents` reverse-scans endpoints `downstream`/gRPC targets/Kafka producers-consumers/scenario
  steps; replicas + CDC workers are excluded as they cascade) — `GET /api/dependents` powers the Delete
  tab's proactive warning and `POST /api/delete` enforces the same guard.
- `frontend/src/*.jsx` — the generic diagram (`SystemDiagram.jsx`) + modals; it renders whatever the
  selected manifest describes.
- `instructions/` — original design briefs (background, not operational).
- `schemas/` — standalone example TypeScript schema files the user pastes into the model bank (e.g.
  `LedgerDB.ts`); **not** loaded by the app.

See `README.md` for the full per-feature walkthrough (each browser action and the exact files it writes).
