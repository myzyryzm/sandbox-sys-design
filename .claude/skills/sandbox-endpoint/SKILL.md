---
name: sandbox-endpoint
description: >-
  Add, edit, or delete an HTTP endpoint in a "Distributed Systems Sandbox" system
  (systems/<id>/). Use whenever the task is to create a route on a service, change an
  existing route, or remove one — it covers the FastAPI app, the endpoint registry that
  drives the diagram trace, and the docker rebuild/verify steps.
---

# Working on a sandbox endpoint

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.

## Protected route: `/health` (never edit or delete)

Every service serves a built-in `/health` liveness route, and the diagram's per-node
health check depends on it (`up`/health PromQL keys off the service responding). It is
**off-limits**: never modify, rename, or delete the `/health` route or its handler in any
`app.py`, and never touch its `endpoints.json` entry. The web app enforces this too — the
endpoint API rejects POST/DELETE on `/health` and the modal shows it as 🔒 built-in — so
match that behavior here. If a task asks to change or remove `/health`, refuse that part
and explain it's a required built-in (you may still add/edit/delete *other* routes).

## Repo layout (working directory is the repo root)

- `systems/<id>/<service>/app.py` — each service is its own FastAPI app (the base one is
  `service-1`; "Add service" clones more). HTTP routes live here. Metrics are
  **hand-written** with `prometheus_client` in a `@app.middleware("http")` function —
  keep that middleware intact and explicit; never swap in a black-box auto-instrumentor.
- `systems/<id>/<service>/{requirements.txt,Dockerfile}` — add a dependency here if a
  route needs one (e.g. a db driver), then rebuild.
- `systems/<id>/endpoints.json` — per-service endpoint registry:
  `{ "<service>": [ { method, path, protocol, request, response, requestModel,
  responseModel, description, downstream:[nodeIds], downstreamDescriptions:{nodeId:"…"},
  downstreamMethods:{nodeId:["METHOD /path", …]}, conversationId } ] }`. `path` is
  **service-local** (e.g. `/items`); the load balancer prefixes `/<service>` at routing.
  `protocol` is usually `http`; `sse` marks a streaming `text/event-stream` route — author it
  differently (see "SSE / streaming endpoints" below).
  `downstream` is the list of node ids this endpoint calls — it's what the diagram's
  lifecycle trace draws, so keep it accurate. `downstreamDescriptions` is a map (node id →
  one short line) describing what *this* endpoint uses each downstream connection for; the
  diagram prints it on the trace line. `downstreamMethods` is a map (node id → list of the
  specific `"METHOD /path"` routes — **service-local** paths, e.g. `"POST /payment/webhook"`)
  this endpoint calls on each downstream that is a **service or external service**; the
  diagram uses it to highlight those exact called method rows when this endpoint is selected.
  Only include service/external-service downstreams here (databases, caches, Kafka, etc. have
  no HTTP methods); keep its keys a subset of `downstream`.
- `systems/<id>/models.json` — the per-system **models bank**:
  `{ "models": [ { name, ts, description, createdAt, updatedAt } ] }`. Each model is a
  reusable **TypeScript** interface (the `ts` field) shared across services; a model may
  reference other models by name. An endpoint's request/response may **reference a model**
  instead of an inline schema: when `requestModel`/`responseModel` is set, that named
  model (in `models.json`) is the body's type — implement the route to match that
  TypeScript shape. The seeded prompt already inlines the referenced model's TS (with its
  transitive deps); `models.json` is the source of truth if you need the full definition.
- `systems/<id>/nginx/nginx.conf` — load balancer. Each service is routed at its own
  `/<service>/` prefix. Adding/removing a *single endpoint* does **not** change nginx
  (routes are per-service, not per-endpoint); only touch it when adding/removing a whole
  service.
- `systems/<id>/docker-compose.yml` — one container per node (services, databases +
  exporters, the lb, prometheus).

## Rebuilding (you run INSIDE the web app's dev server)

- Editing only `manifest.json` or `endpoints.json` shows up within seconds — no rebuild.
- Changing a service's `app.py` / `requirements.txt` / `Dockerfile` needs a rebuild of
  **just that service**:

  ```
  docker compose -f systems/<id>/docker-compose.yml up -d --build <service>
  ```

- Reach a service through the lb at `http://localhost:8080/<service><path>`. The live
  OpenAPI is at `http://localhost:8080/<service>/openapi.json`.

## Add or edit an endpoint

(Never the protected `/health` route — see above.)

1. Add/modify the route in `systems/<id>/<service>/app.py`, keeping the metrics
   middleware untouched. Follow REST conventions already used in the file (e.g. 200 with
   the body, 404 when missing, 503 when a downstream is unreachable).
2. If it reads/writes a database or calls another service, wire that call and set this
   endpoint's `downstream` in `systems/<id>/endpoints.json` to the node ids it touches
   (e.g. `["catalog-db"]`) so the diagram traces it. (For the modal-driven flow the
   registry entry already exists — update it in place; don't duplicate it.)
   **Also write `downstreamDescriptions`** — a brief one-line description per downstream id
   for what this endpoint uses that connection for (e.g.
   `{"catalog-db": "reads product rows for the line items"}`). Keep its keys in sync with
   `downstream`: add an entry for every new downstream node, drop entries for removed ones.
   **And write `downstreamMethods`** for every downstream that is a service/external service —
   the exact `"METHOD /path"` routes (service-local paths) this endpoint calls there, e.g.
   `{"payments-api": ["POST /payment"]}`. This drives the diagram's highlighting of the called
   method rows, so name the real routes (check that downstream service's `app.py` / its
   `endpoints.json`). Omit downstreams with no HTTP methods (databases, caches, Kafka, …).
3. Rebuild that service (command above).
4. Verify through the lb: `curl -s http://localhost:8080/<service><path>` returns what
   the spec describes, and the route appears in `/<service>/openapi.json`.

### SSE / streaming endpoints (`protocol: sse`)

When an endpoint's `protocol` is `sse` (the modal's "SSE" option, echoed in the seed prompt),
author it as a **streaming** route that serves `text/event-stream` (Server-Sent Events) instead of
a single JSON body. The rest of the flow (registry entry, `downstream`, rebuild) is unchanged.

- **Use plain `StreamingResponse`** — no new dependency (do **not** add `sse-starlette`):

  ```python
  import asyncio, json
  from fastapi.responses import StreamingResponse

  @app.get("/updates")
  async def updates():
      async def gen():
          # BOUNDED: stop after a finite number of events (or a capped time budget) so the
          # request terminates — every consumer (curl / lb.stream / the Run panel) waits for
          # the stream to end before it sees results.
          for i in range(10):
              yield f"data: {json.dumps({'seq': i})}\n\n"
              await asyncio.sleep(1)
      return StreamingResponse(
          gen(),
          media_type="text/event-stream",
          # nginx (the lb) buffers proxied responses by default; this disables buffering for THIS
          # response so frames reach the client incrementally — no nginx.conf change needed.
          headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
      )
  ```

- **Frame format:** each event is `data: <payload>\n\n` (a blank line ends the event); the
  endpoint's response schema/model describes one event's `data:` payload. You may add `event:` /
  `id:` lines.
- **Keep it bounded.** A never-ending generator hangs every consumer and leaks a coroutine per
  request. Cap it by event count or a max duration; don't rely solely on client-disconnect
  detection (it's unreliable behind the metrics middleware).
- **Method:** prefer `GET` (the client `lb.stream(...)` helper and browser EventSource are
  GET-only); `POST` works for curl/Python consumers if a request body is needed.
- **Leave the metrics middleware intact.** It records metrics when the response *starts*, then
  streams the body — so an SSE request is counted once in `http_requests_total`, but
  `http_requests_in_flight` / `http_request_duration_seconds` reflect stream **setup (time to first
  byte)**, not the stream's lifetime. That's intended here — do **not** "fix" it by moving metric
  recording into the body generator (it would dump multi-second samples into the latency histogram
  and wreck p95).
- **Protocol is immutable on edit.** The modal can't flip a unary route to SSE in place (that's a
  delete + re-add), so an SSE edit is always already a streaming route — modify it in place.

**Verify** (SSE-aware — a plain `curl -s` would hang): read a few frames with a deadline, and
confirm the content type + incremental delivery through the lb:

```
curl -N -sS -m 4 -D - http://localhost:8080/<service><path>
```

Expect a `Content-Type: text/event-stream` response header and `data:` frames arriving
*incrementally* over the window (not all at once at the end — that would mean the lb buffered
them). curl exiting 28 (the `-m` deadline) is expected. The route still appears in
`/<service>/openapi.json`.

### Editing an existing endpoint

When the task is to **update** a route that already exists (the modal's Edit flow), the
handler is already written — change it **in place**, do not rebuild it from scratch:

1. **Read the current handler first** in `systems/<id>/<service>/app.py` and make the
   smallest change that satisfies the request. The task prompt separates the endpoint's
   *current behavior* from the *change to apply* — preserve everything the change doesn't
   mention, and leave the metrics middleware and the other routes untouched.
2. If only the description changed, adjust the existing logic to match it. If the
   request/response contract changed, reconcile the handler (and its models) with the new
   types while keeping the rest of the behavior intact.
3. Keep `downstream` / `downstreamDescriptions` / `downstreamMethods` in
   `systems/<id>/endpoints.json` in sync with any connection or downstream call the change
   adds or removes (the registry entry already exists for the modal flow — update it in
   place). If the change alters which routes this endpoint calls on a downstream service,
   update that node's `downstreamMethods` list accordingly.
4. Rebuild that service and verify as above.

### Renaming an endpoint's path or alias

When the task is to **rename** an existing endpoint's path and/or its alias (function name),
the app has **already done the mechanical cascade** before this session starts — the
`endpoints.json` record (path/alias + history), every other endpoint's `downstreamMethods`
reference to this route, the client-function `scenarios.json` steps, and (if internal) the
nginx block are all updated. **Do not redo any of that.** Your job depends on which changed:

- **Alias only** (function name): nothing to do in code — the alias lives only in the
  registry/diagram, never in `app.py`. No rebuild. (Stale doc comments that mention the old
  alias are cosmetic; leave them unless asked.)
- **Path changed**: edit code and rebuild.
  1. **Owner service** (`<service>`): in `systems/<id>/<service>/app.py`, change **only this
     route's** decorator path from the old path to the new one (keep the handler logic and
     function name). Fix that route's own comments/docstrings. Do **not** touch sibling routes
     that merely share the prefix (e.g. renaming `/payment/webhook` must leave
     `/payment/webhook/2a` alone).
  2. **Caller services** (the task prompt lists them): in each caller's `app.py`, change **only**
     the outbound call URL for this route (e.g. `f"{OWNER_BASE}/<oldpath>"` →
     `f"{OWNER_BASE}/<newpath>"`, where the call targets `http://<service>:8000<newpath>`).
     Change nothing else about their behavior.
  3. If the same save also changed the request/response contract or description, apply that to
     the **owner** handler too (not the callers).
  4. **Rebuild the owner and every caller** in one go:
     `docker compose -f systems/<id>/docker-compose.yml up -d --build <service> <caller…>`, then
     verify the new lb path responds and the old one 404s.

  Transient note: until the rebuild finishes, the owner container still serves the OLD path, so
  endpoint discovery briefly shows a metadata-less "ghost" row at the old path alongside the
  pending new one — that's expected; don't try to re-add it.

### Update connection descriptions only

If the task is to (re)generate just the connection metadata for an endpoint (the modal's
"Update descriptions" button), edit **only** that endpoint's `downstreamDescriptions` and
`downstreamMethods` maps in `systems/<id>/endpoints.json`:
- `downstreamDescriptions` — one short line per `downstream` id, what the handler uses that
  connection for.
- `downstreamMethods` — for each service/external-service downstream, the exact
  `"METHOD /path"` routes (service-local) the handler calls there.

Both must be accurate to what the handler in `app.py` actually does (read it to be sure;
this is also how you backfill `downstreamMethods` on an older endpoint that lacks it). This
is a **pure JSON edit**: do not change `app.py` or nginx, and do **not** rebuild.

## Delete an endpoint

(Never the protected `/health` route — see above.)

1. Remove the route **and its handler function** from `systems/<id>/<service>/app.py`
   (leave the metrics middleware and other routes alone).
2. Remove the endpoint's entry from `systems/<id>/endpoints.json` if it's still there
   (when triggered from the web app's Delete button, it's already removed — just
   confirm).
3. Rebuild that service (command above).
4. Verify it's gone: `curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/<service><path>`
   returns 404, and the path no longer appears in `/<service>/openapi.json`.
