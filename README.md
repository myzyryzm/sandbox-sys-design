# Distributed Systems Sandbox

Learn distributed-systems behavior **by running it**: spin up small but real
systems, throw load at them, and watch real metrics move on a live diagram.

The repo ships one seed system, `hello-lb` — an nginx load balancer → `service-1`
(a generic FastAPI service), with Prometheus scraping the service and a generic
React frontend rendering the topology with live metrics overlaid. Each service is
reached through the LB at its own `/<service-id>/` prefix, and the LB node lists
those live, routable endpoints.

From there you grow the system **entirely from the browser** — every component is
real Docker, not a mock. From the header and per-node Edit panels you can add:

- **Services** (generic FastAPI) and **HTTP endpoints** on them (Claude-authored),
- **Databases** — Postgres, MongoDB, Redis, or a MinIO blob store (+ exporters),
  and **read replicas** of them,
- **Event streams** — a single-broker Kafka (KRaft) with topics/producers/consumers,
- **gRPC contracts** between services (server/client roles, drawn as edges),
- **Resilience policies** — circuit-breaker + retry on a connection,
- **Custom service types** — typed services that scaffold real containers (the
  first is the peer-to-peer **Download Coordinator**),

and exercise it all: generate load, trace an endpoint's request path, and take a
node offline to watch failures propagate. The frontend stays generic — it renders
whatever the selected system's `manifest.json` describes, so nothing above needs a
frontend edit.

---

## Folder structure (the extensibility contract)

```
repo-root/
  systems/
    hello-lb/                  # one self-contained system
      docker-compose.yml       # runs the whole system (one container per node)
      manifest.json            # topology + per-node PromQL (the frontend reads this)
      endpoints.json           # per-service endpoint registry (drives the trace)
      streams.json             # Kafka topic ⊕ producer/consumer registry (if any)
      scenarios.json           # per-client functions (multi-step call chains, keyed by client)
      load.sh                  # load generator (honors URL + METHOD env)
      prometheus/prometheus.yml
      nginx/nginx.conf         # per-service /<id>/ routes (insertion markers)
      grpc/                    # gRPC contract bank: .proto + generated bindings + shared servicers
      service-1/               # a generic FastAPI service + hand-written metrics
        app.py
        requirements.txt
        Dockerfile
      <node>/data/             # durable bind-mounted state (db data, coordinator chunks, …)
  frontend/                    # generic React app, SHARED across all systems
    src/customTypes/           # frontend rendering for custom service types
    server/                    # Vite dev-server plugins (the backend "control plane")
      templates/service/                # canonical generic service ("Add service" clones this)
      templates/download-coordinator/   # coordinator + worker templates
      customTypes/                       # backend recipes for custom service types
  README.md
```

**Adding a new system later = create a new `systems/<id>/` folder** with its own
compose file + `manifest.json`. The frontend never needs editing — it renders
whatever the selected system's manifest describes. Point the frontend at a
different system by changing `VITE_SYSTEM_ID` in `frontend/.env`.

---

## Prerequisites

- **Docker + Docker Compose** — to run the system (nginx, backend, Prometheus).
- **Node.js 18+** — to run the React frontend dev server.
- Python is **not** needed on the host; the backend runs in a container. (It's
  only useful if you want to run the backend standalone for debugging.)

---

## Quick start (root scripts)

From the repo root, start/stop a system by id — this brings up its Docker stack
**and** the shared frontend pointed at it:

```bash
./start.sh hello-lb     # docker compose up + frontend dev server (VITE_SYSTEM_ID=hello-lb)
./stop.sh  hello-lb     # docker compose down + stop the frontend
```

- `./start.sh hello-lb --no-frontend` — start only the Docker stack.
- `./stop.sh hello-lb --keep-frontend` — tear down Docker but leave the frontend up.
- Run with no args to list available systems.

Create a **new system** by cloning the base (`hello-lb`) — it starts immediately:

```bash
./create_new.sh my-system     # systems/my-system/ from hello-lb, then ./start.sh my-system
```

The id must be lowercase letters/digits/hyphens. It copies `systems/hello-lb/`,
rewrites the manifest's `system_id`/`name`, and brings the new system up. Edit
`systems/my-system/manifest.json` (and `service-1/`, compose) to make it yours.

Only one system holds the shared host ports (8080/8000/9090) at a time, so
`start.sh` (and `create_new.sh`) automatically stops the previously active
system before starting the new one.

Runtime state (frontend pid/log, active system) lives in `.run/` (gitignored).
The sections below describe the same steps run manually.

## Start the system (manual)

```bash
cd systems/hello-lb
docker compose up --build
```

This starts three containers:

| Service      | Host port | Purpose                                           |
| ------------ | --------- | ------------------------------------------------- |
| `lb` (nginx) | `8080`    | Load balancer; routes `/service-1/…` → `service-1` |
| `service-1`  | —         | FastAPI app (`/health`, `/metrics`); internal only |
| `prometheus` | `9090`    | Scrapes `service-1:8000/metrics` directly         |

Restart cleanly any time with `docker compose down` then `docker compose up`.
Prometheus data is intentionally not persisted.

## Start the frontend

In a second terminal:

```bash
cd frontend
npm install        # first time only
npm run dev        # serves on http://localhost:5173
```

Open <http://localhost:5173>. It loads `hello-lb`'s manifest and draws the
diagram.

### How networking / CORS is handled

The browser never talks to Prometheus directly (that causes CORS pain).
Everything is same-origin through the **Vite dev server**:

- **Prometheus queries** — the frontend calls `/api/prometheus/api/v1/query?...`.
  Vite proxies `/api/prometheus/*` → `http://localhost:9090/*` (the prefix is
  stripped). See `frontend/vite.config.js`.
- **Manifest serving** — a tiny custom Vite middleware (`serveSystems` in
  `vite.config.js`) serves the repo's `systems/` directory under `/systems/*`.
  The frontend fetches `/systems/<id>/manifest.json`. This is the
  "serve via the dev server" approach (no separate static server, no CORS).

Because both go through the dev server, the **frontend must be started with
`npm run dev`** for live metrics — a plain static build won't have the proxy.

---

## Editing a system from the browser (Claude terminal)

Click **"Edit with Claude ▸"** in the header to open a terminal panel running an
interactive **Claude Code** session scoped to the system you're viewing. Ask it
to add or change components ("add a Redis cache node", "expose a queue-depth
metric") and apply the changes — the diagram updates live.

How it works (all inside the one `npm run dev` process):

```
xterm.js (browser) ⟷ WebSocket /term ⟷ Vite plugin ⟶ node-pty ⟶ `claude` (TUI)
```

- The plugin `frontend/server/terminal.js` attaches a WebSocket on `/term` to the
  dev server (it claims only that path, leaving Vite's HMR socket alone) and
  spawns `claude` in a real pseudo-terminal — a PTY is required because Claude
  Code is a full-screen TUI.
- The session is made **aware of its role and the current system** via a
  generated `--append-system-prompt` built from the live `manifest.json`
  (see `buildSystemPrompt` in that file). Its working directory is the repo root,
  so it can run `./start.sh <id>` to rebuild after backend/compose changes.
- It runs in **default permission mode** — Claude asks before edits and commands,
  and you approve right in the terminal.
- Manifest-only edits appear in the diagram within seconds (the frontend
  re-fetches `manifest.json` on a timer). Backend/compose/nginx/Prometheus
  changes need a rebuild (`./start.sh <id>`), which Claude can run for you.

**Notes**

- `node-pty` is a native module; `npm install` uses prebuilt binaries (no
  compiler needed). On macOS the prebuilt `spawn-helper` ships without its
  execute bit, so a `postinstall` script restores it — if you ever see
  `posix_spawnp failed`, run
  `chmod +x frontend/node_modules/node-pty/prebuilds/darwin-*/spawn-helper`.
- **Security:** the WebSocket lets the browser drive a real Claude Code session
  on your machine. The Vite dev server binds localhost by default — keep it that
  way and don't expose the dev port publicly. The `?system=` value is validated
  against real `systems/` folders before anything is spawned.

---

## Adding a database from the browser

Click **"＋ Add database"** in the header to provision a real datastore into the
system you're viewing. Pick a type, name it, and declare its entities:

| Type | What runs | Entities | Metrics source |
| --- | --- | --- | --- |
| PostgreSQL (SQL) | `postgres` + `postgres-exporter` | tables (+ columns) | exporter |
| MongoDB (NoSQL) | `mongo` + `mongodb_exporter` | collections (+ sample fields) | exporter |
| Redis (key-value) | `redis` + `redis_exporter` | key namespaces | exporter |
| Blob (simulated S3) | `minio` (+ `mc` init) | buckets | MinIO native `/minio/v2/metrics/cluster` |

On submit the dev-server plugin (`frontend/server/databases.js`) writes the
changes into the active system and rebuilds it:

1. an init script under `systems/<id>/<name>/` that creates the entities
   (`CREATE TABLE` / `db.createCollection` / seeded keys / `mc mb` buckets),
2. the DB service **and** its exporter appended to `docker-compose.yml`,
3. a scrape job appended to `prometheus/prometheus.yml`,
4. a node appended to `manifest.json`, then
5. `docker compose up -d` + a Prometheus restart so the new target is scraped.

Within a few seconds the new node appears on the diagram (live manifest reload)
and its metrics fill in once Prometheus scrapes it.

**Click a database node** to view its **current schema**, read live from the running
container by `GET /api/db-schema` (`frontend/server/dbschema.js`) — not the init
script, so it reflects the database's actual state (e.g. after a service or a Claude
session alters it). Each engine is introspected with its own client via
`docker compose exec`: Postgres → `information_schema` (tables + columns), MongoDB →
`getCollectionNames` + a sampled document (collections + fields), Redis → `--scan`
grouped by `namespace:`, MinIO → `ls /data` (one directory per bucket).

**Notes**

- The new database is **not** auto-connected to any service (no edge) and the
  app is **not** modified to use it — provisioning and wiring a service to a DB
  for CRUD are deliberately separate steps, since a system can have many services.
- DB containers are **not** published to host ports (Prometheus reaches them over
  the compose network), so any number of databases coexist with no port conflicts.
- The `docker-compose.yml` / `prometheus.yml` edits go through a YAML parser that
  preserves the existing hand-written comments.
- Same localhost-only security posture as the terminal: the endpoint runs inside
  the dev server, validates all input against strict whitelists, and only writes
  generated files — keep the dev port private.

---

## Adding a service from the browser

Click **"＋ Add service"** in the header to add a generic service to the system
you're viewing. Give it a name and submit; the dev-server plugin
(`frontend/server/services.js`):

> **Node names are permanent ids.** A service / database / event-stream / external-
> service / client name doubles as its compose service name, nginx route, on-disk
> folder, Prometheus job, and manifest node id — so it must be lowercase letters,
> digits and hyphens (start with a letter), contain **no spaces**, be **unique**
> within the system, and **can't be changed** after creation. The create forms
> validate this client-side (`frontend/src/nodeName.js`) and the server rejects
> anything invalid (`NAME_RE` in `frontend/server/scaffold.js`); there is no rename.

1. clones the canonical service template
   (`frontend/server/templates/service/` — the same hand-instrumented FastAPI app
   with `/health` and `/metrics` that `service-1` is) into `systems/<id>/<name>/`,
2. adds a `build: ./<name>` service to `docker-compose.yml`, an nginx
   `/<name>/` route (upstream + location, at the markers in `nginx.conf`), and a
   scrape job to `prometheus/prometheus.yml`,
3. adds a `service` node to `manifest.json` (metrics scoped to the new scrape
   job), then
4. `docker compose build <name>` + `up -d`, an `nginx -s reload`, and a
   Prometheus restart.

The node appears on the diagram and goes green once scraped, and the service's
endpoints show up on the LB node (see below). Add real endpoints to its `app.py`
(e.g. from the Claude terminal), rebuild, and they appear automatically.

### Load-balancer endpoints

The LB node lists every service's **live, routable** endpoints — discovered by
reading each service's FastAPI `/openapi.json` **through the LB** and prefixing
the path with the service id (`frontend/server/endpoints.js`, served at
`GET /api/endpoints`). `/health` is shown as `GET /service-1/health`; `/metrics`
is omitted (that's Prometheus's, scraped directly). The list refreshes on a timer,
so endpoints added to a service appear without a reload.

### Tracing an endpoint's lifecycle

**Click an endpoint row** in the LB node to trace its request path:
`LB → owning service → the service(s)/db(s) that endpoint calls`. The traced
nodes and a directed (arrowed) edge for each hop are highlighted while everything
else dims; click the row again, or empty canvas, to clear.

The downstream hops come from an optional per-system **endpoint registry**,
`systems/<id>/endpoints.json` — a map of service id → endpoint records
(`{ method, path, protocol, downstream, alias, request, response, requestModel,
responseModel, description, conversationId, history }`). `GET /api/endpoints` merges
this onto the live OpenAPI discovery: it attaches `protocol`/`downstream` (and the
editable `alias`/`request`/`response`/`requestModel`/`responseModel`/`description`/
`history`), surfaces registry endpoints the container isn't serving yet, and drops any
`downstream` id that isn't a real node.
A system without an `endpoints.json` simply traces `LB → service` (no extra hops).
The seeded demo wires `GET /service-1/items` to a Postgres `catalog-db`.

### Authoring an endpoint (Claude-backed)

Each **service** node has an **≡** button in its header that opens the endpoints
modal for that service. It lists the service's endpoints (method · path ·
`alias()` · protocol) and an **＋ Add endpoint** form: method, path, protocol
(`HTTP/S`), a required **function name** (`alias`), a request and response schema
(a JSON `{key: "type"}` map **or** a referenced model — see below), and a
natural-language description (Enter submits).

The request and response each carry a **model dropdown** above the inline schema box.
Leave it on `— inline schema —` to type a `{key: "type"}` map as before, or pick a
model from the [models bank](#models-bank-reusable-typescript-types) to reference a
reusable TypeScript type (stored on the record as `requestModel` / `responseModel`,
which then supersede the inline map for that field). When a model is referenced, the
seeded Claude prompt inlines that model's TypeScript along with every model it
references, transitively.

The **alias** is a required code-style function name for the endpoint; it must be
unique within the service (two different services may reuse the same name), and it's
shown as an `alias()` chip in the list. Editing an existing endpoint pre-fills the
form with its current alias / request / response / description, and shows a read-only
**update history** above the form — one row per save (timestamp, alias, request and
response schemas, description), so you can see every spec the endpoint has been
created or updated with. Each save appends a snapshot to that server-authoritative
`history`; renaming the route (changing method/path) starts a fresh trail.

On submit the frontend generates a Claude **session id** (`crypto.randomUUID()`),
persists the endpoint via `POST /api/endpoints` into `systems/<id>/endpoints.json`
(so it shows immediately as a pending endpoint, with the session id saved), then
opens the terminal panel on a fresh `claude --session-id <id>` session seeded with
an enriched prompt — the description plus the structured spec and concrete build
steps (implement the route in the service's `app.py`, set the endpoint's
`downstream` so the trace lights up, rebuild just that service via
`docker compose … up -d --build <service>`, verify through the LB). Claude writes
the code and the rebuild; the dev server is never touched.

Each endpoint that carries a saved `conversationId` shows a **Resume** button —
it reopens the terminal on `claude --resume <id>` with the full prior context, so
you can iterate on the endpoint later. (Context stays lean via Claude Code's
built-in auto-compaction; you can also type `/compact` in a resumed session.)

The terminal (`frontend/server/terminal.js`) spawns a per-`/term`-connection
`claude` pty; the `session`/`mode`/`prompt` query params select a new
(`--session-id`) or resumed (`--resume`) session, falling back to the general
"edit this system" session when absent.

### Models bank (reusable TypeScript types)

The header's **＋ Models** button opens the **models bank** — a per-system store of
reusable model interfaces authored in **TypeScript**, shared across all of the
system's services. Each model has a **name** (a TypeScript identifier that is its
permanent id, unique within the system — rename = delete + re-add) and a raw
TypeScript **definition** that may reference other models by name (e.g. an `Order`
whose body uses `OrderItem[]`). The bank is stored in `systems/<id>/models.json`
(`{ models: [ { name, ts, description, createdAt, updatedAt } ] }`) and served by
`frontend/server/models.js` at `GET`/`POST`/`DELETE /api/models`. `POST` upserts by
name; `DELETE` is **blocked** (with a message naming the offenders) while any
endpoint still references the model via `requestModel`/`responseModel`.

Models are how an endpoint's request/response reference a shared type instead of an
inline `{key: "type"}` map (see [Authoring an endpoint](#authoring-an-endpoint-claude-backed)).
There's no docker rebuild — like the endpoint registry, this is pure JSON the
endpoint-authoring Claude session reads when implementing the route.

### Deleting a service or database

Each service and database node has an **✕** in its header. Clicking it asks for
confirmation (Cancel, or click outside the modal, to back out), then `POST
/api/delete` (`frontend/server/remove.js`) tears the component down — the inverse
of the create flow: it removes the compose service(s) (a database also owns its
`-exporter`/`-init` sidecars), the nginx route (services), the scrape job, the
manifest node and edges, and the folder, then `docker compose up -d
--remove-orphans` deletes the orphaned containers. Any `depends_on` references to
the removed service are scrubbed so the compose project stays valid. The LB itself
isn't deletable; the base `service-1` is (the system drops to just the LB).

A node **can't be deleted while another node still depends on it**. Before offering
the button the Delete tab probes `GET /api/dependents?system=&id=` and lists every
dependent — an endpoint whose `downstream` calls it (HTTP, with the exact
`METHOD /path`), a gRPC client targeting it, a producer/consumer on its Kafka topics,
or a client function step that calls it — then disables Delete until those calls are
removed. `POST /api/delete` enforces the same guard server-side (a blocked delete
returns `400` with the `dependents` list). Owned children that cascade anyway — a
primary's read replicas (`replicaOf`) and a database's CDC worker (`cdcOf`) — are
excluded, so they don't block their parent's deletion.

---

## External services (outside the system)

Click **"＋ Add external service"** to add a third-party dependency your services
call out to — a payment gateway, an email provider, anything you don't run. It's a
real FastAPI container (so the calls actually work), but the dev-server plugin
(`frontend/server/externalServices.js`) deliberately treats it as living *outside*
your system:

- It's drawn **outside the system-boundary box** on the diagram (a dashed, neutral
  node), and the boundary appears around your in-system nodes as soon as the first
  external service exists.
- It is **not scraped by Prometheus** (no scrape job) and has **no health check**,
  so it never colors green/red — it's not part of your observability surface.
- It carries `type: "external_service"`, which the gRPC layer gates against, so it
  **can't serve or consume gRPC contracts**. It *can*, however, be the **target of
  a circuit breaker** — a resilience policy only requires the *caller* to be one of
  your services.
- It still gets an nginx `/<name>/` route (that's how endpoints are discovered and
  reached), but its endpoints are kept **off the load balancer's advertised
  surface** (`endpointPolicy`) — they belong to the third party, not to you.

Its Edit panel has **Endpoints** (define the third party's API — same flow as a
service, no gRPC tab; the built-in `/health` is hidden, since it's not yours),
**Calls** (read-only — pick one of its methods to trace its call path on the diagram),
**Shutdown** (take the dependency offline to watch a caller's breaker trip), and
**Delete**. To model your service calling out: add an endpoint to one of your services
whose `downstream` includes the external node, then click that connection to attach a
circuit breaker. **The reverse also works** — an external service's *own* endpoint can
have a `downstream` **back into the system** (a webhook/callback, e.g.
`payments-api.completePayment → service-1.paymentFlowStep1`), which the diagram traces
exactly like any other call. What external services don't have is the **Functions**
trigger bank — that's client-only. Everything else (`scaffold.js`, the diagram, the
manifest) is shared with the generic-service flow — only the scrape job, the boundary
placement, and the gRPC exclusion differ.

---

## Clients (multi-step callers)

Click **"＋ Add client"** to add a *caller* that lives outside the system — drawn to
the **left** of the boundary (external services are on the right), giving the diagram
a left-to-right story: **clients → [system] → external services**. A client has **no
container**: it's a manifest node plus its **functions**, so adding one is instant
(`frontend/server/clients.js`, `POST /api/clients` — no docker rebuild).

A client connects to the load balancer (a faint always-on line); selecting a public
endpoint on the LB extends the lifecycle trace back to it — `client → LB → service →
downstream`.

Its Edit panel has one tab, **Functions**. A *function* is a named, argument-taking
sequence of HTTP calls the client makes through the load balancer. Each function is
**owned by that one client** — there is no shared bank and no attach-by-name. External
services have no functions (the trigger bank is client-only; an external service still
calls into the system through its own endpoints' `downstream`). Define one with a
name + argument signature + a plain-English description; that launches a Claude session
(the `sandbox-client-scenario` skill) which authors the call **steps** into
`systems/<id>/scenarios.json`. Each step is a method + an LB path (a discovered route) +
an optional JSON body; tokens substitute at run time — `${args.<name>}` for the
function's arguments and `${N.field}` (1-indexed) for an earlier step's response, so you
can e.g. create an order then pay for it with `${1.id}`. **Run** executes the steps **for
real** through the load balancer (`POST /api/scenarios/run`): each response is captured
and fed into the next step, and the per-step status + response (and the substituted body)
are shown — the same real request path the load generator uses (`localhost:8080`). Each
of the client's functions also appears as a clickable `ƒ` row on its diagram node, which
traces the whole call path. Functions persist in `systems/<id>/scenarios.json`, keyed by
their owner client (`frontend/server/scenarios.js`).

---

## gRPC contracts between services

Each **service** node's Edit panel has a **gRPC** tab. A gRPC *contract* is a
`.proto` service authored once and kept in the per-system **bank** under
`systems/<id>/grpc/` (its `_registry.json` records every method + provenance). You
define a contract's RPC methods + message types — or upload a complete `.proto`,
validated by the real `protoc` — then **attach** it to services as a **server**
(imports the shared servicer + runs a gRPC server) and/or **client** (a stub
pointed at editable targets). Roles live on the service nodes' manifest `grpc`
block, so the diagram draws gRPC edges as soon as you attach.

The registry edit is instant; the `.proto` / `_pb2` / `_servicer.py` codegen and
the `app.py` wiring are done by a launched Claude session (the
`sandbox-grpc-contract` / `sandbox-grpc-attach` skills). Backend:
`frontend/server/grpc.js` + `grpcInstall.js`.

---

## Resilience policies (circuit breaker + retry)

On a **connection** (a source service → target node call) you can attach a
**circuit-breaker + retry** policy. The config (thresholds, retry budget) is
upserted onto the manifest **edge** `{from,to}`, and a shared Python wrapper reads
it at runtime — implementing the CLOSED → OPEN → HALF-OPEN state machine, emitting
per-connection metrics, and exposing a fast `/resilience/state` the browser polls
to watch a breaker trip **live** (faster than the Prometheus scrape). The first
attach to a service wires + rebuilds it; a later threshold change is
manifest-only. Backend: `frontend/server/resilience.js`; the runtime wiring is the
`sandbox-resilience` skill's job.

---

## Event streams (Kafka)

Click **"＋ Add event stream"** to provision a real single-broker **Kafka** (KRaft)
+ a `kafka-exporter`, a Prometheus scrape job, a manifest node, and a
`streams.json` **topic registry**. Kafka speaks a binary protocol, so there's no
nginx route. Producers and consumers aren't something the broker tracks, so they
live in the registry — and that's what the diagram's producer→cluster→consumer
edges are drawn from. The topic view merges the registry with the broker's **live**
topic list. Backend: `frontend/server/eventstreams.js`.

---

## Read replicas

A Postgres / MongoDB / Redis node's Edit panel offers **Add read replica** — a
**real** streaming, read-only standby (`<primary>-<N>`) that streams from the
primary, with its own exporter + scrape job + manifest node. It records
`replicaOf` / `replication` / `readonly`, so the diagram draws the primary↔secondary
arrow and the dotted cluster box. (Object stores have no replica concept.)
Backend: `frontend/server/replicas.js`.

---

## Taking a node offline (outage)

Any node's Edit panel can **shut it down for N seconds** (1–300). The backend
`docker compose stop`s the container — so its port closes and callers get
connection-refused (the LB returns 502) — schedules an automatic restart after the
window, and the diagram paints the node orange until it returns. It's the simplest
way to watch a failure (and any resilience policy) react; "bring back now" cancels
the timer. Backend: `frontend/server/outage.js`.

---

## Custom service types (the Download Coordinator)

**"＋ Add service"** offers a **type** selector. Beyond "Generic service", a
*custom service type* scaffolds real, type-specific container(s) and plugs its own
Edit tabs + diagram rendering into the same modal/diagram everything else uses —
**without forking** any generic layer. A type is exactly two registry entries: a
backend recipe (`frontend/server/customTypes/<type>.js`) and a frontend renderer
(`frontend/src/customTypes/<type>/`); adding the next one touches neither the modal,
the diagram, nor the manifest core.

The first consumer is the **Download Coordinator** — a peer-to-peer "distribute a
large file to many nodes" system. Adding it creates one coordinator container
(origin seed + orchestrator) that ships two gRPC contracts (ChunkTransfer +
Coordination) into the bank. From its **Distribution** tab you **Add node** (spawns
real worker containers), pick a source (a URL or a pre-staged local file), choose a
chunk size, and **Run distribution**. The coordinator chunks the file and hands out
assignments; the moment a worker holds chunk N it becomes a valid source for N, so
distribution shifts from a **star** (everyone pulls from the coordinator) to a
**mesh**. Each node persists its chunk bitmap to disk, so a restarted worker
resumes from on-disk state with no re-fetch. The diagram renders a per-node bitmap
grid + aggregate %, and live chunk-source edges show the star→mesh shift. See the
`sandbox-custom-service-type` / `sandbox-download-coordinator` skills.

---

## Testing the system (the "Test" button)

Click **"🧪 Test"** in the header to open the simulations modal. Today it has one
simulation, **Generate load**: choose a **target endpoint** (method + path, from
the same `/api/endpoints` catalog the LB shows), a rate (req/s), and **Start
load**; request-rate, latency and in-flight metrics move on that service's node.
**Stop** ends it.

The dev-server plugin `frontend/server/simulate.js` spawns `load.sh` detached (in
its own process group) with the chosen `URL` + `METHOD` in its environment, and
tracks it per system — so Stop, or shutting down the dev server, kills the whole
loop. `GET/POST /api/test/load` report and control its state. It's built as a
list so future simulations (latency injection, kill a node, …) can slot in
alongside.

---

## Smoke test — prove the pipeline end to end

The pipeline is: **service exposes metrics → Prometheus pulls them → frontend
queries Prometheus**. If all four checks below agree, it's proven.

**1. Generate load** through the load balancer (leave it running in a terminal):

```bash
cd systems/hello-lb
./load.sh            # or:  while true; do curl -s localhost:8080/service-1/health; done
```

**2. Prometheus Targets page** — open
<http://localhost:9090/targets>. The `service-1` job target should show
**State = UP**.

**3. Prometheus query UI** — open <http://localhost:9090/graph> and run each of
these; with load running they return live numbers:

```promql
sum(rate(http_requests_total{job="service-1"}[1m]))
histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job="service-1"}[1m])) by (le)) * 1000
sum(http_requests_in_flight{job="service-1"})
```

**4. React frontend** — <http://localhost:5173>. The `service-1` node shows
`req/s`, `p95`, `in-flight`, and `errors`, and the numbers **change as load
runs**. The node header is **green** while the service is up. Stop the service
(`docker compose stop service-1`) and within a poll cycle or two the node turns
**red**; restart it (`docker compose start service-1`) and it goes green again.

---

## The manifest (core abstraction)

`systems/hello-lb/manifest.json` declares the topology **and** the metric
queries. The frontend is a generic renderer of this file. Key fields:

- `prometheus_base` — base path the frontend prefixes onto `/api/v1/query`
  (`/api/prometheus`, matching the Vite proxy).
- `poll_interval_ms` — how often the frontend re-runs the queries (default 4000).
- `nodes[]` — each has `id`, `label`, `type`, `position {x,y}` (top-left of the
  box), a list of `metrics[]` (each `{label, query, unit, scale?}`), and an
  optional `health { query, rules[] }`.
- `health.rules[]` — `{ color, when }`; `when` is a tiny safe expression of the
  form `value <op> number` (e.g. `value < 1`). First matching rule wins; no
  value yet → gray. The seed system colors the `service-1` node off
  `up{job="service-1"}`.
- `edges[]` — `{ from, to }` node ids, drawn as lines between box centers.

To extend the diagram, edit the manifest — not the React code.

### Metrics instrumented in the service

Defined by hand with `prometheus_client` in `service-1/app.py` (read
`metrics_middleware` to see exactly how each is produced):

- `http_requests_total` — Counter, labels `method`, `endpoint`, `status`.
- `http_request_duration_seconds` — Histogram, labels `method`, `endpoint`.
- `http_requests_in_flight` — Gauge (up on entry, down on exit).

The `/metrics` endpoint itself is excluded from instrumentation so Prometheus
scrapes don't inflate req/s or latency.

---

## Extending further

Every capability above entered through the same extensibility contract — a
manifest node + compose service + scrape job, scaffolded by a dev-server plugin —
so new ones keep slotting in without reworking the generic frontend. Ideas not
built yet: optional Grafana dashboards, a Locust load generator, latency injection
(netem), and coordinator hot-standby / failover for the Download Coordinator (its
orchestration state is deliberately kept in one separable object as the seam, but
standby itself is out of scope today).

---

## Running a service without Docker (optional debugging)

```bash
cd systems/hello-lb/service-1
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
curl localhost:8000/health
curl localhost:8000/metrics
```
