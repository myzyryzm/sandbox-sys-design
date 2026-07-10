# Distributed Systems Sandbox

Learn distributed-systems behavior **by running it**: spin up small but real
systems, throw load at them, and watch real metrics move on a live diagram.

The repo ships a minimal seed system, `hello-lb` — an nginx load balancer → `service-1`
(a generic FastAPI service), with Prometheus scraping the service and a generic
React frontend rendering the topology with live metrics overlaid. Each service is
reached through the LB at its own `/<service-id>/` prefix, and the LB node lists
those live, routable endpoints. (A richer worked example, `payment-service`, ships
alongside it — see [Folder structure](#folder-structure-the-extensibility-contract).)

From there you grow the system **entirely from the browser** — every component is
real Docker, not a mock. From the header and per-node Edit panels you can add:

- **Services** (generic FastAPI) and **HTTP endpoints** on them (Claude-authored),
- **Databases** — Postgres, MongoDB, Redis, or a MinIO blob store (+ exporters),
  **read replicas**, durable **seed data**, and **CDC** (change-data-capture to Kafka),
- **Event streams** — a single-broker Kafka (KRaft) with topics, producers, and
  per-service **consumer functions**, plus optional model-backed topic schemas,
- **gRPC contracts** between services (server/client roles, drawn as edges),
- **Resilience policies** — circuit-breaker + retry on a connection,
- **Clients** (multi-step callers) and **external services** (third-party deps),
- **Custom service types** — typed services that scaffold real containers (the
  first is the peer-to-peer **Download Coordinator**),

and exercise it all: trace an endpoint's request path, take a node
offline to watch failures propagate, and run **end-to-end test processes** that seed
preconditions, drive clients, and probe for design defects (PASS/FAIL). The frontend
stays generic — it renders whatever the selected system's `manifest.json` describes,
so nothing above needs a frontend edit.

---

## Folder structure (the extensibility contract)

```
repo-root/
  systems/
    hello-lb/                  # minimal seed system (lb -> service-1)
    payment-service/           # richer worked example (~15 nodes; see below)
    <id>/                      # one self-contained system:
      docker-compose.yml       # runs the whole system (one container per node)
      manifest.json            # topology + per-node PromQL + boundary (the frontend reads this)
      endpoints.json           # per-service endpoint registry (drives the trace)
      models.json              # model bank: reusable TypeScript interfaces (if any)
      clients.json             # client roster (multi-step callers, if any)
      scenarios.json           # per-client functions (multi-step call chains, keyed by client)
      consumers.json           # per-service Kafka consumer functions (if any)
      endtoend.json            # named end-to-end test processes (if any)
      endtoend-runs/           # persisted PASS/FAIL run reports
      prometheus/prometheus.yml
      nginx/nginx.conf         # per-service /<id>/ routes (insertion markers)
      grpc/                    # gRPC contract bank: .proto + generated bindings + shared servicers
      service-1/               # a generic FastAPI service + hand-written metrics
        app.py
        requirements.txt
        Dockerfile
      <db>/                    # a datastore: init script, seeds.json + seed.sql|seed.js, cdc.json
      <db>-cdc/                # a CDC worker container (Dockerfile + app.py) if CDC is enabled
      <cluster>/streams.json   # per-Kafka-cluster topic ⊕ producer/consumer registry
      clients/                 # per-client Python (module.py) + shared lbclient.py
      <node>/data/             # durable bind-mounted state (db data, coordinator chunks, …)
  frontend/                    # generic React app, SHARED across all systems
    src/                       # the generic diagram + modals (renders any manifest)
    src/customTypes/           # frontend rendering for custom service types
    server/                    # Vite dev-server plugins (the backend "control plane")
      templates/service/                # canonical generic service ("Add service" clones this)
      templates/client/                 # generic client Python (clients scaffold from this)
      templates/download-coordinator/   # coordinator + worker templates
      customTypes/                       # backend recipes for custom service types
  README.md
```

**Adding a new system later = create a new `systems/<id>/` folder** with its own
compose file + `manifest.json`. The frontend never needs editing — it renders
whatever the selected system's manifest describes. Point the frontend at a
different system by changing `VITE_SYSTEM_ID` in `frontend/.env`.

The repo ships two systems: **`hello-lb`** (the minimal seed) and
**`payment-service`** — a richer example (~15 nodes) that exercises most features at
once: five FastAPI services behind the lb, two Postgres databases with
model-bank-backed schemas, two Kafka clusters fed by a **CDC worker** off the ledger
database, a Kafka **consumer function**, two external services, two clients with
multi-step functions, and a saved **end-to-end test process**.

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

Create a **new system** from scratch — it starts immediately:

```bash
./create_new.sh my-system     # fresh minimal systems/my-system/, then ./start.sh my-system
```

The id must be lowercase letters/digits/hyphens. It generates a **fresh minimal
system** — an nginx LB → one generic `service-1` + Prometheus, no databases or edges —
copying the service files (`app.py`, `requirements.txt`, `Dockerfile`) from the same
`frontend/server/templates/service/` template the UI's "Add service" uses (it does
**not** clone `hello-lb`), then brings it up. Grow it from the browser, or edit
`systems/my-system/` by hand.

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

## What you can do from the header

The header shows the system name/id and a row of top-level actions. Left to right:

| Button | What it opens / does |
| --- | --- |
| **Drag** | Toggle drag mode — reposition nodes and move/resize the system-boundary box; the layout is saved to the manifest (`POST /api/layout`), no rebuild. |
| **🔁 End-to-End** | End-to-end test processes — define + run seed→drive→probe processes with a PASS/FAIL verdict. |
| **📖 Skills** | Browse the Claude Code skills a launched session can use (served live from `.claude/skills/`). |
| **＋ Add service** | Add a generic FastAPI service — or a **custom service type** (e.g. Download Coordinator). |
| **＋ Add external service** | Add a third-party dependency, drawn outside the system boundary. |
| **＋ Add client** | Add a multi-step caller (no container), drawn left of the boundary. |
| **＋ Add database** | Provision Postgres / MongoDB / Redis / MinIO (+ exporter). |
| **＋ Add event stream** | Provision a single-broker Kafka cluster (+ exporter). |
| **＋ Add WebSockets** | Provision a whole websocket tier: haproxy L4 lb + N `ws` relay servers + redis pub/sub bus + redis presence cache + a host-run client pool. |
| **＋ gRPC contract** | Open the gRPC contract bank (define / upload `.proto` contracts). |
| **＋ Models** | Open the model bank (reusable TypeScript interfaces). |
| **🗒 Queue** | Show the edit queue — pending Claude sessions run one at a time. |
| **Edit with Claude ▸** | Toggle the embedded Claude Code terminal. |

Per-node actions live on each node's **Edit** panel (opened from the diagram): a
service has Endpoints / gRPC / Shutdown / Delete; a database adds Seed / CDC / Add
read replica / Schema; a Kafka cluster has Topics / Consumers; a client has Functions;
and so on — each is covered in its section below.

**The edit queue.** Most feature actions that need judgment (author an endpoint, a
Kafka consumer loop, a client function, a CDC worker, …) launch a Claude Code session.
To keep concurrent sessions from clobbering each other's rebuilds, each is **enqueued**
and they run **one at a time** in the single embedded terminal; the **🗒 Queue** button
shows how many are pending. "Resume" / "show this session" actions bypass the queue.

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

## Seeding a database with fixture data

A **Postgres or MongoDB** node's Edit panel has a **Seed** tab that fills the database
with durable fixture rows that **survive rebuilds**. Pick a table/collection, fill in
the fields (the form is driven by the database's *live* introspected schema, so a blank
field falls back to the DB default), and **Add entry**. Backend: `frontend/server/dbseed.js`.

Each add does two things: it applies the row **live** to the running container (so an
FK/constraint violation surfaces immediately) and appends it to a source-of-truth
`systems/<id>/<db>/seeds.json`, from which an **idempotent** `seed.sql` (Postgres) or
`seed.js` (Mongo) is regenerated and mounted into the container's init directory
**after** the schema script. So a from-scratch rebuild (`down -v` + up) runs the schema
and then replays the seed data. A **Re-seed now** link re-applies everything after a
test wipe. No Claude session and no image rebuild — it's mechanical and live. Read
replicas and non-field stores (Redis / MinIO) are excluded.

Routes: `GET /api/db-seed`, `POST /api/db-seed` (add), `POST /api/db-seed-remove`,
`POST /api/db-seed-apply` (re-apply all).

---

## Change data capture (CDC → Kafka)

A **Postgres or MongoDB** node's Edit panel has a **CDC** tab that streams a table's row
changes to a Kafka topic. Add a rule — a table, which operations to capture
(`INSERT` / `UPDATE` / `DELETE`), and a target event stream + topic — and the change
feed becomes **real**: a per-database worker container `<db>-cdc` tails Postgres logical
replication / Mongo change streams and produces to the broker. Backend:
`frontend/server/cdc.js`; the worker code is authored by the `sandbox-database-cdc` skill.

The first rule does the heavy lifting: it enables the engine's capture mode (Postgres
`wal_level=logical`; a single-node Mongo replica set), scaffolds the worker (a
`type:"cdc"`, `cdcOf:"<db>"` manifest node + `<db>` → `<db>-cdc` → topic edges + a scrape
job), registers the worker as a producer on the topic, and launches a Claude session
that writes `systems/<id>/<db>-cdc/{Dockerfile,app.py,requirements.txt}` and builds it.
Later rule edits are pure JSON — they rewrite `systems/<id>/<db>/cdc.json` and `restart`
the worker (which re-reads the mounted rules). Removing the last rule tears the worker
down. The worker exports `cdc_events_captured_total` / `cdc_events_produced_total` /
`cdc_errors_total`, shown on its node.

Routes: `GET /api/db-cdc`, `POST /api/db-cdc` (add/update a rule), `POST /api/db-cdc-remove`.

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
routine the client runs against the load balancer. Each function is **owned by that one
client** — there is no shared bank and no attach-by-name. External services have no
functions (an external service still calls into the system through its own endpoints'
`downstream`). Define one with a name + argument signature + a plain-English description;
that launches a Claude session (the `sandbox-client-scenario` skill) which authors the
function as **real Python** — a `def <name>(...)` in `systems/<id>/clients/<module>.py`
that calls LB endpoints through a shared `lb` helper with real control flow (if/else,
loops) and chains one call's response into the next. The registry entry (name, args,
description, history) lives in `systems/<id>/scenarios.json`; the call **steps** drawn on
the diagram are **statically re-inferred** from that Python on every read (a scan of
`lb.<method>("/path")` literals), not hand-authored. **Run** executes the function **for
real** — `POST /api/scenarios/run` spawns `python3 clients/<module>.py --<name> <args>`
against the load balancer (`localhost:8080`) and shows the per-step status + response. Each
of the client's functions also appears as a clickable `ƒ` row on its diagram node, which
traces the whole call path. Backends: `frontend/server/clients.js` (the node),
`scenarios.js` (registry + runner), and the `clientScript.js` helper (the on-disk Python +
step inference).

---

## gRPC contracts between services

The header's **"＋ gRPC contract"** button opens the contract **bank**, and each
**service** node's Edit panel has a **gRPC** tab for attaching. A gRPC *contract* is a
`.proto` service authored once and kept in the per-system bank under `systems/<id>/grpc/`
(its `_registry.json` records every method + provenance). You define a contract's RPC
methods + message types in the bank — or upload a complete `.proto`, validated by the
real `protoc` — then from a service's gRPC tab **attach** it as a **server** (imports the
shared servicer + runs a gRPC server) and/or **client** (a stub pointed at editable
targets). Roles live on the service nodes' manifest `grpc` block, so the diagram draws
gRPC edges as soon as you attach.

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
+ a `kafka-exporter`, a Prometheus scrape job, a manifest node, and a per-cluster
`streams.json` **topic registry**. Kafka speaks a binary protocol, so there's no
nginx route. Producers and consumers aren't something the broker tracks, so they
live in the registry — and that's what the diagram's producer→cluster→consumer
edges are drawn from. The topic view merges the registry with the broker's **live**
topic list. Backend: `frontend/server/eventstreams.js`.

The cluster node's Edit panel has two tabs:

- **Topics** — the topic list. A topic can carry a **message schema** by referencing a
  [model-bank](#models-bank-reusable-typescript-types) type (`schemaModel`); flip
  **enforce** on (`enforceSchema`) and a launched session adds runtime validation to the
  producing/consuming services. Producers are declared here as service ids.
- **Consumers** — **consumer functions**: a named, per-service background poll loop
  (identity `(service, name)`, stored in the system-level `consumers.json`). Adding one
  is a live registry write (entry + consumer group + a `service → cluster` edge); the
  actual `KafkaConsumer` loop is authored **into that service's `app.py`** as a daemon
  thread by a launched Claude session (the `sandbox-event-stream` skill), which sets
  `implemented: true`. Description-only edits are registry-only; changing the topic/poll
  rate re-launches the session to rewrite the loop. Backend: `frontend/server/consumers.js`.

A cluster also has a **pause** toggle (`consumersPaused`) that consumer loops read live
(no rebuild) so you can freeze consumption and watch lag build. Routes:
`GET/POST /api/consumers`, `PUT /api/consumers` (rename), `DELETE /api/consumers`;
`GET /api/event-stream`, `POST /api/event-stream` (topic schema / pause),
`POST /api/event-streams` (create cluster).

---

## etcd (service discovery on a real Raft cluster)

Click **"＋ Add etcd"** to provision a real **N-member etcd cluster** (N odd — 3/5/7 —
one container per member, static bootstrap, no host ports, scraped natively by
Prometheus). Only **one** etcd setup may exist per system: the menu item hides while a
cluster is on the diagram and the backend 409s a second create. The create modal derives
the Raft math live — **quorum = ⌊N/2⌋+1, tolerates ⌊N/2⌋ failures** — and takes the two
Raft timing knobs (**heartbeat interval**, **election timeout**, validated ≥ 5×heartbeat)
plus the **lease TTL**. Backend: `frontend/server/etcd.js`.

The cluster renders as ONE diagram node: quorum-aware health (**red below quorum**,
yellow degraded, green full), a per-member **dot strip** (leader ringed — kill the leader
and watch the ring move), the derived quorum caption, and one clickable **KEY row per
keyspace** — clicking it traces the discovery flow: registrant service **→ etcd**
(lease-put keepalive) and **etcd →** each listener (watch push). No permanent edges;
the arrows exist only while selected.

Its Edit panel has two tabs:

- **Cluster** — size / heartbeat / election / TTL with the derived quorum line. A
  **TTL-only** save is a pure `etcd.json` write applied **live** (registration loops
  re-read the mounted file by mtime — no rebuild); changing size or a Raft knob
  **recreates the cluster** (fresh bootstrap token; leased registrations re-establish
  themselves on reconnect). Below that, a per-member list with live health / leader
  status and **Stop/Start** buttons — stop ⌈N/2⌉ members to lose quorum and watch
  writes fail + the node turn red, then start one back and watch everything self-heal.
- **Keyspaces** — **register a service** (creates `/services/<service>/`; each of its
  workers — or every instance of a load-balanced service — keeps a **leased key** alive
  there with value `host:port`) and add **listeners** per keyspace (a real etcd
  `watch_prefix` — updates are **pushed**, never polled). The registry entry + compose
  env/mount (`ETCD_WORKER_ID`, `ETCD_ENDPOINTS`, `etcd.json:ro`) are written
  mechanically; the lease-keepalive / watch loops are authored into the service's
  `app.py` by a launched Claude session (the `sandbox-etcd` skill), which flips
  `implemented: true`. The tab lists each keyspace's **live workers** straight from the
  cluster (kill a worker and its key vanishes within one TTL) and each listener exposes
  `GET /<listener>/discovery/<service>` with its live view.

Deleting is guarded like everything else: the cluster can't be deleted while keyspaces
exist, and a service can't be deleted while others watch its keyspace. Routes:
`POST/GET/PUT /api/etcd`, `POST/DELETE /api/etcd/keyspace`, `POST/DELETE
/api/etcd/listener`, `POST /api/etcd/member` (the quorum demo).

---

## WebSockets (an L4-balanced real-time tier)

Click **"＋ Add WebSockets"** to provision a complete websocket tier in one mechanical
POST (`POST /api/websockets` — no launched session; the server code is a deterministic
template). For a tier named `ws` you get, in one shot:

- **`ws-lb`** — an **haproxy L4 (`mode tcp`) load balancer** publishing host port
  **8090**, with a selectable algorithm (**least connections** default, round robin,
  source hash). Its native Prometheus exporter (`:8405`, scraped over the docker
  network) gives the diagram total sessions, conns/s, servers-up — and **per-server
  live session counts**, so you can watch leastconn balance in real time.
- **`ws-server-1..N`** (count picked in the modal, default 2) — **Node.js `ws` relay
  servers** (template: `frontend/server/templates/websocket/server/`). Each keeps a
  local `clientId → connection` map, routes frames by recipient — local delivery, or a
  hop over the **pub/sub bus** to the recipient's server (`server:<id>` channels) —
  heartbeats clients every 30s, and self-instruments ws-native metrics with
  `prom-client` (`ws_connections`, msgs in/s, local/s vs remote/s deliveries, drops,
  delivery-latency histogram).
- **`ws-bus`** + **`ws-presence`** — two redis nodes (+ `redis_exporter` sidecars):
  the cross-server pub/sub bus, and the presence cache mapping
  `presence:<clientId> → serverId` (TTL 60s, refreshed on heartbeat pongs). Both
  selectors in the modal are dropdowns locked to **redis** today, ready to grow.
- **`ws-client`** — a container-less `client` node whose behavior is a **host-run pool
  script** `systems/<id>/ws-clients/ws-client.mjs` (node ≥ 22, zero npm deps — uses the
  built-in `WebSocket`, mirroring the stdlib-only `lbclient.py` convention). It spawns
  `--count N` clients that message random peers at `--rate` msgs/s for `--duration`
  seconds, dedupe by `msgId`, and print one `__WS_RESULTS__ {spawned, connected, sent,
  delivered, duplicates, errors, latencyMs}` line — also runnable via
  `POST /api/websockets/run { system, client, count, durationSeconds, rate }`.

The tier registry `systems/<id>/<lb>/websockets.json` (algorithm + server list + ports)
is the durable source `haproxy.cfg` is rendered from. Tier membership lives on the
manifest nodes (`wsTier`/`wsRole`, origin `create-websockets`): deleting the **lb
cascades the whole tier**; a single server is individually deletable (the cfg is
regenerated + the lb restarted); the two redis nodes are delete-blocked while servers
depend on them. One tier per system today. In **end-to-end processes**, `websocket_list`
rows (`{ client, clientCount, messagesPerSecond }`) make the **number of clients to
spawn** a per-process test variable. Backend: `frontend/server/websockets.js`; skill:
`sandbox-websocket`.

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

## End-to-end test processes (the "End-to-End" button)

Click **"🔁 End-to-End"** to define and run **end-to-end test processes** — the sandbox's
way of asking "does this system actually hold together under realistic use?" A process
bundles three things (`systems/<id>/endtoend.json`, backend `frontend/server/endtoend.js`):

- a **client list** — which client functions to drive, each at its own interval,
- a **failure list** — bad states that must never occur in a valid world (a double charge,
  an orphaned payment, ledger debits ≠ credits, …): the system's own **design defects**,
- a **constraint list** — rules of the valid world the test must uphold (seed the
  preconditions it assumes, use only legal inputs).

Defining a process is pure data entry (no rebuild). **Running** one launches a Claude
session (the `sandbox-end-to-end-process` skill) that does the real work: it **seeds** the
out-of-scope preconditions the constraints imply straight into the datastores, synthesizes
**legal arguments**, drives each client function through the load balancer at its rate for
the chosen duration, then **probes** for each failure state (datastore queries, Prometheus,
the call log). It reports an overall **PASS** — or **FAIL** if any failure state is
observed — and persists a full run report to `systems/<id>/endtoend-runs/`, whose verdict
shows as a badge on the process row.

Start/stop is coordinated by an in-memory run flag (`POST /api/endtoend/start|stop`) that
the session polls, so **Stop** (or starting a newer run) ends the current one early; a
generous backstop timer clears a stuck flag if the session dies. Nothing here rebuilds a
container — the process only seeds data and drives the already-running system.

---

## Smoke test — prove the pipeline end to end

The pipeline is: **service exposes metrics → Prometheus pulls them → frontend
queries Prometheus**. If all four checks below agree, it's proven.

**1. Generate load** through the load balancer (leave it running in a terminal):

```bash
while true; do curl -s localhost:8080/service-1/health; done
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
  optional `health { query, rules[] }`. `type` is `load_balancer | service |
  external_service | client | postgres | mongodb | redis | object-store | kafka |
  cdc` (or a custom service type). Nodes also carry keys the scaffolding writes:
  `origin` (which flow created it), `external:true` (clients + external services,
  drawn outside the boundary), `schemaModels[]` (model-bank names backing a DB's
  schema), and the ownership links `replicaOf` / `cdcOf` + `grpc` / `resilience` blocks.
- `health.rules[]` — `{ color, when }`; `when` is a tiny safe expression of the
  form `value <op> number` (e.g. `value < 1`). First matching rule wins; no
  value yet → gray. The seed system colors the `service-1` node off
  `up{job="service-1"}`.
- `edges[]` — `{ from, to }` node ids, drawn as lines between box centers; an
  optional `origin` (e.g. `consumer-fn`) tags special edges.
- `boundary { x, y, w, h }` — the dotted system-boundary rectangle; drag mode saves
  it (and node positions) via `POST /api/layout` — pure render state, no rebuild.

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
built yet: optional Grafana dashboards, latency injection
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
