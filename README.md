# Distributed Systems Sandbox

Learn distributed-systems behavior **by running it**: grow small but real systems from a
browser, throw load at them, and watch real metrics move on a live diagram.

There is **no mock layer**. Every node on the diagram is a real Docker container — a real
FastAPI service, a real Postgres, a real Kafka broker, a real etcd Raft cluster — reached
through a real nginx load balancer and scraped by a real Prometheus. When you kill a node,
it actually dies; when a breaker trips, it tripped for real.

You grow the system **entirely from the browser**. The frontend is a generic renderer of
the system's `manifest.json`, so none of the components below required a frontend edit to
exist — each one slots into the same contract (a manifest node + a compose service +
a scrape job).

## What you can build

**Compute**

| Component | What actually runs |
| --- | --- |
| **Service** | A generic FastAPI container with hand-written Prometheus metrics. Its HTTP **endpoints** are authored by Claude into `app.py`. |
| **Per-service load balancer** | Any service can be scaled to N instances behind its own **haproxy sidecar** — transparently, with no caller changes. |
| **External service** | A third-party dependency you don't own — a real container (so calls work), but drawn outside the system boundary and left out of your observability surface. |
| **Client** | A caller outside the system that runs real multi-step **functions** (Python) through the load balancer. Optionally **stateful** (its calls persist across runs). |
| **Custom service types** | Typed services that scaffold their own containers, Edit tabs, and diagram rendering: the peer-to-peer **Download Coordinator**, the **LLM Worker** (simulated inference with continuous batching), its **persistence readers**, and **Kafka consumer groups**. |

**Data**

| Component | What actually runs |
| --- | --- |
| **Postgres / MongoDB / Cassandra / DynamoDB** | Real datastores + exporters, with schemas driven from the model bank, durable **seed data**, **read replicas**, and **CDC** (change-data-capture into Kafka). |
| **Redis** | A cache with typed **key namespaces**, and a **topology** you can reshape live: standalone → replicated (real Sentinel failover) → sharded (a real Redis Cluster). |
| **Blob store** | MinIO, S3-compatible, with buckets. |

**Messaging & coordination**

| Component | What actually runs |
| --- | --- |
| **Event stream (Kafka)** | A real KRaft broker + exporter, with topics, model-backed message schemas, producers, and per-service **consumer functions** — plus **consumer groups** that autoscale on lag. |
| **gRPC contracts** | Real `.proto` contracts (compiled by real `protoc`) served by one owning service and dialed by others. |
| **etcd** | A real N-member Raft cluster for **service discovery** (leased keys + pushed watches) and **config**. Kill members and watch quorum break. |
| **WebSockets** | A whole real-time tier: an haproxy **L4** load balancer, N Node.js relay servers, a redis pub/sub bus, a redis presence cache, and a host-run client pool. |

**Cross-cutting**

| Component | What actually runs |
| --- | --- |
| **Prometheus** | Add it as a node to turn observability on; remove it and the diagram goes dark. |
| **Resilience policies** | Circuit-breaker + retry on any connection, with a live CLOSED → OPEN → HALF-OPEN state machine. |
| **Connection pools** | A real, sized pool on an outbound call (psycopg_pool / MongoClient / httpx limits), with live `active/max/idle` on the wire. |
| **Autoscaling** | A scaler sidecar computes a desired replica count from lag or batch utilization; the control plane really adds and removes containers. |
| **Outages** | Stop any node for N seconds and watch failure propagate. |
| **End-to-end processes** | Seed → drive clients → probe for design defects → PASS/FAIL. |

Anything mechanical (compose/nginx/Prometheus/manifest edits, rebuilds) is done by the
backend. Anything requiring **judgment** — writing a route, authoring a schema, wiring a
consumer loop — is delegated to a launched **Claude Code** session following a skill.

**The `systems/` folder holds example systems that have been built with the sandbox** —
today `payment-service`, `llm-app`, and `chat-app`. They're worked examples to poke at and
learn from, not part of the machinery; each carries its own `README.md`. You'll normally
create your own from the entry screen at <http://localhost:5173/>.

---

## Folder structure (the extensibility contract)

```
repo-root/
  systems/
    payment-service/           # example systems built with the sandbox
    llm-app/                   #   (each has its own README.md;
    chat-app/                  #    create your own from the entry screen)
    <id>/                      # one self-contained system:
      docker-compose.yml       # runs the whole system (one container per node)
      manifest.json            # topology + per-node PromQL + boundary (the frontend reads this)
      endpoints.json           # per-service endpoint registry (drives the trace)
      models.json              # model bank: reusable TypeScript interfaces
      clients.json             # client roster (multi-step callers)
      scenarios.json           # per-client functions (multi-step call chains, keyed by client)
      consumers.json           # per-service Kafka consumer functions
      etcd.json                # etcd cluster config + keyspace registry (discovery + config)
      persistence.json         # LLM persistence-reader groups
      endtoend.json            # named end-to-end test processes
      endtoend-runs/           # persisted PASS/FAIL run reports
      prometheus/prometheus.yml
      nginx/nginx.conf         # per-service /<id>/ routes (insertion markers)
      grpc/                    # gRPC contract bank: _registry.json + .proto + generated bindings
      service-1/               # a generic FastAPI service + hand-written metrics
        app.py
        requirements.txt
        Dockerfile
      <db>/                    # a datastore: init script, seeds.json + seed.sql|js|cql|sh, cdc.json
      <db>-cdc/                # a CDC worker container (Dockerfile + app.py) if CDC is enabled
      <svc>-lb/haproxy.cfg     # a load-balanced service's haproxy sidecar config (generated)
      <cluster>/streams.json   # per-Kafka-cluster topic ⊕ producer/consumer registry
      <worker>/worker.json     # LLM worker tunables (+ hook.json, scaler.json) — live-mounted
      <ws-lb>/websockets.json  # websocket tier registry (haproxy.cfg is rendered from it)
      ws-shared/hooks.js       # the websocket tier's shared server hooks (mounted, no rebuild)
      clients/                 # per-client Python (module.py) + shared lbclient.py
      ws-clients/              # host-run websocket client pool script (.mjs)
      <node>/data/             # durable bind-mounted state (db data, coordinator chunks, …)
  frontend/                    # generic React app, SHARED across all systems
    src/                       # the generic diagram + modals (renders any manifest)
    src/customTypes/           # frontend rendering for custom service types
    server/                    # Vite dev-server plugins (the backend "control plane")
      templates/service/                # canonical generic service ("Add service" clones this)
      templates/client/                 # generic client Python (clients scaffold from this)
      templates/websocket/              # ws relay server + host client pool templates
      templates/llm-worker/             # LLM worker (+ llm-scaler) templates
      templates/consumer-scaler/        # Kafka consumer-group scaler sidecar
      templates/download-coordinator/   # coordinator + worker templates
      customTypes/                      # backend recipes for custom service types
  settings.json                # app-wide settings (prefix colors, Claude flags) — gitignored
  README.md
```

**Adding a new system later = create a new `systems/<id>/` folder** with its own compose
file + `manifest.json` (the entry screen's **New system** button does exactly this). The
frontend never needs editing — it renders whatever the selected system's manifest describes.
The entry screen at `/` lists every system; a system's page is `/systems/<id>`, and opening
one starts its docker stack (stopping whichever system previously held the shared ports).

---

## Prerequisites

- **Docker + Docker Compose** — every node in a system is a container.
- **Node.js 18+** — to run the React frontend / dev-server backend. (**Node 22+** if you want
  to run the WebSocket client pool, which uses node's built-in `WebSocket`.)
- **Python 3** on the host — client functions are executed for real by spawning
  `python3 systems/<id>/clients/<module>.py`. The scripts are **stdlib-only**, so no
  `pip install` is needed.
- **Claude Code** on your PATH — the browser launches real `claude` sessions to author code.

---

## Quick start (root scripts)

From the repo root:

```bash
./start.sh                   # frontend only — pick (or create) a system at http://localhost:5173/
./start.sh payment-service   # bring that system's docker stack up first, then the frontend
./stop.sh  payment-service   # docker compose down + stop the frontend
```

- `./start.sh <id> --no-frontend` — start only the Docker stack.
- `./stop.sh <id> --keep-frontend` — tear down Docker but leave the frontend up.
- Run `./start.sh --help` to list available systems.

Create a **new system** from the entry screen's **New system** button (system ids are lowercase
letters/digits/hyphens). It scaffolds a **fresh minimal system** — an nginx LB → one generic
`service-1`, scraped by Prometheus, no databases or edges — copying the service files
(`app.py`, `requirements.txt`, `Dockerfile`) from the same `frontend/server/templates/service/`
template the UI's "Add service" uses (it does **not** clone an example system), then opening it
brings it up. Grow it from the browser.

Only one system holds the shared host ports (8080 lb / 9090 prometheus / 8090 websockets) at a
time, so opening a system (or `./start.sh <id>`) automatically stops the previously active
system before starting the new one.

Runtime state (frontend pid/log, active system) lives in `.run/` (gitignored). The sections
below describe the same steps run manually.

## Start the system (manual)

```bash
cd systems/<id>
docker compose up --build
```

A freshly created system starts three containers:

| Service      | Host port | Purpose                                            |
| ------------ | --------- | -------------------------------------------------- |
| `lb` (nginx) | `8080`    | Load balancer; routes `/service-1/…` → `service-1`  |
| `service-1`  | —         | FastAPI app (`/health`, `/metrics`); internal only  |
| `prometheus` | `9090`    | Scrapes `service-1:8000/metrics` directly           |

Everything you add from the browser appends containers to this same compose file. Databases,
exporters, brokers, and etcd members are **not** published to host ports (Prometheus reaches
them over the compose network), so any number of them coexist with no port conflicts.

Restart cleanly any time with `docker compose down` then `docker compose up`. Prometheus data
is intentionally not persisted.

## Start the frontend

In a second terminal:

```bash
cd frontend
npm install        # first time only
npm run dev        # serves on http://localhost:5173
```

Open <http://localhost:5173>. The entry screen lists every system; opening one loads its
manifest at `/systems/<id>` and draws the diagram.

### The frontend IS the backend

There is no separate API server. `frontend/server/*.js` are **Vite dev-server plugins**
(wired in `frontend/vite.config.js`) that expose `/api/*` routes, mutate `systems/<id>/` on
disk, and run `docker compose`. `npm run dev` is the whole control plane.

### How networking / CORS is handled

The browser never talks to Prometheus or a container directly (that causes CORS pain).
Everything is same-origin through the **Vite dev server**:

- **Prometheus queries** — the frontend calls `/api/prometheus/api/v1/query?...`. Vite proxies
  `/api/prometheus/*` → `http://localhost:9090/*` (the prefix is stripped).
- **Manifest + registry serving** — a tiny custom middleware (`serveSystems` in
  `vite.config.js`) serves the repo's `systems/` directory under `/systems/*`.

Because both go through the dev server, the **frontend must be started with `npm run dev`** for
live metrics — a plain static build won't have the proxy.

---

## What you can do from the header

The header shows the system name/id and a row of top-level actions. Left to right:

| Button | What it opens / does |
| --- | --- |
| **✥ Edit** | Toggle drag mode — reposition nodes and move/resize the system-boundary box; the layout is saved to the manifest (`POST /api/layout`), no rebuild. |
| **🔁 End-to-End** | End-to-end test processes — define + run seed→drive→probe processes with a PASS/FAIL verdict. |
| **📖 Skills** | Browse the Claude Code skills a launched session can use (served live from `.claude/skills/`). |
| **⚙ Settings** | App-wide settings — diagram prefix colors and Claude Code permission mode. |
| **＋ Add ▾** | The Add menu (below). |
| **🗒 Queue** | Show the edit queue — pending Claude sessions run one at a time. Shows a count when non-empty. |
| **Edit with Claude ▸** | Toggle the embedded Claude Code terminal. |

The **＋ Add ▾** menu has two groups:

- **Nodes** — `Service` · `External service` · `Client` · `Database` · `Event stream` · `etcd` ·
  `WebSockets` · `Prometheus`
- **Contracts & schemas** — `gRPC contract` · `Models`

`etcd` and `Prometheus` are **singletons**: their menu items disappear once one is on the
diagram (and the backend 409s a second create).

### Per-node Edit tabs

Per-node actions live on each node's **Edit** panel, opened from the diagram. **Shutdown** and
**Delete** are appended to almost everything; the feature tabs depend on the node's kind:

| Node | Tabs |
| --- | --- |
| **Service** (and a load-balanced cluster entry) | Endpoints · gRPC · Calls · Load Balancing · Subscribers *(if the system has etcd)* · [custom-type tabs] |
| **External service** | Endpoints · Calls *(never gRPC — it's a third party)* |
| **Client** | WebSocket *(ws clients only)* · Functions · State *(no Shutdown — a client has no container)* |
| **Database** | Schema *(or Replica)* · CDC · Seed — plus **Keyspaces** and **Topology** on redis |
| **Event stream** | Topics · Consumers |
| **etcd** | Cluster · Keyspaces |
| **Prometheus** | Delete only — no Shutdown; it's shared infra every node's metrics depend on |
| **LLM Worker** | LLM Worker · Scaling · Persistence |
| **Download Coordinator** | Distribution |
| **WebSocket tier** | Edited as one unit (Methods · Shutdown · Delete), not per-server |

**The edit queue.** Most feature actions that need judgment (author an endpoint, a Kafka
consumer loop, a client function, a CDC worker, …) launch a Claude Code session. To keep
concurrent sessions from clobbering each other's rebuilds, each is **enqueued** and they run
**one at a time** in the single embedded terminal; the **🗒 Queue** button shows how many are
pending. "Resume" / "show this session" actions bypass the queue.

---

## Settings

The header's **⚙ Settings** opens app-wide (not per-system) settings, persisted to a
gitignored `settings.json` at the repo root. Backend: `frontend/server/settings.js`
(`GET`/`POST /api/settings`).

- **Prefix colors** — the seven role colors the diagram uses for its row badges and the edges
  they trace (`http`, `function`, `consumer`, `grpc`, `etcdKey`, `redisKey`, `etcdEdge`).
  Applied live via CSS vars; per-row and global reset.
- **Claude Code — "Dangerously skip permissions"** — when on, every launched `claude` session
  gets `--dangerously-skip-permissions`, so it applies edits without stopping to ask. Read
  server-side (never passed from the browser).

---

## Editing a system from the browser (Claude terminal)

Click **"Edit with Claude ▸"** in the header to open a terminal panel running an interactive
**Claude Code** session scoped to the system you're viewing. Ask it to add or change components
and apply the changes — the diagram updates live.

How it works (all inside the one `npm run dev` process):

```
xterm.js (browser) ⟷ WebSocket /term ⟷ Vite plugin ⟶ node-pty ⟶ `claude` (TUI)
```

- The plugin `frontend/server/terminal.js` attaches a WebSocket on `/term` to the dev server (it
  claims only that path, leaving Vite's HMR socket alone) and spawns `claude` in a real
  pseudo-terminal — a PTY is required because Claude Code is a full-screen TUI.
- The session is made **aware of its role and the current system** via a generated
  `--append-system-prompt` built from the live `manifest.json` (see `buildSystemPrompt`). Its
  working directory is the repo root, so the project skills in `.claude/skills/` auto-load.
- It runs in **default permission mode** — Claude asks before edits and commands — unless you
  turn on "Dangerously skip permissions" in Settings.
- Manifest-only edits appear in the diagram within seconds (the frontend re-fetches
  `manifest.json` on a timer). Code/compose changes need a rebuild, which Claude runs for you
  with `docker compose -f systems/<id>/docker-compose.yml up -d --build <service>`.

### Mutations are done by launched sessions + skills

The dev-server plugins handle the mechanical scaffold. Anything requiring judgment is delegated
to a spawned session whose prompt is built from the persisted metadata, following a skill in
`.claude/skills/`:

| Task | Skill |
| --- | --- |
| Add/edit/delete an HTTP route on a service | `sandbox-endpoint` |
| Add/update/delete a datastore **or a read replica** | `sandbox-database` |
| Retrofit redis writers/readers after a **Topology** change | `sandbox-redis-topology` |
| Retrofit postgres users after a **Topology** change (failover-safe DSN) | `sandbox-postgres-topology` |
| Build a database's CDC worker | `sandbox-database-cdc` |
| Kafka clusters, topics, and per-service **consumer functions** | `sandbox-event-stream` |
| Wire **etcd** discovery/config (leased keys + watch listeners) | `sandbox-etcd` |
| Author a **client function** | `sandbox-client-scenario` |
| Propagate a gRPC contract shape change | `sandbox-grpc-contract` |
| Serve a contract, or wire a caller | `sandbox-grpc-attach` |
| Circuit-breaker + retry on a connection | `sandbox-resilience` |
| A real **connection pool** on a connection | `sandbox-connection-pool` |
| Work on a **load-balanced** service | `sandbox-service-lb` |
| Implement a **WebSocket** shared hook | `sandbox-websocket` |
| Register a new **custom service type** | `sandbox-custom-service-type` |
| The **Download Coordinator** | `sandbox-download-coordinator` |
| The **LLM Worker** (cache-evict hook, tunables) | `sandbox-llm-worker` |
| An **LLM persistence reader** group | `sandbox-llm-persistence` |
| **Run** an end-to-end test process | `sandbox-end-to-end-process` |

`frontend/server/skills.js` serves these at `GET /api/skills` (the **📖 Skills** viewer);
adding a `SKILL.md` makes it available with no code change.

**Notes**

- `node-pty` is a native module; `npm install` uses prebuilt binaries (no compiler needed). On
  macOS the prebuilt `spawn-helper` ships without its execute bit, so a `postinstall` script
  restores it — if you ever see `posix_spawnp failed`, run
  `chmod +x frontend/node_modules/node-pty/prebuilds/darwin-*/spawn-helper`.
- **Security:** the dev server lets the browser drive real `docker` and a real `claude` PTY on
  your machine. It binds localhost by default — keep it that way and don't expose the dev port.
  Backend plugins invoke docker with `execFile` + arg arrays (never shell strings) and validate
  every `?system=` / node name against strict whitelists.

---

## Adding a service from the browser

**＋ Add ▸ Service** adds a service to the system you're viewing. Give it a name, pick a **type**
(Generic service, or a [custom service type](#custom-service-types)), and submit.

> **Node names are permanent ids.** A name doubles as its compose service name, nginx route,
> on-disk folder, Prometheus job, and manifest node id — so it must be lowercase letters, digits
> and hyphens (start with a letter), contain **no spaces**, be **unique** within the system, and
> **can't be changed** after creation. The create forms validate this client-side
> (`frontend/src/nodeName.js`) and the server rejects anything invalid (`NAME_RE` in
> `frontend/server/scaffold.js`); there is no rename — delete and re-add.

For a generic service the plugin (`frontend/server/services.js`):

1. clones the canonical service template (`frontend/server/templates/service/` — a
   hand-instrumented FastAPI app with `/health` and `/metrics`) into `systems/<id>/<name>/`,
2. adds a `build: ./<name>` service to `docker-compose.yml`, an nginx `/<name>/` route (upstream +
   location, at the markers in `nginx.conf`), and a scrape job to `prometheus/prometheus.yml`,
3. adds a `service` node to `manifest.json` (metrics scoped to the new scrape job), then
4. `docker compose build <name>` + `up -d`, an `nginx -s reload`, and a Prometheus restart.

The node appears on the diagram and goes green once scraped.

### Load-balancer endpoints

The LB node lists every service's **live, routable** endpoints — discovered by reading each
service's FastAPI `/openapi.json` **through the LB** and prefixing the path with the service id
(`frontend/server/endpoints.js`, served at `GET /api/endpoints`). `/health` is shown as
`GET /<service>/health`; `/metrics` is omitted (that's Prometheus's, scraped directly). The list
refreshes on a timer, so endpoints added to a service appear without a reload.

### Tracing an endpoint's lifecycle

**Click an endpoint row** in the LB node to trace its request path:
`client → LB → owning service → the service(s)/db(s) that endpoint calls`. The traced nodes and a
directed (arrowed) edge for each hop are highlighted while everything else dims; click the row
again, or empty canvas, to clear.

The downstream hops come from the per-system **endpoint registry**, `systems/<id>/endpoints.json`
— a map of service id → endpoint records (`{ method, path, protocol, downstream, alias, request,
response, requestModel, responseModel, description, conversationId, history }`).
`GET /api/endpoints` merges this onto the live OpenAPI discovery: it attaches
`protocol`/`downstream` (and the editable fields), surfaces registry endpoints the container isn't
serving yet, and drops any `downstream` id that isn't a real node. A system without an
`endpoints.json` simply traces `LB → service` (no extra hops).

### Authoring an endpoint (Claude-backed)

A service's **Endpoints** tab lists its endpoints (method · path · `alias()` · protocol) and offers
**＋ Add endpoint**: method, path, protocol, a required **function name** (`alias`), a request and
response schema (a JSON `{key: "type"}` map **or** a referenced model), and a natural-language
description.

The request and response each carry a **model dropdown** above the inline schema box. Leave it on
`— inline schema —` to type a `{key: "type"}` map, or pick a model from the
[model bank](#the-model-bank-reusable-typescript-types) to reference a reusable TypeScript type
(stored as `requestModel` / `responseModel`, which supersede the inline map). When a model is
referenced, the seeded Claude prompt inlines that model's TypeScript along with every model it
references, transitively.

The **alias** is a required code-style function name, unique within the service. Editing an
endpoint pre-fills the form with its current spec and shows a read-only **update history** — one
row per save (timestamp, alias, schemas, description), so you can see every spec the endpoint has
been created or updated with. Renaming the route (changing method/path) starts a fresh trail.

On submit the frontend generates a Claude **session id**, persists the endpoint via
`POST /api/endpoints` (so it shows immediately as pending), then enqueues a `claude --session-id
<id>` session seeded with the description plus the structured spec and concrete build steps
(implement the route in `app.py`, set the endpoint's `downstream` so the trace lights up, rebuild
just that service, verify through the LB). Endpoints with a saved `conversationId` show a
**Resume** button that reopens `claude --resume <id>` with full prior context.

### The model bank (reusable TypeScript types)

**＋ Add ▸ Models** opens the **model bank** — a per-system store of reusable model interfaces
authored in **TypeScript**, shared across all of the system's services. Each model has a **name**
(a TypeScript identifier that is its permanent id) and a raw TypeScript **definition** that may
reference other models by name (e.g. an `Order` whose body uses `OrderItem[]`). Stored in
`systems/<id>/models.json`; served by `frontend/server/models.js` at `GET`/`POST`/`DELETE
/api/models`. `POST` upserts by name; `DELETE` is **blocked** (naming the offenders) while any
endpoint still references the model.

Models are how an endpoint's request/response reference a shared type instead of an inline map,
how a **database schema** is authored (a model becomes a table/collection; a model→model reference
becomes an FK), and how a **Kafka topic** declares its message contract. **`//` comments in a
model's TypeScript are authoritative schema directives** (PK/FK/unique/index/length/…) that the
DB-authoring prompt honors. There's no docker rebuild — it's pure JSON that launched sessions read.

### Deleting a node

Each node's Edit panel has a **Delete** tab. `POST /api/delete` (`frontend/server/remove.js`) tears
the component down — the inverse of the create flow: it removes the compose service(s) (a database
also owns its `-exporter`/`-init` sidecars), the nginx route, the scrape job, the manifest node and
edges, and the folder, then `docker compose up -d --remove-orphans` deletes the orphaned
containers. Any `depends_on` references are scrubbed so the compose project stays valid. The LB
itself isn't deletable.

A node **can't be deleted while another node still depends on it**. Before offering the button the
Delete tab probes `GET /api/dependents?system=&id=` and lists every dependent — an endpoint whose
`downstream` calls it (with the exact `METHOD /path`), a gRPC client targeting it, a
producer/consumer on its Kafka topics, a client function step that calls it, or an etcd
registrant/listener — then disables Delete until those calls are removed. `POST /api/delete`
enforces the same guard server-side. Owned children that **cascade** anyway are excluded, so they
don't block their parent: a primary's read replicas (`replicaOf`), a database's CDC worker
(`cdcOf`), a load-balanced service's instances (`instanceOf`), an LLM worker's token-stream redis
and scaler, and every member of a websocket tier.

---

## Per-service load balancing

A service's **Load Balancing** tab runs it as **N real instances behind its own haproxy sidecar** —
transparently, so every existing caller balances with **no code changes**. Pick **instances** (1–8)
and an **algorithm** (`roundrobin` / `leastconn` / `source`) and apply. Backend:
`frontend/server/serviceLb.js` (`GET`/`POST /api/service-lb`); it's fully mechanical (no Claude
session), though it does rebuild.

The trick is that the `<name>` container **becomes** the haproxy sidecar, keeping the network name:

- the service's compose def is cloned into `<name>-1 … <name>-N` (same build, same image),
- `<name>` is swapped to `haproxy:3.0-alpine` on the same `:8000`, with a generated
  `systems/<id>/<name>-lb/haproxy.cfg` (`balance <algorithm>`, `option httpchk GET /health`, one
  `server` line per instance) and haproxy's native Prometheus exporter on `:8405`,
- **nginx is never touched** — `upstream <name> { server <name>:8000; }` and any service-to-service
  `http://<name>:8000` call now resolve to the sidecar. That's the transparency invariant.

On the manifest, `<name>` flips to `type: "service-lb"` and gains `svcLb: { algorithm, instances }`;
each instance is a `type:"service"` node carrying `instanceOf: "<name>"`. The cluster entry
**keeps owning** the service's endpoints, gRPC, and consumers — no registry migration — and a
resilience policy stays a single cluster-level breaker. Instances have no Edit tabs of their own.
Its metrics become haproxy's (sessions, req/s, servers up). Setting instances back to 1 reverses
everything. gRPC-**serving** services can't be load balanced (haproxy `mode http` won't proxy h2).

---

## Adding a database

**＋ Add ▸ Database** provisions a real datastore. Pick a type, name it, and declare its entities —
or, for the schema-bearing engines, select **models** from the bank and let a Claude session author
the schema. Backend: `frontend/server/databases.js`.

| Type | What runs | Entities | Metrics source |
| --- | --- | --- | --- |
| **PostgreSQL** (SQL) | `postgres:16-alpine` + `postgres-exporter` | tables (+ typed columns) | exporter |
| **MongoDB** (NoSQL) | `mongo:7` + `mongodb_exporter` | collections (+ sample fields) | exporter |
| **Redis** (key-value) | `redis:7-alpine` + `redis_exporter` (+ a one-shot seeder) | key namespaces | exporter |
| **Cassandra** (wide-column) | `cassandra:5` (+ init) | tables in a keyspace | custom exporter |
| **DynamoDB** (NoSQL key-value) | `amazon/dynamodb-local` (+ aws-cli init) | tables (`id` HASH key, streams on) | custom exporter |
| **Blob** (simulated S3) | `minio` (+ `mc` init) | buckets | MinIO native `/minio/v2/metrics/cluster` |

On submit the plugin writes the changes into the active system and rebuilds it:

1. an init script under `systems/<id>/<name>/` that creates the entities (`init.sql` /
   `init.js` / `init.cql` / `init.sh` / seeded keys / `mc mb` buckets),
2. the DB service **and** its exporter appended to `docker-compose.yml`,
3. a scrape job appended to `prometheus/prometheus.yml`,
4. a node appended to `manifest.json`, then
5. `docker compose up -d` + a Prometheus restart so the new target is scraped.

**Model-backed schemas.** Postgres, MongoDB, Cassandra and DynamoDB can instead be created from
**model-bank** types: the container comes up empty, the node records `schemaModels`, and a launched
`sandbox-database` session authors the real schema from the TypeScript (a model→model reference
becomes a foreign key; `//` comments are honored as schema directives).

**Live schema.** A database's **Schema** tab reads the **running container**
(`GET /api/db-schema`, `frontend/server/dbschema.js`) — not the init script — so it reflects the
database's actual state after a service or a Claude session alters it. Each engine is introspected
with its own client via `docker compose exec`: Postgres → `information_schema`, MongoDB →
`getCollectionNames` + a sampled document, Cassandra → `system_schema`, DynamoDB → `list-tables` +
a sampled item, Redis → `--scan` grouped by namespace, MinIO → one directory per bucket.

**Note:** a new database is **not** auto-connected to any service (no edge), and no app code is
modified to use it — provisioning and wiring a service to a DB for CRUD are deliberately separate
steps, since a system can have many services.

### Seeding a database with fixture data

The **Seed** tab (Postgres / MongoDB / Cassandra / DynamoDB) fills a database with durable fixture
rows that **survive rebuilds**. Pick a table/collection, fill in the fields (the form is driven by
the *live* introspected schema, so a blank field falls back to the DB default), and **Add entry**.
Backend: `frontend/server/dbseed.js`.

Each add does two things: it applies the row **live** to the running container (so an
FK/constraint violation surfaces immediately) and appends it to a source-of-truth
`systems/<id>/<db>/seeds.json`, from which an **idempotent** artifact (`seed.sql` / `seed.js` /
`seed.cql` / `seed.sh`) is regenerated and mounted so it replays **after** the schema script. So a
from-scratch rebuild (`down -v` + up) runs the schema and then replays the seed data. A **Re-seed
now** link re-applies everything after a test wipe. No Claude session and no image rebuild.

Routes: `GET /api/db-seed`, `POST /api/db-seed`, `POST /api/db-seed-remove`,
`POST /api/db-seed-apply`.

### Change data capture (CDC → Kafka)

The **CDC** tab (Postgres / MongoDB / Cassandra / DynamoDB) streams a table's row changes to a
Kafka topic. Add a rule — a table, which operations to capture (`INSERT` / `UPDATE` / `DELETE`),
and a target event stream + topic — and the change feed becomes **real**: a per-database worker
container `<db>-cdc` tails the engine's change feed and produces to the broker. Backend:
`frontend/server/cdc.js`; the worker code is authored by the `sandbox-database-cdc` skill.

The first rule does the heavy lifting: it enables the engine's capture mode where one is needed
(Postgres `wal_level=logical`; a single-node Mongo replica set — DynamoDB Streams are already on,
and Cassandra is captured by polling), scaffolds the worker (a `type:"cdc"`, `cdcOf:"<db>"`
manifest node + `<db>` → `<db>-cdc` → topic edges + a scrape job), registers the worker as a
producer on the topic, and launches a session that writes
`systems/<id>/<db>-cdc/{Dockerfile,app.py,requirements.txt}` and builds it. Later rule edits are
pure JSON — they rewrite `systems/<id>/<db>/cdc.json` and `restart` the worker (which re-reads the
mounted rules). Removing the last rule tears the worker down. The worker exports
`cdc_events_captured_total` / `cdc_events_produced_total` / `cdc_errors_total`.

Routes: `GET /api/db-cdc`, `POST /api/db-cdc`, `POST /api/db-cdc-remove`.

### Read replicas

A Postgres / MongoDB / Cassandra node's Edit panel offers **Add read replica** — a **real**
streaming, read-only standby (`<primary>-<N>`) with its own exporter + scrape job + manifest node.
It records `replicaOf` / `replication` (`async` | `sync`, sync being Postgres-only) / `readonly`,
so the diagram draws the primary↔secondary arrow and the dotted cluster box. (Cassandra's
"replica" is a **ring peer**, not a read-only standby — it joins the cluster and the keyspace's
replication factor is raised live.) Redis replication is managed by its **Topology** tab instead;
object stores and DynamoDB have no replica concept. Backend: `frontend/server/replicas.js`
(`POST /api/db-replicas`).

---

## Redis: keyspaces and topology

A redis node gets two extra tabs. Both are worth calling out because they're where the sandbox
gets to teach the interesting parts of redis.

### Keyspaces (typed key namespaces)

The **Keyspaces** tab declares the node's **key namespaces** — `{ name, match: prefix|exact,
type: string|list|set|hash|zset|stream|geo }` — and, per keyspace, which services **write** to it
and which **read** from it. That's what the diagram's clickable **KEY** rows trace. Every redis
node has this tab, whichever flow created it (a cache, an LLM worker's token stream, a websocket
bus). The registry lives **on the manifest node** as `keyspaces[]`. Backend:
`frontend/server/redisKeyspaces.js` — **fully live: every action is a manifest edit, no rebuild and
no Claude session.**

**Scan** (`POST /api/redis/scan`) reads the **live container** (a single `SCAN`+`TYPE` Lua pass;
cluster-aware) and reconciles reality against your declarations: it surfaces undeclared namespaces
that actually exist, flags declared-vs-observed **type drift**, source-greps for suggested
writers/readers you can accept or dismiss, and greps writers for a real `WAIT` call.

Per writer, a keyspace can also declare a **write mode**: the default is **async** (fire-and-forget,
never stored), or **`wait`** — `r.wait(numreplicas, timeoutMs)` after each write, i.e. pseudo-
synchronous replication. Changing the params resets `implemented:false` until a session re-wires it.

Routes: `GET /api/redis/keyspaces`, `POST`/`DELETE /api/redis/keyspace`,
`POST /api/redis/keyspace/verify`, `POST /api/redis/keyspace/suggestion`, `POST /api/redis/scan`.

### Topology (standalone → Sentinel → Cluster)

The **Topology** tab reshapes a redis node into one of three **real** topologies. Backend:
`frontend/server/redisTopology.js` (`GET`/`POST /api/redis/topology`) — the container/scrape/manifest
reconciliation is purely mechanical.

- **Standalone** — one container. The default.
- **Replicated (adds Redis Sentinel)** — the primary plus 1–4 real replica nodes (`replicaOf`,
  read-only), fronted by a real **3-node Sentinel** quorum (`<id>-sentinel-1..3`, containers rather
  than diagram nodes). The primary carries a `sentinel: { size, quorum, masterName, members,
  downAfterMs, failoverTimeoutMs }` block. Kill the master and watch Sentinel promote a replica.
- **Sharded (adds Redis Cluster)** — a real Redis Cluster over the 16384 hash slots: 3–5 shards ×
  (1 + 0–2 replicas per shard) member containers `<id>-1..M`, formed by a one-shot cluster-init.
  There is **no bare `<id>` container** — the one manifest node carries
  `redisCluster: { shards, replicasPerShard, members }` and the diagram draws member dots
  (masters ringed).

`sentinel` and `redisCluster` are mutually exclusive. Converting into or out of cluster mode
**recreates the data-bearing containers** (data is cleared and seeds replayed) — the tab warns you
first. Health is quorum-aware, so the node goes red when the cluster actually can't serve.

Because the topology change is mechanical, the **service code doesn't follow automatically**: if any
keyspace has attached writers/readers, applying enqueues a `sandbox-redis-topology` session to
retrofit them — Sentinel-based master discovery for writes, `RedisCluster` clients with MOVED
handling and hash-tag awareness, and any `r.wait(...)` write modes.

---

## Postgres topology (replication · sync commits · failover)

A postgres node has its own **Topology** tab (`POST /api/postgres/topology`,
`frontend/server/postgresTopology.js`) with two shapes:

- **Standalone** — the single container "Add database" created.
- **Replicated** — 1–4 real streaming standbys (`<db>-1..N`, `replicaOf` nodes) plus one
  `<db>-failover` **watcher container** — the postgres answer to Sentinel. It promotes the most
  caught-up standby when the primary dies, repoints the survivors, and keeps
  `synchronous_standby_names` honest. The primary carries a `postgresHa` block.

Each standby is independently **async or synchronous**. Synchronous standbys are enforced as a real
quorum — `synchronous_standby_names = ANY k ("<db>-1", …)` — with a `synchronous_commit` level
(`on` waits for the standby to *flush* WAL; `remote_apply` waits for it to be *replayed*, giving
read-your-writes on that standby). Async means the primary commits without waiting, so a crash can
lose the last transactions; sync means a promoted standby has every committed row, at the cost of a
round-trip per write.

Enabling replication **does not touch the primary** — no recreate, no restart, no data loss: the
`pg_hba` replication line goes in live, and the sync/WAL settings are runtime `ALTER SYSTEM`s.

Three things make the failover actually survivable, and they're the interesting part:

- **Writers follow the new primary with no code change.** Services connect with a *multi-host* libpq
  DSN (`postgresql://…@<db>:5432,<db>-1:5432,<db>-2:5432/…?target_session_attrs=read-write`). libpq
  tries each host and keeps the one that is actually writable — so a promoted standby is found
  automatically. That one string is the whole client contract.
- **A returning old primary is FENCED, not a split brain.** It comes back believing it is still
  primary; the watcher sets `default_transaction_read_only = on`, which makes it answer
  `transaction_read_only = on` — exactly what `target_session_attrs=read-write` skips. Writers keep
  reaching the real primary even though the stale node is up and first in the host list.
- **Rejoin** (a button per fenced member) discards its stale data dir and re-clones it from the live
  primary, making it a healthy standby again.

**Roles are runtime, membership is manifest**: after a failover the live primary is a `<db>-<n>`
container while `<db>` is still the cluster entry. The diagram rings the *live* primary's member dot
using the watcher's `pg_ha_is_primary` series — never the manifest.

Applying enqueues a `sandbox-postgres-topology` session to retrofit the services that use the
database (from their `endpoints.json` / `consumers.json` `downstream`) onto the multi-host DSN.

To see it: kill the primary from its **Shutdown** tab and watch the ring jump to a standby while the
write endpoint keeps working.

---

## Event streams (Kafka)

**＋ Add ▸ Event stream** provisions a real single-broker **Kafka** (KRaft) + a `kafka-exporter`, a
Prometheus scrape job, a manifest node, and a per-cluster `streams.json` **topic registry**. Kafka
speaks a binary protocol, so there's no nginx route. Producers and consumers aren't something the
broker tracks, so they live in the registry — and that's what the diagram's
producer→cluster→consumer edges are drawn from. The topic view merges the registry with the
broker's **live** topic list. Backend: `frontend/server/eventstreams.js`.

The cluster node's Edit panel has two tabs:

- **Topics** — the topic list. A topic can carry a **message schema** by referencing a
  [model-bank](#the-model-bank-reusable-typescript-types) type (`schemaModel`); flip **enforce** on
  (`enforceSchema`) and a launched session adds runtime validation to the producing/consuming
  services. Producers are declared here as service ids.
- **Consumers** — **consumer functions**: a named, per-service background poll loop (identity
  `(service, name)`, stored in the system-level `consumers.json`). Adding one is a live registry
  write (entry + consumer group + a `service → cluster` edge); the actual `KafkaConsumer` loop is
  authored **into that service's `app.py`** as a daemon thread by a launched session (the
  `sandbox-event-stream` skill), which sets `implemented: true`. Description-only edits are
  registry-only; changing the topic/poll rate re-launches the session to rewrite the loop. Backend:
  `frontend/server/consumers.js`.

A cluster also has a **pause** toggle (`consumersPaused`) that consumer loops read live (no rebuild)
so you can freeze consumption and watch lag build.

### Consumer groups (and autoscaling on lag)

From the Consumers tab you can also create a **Kafka Consumer Group** — a consuming service that
runs as **N member containers sharing one group id**, so the broker itself rebalances the topic's
partitions across them. Members are `<name>-2 … <name>-N` alongside the base; there's no load
balancer, because Kafka *is* the load balancer here.

It ships a **scaler sidecar** (`<name>-scaler`) that watches the group's **total lag** on the broker
and computes a desired member count. See [Autoscaling](#autoscaling).

Routes: `GET/POST /api/consumers`, `PUT /api/consumers` (rename), `DELETE /api/consumers`;
`GET /api/event-stream`, `POST /api/event-stream` (topic schema / pause), `POST /api/event-streams`
(create cluster); `POST /api/custom/consumer-group/scale` · `/policy` · `/state`.

---

## gRPC contracts between services

**＋ Add ▸ gRPC contract** opens the contract **bank**, and each service node has a **gRPC** tab for
serving. A gRPC *contract* is pure **shape** — a `.proto` service (RPC methods + message types) kept
in the per-system bank under `systems/<id>/grpc/` (`_registry.json` records every method).

The bank works like the model bank: creating a **new** contract (form method or a complete `.proto`,
validated by the real `protoc`) persists immediately — the backend synthesizes/splices the proto and
generates the `_pb2` bindings itself, no Claude session. Edits to an **existing** contract
(add/edit/delete a method, re-upload, delete the contract) are **staged**, badged, then "Review &
save" shows the affected services and applies the batch in one POST; if any changed contract is
attached, ONE propagation session updates the affected code (`sandbox-grpc-contract` skill).

Behavior lives with the **server**: each contract is served by exactly **one** owning service
(attaching it elsewhere 409s until detached). From a service's gRPC tab you attach an unowned
contract and write a **description per method**; a launched session (`sandbox-grpc-attach`) authors
`systems/<id>/grpc/<Contract>_servicer.py` from those descriptions (blank = an UNIMPLEMENTED stub),
wires the gRPC server on `:50051`, and rebuilds. Later description edits update just those method
bodies in place; detach unwires (blocked while other services still dial it). **Client** wiring is
not edited in the tab — the flows that make a service call a contract (endpoints, consumer functions,
custom types) write the manifest `grpc.clients` block themselves, which is what draws the purple gRPC
edges. External services can't serve or consume contracts. Backend: `frontend/server/grpc.js` +
`grpcProto.js` + `grpcInstall.js`.

---

## etcd (service discovery on a real Raft cluster)

**＋ Add ▸ etcd** provisions a real **N-member etcd cluster** (N odd — 3/5/7 — one container per
member, static bootstrap, no host ports, scraped natively). Only **one** etcd setup may exist per
system. The create modal derives the Raft math live — **quorum = ⌊N/2⌋+1, tolerates ⌊N/2⌋
failures** — and takes the two Raft timing knobs (**heartbeat interval**, **election timeout**,
validated ≥ 5×heartbeat) plus the **lease TTL**. Backend: `frontend/server/etcd.js`.

The cluster renders as ONE diagram node: quorum-aware health (**red below quorum**, yellow degraded,
green full), a per-member **dot strip** (leader ringed — kill the leader and watch the ring move),
the derived quorum caption, and one clickable **KEY row per keyspace** — clicking it traces the flow:
registrant service **→ etcd** (lease-put keepalive) and **etcd →** each listener (watch push).

Its Edit panel has two tabs:

- **Cluster** — size / heartbeat / election / TTL with the derived quorum line. A **TTL-only** save
  is a pure `etcd.json` write applied **live** (registration loops re-read the mounted file by
  mtime — no rebuild); changing size or a Raft knob **recreates the cluster** (leased registrations
  re-establish themselves on reconnect; config values are replayed by the backend). Below that, a
  per-member list with live health / leader status and **Stop/Start** buttons — stop ⌈N/2⌉ members
  to lose quorum and watch writes fail + the node turn red, then start one back and watch it
  self-heal.
- **Keyspaces** — two kinds, both stored in `systems/<id>/etcd.json`:
  - **Discovery** (`/services/<service>/`) — **register a service**, and each of its workers (or
    every instance of a load-balanced service) keeps a **leased key** alive there with value
    `host:port`. The tab lists each keyspace's **live workers** straight from the cluster — kill a
    worker and its key vanishes within one TTL.
  - **Config** (`/config/<name>/`) — generic **persistent key/values** (no lease), edited right in
    the tab and put via etcdctl.

  Either kind takes **listeners**: a real etcd `watch_prefix` loop, so updates are **pushed**, never
  polled. The registry entry + compose env/mount (`ETCD_WORKER_ID`, `ETCD_ENDPOINTS`, `etcd.json:ro`)
  are written mechanically; the lease-keepalive / watch loops are authored into the service's
  `app.py` by a launched session (`sandbox-etcd`), which flips `implemented: true`.

A service's **Subscribers** tab is the mirror image of this, from the service's side: it lists the
keyspaces *that* service watches and lets you subscribe it to another one from a description.

Routes: `POST/GET/PUT /api/etcd`, `POST/DELETE /api/etcd/keyspace`, `POST/DELETE /api/etcd/listener`,
`POST /api/etcd/member` (the quorum demo).

---

## WebSockets (an L4-balanced real-time tier)

**＋ Add ▸ WebSockets** provisions a complete websocket tier in one mechanical POST (no launched
session — the server code is a deterministic template). For a tier named `ws` you get, in one shot:

- **`ws-lb`** — an **haproxy L4 (`mode tcp`) load balancer** publishing host port **8090**, with a
  selectable algorithm (**least connections** default, round robin, source hash). Its native
  Prometheus exporter gives the diagram total sessions, conns/s, servers-up — and **per-server live
  session counts**, so you can watch leastconn balance in real time.
- **`ws-server-1..N`** (1–8, default 2) — **Node.js `ws` relay servers**. Each keeps a local
  `clientId → connection` map, routes frames by recipient (local delivery, or a hop over the pub/sub
  bus to the recipient's server), heartbeats clients every 30s, and self-instruments ws-native
  metrics with `prom-client` (`ws_connections`, msgs in/s, local/s vs remote/s deliveries, drops,
  delivery-latency histogram).
- **`ws-bus`** + **`ws-presence`** — two redis nodes (+ exporters): the cross-server pub/sub bus
  (`server:<id>` channels), and the presence cache mapping `presence:<clientId> → serverId` (TTL 60s,
  refreshed on heartbeat pongs).
- **`ws-client`** — a container-less `client` node whose behavior is a **host-run pool script**
  `systems/<id>/ws-clients/ws-client.mjs` (Node 22+, zero npm deps — it uses the built-in
  `WebSocket`, mirroring the stdlib-only `lbclient.py` convention). Its two built-in methods:
  `send(count, durationSeconds, rate)` spawns N clients that message random peers through the L4 lb,
  and `onReceive(message)` dedupes by `msgId` and measures latency. A run prints one
  `__WS_RESULTS__ {spawned, connected, sent, delivered, duplicates, errors, latencyMs}` line.

**Shared methods.** The relay servers' base routing is fixed, but the tier has one shared hooks file
— `systems/<id>/ws-shared/hooks.js`, bind-mounted read-only into every server — with two hooks you
can extend from a description: **`onMessage(msg, ctx)`** (after a client frame is received and
routed) and **`onSend(clientId, payload, ctx)`** (when a payload is delivered to a locally-connected
client). Saving a description launches a `sandbox-websocket` session that authors the hook and
**restarts** the servers (no image rebuild).

The tier registry `systems/<id>/<lb>/websockets.json` (algorithm + server list + ports) is the
durable source `haproxy.cfg` is rendered from. Tier membership lives on the manifest nodes
(`wsTier` / `wsRole`): the tier is one deletion unit (deleting the lb cascades all of it), and it's
edited as one unit too. One tier per system today. Backend: `frontend/server/websockets.js`;
routes `GET`/`POST /api/websockets`, `POST /api/websockets/methods`, `POST /api/websockets/run`.

---

## Custom service types

**＋ Add ▸ Service** offers a **type** selector. Beyond "Generic service", a *custom service type*
scaffolds real, type-specific container(s) and plugs its own Edit tabs + diagram rendering into the
same modal/diagram everything else uses — **without forking** any generic layer.

A type is exactly **two registry entries**: a backend recipe
(`frontend/server/customTypes/<type>.js` — an `onAdd` that composes the shared `scaffold.js`
primitives, plus its own `/api/custom/<type>/*` routes) and a frontend renderer
(`frontend/src/customTypes/<type>/` — its Edit tabs, its diagram body, its runtime poll). Adding the
next one touches neither the modal, the diagram, nor the manifest core. Nodes carry
`service_type: "<type>"` as the discriminator. See the `sandbox-custom-service-type` skill.

### Download Coordinator

A peer-to-peer "distribute a large file to many nodes" system. Adding it creates one coordinator
container (origin seed + orchestrator) that ships two gRPC contracts (ChunkTransfer + Coordination)
into the bank. From its **Distribution** tab you **Add node** (spawning real worker containers), pick
a source (a URL or a pre-staged local file), choose a chunk size, and **Run distribution**. The
coordinator chunks the file and hands out assignments; the moment a worker holds chunk N it becomes a
valid source for N, so distribution shifts from a **star** (everyone pulls from the coordinator) to a
**mesh**. Each node persists its chunk bitmap to disk, so a restarted worker resumes from on-disk
state with no re-fetch. The diagram renders a per-node bitmap grid + aggregate %, and live
chunk-source edges show the star→mesh shift. Skill: `sandbox-download-coordinator`.

### LLM Worker

A **simulated inference server** — a real container running continuous batching over a toy numpy
transformer (the batching engine is genuine; the "model" is toy math). Adding one creates three
nodes: the worker, its linked **token-stream redis** (plus that redis's exporter container), and a
**scaler** sidecar.

It serves a gRPC `Worker` contract (`AddPrompt`, `GetStatus`). A worker loop prefills each admitted
sequence's KV cache, then takes one decode step per sequence per tick, streaming every token into
redis:

- `runs:started` — one entry per **accepted** prompt (the work queue).
- `tokens:<run_id>` — that run's typed token stream: `{type: "token"|"done"|"error", text}`, capped
  and expiring ~600s after it finishes.

Its **LLM Worker** tab exposes live tunables (`systems/<id>/<worker>/worker.json`, mtime-polled — no
rebuild): `ttl_seconds` (the prefix cache's TTL; 0 disables caching), `chat_db` (a postgres node to
pull prior chat messages from on a cache miss), and `max_active` (batch capacity — `AddPrompt` rejects
with "worker full" beyond it). An **`on_cache_evict(entry)` hook** — an *eviction-policy* hook, not a
completion hook — is authored from a description into `hooks.py` and applied by a restart. Skill:
`sandbox-llm-worker`.

**Persistence readers.** Without a reader, a generation is ephemeral — the token stream just expires.
The worker's **Persistence** tab creates a **reader group**: N containers forming one redis
`XREADGROUP` consumer group over `runs:started`, so redis divides announcements across members (one
reader per run). The claiming member accumulates that run's `tokens:<run_id>`, persists the finished
output to a database table/field, and XACKs **only after persisting** (crash-safe). Registry:
`systems/<id>/persistence.json`. Skill: `sandbox-llm-persistence`.

---

## Autoscaling

Worker-group types (**Kafka consumer groups** and **LLM workers**) have a **Scaling** tab: set the
member count manually, or tick **Autoscale** and define a policy.

The split is deliberate. Each type ships a real **scaler sidecar container** (`<base>-scaler`) that
only **computes** a desired member count and exposes it at `GET /state` — it has no docker
privileges. The control-plane half (`frontend/server/autoscale.js`) polls every scaler through the LB
every 10s and, when the desired count differs from the live one, applies it through the **same**
reconciler the manual Scaling tab uses (`replicaGroup.js`) — so autoscaling **really adds and removes
containers**, guarded by a per-group in-flight flag, a 60s cooldown, and a manifest re-read right
before applying.

| | Kafka consumer group | LLM worker |
| --- | --- | --- |
| Signal | **total consumer lag** (end offset − committed, over all partitions) | **batch utilization** (active sequences / `max_active`, over reachable workers) |
| Thresholds | `scale_up_lag` / `scale_down_lag` | `scale_up_util` / `scale_down_util` |
| Common knobs | `enabled`, `min`, `max` (≤8), `up_stable_seconds`, `down_stable_seconds`, `cooldown_seconds` | same |

The rule is the same for both: above the up-threshold continuously for `up_stable_seconds` → add a
member; below the down-threshold for `down_stable_seconds` → remove one; the cooldown gates
consecutive steps. Scale-up is suppressed while a cluster's consumers are **paused** (lag grows by
design then), and capped by the topic's partition count. The policy lives in
`systems/<id>/<base>/scaler.json`, which the sidecar **mtime-polls** — so **policy edits apply live**;
only the resulting member change rebuilds.

Members of a worker group are `<base>-2 … <base>-N` (the base is ordinal 1), each with its own scrape
job and an nginx route for control-plane polling — but **no load balancer**, because the data plane is
gRPC (LLM) or Kafka partitions (consumer group), not HTTP.

---

## External services (outside the system)

**＋ Add ▸ External service** adds a third-party dependency your services call out to — a payment
gateway, an email provider, anything you don't run. It's a real FastAPI container (so the calls
actually work), but the plugin (`frontend/server/externalServices.js`) deliberately treats it as
living *outside* your system:

- It's drawn **outside the system-boundary box** (a dashed, neutral node), and the boundary appears
  around your in-system nodes as soon as the first external service exists.
- It is **not scraped by Prometheus** and has **no health check**, so it never colors green/red —
  it's not part of your observability surface.
- It carries `type: "external_service"`, so it **can't serve or consume gRPC contracts**. It *can*
  be the **target of a circuit breaker** — a resilience policy only requires the *caller* to be one
  of your services.
- It still gets an nginx `/<name>/` route (that's how endpoints are discovered and reached), but its
  endpoints are kept **off the load balancer's advertised surface** — they belong to the third party.

Its Edit panel has **Endpoints** (define the third party's API — same flow as a service, no gRPC tab),
**Calls** (read-only — trace one of its methods on the diagram), **Shutdown**, and **Delete**. To
model your service calling out: add an endpoint to one of your services whose `downstream` includes
the external node, then click that connection to attach a circuit breaker. **The reverse also
works** — an external service's *own* endpoint can have a `downstream` **back into the system** (a
webhook/callback), which the diagram traces exactly like any other call. What external services don't
have is **Functions** — that's client-only.

---

## Clients (multi-step callers)

**＋ Add ▸ Client** adds a *caller* that lives outside the system — drawn to the **left** of the
boundary (external services are on the right), giving the diagram a left-to-right story:
**clients → [system] → external services**. A client has **no container**: it's a manifest node plus
its **functions**, so adding one is instant (no docker rebuild). It connects to the load balancer
with a faint always-on line; selecting a public endpoint on the LB extends the lifecycle trace back
to it.

**Functions.** A *function* is a named, argument-taking routine the client runs against the load
balancer, **owned by that one client** (there is no shared bank). Define one with a name + argument
signature + a plain-English description; that launches a session (`sandbox-client-scenario`) which
authors it as **real Python** — a `def <name>(...)` in `systems/<id>/clients/<module>.py` that calls
LB endpoints through a shared stdlib-only `lb` helper with real control flow (if/else, loops) and
chains one call's response into the next. The registry entry lives in `systems/<id>/scenarios.json`;
the call **steps** drawn on the diagram are **statically re-inferred** from that Python on every read
(a scan of `lb.<method>("/path")` literals), not hand-authored.

**Run** executes the function **for real** — `POST /api/scenarios/run` spawns
`python3 clients/<module>.py --<name> <args>` against the load balancer and shows the per-step status
+ response. Each function also appears as a clickable `ƒ` row on the client's diagram node, tracing
the whole call path.

**State.** The **State** tab makes a client **stateful**: instead of each run being an independent
fire-and-forget subprocess, its values and call history persist across runs in a durable per-client
store (`systems/<id>/clients/<module>.state.json`), which the tab shows live and can clear. The flag
is a pure manifest edit (no docker). It also changes how the client behaves in an **end-to-end
process**: a stateless client's row is a **call rate** (req/s), while a stateful client's row is
instead a count of **concurrent session-loop instances** to keep alive.

Backends: `frontend/server/clients.js` (the node + state), `scenarios.js` (registry + runner), and
the `clientScript.js` helper (the on-disk Python + step inference).

---

## Resilience policies (circuit breaker + retry)

**Click a connection** (a source service → target node call) on the diagram to attach a
**circuit-breaker + retry** policy. The config (thresholds, retry budget) is upserted onto the
manifest **edge** `{from, to}`, and a shared Python wrapper reads it **at runtime** — implementing the
CLOSED → OPEN → HALF-OPEN state machine, emitting per-connection metrics, and exposing a fast
`/resilience/state` the browser polls to watch a breaker trip **live** (faster than the Prometheus
scrape). The first attach to a service wires + rebuilds it; a later threshold change is
**manifest-only** (read live). Pair it with **Shutdown** on the target to watch the breaker open for
real. Backend: `frontend/server/resilience.js`; the runtime wiring is the `sandbox-resilience` skill.

## Connection pools

The same connection modal has a **Connection pool** section (off by default; hidden for external
targets). Turn it on and set the four sizes — `max_connections`, `min_idle`,
`idle_timeout_seconds`, `max_lifetime_seconds` — and a launched session (`sandbox-connection-pool`)
replaces that service's per-request connect with a real, **module-level shared pool**:
`psycopg_pool` for Postgres, `MongoClient` pool params for Mongo, or a shared `httpx.Client` +
`Limits` for service→service. It emits `connection_pool_max` / `_active` / `_idle` gauges and a
`GET /pool/state`, which the browser polls every 750ms to render a live `pool 3/10 · 2 idle` badge
right on the connection line.

The config is upserted onto the manifest **edge** as `connection_pool` (alongside any `resilience`
block). The key difference from resilience: **pool sizes are construction-time**, so even a pure size
edit needs that service rebuilt/restarted — thresholds are re-read live, pool sizes are not. Backend:
`frontend/server/connectionPool.js` (`GET`/`POST`/`DELETE /api/connection-pool`,
`GET /api/connection-pool-state`).

---

## Prometheus (as a node)

**＋ Add ▸ Prometheus** puts Prometheus itself on the diagram (`POST /api/prom-node`) — it adds a
self-scrape job and a node showing targets-up, series, ingest rate, and API req/s. The container
already runs in every system; this is about making the observability layer **visible and
deletable**.

Deleting the node (`DELETE /api/prom-node`) doesn't stop the container — but the frontend **skips all
Prometheus queries when no prometheus node is on the diagram**, so every node goes gray and reads "no
metrics". It's a one-click way to show what a system looks like when you lose observability. One per
system. Backend: `frontend/server/prometheus.js`. (Not to be confused with the `/api/prometheus`
Vite **proxy**, which is how the browser queries Prometheus at all.)

---

## Taking a node offline (outage)

Any node's Edit panel can **shut it down for N seconds** (1–300). The backend `docker compose stop`s
the container — so its port closes and callers get connection-refused (the LB returns 502) — schedules
an automatic restart after the window, and the diagram paints the node orange until it returns. It's
the simplest way to watch a failure (and any resilience policy) react; "bring back now" cancels the
timer. Backend: `frontend/server/outage.js`.

---

## End-to-end test processes

**🔁 End-to-End** defines and runs **end-to-end test processes** — the sandbox's way of asking "does
this system actually hold together under realistic use?" A process bundles four things
(`systems/<id>/endtoend.json`, backend `frontend/server/endtoend.js`):

- a **client list** — which client functions to drive. A **stateless** client's row carries a call
  rate (`requestsPerSecond`, fractional: 0.1 = one call every 10s); a **stateful** client's row
  instead carries `instances` — how many concurrent session-loop instances to keep alive.
- a **websocket list** — which websocket client pools to keep connected, and **how many clients** to
  spawn (making connection count a per-process test variable),
- a **failure list** — bad states that must never occur in a valid world (a double charge, an orphaned
  payment, ledger debits ≠ credits, …): the system's own **design defects**,
- a **constraint list** — rules of the valid world the test must uphold (seed the preconditions it
  assumes, use only legal inputs).

Defining a process is pure data entry (no rebuild). **Running** one launches a session (the
`sandbox-end-to-end-process` skill) that does the real work: it **seeds** the out-of-scope
preconditions the constraints imply straight into the datastores, synthesizes **legal arguments**,
drives each client function through the load balancer at its rate (or as respawned instance pools) for
the chosen duration, spawns the websocket pools, then **probes** for each failure state (datastore
queries, Prometheus, the call log). It reports an overall **PASS** — or **FAIL** if any failure state
is observed — and persists a full run report to `systems/<id>/endtoend-runs/`, whose verdict shows as
a badge on the process row.

Start/stop is coordinated by an in-memory run flag (`POST /api/endtoend/start|stop`) that the session
polls, so **Stop** (or starting a newer run) ends the current one early; a generous backstop timer
clears a stuck flag if the session dies. Nothing here rebuilds a container — the process only seeds
data and drives the already-running system.

---

## Smoke test — prove the pipeline end to end

The pipeline is: **service exposes metrics → Prometheus pulls them → frontend queries Prometheus**. If
all four checks below agree, it's proven. (Run it on a fresh system: create `demo` from the
entry screen and open it.)

**1. Generate load** through the load balancer (leave it running in a terminal):

```bash
while true; do curl -s localhost:8080/service-1/health; done
```

**2. Prometheus Targets page** — open <http://localhost:9090/targets>. The `service-1` job target
should show **State = UP**.

**3. Prometheus query UI** — open <http://localhost:9090/graph> and run each of these; with load
running they return live numbers:

```promql
sum(rate(http_requests_total{job="service-1"}[1m]))
histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job="service-1"}[1m])) by (le)) * 1000
sum(http_requests_in_flight{job="service-1"})
```

**4. React frontend** — <http://localhost:5173>. The `service-1` node shows `req/s`, `p95`,
`in-flight`, and `errors`, and the numbers **change as load runs**. The node header is **green** while
the service is up. Stop the service (`docker compose stop service-1`) and within a poll cycle or two
the node turns **red**; restart it and it goes green again.

---

## The manifest (core abstraction)

`systems/<id>/manifest.json` declares the topology **and** the metric queries. The frontend is a
generic renderer of this file — **to change what the user sees, edit the manifest, not the React
code.** Key fields:

- `prometheus_base` — base path the frontend prefixes onto `/api/v1/query` (`/api/prometheus`,
  matching the Vite proxy).
- `poll_interval_ms` — how often the frontend re-runs the queries (default 4000).
- `nodes[]` — each has `id`, `label`, `type`, `position {x,y}`, a list of `metrics[]` (each
  `{label, query, unit, scale?}`), and an optional `health { query, rules[] }`.
- `health.rules[]` — `{ color, when }`; `when` is a tiny safe expression of the form
  `value <op> number` (e.g. `value < 1`). First matching rule wins; no value yet → gray.
- `edges[]` — `{ from, to }` node ids, drawn as lines between box centers, plus an optional `origin`
  (e.g. `consumer-fn`, `service-lb`) and any `resilience` / `connection_pool` block attached to that
  connection.
- `boundary { x, y, w, h }` — the dotted system-boundary rectangle; drag mode saves it (and node
  positions) via `POST /api/layout` — pure render state, no rebuild.

**Node types:** `load_balancer` · `service` · `service-lb` · `external_service` · `client` ·
`postgres` · `mongodb` · `redis` · `cassandra` · `dynamodb` · `object-store` · `kafka` · `etcd` ·
`cdc` · `prometheus` · `websocket-lb` · `websocket-server`. A custom service type is a
`type:"service"` node discriminated by `service_type`.

**Node keys the scaffolding writes and the diagram reads:** `origin` (which flow created it:
`create-service`, `create-database`, `create-event-stream`, `create-etcd`, `create-cdc`,
`create-external-service`, `create-client`, `create-custom-service`, `create-websockets`,
`create-prometheus`), `external: true` (clients + external services, drawn outside the boundary),
`schemaModels[]` (model-bank names backing a DB's schema), `stateful` (a client), and the ownership
links: `replicaOf` / `replication` / `readonly` (read replicas), `cdcOf` (a CDC worker), `instanceOf`
+ `svcLb` (a load-balanced cluster), `replicas.instances` (a worker group), `scalerOf` (a scaler
sidecar), `streamOf` (an LLM worker's redis), `wsTier` / `wsRole` (websocket tier membership), plus
the feature blocks `grpc`, `keyspaces`, `sentinel` / `redisCluster`, `etcd`, `llm`, `persistence`,
and `consumerGroup`.

### Metrics instrumented in a service

Defined **by hand** with `prometheus_client` in each service's `app.py` (read `metrics_middleware` to
see exactly how each is produced) — keep that middleware explicit; never swap in an auto-instrumentor:

- `http_requests_total` — Counter, labels `method`, `endpoint`, `status`.
- `http_request_duration_seconds` — Histogram, labels `method`, `endpoint`.
- `http_requests_in_flight` — Gauge (up on entry, down on exit).

The `/metrics` endpoint itself is excluded from instrumentation so Prometheus scrapes don't inflate
req/s or latency. A node's `query` PromQL and the metric it reads must stay consistent with what the
service actually exports.

---

## The extensibility contract

Every feature above is the same shape: **a manifest node + a compose service (+ exporter) + a
Prometheus scrape job (+ an nginx route for HTTP)**, scaffolded by one dev-server plugin, so new
capabilities slot in without touching the generic frontend. Shared primitives live in
`frontend/server/scaffold.js` (`cloneTemplate`, `addComposeService`, `addScrapeJob`, `addNginxRoute`,
`addManifestNode`, `serviceMetrics`, `serviceHealth`, `rebuild`) and `frontend/server/systems.js`.
Compose/Prometheus YAML edits go through a **comment-preserving** parser; nginx edits splice at
`# === end upstreams/locations ===` markers — generated files keep reading like the hand-authored
ones. Don't fork these; compose them.

Ideas not built yet: optional Grafana dashboards, latency injection (netem), and coordinator
hot-standby / failover for the Download Coordinator (its orchestration state is deliberately kept in
one separable object as the seam, but standby itself is out of scope today).

---

## Running a service without Docker (optional debugging)

```bash
cd systems/<id>/service-1
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
curl localhost:8000/health
curl localhost:8000/metrics
```
