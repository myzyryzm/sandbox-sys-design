---
name: sandbox-custom-service-type
description: >-
  Register a new "custom service type" in a "Distributed Systems Sandbox" system — a typed
  service that installs itself into the EXISTING primitives (the Add-service modal, the
  per-node Edit tabs, the gRPC bank, the manifest, and the diagram) instead of forking them.
  Use whenever the task is to add a new selectable service type (e.g. a sharded DB, a download
  coordinator) that creates real container(s) on add, contributes custom Edit tab(s), and/or
  renders a type-specific diagram body. The first consumer is [[sandbox-download-coordinator]];
  gRPC contracts go through the same bank as [[sandbox-grpc-contract]] / [[sandbox-grpc-attach]].
---

# Adding a custom service type

You are in the "Distributed Systems Sandbox" web app. A **custom service type** is a typed
service that appears in the **Add service** modal alongside "Generic service"; selecting it
runs a type-specific "on add" routine (real containers + manifest node + optional gRPC
contracts) instead of creating a generic stub. It then plugs custom Edit tabs and custom
diagram rendering into the same modal/diagram everything else uses.

**Overriding rule — compose from primitives, never fork them.** A custom type must register
into the existing extension points. It must NOT fork the add-service flow, create a parallel
gRPC path, or hardcode type-specific logic into any generic layer (the scaffold, the manifest
schema, the generic diagram renderer). If v1 seems to need type logic in a generic layer,
STOP and add a typed extension point there, then register through it. Test of success: the
*next* custom type reuses all this machinery and only adds its own domain logic.

**Never run `./start.sh`** — it tears down the dev server you are attached to. Control the
stack only with `docker compose -f systems/<id>/docker-compose.yml ...`.

## A custom service type is exactly two registry entries (working dir = repo root)

1. **Backend recipe** — `frontend/server/customTypes/<type>.js`, listed in
   `frontend/server/customTypes/index.js`.
2. **Frontend rendering** — `frontend/src/customTypes/<type>/index.jsx`, listed in
   `frontend/src/customTypes/index.js`.

The generic dispatcher (`frontend/server/customServices.js`, already a registered Vite plugin)
exposes `GET /api/custom-types`, `POST /api/custom-services`, and mounts every type's control
routes. The Add modal, `NodeEditModal`, `SystemDiagram`, and `App` already read the registries.
Adding a type touches none of them.

## Backend recipe — `frontend/server/customTypes/<type>.js`

Export an object: `{ serviceType, displayName, description, onAdd, routes }`.

- **`onAdd({ system, name, manifest })`** builds the typed node(s) by COMPOSING the shared
  primitives in `frontend/server/scaffold.js` (do not reimplement them):
  - `cloneTemplate(system, name, templateDir, files)` — copy a service template into
    `systems/<id>/<name>/` (put templates under `frontend/server/templates/<type>/`).
  - `addComposeService(system, name, serviceObj, comment)` — `serviceObj` is the compose value,
    e.g. `{ build: './<name>', environment: {...}, volumes: [...] }` (use read-only volume
    mounts for shared code + `:ro` the manifest, matching the repo convention — see service-1).
  - `addNginxRoute(system, name)` + `addScrapeJob(system, name, 8000, comment)`.
  - `serviceMetrics(name)` + `serviceHealth(name)` for the standard FastAPI metric/health block.
  - `addManifestNode(system, manifest, node)` — append a fully-formed node. Set
    `type: 'service'` (so the gRPC bank + diagram treat it as a service), `origin:
    'create-custom-service'`, and **`service_type: '<type>'`** (the discriminator the Edit tabs
    + diagram key off). The manifest is free-form JSON — no schema change is needed for new
    fields. Keep any orchestration/config in a clearly-separable sub-object.
  - `await rebuild(system, name)` — the frontend-safe `docker compose build/up` + nginx reload
    + prometheus restart (honors `CREATE_SVC_SKIP_REBUILD=1` for tests).
- **`routes`** — array of `{ path: '/api/custom/<type>/...', handler }` for type-specific
  control endpoints (add more nodes, run actions, expose live state). `handler(req, res, next,
  ctx)` gets `ctx.json` + `ctx.readJsonBody`. Proxy to a node's own HTTP API through the LB
  (`http://localhost:8080/<node>/...`) when you need live in-container state.

Then add one line to `frontend/server/customTypes/index.js`.

### gRPC contracts: same bank, no hidden path
If the type ships fixed gRPC contracts, install them with
`frontend/server/grpcInstall.js#installContracts(system, specs)` — it direct-writes the
`.proto` + shared servicer into `systems/<id>/grpc/`, runs the real `protoc` to generate the
bindings, and upserts `_registry.json` in the SAME shape as a modal-authored contract (so the
gRPC modal lists/views/edits it identically). Pin `grpcio-tools` to match the runtime
`grpcio`/`protobuf` versions in your service image. Wire roles by writing the node's manifest
`grpc` block (`{ servers, clients:[{contract,targets}], overrides }`) — see
[[sandbox-grpc-attach]]. (Templated, deterministic app code may wire the servicer/stub directly
instead of launching a Claude session; the bank is still the single source of truth.)

## Frontend rendering — `frontend/src/customTypes/<type>/index.jsx`

Export `{ serviceTypes, editTabs, runtime, DiagramBody, diagramHeight, diagramEdges,
endpointPolicy }` (all optional except `serviceTypes`). One module may own several related
`service_type`s.

- **`serviceTypes: ['<type>', ...]`** — the manifest `service_type`s this module renders.
- **`editTabs(node) -> [{ id, label, Component }]`** — tabs injected into `NodeEditModal`
  between the kind tabs and Shutdown/Delete. `Component` is rendered embedded (body only) with
  `{ systemId, node, current, manifest, onClose, onLaunch, onBusyChange }`; lift in-flight
  state via `onBusyChange` so the modal locks while an action runs.
- **`runtime: { url(systemId) -> string }`** — a poll endpoint returning
  `{ ok, nodes: { [nodeId]: state } }`. `App` merges every type's result into one `customState`
  map (keyed by node id) that the diagram + tabs read.
- **`DiagramBody({ node, runtime, width, top })`** — SVG drawn inside the node box (e.g. a
  status grid). **`diagramHeight(node, runtime, width)`** must return the exact px it draws so
  `SystemDiagram` reserves the right space (metrics → custom band → Edit button).
- **`diagramEdges({ manifest, customState }) -> [{ from, to, label?, className? }]`** —
  extra diagram edges (e.g. a live who-talks-to-whom view).
- **`endpointPolicy(node, localPath, endpoint) -> { visibility?, locked? } | null`** —
  classify THIS type's own routes for the generic `src/endpointPolicy.js` seam.
  `visibility`: `'public'` (external client API — shown on the load balancer, editable),
  `'internal'` (operational/control-plane — kept off the LB, listed in the Endpoints tab
  badged "internal"), `'hidden'` (never listed; e.g. a route a custom Edit tab owns).
  `locked: true` forbids edit/delete in the Endpoints tab. Return `null` to fall through
  to the generic classification (`/health`, `/resilience/*`). Keep type-specific paths
  HERE, not in the generic endpoint layer.

Then add one line to `frontend/src/customTypes/index.js`.

## Verify (running system under `npm run dev`; never `./start.sh`)
1. `cd frontend && npm run build` compiles.
2. `curl -s localhost:5173/api/custom-types` lists the new type; it appears in **Add service**.
3. Selecting it (POST `/api/custom-services`) creates a real, healthy, scraped node whose
   manifest entry carries `service_type` — not a generic stub. `docker compose ... ps` shows it.
4. Any contracts it ships appear in the gRPC modal identically to modal-authored ones, with
   files under `systems/<id>/grpc/`.
5. The node's Edit modal shows the custom tab(s); the diagram renders the custom body/edges.
6. No type-specific logic leaked into `scaffold.js`, `SystemDiagram`, or the manifest core —
   all of it entered through the two registry entries.
