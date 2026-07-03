---
name: sandbox-download-coordinator
description: >-
  Work on the Download Coordinator custom service type in a "Distributed Systems Sandbox"
  system — a peer-to-peer "distribute a large file to many nodes" system (a coordinator seeds a
  file; worker nodes pull chunks from each other, star → mesh). Use whenever the task touches
  its gRPC contracts (ChunkTransfer / Coordination), its order-of-operations invariants,
  peer-seeding / source-selection / load-balancing, the on-disk bitmap / durable-state rules,
  failure handling, or the standby seam. It is registered via the [[sandbox-custom-service-type]]
  mechanism and its contracts live in the bank like [[sandbox-grpc-contract]] ones.
---

# Download Coordinator

A peer-to-peer file-distribution service type. **Add service → Download Coordinator** creates
ONE coordinator container (origin seed + orchestrator); **Add node** (in the coordinator's
Edit → Distribution tab) spawns real worker containers. The coordinator chunks a file and
hands out assignments; the moment a worker holds chunk N it becomes a valid source for N, so
distribution shifts from a star (everyone pulls from the coordinator) to a mesh.

**Never run `./start.sh`.** Control the stack only with
`docker compose -f systems/<id>/docker-compose.yml ...`.

## Where it lives (working dir = repo root)
- **Backend recipe + control routes:** `frontend/server/customTypes/downloadCoordinator.js`
  (registered in `frontend/server/customTypes/index.js`). Routes:
  `/api/custom/download-coordinator/{add-node,sources,distribute,state}`.
- **Templates:** `frontend/server/templates/download-coordinator/` — `coordinator/` and
  `worker/` (each `app.py` + `Dockerfile` + `requirements.txt`), `grpc/` (the two `.proto` +
  shared servicers), `dc_common/chunkstore.py` (the shared on-disk store).
- **Contracts in the bank:** installed into `systems/<id>/grpc/` (direct-write, identical to
  modal-authored). Shared code is mounted read-only into every node: `./grpc:/app/grpc_pkg:ro`
  + `./dc_common:/app/dc_common:ro`; durable state at `./<node>/data:/data`.
- **Frontend:** `frontend/src/customTypes/downloadCoordinator/` — `CoordinatorTab.jsx` (Add
  node + Run distribution + live status), `DiagramBody.jsx` (per-node bitmap grid + aggregate
  %), `index.jsx` (registration + live chain/source edges).

## gRPC contracts (two, in the bank)
- **ChunkTransfer** — served AND consumed by every node. `GetChunk(ChunkRequest) -> stream
  ChunkFrame` (a header frame: chunk_id/size/checksum, then data frames). The single shared
  servicer serves any chunk the node's ChunkStore holds.
- **Coordination** — served only by the coordinator (workers are clients):
  - `Register(worker_id) -> FileManifest` (chunk count/size, per-chunk checksums, full hash).
  - `RequestAssignment(worker_id, bitmap) -> Assignment` (`ASSIGN chunk_id + source_addr` |
    `WAIT` | `DONE`).
  - `Heartbeat(worker_id, bitmap, status) -> Ack`.
  - `ReportComplete(worker_id, full_hash_ok) -> Ack`.

## Order-of-operations invariants (encode EXACTLY — these are the teaching point)
- A worker writes a received chunk to disk, **verifies its checksum, flips its bitmap bit, and
  persists the bitmap BEFORE requesting the next assignment.** The on-disk bitmap must never
  claim a chunk that isn't fully on disk (`ChunkStore.write_chunk` does write → atomic replace
  → flip+fsync bitmap, in that order).
- **There is no "chunk complete" RPC.** Completion of a chunk is signalled implicitly by the
  updated bitmap on the *next* `RequestAssignment`.
- `ReportComplete` is sent **once**, only after the worker holds every chunk AND the full-file
  hash verifies.
- `Heartbeat` and `RequestAssignment` both carry the bitmap but on **different cadences**
  (timer-based liveness vs on-demand work pull) — keep them separate; do not merge.

## Peer seeding, source selection, load-balancing
- The coordinator is the origin seed (holds every chunk) and the orchestrator. On
  `RequestAssignment` it picks a chunk the worker lacks, then a source **load-balanced
  (round-robin) across all current ALIVE holders** of that chunk (coordinator + any workers
  whose last heartbeat is within the alive window AND whose bitmap has the bit). It returns
  `source_addr = <holder>:50051`; the worker dials that for `GetChunk`.
- Source addresses are coordinator-directed per assignment, so adding a worker needs no restart
  of peers; the manifest `grpc` client `targets` are only the static (star) hint for the
  diagram. The live mesh shows in the chain/source edges (`diagramEdges` from the coordinator's
  recent transfers).

## Endpoint surface (what the load balancer advertises)
- None of the `/dc/*` HTTP routes are an external client API — they're the control plane
  the Distribution tab + diagram poll drive. The module's `endpointPolicy` (in
  `index.jsx`) classifies them so the generic endpoint layer stays type-agnostic:
  `/dc/distribute` is **hidden** (owned by the tab; never listed), and `/dc/sources`,
  `/dc/state`, `/dc/worker` are **internal** (off the load balancer; listed in the
  coordinator/worker Edit → Endpoints tab badged "internal" and locked from edit/delete).
  Generic `/health` + `/resilience/*` are handled by `src/endpointPolicy.js` itself.
- Layout: coordinator/worker nodes carry a bitmap-grid body (~220px tall), so the recipe
  places workers with extra vertical clearance below their coordinator (`workerPosition`
  in `downloadCoordinator.js`) — the generic 180px row pitch would overlap them.

## Durable state & resume
- Each node's chunks + bitmap live under `systems/<id>/<node>/data/` (bind-mounted), so a
  restarted node resumes from on-disk state. On (re)start a worker re-registers and calls
  `ChunkStore.set_manifest`, which **rebuilds the bitmap from the chunk files actually on disk**
  — so it re-fetches only the chunks it is missing, never the ones it already has.

## Failure handling (v1)
- Heartbeat timeout → the coordinator stops counting that worker as alive, so it is no longer
  handed out as a source (and shows stale on the diagram).
- A dead source is simply not selected; the coordinator always holds every chunk, so a fallback
  source always exists and consumers reassign automatically on their next `RequestAssignment`.
- A worker that died mid-download resumes from its persisted bitmap on restart.
- **Coordinator failure / hot standby is OUT OF SCOPE in v1.** All orchestration state lives in
  a single, deliberately-separable `Orchestrator` object (the seam a standby would mirror);
  note the seam, do not implement standby.

## Common tasks
- **Add a worker:** `POST /api/custom/download-coordinator/add-node {system, coordinator}` (the
  Distribution tab's "Add node"). Spawns a real worker that differs from peers only by config
  (`SERVICE_ID`, `COORDINATOR`); it shares the one ChunkTransfer servicer.
- **Run a distribution:** `POST .../distribute {system, node, source:{type:'url'|'local',
  value}, chunk_size?}` (proxied to the coordinator's `/dc/distribute`). Local sources are
  pre-staged files under the coordinator's `data/source/` (list via `/dc/sources`). Default
  chunk size 64 MB (≈16–80 chunks for a 1–5 GB file); tune for tests.
- **Live state:** `GET .../state?system=<id>` aggregates every coordinator's `/dc/state` into
  `{ nodes: { [id]: { role, bitmap, held, complete, alive, progress, recent, ... } } }` — what
  the diagram + tab render.
- Editing a servicer/contract: it is a normal bank contract — see [[sandbox-grpc-contract]] /
  [[sandbox-grpc-attach]]. Rebuild only the affected node(s):
  `docker compose -f systems/<id>/docker-compose.yml up -d --build <node>`.

## Verify (running `hello-lb` under `npm run dev`)
1. Add a coordinator → it appears as a typed node; ChunkTransfer + Coordination show in the
   gRPC modal with files under `systems/<id>/grpc/`.
2. Add 2+ workers; stage a synthetic file under the coordinator's `data/source/`; run a local
   distribution → every worker's bitmap fills (out-of-order), each full-file hash verifies, all
   report complete; the coordinator shows 100%.
3. `recent` shows worker sources (not only the coordinator) → peer seeding; the diagram's
   chain edges show star → mesh.
4. Restart a worker mid/after distribution → it resumes from its bitmap with no re-fetch.
5. Stop peers, force a worker to re-download → it completes solely from the coordinator
   (fallback), and stopped peers show stale.
