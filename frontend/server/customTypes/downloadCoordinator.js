// Custom service type: Download Coordinator (the first consumer of the mechanism).
//
// A peer-to-peer "distribute a large file to many nodes" system. "Add service" →
// "Download Coordinator" creates ONE coordinator service (FastAPI + gRPC) and registers
// its two gRPC contracts into the bank (direct-write, identical to modal-authored ones).
// Workers are spawned from the coordinator's custom Edit tab via the add-node route, and
// each is a real separate service that differs from its peers only by config.
//
// Everything composes from scaffold.js + grpcInstall.js — no forking of the generic
// add-service flow, and no hidden contract path. The type also contributes namespaced
// control routes (/api/custom/download-coordinator/*): add-node, plus thin proxies to a
// coordinator container's control API (sources / distribute / state) through the LB.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  bad, serviceMetrics, serviceHealth, cloneTemplate, addComposeService,
  addNginxRoute, addScrapeJob, addManifestNode, rebuild,
} from '../scaffold.js'
import { installContracts } from '../grpcInstall.js'
import { isValidSystem, systemDir, nextNodePosition } from '../systems.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const TPL = path.join(__dirname, '..', 'templates', 'download-coordinator')
const COORD_DIR = path.join(TPL, 'coordinator')
const WORKER_DIR = path.join(TPL, 'worker')
const GRPC_DIR = path.join(TPL, 'grpc')
const DC_COMMON_DIR = path.join(TPL, 'dc_common')
const SERVICE_FILES = ['app.py', 'requirements.txt', 'Dockerfile']

const DEFAULT_CHUNK_SIZE = 64 * 1024 * 1024 // 64 MB → ~16–80 chunks for a 1–5 GB file
const LB = 'http://localhost:8080' // the system's load balancer (compose maps 8080:80)
const NODE_RE = /^[a-z][a-z0-9-]*$/
const SKIP_REBUILD = () => process.env.CREATE_SVC_SKIP_REBUILD === '1'

const read = (dir, f) => fs.readFileSync(path.join(dir, f), 'utf8')
const readManifest = (system) => JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))

// The two fixed contracts this type owns, with provenance text recorded in the bank.
function contractSpecs() {
  return [
    {
      contract: 'ChunkTransfer',
      proto: read(GRPC_DIR, 'ChunkTransfer.proto'),
      servicer: read(GRPC_DIR, 'ChunkTransfer_servicer.py'),
      source: 'download_coordinator',
      instruction:
        'GetChunk streams a held chunk (a header frame then data frames) to any peer that ' +
        'requests it. Every node serves the chunks it holds, turning the initial star ' +
        '(everyone pulls from the coordinator) into a peer-to-peer mesh.',
    },
    {
      contract: 'Coordination',
      proto: read(GRPC_DIR, 'Coordination.proto'),
      servicer: read(GRPC_DIR, 'Coordination_servicer.py'),
      source: 'download_coordinator',
      instruction:
        'Coordinator-only. Register returns the file manifest (chunk count/size, per-chunk ' +
        'checksums, full-file hash); RequestAssignment hands out the next needed chunk plus a ' +
        'load-balanced source address; Heartbeat tracks liveness; ReportComplete is sent once, ' +
        'after a worker holds every chunk and the full-file hash verifies.',
    },
  ]
}

// Vendor the shared chunk store into systems/<id>/dc_common/ (mounted read-only into
// every node). Idempotent across multiple coordinators in one system.
function installDcCommon(system) {
  const dest = path.join(systemDir(system), 'dc_common')
  fs.mkdirSync(dest, { recursive: true })
  for (const f of fs.readdirSync(DC_COMMON_DIR)) {
    fs.copyFileSync(path.join(DC_COMMON_DIR, f), path.join(dest, f))
  }
}

// The compose volumes every download node shares: the gRPC package + chunk store
// (read-only, single source of truth) + the manifest + this node's durable data dir.
function nodeVolumes(name) {
  return [
    './grpc:/app/grpc_pkg:ro',
    './dc_common:/app/dc_common:ro',
    './manifest.json:/manifest.json:ro',
    `./${name}/data:/data`,
  ]
}

// ---------------------------------------------------------------------------
// Create the coordinator (the add-service "onAdd")
// ---------------------------------------------------------------------------
async function onAdd({ system, name, manifest }) {
  // 1. Install the gRPC contracts into the bank (direct-write + protoc generate).
  await installContracts(system, contractSpecs())

  // 2. Vendor the shared chunk store, and create the node's durable data dir.
  installDcCommon(system)
  fs.mkdirSync(path.join(systemDir(system), name, 'data', 'source'), { recursive: true })

  // 3. Scaffold the coordinator service (template + compose + nginx + prometheus).
  cloneTemplate(system, name, COORD_DIR, SERVICE_FILES)
  addComposeService(
    system,
    name,
    { build: `./${name}`, environment: { SERVICE_ID: name }, volumes: nodeVolumes(name) },
    ` Download Coordinator "${name}" — custom service type`,
  )
  addNginxRoute(system, name)
  addScrapeJob(system, name, 8000, ` Download Coordinator "${name}" — custom service type`)

  // 4. Manifest node — a real service, tagged service_type, wired as ChunkTransfer +
  //    Coordination server. The `coordinator` sub-object is the separable orchestration
  //    config a future hot standby would mirror.
  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'service',
    origin: 'create-custom-service',
    service_type: 'download_coordinator',
    position: nextNodePosition(manifest),
    metrics: serviceMetrics(name),
    health: serviceHealth(name),
    grpc: { servers: ['ChunkTransfer', 'Coordination'], clients: [], overrides: [] },
    coordinator: { chunk_size: DEFAULT_CHUNK_SIZE },
  })

  const log = SKIP_REBUILD() ? '(rebuild skipped)' : await rebuild(system, name)
  return { ok: true, node, log }
}

// Place a new worker in a tidy grid BELOW its coordinator. The generic
// nextNodePosition uses a 180px row pitch, but coordinator/worker nodes carry a
// bitmap-grid body that makes them ~220px tall — enough to overlap the row beneath
// (which buried the coordinator's Edit button). This type knows its nodes are tall,
// so it owns the extra clearance here (the generic layout stays generic).
function workerPosition(manifest, coord) {
  const COLS = 3
  const COLW = 300
  const ROWH = 280
  const siblings = (manifest.nodes || []).filter(
    (n) => n.service_type === 'download_worker' && n.coordinatorId === coord.id,
  ).length
  return {
    x: 80 + (siblings % COLS) * COLW,
    y: (coord.position?.y || 0) + 300 + Math.floor(siblings / COLS) * ROWH,
  }
}

// ---------------------------------------------------------------------------
// Add a worker (custom Edit-tab action)
// ---------------------------------------------------------------------------
async function addWorker(system, coordinatorId) {
  const manifest = readManifest(system)
  const coord = manifest.nodes.find(
    (n) => n.id === coordinatorId && n.service_type === 'download_coordinator',
  )
  if (!coord) throw bad(`"${coordinatorId}" is not a download coordinator in this system`)

  // Next free worker id for this coordinator: <coordinator>-wN.
  const ids = new Set(manifest.nodes.map((n) => n.id))
  let i = 1
  let name = `${coordinatorId}-w${i}`
  while (ids.has(name)) name = `${coordinatorId}-w${++i}`
  if (name.length > 60) throw bad('worker name too long')

  fs.mkdirSync(path.join(systemDir(system), name, 'data', 'source'), { recursive: true })
  cloneTemplate(system, name, WORKER_DIR, SERVICE_FILES)
  addComposeService(
    system,
    name,
    {
      build: `./${name}`,
      environment: { SERVICE_ID: name, COORDINATOR: coordinatorId },
      volumes: nodeVolumes(name),
    },
    ` Download Worker "${name}" — peer of "${coordinatorId}"`,
  )
  addNginxRoute(system, name)
  addScrapeJob(system, name, 8000, ` Download Worker "${name}"`)

  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'service',
    origin: 'create-custom-service',
    service_type: 'download_worker',
    coordinatorId,
    position: workerPosition(manifest, coord),
    metrics: serviceMetrics(name),
    health: serviceHealth(name),
    // Static gRPC wiring (the initial star to the coordinator); actual peer dial
    // addresses are coordinator-directed per assignment (the mesh shows in live edges).
    grpc: {
      servers: ['ChunkTransfer'],
      clients: [
        { contract: 'ChunkTransfer', targets: [coordinatorId] },
        { contract: 'Coordination', targets: [coordinatorId] },
      ],
      overrides: [],
    },
  })

  const log = SKIP_REBUILD() ? '(rebuild skipped)' : await rebuild(system, name)
  return { node, log }
}

// ---------------------------------------------------------------------------
// Control routes (mounted by customServices.js with a { json, readJsonBody } ctx)
// ---------------------------------------------------------------------------
const fail = (ctx, res, err) => ctx.json(res, err.statusCode || 500, { ok: false, error: err.message })

async function handleAddNode(req, res, _next, ctx) {
  if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'POST only' })
  try {
    const { system, coordinator } = await ctx.readJsonBody(req)
    if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
    const { node, log } = await addWorker(system, coordinator)
    ctx.json(res, 200, { ok: true, node, log })
  } catch (err) {
    fail(ctx, res, err)
  }
}

async function handleSources(req, res, _next, ctx) {
  if (req.method !== 'GET') return ctx.json(res, 405, { ok: false, error: 'GET only' })
  try {
    const url = new URL(req.url, 'http://localhost')
    const system = url.searchParams.get('system')
    const node = url.searchParams.get('node')
    if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
    if (!node || !NODE_RE.test(node)) throw bad('invalid node')
    const r = await fetch(`${LB}/${node}/dc/sources`)
    ctx.json(res, 200, await r.json())
  } catch (err) {
    fail(ctx, res, err)
  }
}

async function handleDistribute(req, res, _next, ctx) {
  if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'POST only' })
  try {
    const { system, node, source, chunk_size } = await ctx.readJsonBody(req)
    if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
    if (!node || !NODE_RE.test(node)) throw bad('invalid node')
    const r = await fetch(`${LB}/${node}/dc/distribute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source, chunk_size }),
    })
    ctx.json(res, r.status, await r.json())
  } catch (err) {
    fail(ctx, res, err)
  }
}

// Aggregate every coordinator's /dc/state into one node-keyed map the diagram + tabs read.
async function handleState(req, res, _next, ctx) {
  if (req.method !== 'GET') return ctx.json(res, 405, { ok: false, error: 'GET only' })
  try {
    const url = new URL(req.url, 'http://localhost')
    const system = url.searchParams.get('system')
    if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
    const manifest = readManifest(system)
    const coords = manifest.nodes.filter((n) => n.service_type === 'download_coordinator')
    const nodes = {}
    await Promise.all(
      coords.map(async (c) => {
        try {
          const r = await fetch(`${LB}/${c.id}/dc/state`)
          if (!r.ok) return
          const s = await r.json()
          if (!s.ok) return
          nodes[c.id] = {
            role: 'coordinator',
            phase: s.phase,
            ready: s.ready,
            error: s.error,
            chunk_count: s.chunk_count,
            chunk_size: s.chunk_size,
            file_size: s.file_size,
            full_hash: s.full_hash,
            progress: s.progress,
            bitmap: s.coordinator?.bitmap || [],
            held: s.coordinator?.held || 0,
            recent: s.recent || [],
          }
          for (const [wid, w] of Object.entries(s.workers || {})) {
            nodes[wid] = { role: 'worker', chunk_count: s.chunk_count, ...w }
          }
        } catch {
          /* coordinator not reachable yet (still building) */
        }
      }),
    )
    ctx.json(res, 200, { ok: true, nodes })
  } catch (err) {
    fail(ctx, res, err)
  }
}

export default {
  serviceType: 'download_coordinator',
  displayName: 'Download Coordinator',
  description:
    'Peer-to-peer file distribution: a coordinator seeds a large file and worker nodes pull chunks from each other (star → mesh).',
  onAdd,
  routes: [
    { path: '/api/custom/download-coordinator/add-node', handler: handleAddNode },
    { path: '/api/custom/download-coordinator/sources', handler: handleSources },
    { path: '/api/custom/download-coordinator/distribute', handler: handleDistribute },
    { path: '/api/custom/download-coordinator/state', handler: handleState },
  ],
}
