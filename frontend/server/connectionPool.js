// Vite dev-server plugin: attach a connection-pool config to an INTERNAL connection
// (a source service -> internal target node outbound call). Sibling of resilience.js —
// the pool config is stored on the same manifest edge as the `resilience` block.
//
//   GET    /api/connection-pool?system=<id>
//     -> { ok, connections: [{ from, to, connection_pool }] }  (the manifest edges that
//        currently carry a `connection_pool` block)
//   POST   /api/connection-pool
//          { system, from, to, connection_pool, conversationId, instruction }
//     -> validates the pool shape and upserts a `connection_pool` block onto the
//        manifest edge {from,to} (creating the edge if the dependency was only
//        implicit). Config only — no docker rebuild here. The per-service pooling code
//        (psycopg_pool / pymongo pool / a shared httpx.Client) that reads these sizes at
//        STARTUP, the pool metrics, and the /pool/state endpoint are written by the
//        Claude session the modal then launches (sandbox-connection-pool skill). Returns
//        `firstAttach` = the `from` service has no pooled connection yet (so the session
//        must wire + rebuild it). Note: unlike resilience, pool sizes are construction-
//        time, so even a later edit needs a single-service restart (no live re-read).
//   DELETE /api/connection-pool  { system, from, to }
//     -> strips the `connection_pool` block from the edge (and drops an edge that existed
//        only to carry it; an edge still carrying a `resilience` block is kept).
//
//   GET    /api/connection-pool-state?system=<id>
//     -> { ok, connections: { "from->to": { ...live pool state... } } }  aggregates each
//        service's /pool/state through the LB so the browser can poll it same-origin to
//        show live active/idle counts on the diagram. Services not wired yet are skipped.
//
// Pooling is for INTERNAL connections only — external services/clients sit outside the
// system boundary and are rejected here (matching the diagram's poolEligible gate).
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'

const LB_BASE = 'http://localhost:8080'

class HttpError extends Error {
  constructor(status, message) {
    super(message)
    this.statusCode = status
  }
}
const bad = (msg) => new HttpError(400, msg)

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > 200_000) reject(bad('request body too large'))
    })
    req.on('end', () => {
      if (!data) return resolve({})
      try {
        resolve(JSON.parse(data))
      } catch {
        reject(bad('invalid JSON body'))
      }
    })
    req.on('error', reject)
  })
}

const manifestFile = (system) => path.join(systemDir(system), 'manifest.json')
function readManifest(system) {
  return JSON.parse(fs.readFileSync(manifestFile(system), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(manifestFile(system), JSON.stringify(manifest, null, 2) + '\n')
}

// A positive number (optionally integer-only). Rejects NaN, ≤0, and non-numbers.
function posNumber(value, label, { integer = false } = {}) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    throw bad(`${label} must be a positive number`)
  }
  if (integer && !Number.isInteger(value)) throw bad(`${label} must be a whole number`)
  return value
}

// A non-negative integer (0 allowed). `min_idle` may legitimately be 0, which posNumber
// rejects, so it gets its own validator.
function nonNegInt(value, label) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || !Number.isInteger(value)) {
    throw bad(`${label} must be a non-negative whole number`)
  }
  return value
}

// Validate + normalize the pool sub-config. When disabled we return a bare {enabled:false}
// (a disabled pool isn't applied, so its numbers don't matter).
function validatePool(value) {
  if (value == null) return { enabled: false }
  if (typeof value !== 'object' || Array.isArray(value)) throw bad('connection_pool must be an object')
  if (!value.enabled) return { enabled: false }
  const max_connections = posNumber(value.max_connections, 'max_connections', { integer: true })
  const min_idle = nonNegInt(value.min_idle, 'min_idle')
  if (min_idle > max_connections) throw bad('min_idle must be ≤ max_connections')
  return {
    enabled: true,
    max_connections,
    min_idle,
    idle_timeout_seconds: posNumber(value.idle_timeout_seconds, 'idle_timeout_seconds'),
    max_lifetime_seconds: posNumber(value.max_lifetime_seconds, 'max_lifetime_seconds'),
  }
}

function listConnections(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const connections = (manifest.edges || [])
    .filter((e) => e && e.connection_pool)
    .map((e) => ({ from: e.from, to: e.to, connection_pool: e.connection_pool }))
  return { ok: true, connections }
}

function upsertPolicy(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)

  const from = body.from
  const to = body.to
  const fromNode = manifest.nodes.find((n) => n.id === from)
  if (!fromNode || fromNode.type !== 'service') throw bad(`"${from}" is not a service in this system`)
  const toNode = manifest.nodes.find((n) => n.id === to)
  if (!toNode) throw bad(`target "${to}" is not a node in this system`)
  if (from === to) throw bad('a connection cannot point at itself')
  // Internal connections only — external services/clients are drawn outside the boundary
  // and have no in-system pool to size.
  if (toNode.external) throw bad(`target "${to}" is external — connection pooling is for internal connections only`)

  const connection_pool = validatePool(body.connection_pool)
  if (!connection_pool.enabled) throw bad('enable the connection pool, or delete it')
  const instruction = typeof body.instruction === 'string' ? body.instruction : ''
  if (instruction.length > 8000) throw bad('instruction is too long')
  const conversationId = body.conversationId
  if (typeof conversationId !== 'string') throw bad('conversationId is required')

  // First attach for this service = it carries no pooled connection yet, so the session
  // must add the pool wiring + rebuild. Computed BEFORE we write.
  const firstAttach = !(manifest.edges || []).some((e) => e.from === from && e.connection_pool)

  manifest.edges = manifest.edges || []
  let edge = manifest.edges.find((e) => e.from === from && e.to === to)
  if (!edge) {
    edge = { from, to }
    manifest.edges.push(edge)
  }
  edge.connection_pool = connection_pool
  if (instruction) edge.connection_pool.instruction = instruction

  writeManifest(system, manifest)
  return { ok: true, from, to, firstAttach, connection_pool: edge.connection_pool }
}

function deletePolicy(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const { from, to } = body
  const edge = (manifest.edges || []).find((e) => e.from === from && e.to === to)
  if (edge) {
    delete edge.connection_pool
    // Drop an edge that only ever existed to carry this config (no other metadata — a
    // remaining resilience block counts as meaningful and keeps the edge).
    const meaningful = Object.keys(edge).filter((k) => k !== 'from' && k !== 'to')
    if (meaningful.length === 0) {
      manifest.edges = manifest.edges.filter((e) => e !== edge)
    }
    writeManifest(system, manifest)
  }
  return { ok: true, from, to, removed: !!edge }
}

// ---------------------------------------------------------------------------
// Fast in-memory state read — aggregate each service's /pool/state via the LB.
// ---------------------------------------------------------------------------

async function fetchJson(url, ms = 1500) {
  const ctrl = new AbortController()
  const t = setTimeout(() => ctrl.abort(), ms)
  try {
    const res = await fetch(url, { signal: ctrl.signal })
    if (!res.ok) return null
    return await res.json()
  } catch {
    return null
  } finally {
    clearTimeout(t)
  }
}

async function poolState(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const services = manifest.nodes.filter((n) => n.type === 'service').map((n) => n.id)
  const connections = {}
  await Promise.all(
    services.map(async (id) => {
      const data = await fetchJson(`${LB_BASE}/${id}/pool/state`)
      for (const c of data?.connections || []) {
        if (!c || typeof c.to !== 'string') continue
        // The service reports its outbound pools by target; the `from` is the service
        // itself. Key by "from->to" to match the diagram edge + metric label.
        connections[`${id}->${c.to}`] = { from: id, ...c }
      }
    }),
  )
  return { ok: true, connections }
}

export default function connectionPool() {
  return {
    name: 'connection-pool',
    configureServer(server) {
      const json = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }

      server.middlewares.use('/api/connection-pool', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            return json(res, 200, listConnections(system))
          }
          if (req.method === 'POST') {
            return json(res, 200, upsertPolicy(await readJsonBody(req)))
          }
          if (req.method === 'DELETE') {
            return json(res, 200, deletePolicy(await readJsonBody(req)))
          }
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })

      server.middlewares.use('/api/connection-pool-state', async (req, res, next) => {
        if (req.method !== 'GET') return next()
        try {
          const system = new URL(req.url, 'http://localhost').searchParams.get('system')
          return json(res, 200, await poolState(system))
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
