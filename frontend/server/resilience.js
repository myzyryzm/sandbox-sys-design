// Vite dev-server plugin: attach circuit-breaker + retry resilience policies to a
// connection (a source service -> target node outbound call).
//
//   GET    /api/connection-resilience?system=<id>
//     -> { ok, connections: [{ from, to, resilience }] }  (the manifest edges that
//        currently carry a `resilience` block)
//   POST   /api/connection-resilience
//          { system, from, to, circuit_breaker, retry, conversationId, instruction }
//     -> validates the policy shape and upserts a `resilience` block onto the
//        manifest edge {from,to} (creating the edge if the dependency was only
//        implicit). Config only — no docker rebuild here. The shared Python wrapper
//        that reads this policy at runtime, the per-service wiring, the metrics and
//        the fast /resilience/state endpoint are written by the Claude session the
//        modal then launches (sandbox-resilience skill). Returns `firstAttach` = the
//        `from` service has no resilience policy yet (so the session must wire +
//        rebuild it; a later threshold edit is manifest-only).
//   DELETE /api/connection-resilience  { system, from, to }
//     -> strips the `resilience` block from the edge (and drops an edge that existed
//        only to carry the policy).
//
//   GET    /api/resilience-state?system=<id>
//     -> { ok, connections: { "from->to": { ...live state... } } }  the fast,
//        in-memory current state read: aggregates each service's /resilience/state
//        through the LB so the browser can poll it same-origin, faster than the
//        Prometheus scrape, to watch a breaker trip live. Services that don't expose
//        it yet (before they're wired) are simply skipped.
//
// Mirrors grpc.js / endpoints.js (manifest/registry edit, no rebuild — the wiring is
// the session's job). A connection is identified by {from,to} and stored in
// manifest.edges[]; the diagram draws every edge, so a policied edge "just works".
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

const OPEN_BEHAVIORS = new Set(['fail_fast', 'fallback'])
const RETRY_STRATEGIES = new Set(['exponential_backoff', 'exponential_backoff_jitter'])

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

// A positive number (optionally integer-only). Rejects NaN, ≤0, and non-numbers so
// a malformed threshold can never reach the wrapper.
function posNumber(value, label, { integer = false } = {}) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    throw bad(`${label} must be a positive number`)
  }
  if (integer && !Number.isInteger(value)) throw bad(`${label} must be a whole number`)
  return value
}

// Validate + normalize the circuit-breaker sub-policy. When disabled we keep the
// fields the modal sent (so re-opening preserves them) but only enforce the shape
// when enabled — a disabled policy isn't applied, so its numbers don't matter.
function validateBreaker(value) {
  if (value == null) return { enabled: false }
  if (typeof value !== 'object' || Array.isArray(value)) throw bad('circuit_breaker must be an object')
  const enabled = !!value.enabled
  if (!enabled) return { enabled: false }
  const open_behavior = value.open_behavior
  if (!OPEN_BEHAVIORS.has(open_behavior)) {
    throw bad('open_behavior must be "fail_fast" or "fallback"')
  }
  const out = {
    enabled: true,
    failure_threshold: posNumber(value.failure_threshold, 'failure_threshold', { integer: true }),
    pause_duration_seconds: posNumber(value.pause_duration_seconds, 'pause_duration_seconds'),
    half_open_trial_calls: posNumber(value.half_open_trial_calls, 'half_open_trial_calls', { integer: true }),
    open_behavior,
    fallback_response: null,
  }
  if (open_behavior === 'fallback') {
    // The fallback value is served verbatim while OPEN; it must be present (any JSON
    // value, including a string body). undefined means the user didn't fill it in.
    if (value.fallback_response === undefined) {
      throw bad('a fallback response is required when open_behavior is "fallback"')
    }
    out.fallback_response = value.fallback_response
  }
  return out
}

function validateRetry(value) {
  if (value == null) return { enabled: false }
  if (typeof value !== 'object' || Array.isArray(value)) throw bad('retry must be an object')
  const enabled = !!value.enabled
  if (!enabled) return { enabled: false }
  if (!RETRY_STRATEGIES.has(value.strategy)) {
    throw bad('strategy must be "exponential_backoff" or "exponential_backoff_jitter"')
  }
  const base_delay_seconds = posNumber(value.base_delay_seconds, 'base_delay_seconds')
  const max_delay_seconds = posNumber(value.max_delay_seconds, 'max_delay_seconds')
  if (max_delay_seconds < base_delay_seconds) {
    throw bad('max_delay_seconds must be ≥ base_delay_seconds')
  }
  return {
    enabled: true,
    max_attempts: posNumber(value.max_attempts, 'max_attempts', { integer: true }),
    strategy: value.strategy,
    base_delay_seconds,
    max_delay_seconds,
  }
}

function listConnections(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const connections = (manifest.edges || [])
    .filter((e) => e && e.resilience)
    .map((e) => ({ from: e.from, to: e.to, resilience: e.resilience }))
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
  if (!manifest.nodes.some((n) => n.id === to)) throw bad(`target "${to}" is not a node in this system`)
  if (from === to) throw bad('a connection cannot point at itself')

  const circuit_breaker = validateBreaker(body.circuit_breaker)
  const retry = validateRetry(body.retry)
  if (!circuit_breaker.enabled && !retry.enabled) {
    throw bad('enable circuit breaking and/or retry, or delete the policy')
  }
  const instruction = typeof body.instruction === 'string' ? body.instruction : ''
  if (instruction.length > 8000) throw bad('instruction is too long')
  const conversationId = body.conversationId
  if (typeof conversationId !== 'string') throw bad('conversationId is required')

  // First attach for this service = it carries no resilience policy yet, so the
  // session must add the wrapper module + wiring + rebuild. Computed BEFORE we write,
  // and only true the very first time any connection FROM this service is policied.
  const firstAttach = !(manifest.edges || []).some((e) => e.from === from && e.resilience)

  manifest.edges = manifest.edges || []
  let edge = manifest.edges.find((e) => e.from === from && e.to === to)
  if (!edge) {
    edge = { from, to }
    manifest.edges.push(edge)
  }
  edge.resilience = { circuit_breaker, retry }
  if (instruction) edge.resilience.instruction = instruction

  writeManifest(system, manifest)
  return { ok: true, from, to, firstAttach, resilience: edge.resilience }
}

function deletePolicy(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const { from, to } = body
  const edge = (manifest.edges || []).find((e) => e.from === from && e.to === to)
  if (edge) {
    delete edge.resilience
    // Drop an edge that only ever existed to carry the policy (no other metadata).
    const meaningful = Object.keys(edge).filter((k) => k !== 'from' && k !== 'to')
    if (meaningful.length === 0) {
      manifest.edges = manifest.edges.filter((e) => e !== edge)
    }
    writeManifest(system, manifest)
  }
  return { ok: true, from, to, removed: !!edge }
}

// ---------------------------------------------------------------------------
// Fast in-memory state read — aggregate each service's /resilience/state via the LB.
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

async function resilienceState(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  // Poll each service that is reachable through the lb at /<id>/. A load-balanced
  // service's cluster entry (`service-lb`) is routable via its haproxy sidecar; its
  // instances (`instanceOf`) are NOT individually routed, so skip them (they'd 404).
  const services = manifest.nodes
    .filter((n) => (n.type === 'service' || n.type === 'service-lb') && !n.instanceOf)
    .map((n) => n.id)
  const connections = {}
  await Promise.all(
    services.map(async (id) => {
      const data = await fetchJson(`${LB_BASE}/${id}/resilience/state`)
      for (const c of data?.connections || []) {
        if (!c || typeof c.to !== 'string') continue
        // The service reports its outbound connections by target; the `from` is the
        // service itself. Key by "from->to" to match the diagram edge + metric label.
        connections[`${id}->${c.to}`] = { from: id, ...c }
      }
    }),
  )
  return { ok: true, connections }
}

export default function resilience() {
  return {
    name: 'connection-resilience',
    configureServer(server) {
      const json = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }

      server.middlewares.use('/api/connection-resilience', async (req, res, next) => {
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

      server.middlewares.use('/api/resilience-state', async (req, res, next) => {
        if (req.method !== 'GET') return next()
        try {
          const system = new URL(req.url, 'http://localhost').searchParams.get('system')
          return json(res, 200, await resilienceState(system))
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
