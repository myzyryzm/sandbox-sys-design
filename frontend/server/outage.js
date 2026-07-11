// Vite dev-server plugin: temporarily take a node offline ("shut down for N seconds").
//
//   GET    /api/outage?system=<id>
//     -> { ok, outages: [{ node, until, remaining_seconds }] }  the nodes currently
//        in a user-initiated outage (the diagram polls this to paint them orange).
//   POST   /api/outage  { system, node, duration_seconds }
//     -> stops the node's container (`docker compose stop`) so it rejects all inbound
//        connections, schedules an automatic `docker compose start` after the window,
//        and returns the new status. 1 <= duration_seconds <= 300.
//   DELETE /api/outage  { system, node }   ("bring back now")
//     -> cancels the timer and restarts the container immediately.
//
// Why stop the *container* (not an app-level "refuse"): "not accepting connections"
// has to apply uniformly to a service, a database, and an event stream — only taking
// the container down does that for all node types. A stopped container's port is
// closed, so callers get connection-refused and the LB returns 502.
//
// Keeps in-memory tracked state + timers + cleanup when the dev server closes, and
// reuses the docker-compose-via-execFile shape from dbschema.js (arg arrays, never a
// shell string). Node id == compose service name throughout this app.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { repoRoot, systemsDir, systemDir, isValidSystem } from './systems.js'

const pexec = promisify(execFile)

// system id -> Map(nodeId -> { until, timer })
const outages = new Map()

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
      if (data.length > 100_000) reject(bad('request body too large'))
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

// `docker compose -f <system compose> <args...>` from the repo root. Same invocation
// shape as dbschema.js: an arg array (no shell), bounded timeout.
function compose(system, ...args) {
  const composeFile = path.join(systemsDir, system, 'docker-compose.yml')
  return pexec('docker', ['compose', '-f', composeFile, ...args], {
    cwd: repoRoot,
    timeout: 30_000,
    maxBuffer: 4 * 1024 * 1024,
  })
}

// The set of nodes a user may take down: services, external services, databases,
// and event streams — the same rule the diagram uses to decide a node is deletable.
// Shutting down an external service simulates the third-party API going dark, which
// is exactly when a circuit breaker on the calling service should trip. The load
// balancer and metric exporters are infrastructure and are not controllable here.
function isControllable(node) {
  return (
    node.type === 'service' ||
    node.type === 'external_service' ||
    node.origin === 'create-database' ||
    node.origin === 'create-event-stream' ||
    // The etcd cluster node: shutdown kills ALL members at once (total quorum loss —
    // registrations error-loop until it's back). Per-member kills live in the
    // Cluster tab (/api/etcd/member), not here.
    node.origin === 'create-etcd' ||
    // WebSocket-tier nodes are real containers too (kill a relay and watch the
    // haproxy lb shift sessions) — except the client, which runs on the host.
    (node.origin === 'create-websockets' && node.type !== 'client')
  )
}

// The compose services a node's outage acts on: an etcd node maps to its N member
// containers (the node id itself is not a compose service); everything else is 1:1.
function containersOf(system, node) {
  try {
    const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
    const target = manifest.nodes.find((n) => n.id === node)
    if (target?.origin === 'create-etcd' && target.etcd?.members?.length) return [...target.etcd.members]
    // A clustered redis maps to its member containers (etcd semantics: total outage).
    // A sentinel-replicated primary stays 1:1 ON PURPOSE — killing only the primary
    // container is the failover demo (the sentinels detect it and promote a replica).
    if (target?.redisCluster?.members?.length) return [...target.redisCluster.members]
  } catch {
    /* fall through to the 1:1 default */
  }
  return [node]
}

function remainingSeconds(until) {
  return Math.max(0, Math.ceil((until - Date.now()) / 1000))
}

function status(system) {
  const m = outages.get(system)
  const list = m
    ? [...m.entries()].map(([node, e]) => ({
        node,
        until: e.until,
        remaining_seconds: remainingSeconds(e.until),
      }))
    : []
  return { ok: true, outages: list }
}

function clearEntry(system, node) {
  const m = outages.get(system)
  const e = m?.get(node)
  if (e) {
    clearTimeout(e.timer)
    m.delete(node)
    if (m.size === 0) outages.delete(system)
  }
}

async function startOutage(body) {
  const { system, node } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const target = manifest.nodes.find((n) => n.id === node)
  if (!target) throw bad(`"${node}" is not a node in this system`)
  if (!isControllable(target)) throw bad(`"${node}" can't be shut down (only services, databases and event streams can)`)

  const duration = Number(body.duration_seconds)
  if (!Number.isInteger(duration) || duration < 1 || duration > 300) {
    throw bad('duration_seconds must be a whole number between 1 and 300')
  }

  // Replace any existing outage for this node (re-arm the timer / extend the window).
  clearEntry(system, node)

  const containers = containersOf(system, node)
  try {
    // `kill` (SIGKILL), not a graceful `stop`: an outage should reject inbound
    // connections *immediately* for the whole window, with no graceful-drain tail
    // that could still serve a request in the first second. `docker compose start`
    // brings the container back; databases/streams do crash recovery on restart.
    await compose(system, 'kill', ...containers)
  } catch (err) {
    const detail = `${err.stderr || ''}${err.stdout || ''}`.trim() || err.message
    throw new HttpError(502, `could not shut down "${node}": ${detail}`)
  }

  const until = Date.now() + duration * 1000
  const timer = setTimeout(() => {
    // Window elapsed: bring the node back and forget the outage. Errors here are
    // logged but can't be surfaced (no request in flight); the node simply stays down.
    compose(system, 'start', ...containers).catch((err) => {
      console.warn(`outage: failed to restart "${node}" in ${system}:`, err.message)
    })
    const m = outages.get(system)
    if (m?.get(node)?.timer === timer) {
      m.delete(node)
      if (m.size === 0) outages.delete(system)
    }
  }, duration * 1000)
  // Don't let a pending restart timer keep the dev server's event loop alive.
  timer.unref?.()

  if (!outages.has(system)) outages.set(system, new Map())
  outages.get(system).set(node, { until, timer, containers })
  return status(system)
}

async function endOutage(body) {
  const { system, node } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  clearEntry(system, node)
  try {
    await compose(system, 'start', ...containersOf(system, node))
  } catch (err) {
    const detail = `${err.stderr || ''}${err.stdout || ''}`.trim() || err.message
    throw new HttpError(502, `could not restart "${node}": ${detail}`)
  }
  return { ok: true, restored: true, ...status(system) }
}

export default function outage() {
  return {
    name: 'node-outage',
    configureServer(server) {
      const json = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }

      server.middlewares.use('/api/outage', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
            return json(res, 200, status(system))
          }
          if (req.method === 'POST') {
            return json(res, 200, await startOutage(await readJsonBody(req)))
          }
          if (req.method === 'DELETE') {
            return json(res, 200, await endOutage(await readJsonBody(req)))
          }
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })

      // Never leave a container stopped when the dev server goes away: restart every
      // node still in an outage (best-effort cleanup on shutdown).
      server.httpServer?.on('close', () => {
        for (const [system, m] of outages) {
          for (const [node, e] of m) {
            clearTimeout(e.timer)
            compose(system, 'start', ...(e.containers || [node])).catch(() => {})
          }
        }
        outages.clear()
      })
    },
  }
}
