// Vite dev-server plugin: drive "simulations" against the running system.
//
//   GET  /api/test/load?system=<id>   -> { running, rps, startedAt }
//   POST /api/test/load  { system, action: 'start'|'stop', rps }
//
// The first (and for now only) simulation is "generate load": it runs the
// system's load.sh, which hammers the nginx LB in an infinite loop so the
// metrics move. Because that loop only stops when killed, the process is spawned
// detached (its own process group) and tracked per system so we can stop it.
//
// Mirrors the other server plugins: same origin, same dev-server port, no CORS.
import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import { systemDir, isValidSystem } from './systems.js'

// system id -> { proc, rps, startedAt }
const loaders = new Map()

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > 100_000) reject(new Error('request body too large'))
    })
    req.on('end', () => {
      if (!data) return resolve({})
      try {
        resolve(JSON.parse(data))
      } catch {
        reject(new Error('invalid JSON body'))
      }
    })
    req.on('error', reject)
  })
}

const LB_BASE = 'http://localhost:8080'
const HTTP_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'])
const PATH_RE = /^\/[A-Za-z0-9._~\-/]*$/

function status(system) {
  const e = loaders.get(system)
  return e
    ? { running: true, rps: e.rps, method: e.method, path: e.path, startedAt: e.startedAt }
    : { running: false }
}

function stopLoad(system) {
  const e = loaders.get(system)
  if (!e) return
  loaders.delete(system)
  try {
    // Negative pid kills the whole process group (bash + curl + sleep).
    process.kill(-e.proc.pid, 'SIGTERM')
  } catch {
    /* already gone */
  }
}

function startLoad(system, { rps, method, path: urlPath }) {
  // Restart cleanly if one is already running (lets the user change the
  // rate/target).
  stopLoad(system)

  const delay = (1 / rps).toFixed(4) // load.sh takes seconds-between-requests
  const proc = spawn('bash', ['load.sh', String(delay)], {
    cwd: systemDir(system),
    detached: true, // own process group so we can kill the whole loop
    stdio: 'ignore',
    // load.sh reads URL + METHOD from the environment.
    env: { ...process.env, URL: `${LB_BASE}${urlPath}`, METHOD: method },
  })
  proc.unref()
  proc.on('exit', () => {
    // Only clear if this is still the tracked process (not a newer one).
    if (loaders.get(system)?.proc === proc) loaders.delete(system)
  })
  loaders.set(system, { proc, rps, method, path: urlPath, startedAt: Date.now() })
  return status(system)
}

export default function simulate() {
  return {
    name: 'simulate',
    configureServer(server) {
      server.middlewares.use('/api/test/load', async (req, res, next) => {
        const json = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(400, { ok: false, error: 'unknown system' })
            return json(200, { ok: true, ...status(system) })
          }
          if (req.method === 'POST') {
            const { system, action, rps, method, path: urlPath } = await readJsonBody(req)
            if (!isValidSystem(system)) return json(400, { ok: false, error: 'unknown system' })
            if (!fs.existsSync(path.join(systemDir(system), 'load.sh'))) {
              return json(400, { ok: false, error: 'this system has no load.sh' })
            }
            if (action === 'stop') {
              stopLoad(system)
              return json(200, { ok: true, ...status(system) })
            }
            if (action === 'start') {
              const n = Number(rps)
              if (!Number.isFinite(n) || n < 1 || n > 500) {
                return json(400, { ok: false, error: 'rps must be between 1 and 500' })
              }
              const m = String(method || 'GET').toUpperCase()
              if (!HTTP_METHODS.has(m)) return json(400, { ok: false, error: `unsupported method "${m}"` })
              const p = urlPath || '/service-1/health'
              if (typeof p !== 'string' || !PATH_RE.test(p)) {
                return json(400, { ok: false, error: 'invalid target path' })
              }
              return json(200, { ok: true, ...startLoad(system, { rps: n, method: m, path: p }) })
            }
            return json(400, { ok: false, error: 'unknown action' })
          }
          return next()
        } catch (err) {
          json(500, { ok: false, error: err.message })
        }
      })

      // Don't leave a load loop running after the dev server stops.
      server.httpServer?.on('close', () => {
        for (const system of [...loaders.keys()]) stopLoad(system)
      })
    },
  }
}
