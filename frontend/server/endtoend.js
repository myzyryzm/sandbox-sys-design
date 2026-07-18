// Vite dev-server plugin: per-system "end-to-end processes" — named test processes the user
// defines and then RUNS. A process names a set of client methods to drive — a row for a stateless
// client carries `requestsPerSecond` (a fractional call rate: 0.1 = one call every 10s); a row
// for a STATEFUL client (manifest node stateful:true) instead carries `instances`, how many
// concurrent session-loop instances of the function to keep alive — optional websocket client
// pools to keep connected (with a configurable client count), a list of freeform "failure"
// conditions (a bug occurred if any happens) and "constraint" invariants (must never be
// violated). Defining a process is pure data entry; RUNNING it hands off to a launched Claude
// session (the sandbox-end-to-end-process skill), which does all the real work — driving the
// methods for a duration, spawning the ws pools, synthesizing arguments, evaluating the
// conditions, and writing a run report.
//
//   GET    /api/endtoend?system=<id>
//     -> { ok, processes: [{ id, name, client_list, websocket_list, failure_list, constraint_list,
//                            createdAt, updatedAt, lastRun? }], run }
//        `run` is { running:false } or { running:true, id, name, startedAt, durationSeconds,
//        remaining_seconds }. `lastRun` is the newest persisted run report for that process.
//        client_list rows are normalized to each client's CURRENT stateful mode on read (a legacy
//        intervalSeconds row surfaces as requestsPerSecond = 1/interval; a row saved before a
//        stateful flip surfaces in the flag's shape) — the file itself is only rewritten on upsert.
//   POST   /api/endtoend  { system, id?, name, client_list, websocket_list?, failure_list,
//                           constraint_list }
//     -> upsert a process definition. No/unknown id creates (uuid + createdAt); a known id updates
//        in place (createdAt preserved, updatedAt bumped). 409 if the id is currently running.
//   DELETE /api/endtoend  { system, id }   -> remove a process (409 if it's running).
//   POST   /api/endtoend/start  { system, id, duration_seconds }
//     -> mark the process running (in-memory flag + a generous self-healing backstop timer), one
//        run at a time per system (409 otherwise). The launched Claude session polls the GET to
//        know when to halt; the flag is a pure COORDINATION marker — the backend spawns nothing.
//   POST   /api/endtoend/stop   { system, id? }   -> clear the running flag (idempotent; if `id`
//        is given, only clears when it matches the running id, so a stale session can't stop a
//        newer run).
//
// Mirrors scenarios.js (registry read/write + validators) and outage.js (in-memory tracked
// state + a timer + cleanup when the dev server closes). Same-origin, same dev-server port,
// no CORS. Unlike outage there is NO process/container to kill — `stop` is just map.delete.
import fs from 'node:fs'
import path from 'node:path'
import { randomUUID } from 'node:crypto'
import { systemDir, isValidSystem } from './systems.js'
import { bad, readJsonBody, HttpError } from './scaffold.js'

const conflict = (msg) => new HttpError(409, msg)
const notFound = (msg) => new HttpError(404, msg)

// A client method name is a Python-style identifier (mirrors scenarios.js FUNCTION_NAME_RE).
const METHOD_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
const MAX_NAME = 120
const MAX_ROWS = 20
const MAX_COND = 500
// client_list bounds — a stateless row is a call rate in req/s (fractional; the floor covers a
// legacy intervalSeconds of 60 → 1/60 ≈ 0.02); a stateful client's row is instead how many
// concurrent session-loop instances to keep alive (each instance is one python subprocess).
const MIN_RPS = 0.01
const MAX_RPS = 20
const MAX_INSTANCES = 20
// websocket_list bounds — mirror websockets.js' pool-run route (each pool client
// is one host fd; the macOS default soft ulimit is often 256).
const MAX_WS_CLIENTS = 200
const MAX_WS_RATE = 20
const MAX_DURATION = 600

// system id -> { id, name, startedAt, durationSeconds, timer }. A pure coordination flag: only one
// end-to-end run may be active per system (matches the single shared terminal / edit queue).
const runs = new Map()

// --- registry (systems/<id>/endtoend.json) --------------------------------------

function endtoendFile(system) {
  return path.join(systemDir(system), 'endtoend.json')
}
// Tolerate an absent/garbled file (a system with no processes yet has an empty list).
function readProcesses(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(endtoendFile(system), 'utf8'))
    return Array.isArray(raw?.processes) ? { processes: raw.processes } : { processes: [] }
  } catch {
    return { processes: [] }
  }
}
function writeProcesses(system, data) {
  fs.writeFileSync(endtoendFile(system), JSON.stringify(data, null, 2) + '\n')
}
// Inlined readers (scenarios.js doesn't export its own) for the (client, method) existence check.
function readScenarioFunctions(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'scenarios.json'), 'utf8'))
    return Array.isArray(raw?.functions) ? raw.functions : []
  } catch {
    return []
  }
}
function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}

// --- run reports (systems/<id>/endtoend-runs/*.json) ----------------------------
//
// The launched session writes one report file per run (named ${PROCESS_NAME}_${TIMESTAMP}.json)
// carrying at least { processId, verdict, endedAt }. For the process list we surface each process's
// NEWEST report. Read the dir once, newest-file-first, and take the first report seen per process id.
function latestRuns(system, processIds) {
  const dir = path.join(systemDir(system), 'endtoend-runs')
  const want = new Set(processIds)
  const out = {}
  let entries
  try {
    entries = fs.readdirSync(dir)
      .filter((f) => f.endsWith('.json'))
      .map((f) => {
        const full = path.join(dir, f)
        let mtime = 0
        try {
          mtime = fs.statSync(full).mtimeMs
        } catch {
          /* raced deletion — treat as oldest */
        }
        return { f, full, mtime }
      })
      .sort((a, b) => b.mtime - a.mtime)
  } catch {
    return out // no reports dir yet
  }
  for (const { f, full } of entries) {
    if (want.size === 0) break
    let rep
    try {
      rep = JSON.parse(fs.readFileSync(full, 'utf8'))
    } catch {
      continue
    }
    const pid = rep?.processId
    if (!pid || !want.has(pid)) continue
    out[pid] = { verdict: rep.verdict ?? null, endedAt: rep.endedAt ?? null, file: f }
    want.delete(pid)
  }
  return out
}

// --- run-state -----------------------------------------------------------------

function remainingSeconds(until) {
  return Math.max(0, Math.ceil((until - Date.now()) / 1000))
}
function runStatus(system) {
  const e = runs.get(system)
  if (!e) return { running: false }
  return {
    running: true,
    id: e.id,
    name: e.name,
    startedAt: e.startedAt,
    durationSeconds: e.durationSeconds,
    remaining_seconds: remainingSeconds(e.startedAt + e.durationSeconds * 1000),
  }
}
function clearRun(system) {
  const e = runs.get(system)
  if (e) {
    clearTimeout(e.timer)
    runs.delete(system)
  }
}

// For the interview reset (interview.js): it must refuse to wipe a system mid-run,
// and clear a stale flag before wiping. The map itself stays module-private.
export function isRunActive(system) {
  return runs.has(system)
}
export function clearRunState(system) {
  clearRun(system)
}

// --- validation -----------------------------------------------------------------

function validateProcessInput(system, body) {
  const name = typeof body.name === 'string' ? body.name.trim() : ''
  if (!name) throw bad('process name is required')
  if (name.length > MAX_NAME) throw bad(`process name is too long (max ${MAX_NAME})`)

  const rawClients = Array.isArray(body.client_list) ? body.client_list : []
  const rawWs = body.websocket_list == null ? [] : body.websocket_list
  if (!Array.isArray(rawWs)) throw bad('websocket_list must be an array')
  if (rawClients.length === 0 && rawWs.length === 0) {
    throw bad('the process needs at least one client method or websocket client pool')
  }
  if (rawClients.length > MAX_ROWS) throw bad(`client_list can have at most ${MAX_ROWS} rows`)
  if (rawWs.length > MAX_ROWS) throw bad(`websocket_list can have at most ${MAX_ROWS} rows`)

  // The set of (client, method) pairs that actually exist, and the set of client nodes.
  const functions = readScenarioFunctions(system)
  const manifest = readManifest(system)
  const clientIds = new Set(manifest.nodes.filter((n) => n.type === 'client').map((n) => n.id))
  const statefulIds = new Set(
    manifest.nodes.filter((n) => n.type === 'client' && n.stateful).map((n) => n.id),
  )
  const pairExists = (client, method) =>
    functions.some((f) => f && f.client === client && f.name === method)

  const client_list = rawClients.map((r) => {
    const client = typeof r?.client === 'string' ? r.client : ''
    const method = typeof r?.method === 'string' ? r.method : ''
    if (!client || !method) throw bad('each client_list row needs a client and a method')
    if (!METHOD_RE.test(method)) throw bad(`method "${method}" is not a valid function name`)
    if (!clientIds.has(client)) throw bad(`"${client}" is not a client in this system`)
    if (!pairExists(client, method)) throw bad(`client "${client}" has no method "${method}"`)
    if (statefulIds.has(client)) {
      // Stateful row: how many concurrent instances of the function to keep running.
      const instances = Number(r?.instances)
      if (!Number.isInteger(instances) || instances < 1 || instances > MAX_INSTANCES) {
        throw bad(
          `instances for "${client}.${method}" must be a whole number between 1 and ${MAX_INSTANCES} ` +
            '(this client is stateful — the row sets how many concurrent instances to keep running)',
        )
      }
      return { client, method, instances }
    }
    // Stateless row: a call rate. Accept a legacy intervalSeconds from raw API callers as its
    // reciprocal (call every N seconds ≡ 1/N req/s).
    const rps =
      r?.requestsPerSecond != null
        ? Number(r.requestsPerSecond)
        : r?.intervalSeconds != null
          ? 1 / Number(r.intervalSeconds)
          : NaN
    if (!Number.isFinite(rps) || rps < MIN_RPS || rps > MAX_RPS) {
      throw bad(`requestsPerSecond for "${client}.${method}" must be a number between ${MIN_RPS} and ${MAX_RPS}`)
    }
    return { client, method, requestsPerSecond: Math.round(rps * 100) / 100 }
  })

  // WebSocket client pools: how many pool clients to spawn (the run session drives
  // `node ws-clients/<client>.mjs --count <clientCount>` for the run's duration).
  // Bounds mirror the /api/websockets/run route: each pool client is one host fd.
  const websocket_list = rawWs.map((r) => {
    const client = typeof r?.client === 'string' ? r.client : ''
    const node = manifest.nodes.find(
      (n) => n.id === client && n.type === 'client' && n.origin === 'create-websockets',
    )
    if (!node) throw bad(`"${client}" is not a websocket client in this system`)
    const clientCount = Number(r?.clientCount)
    if (!Number.isInteger(clientCount) || clientCount < 1 || clientCount > MAX_WS_CLIENTS) {
      throw bad(`clientCount for "${client}" must be a whole number between 1 and ${MAX_WS_CLIENTS}`)
    }
    const messagesPerSecond = r?.messagesPerSecond == null ? 1 : Number(r.messagesPerSecond)
    if (!Number.isInteger(messagesPerSecond) || messagesPerSecond < 1 || messagesPerSecond > MAX_WS_RATE) {
      throw bad(`messagesPerSecond for "${client}" must be a whole number between 1 and ${MAX_WS_RATE}`)
    }
    return { client, clientCount, messagesPerSecond }
  })

  const cleanConditions = (raw, label) => {
    if (raw == null) return []
    if (!Array.isArray(raw)) throw bad(`${label} must be an array`)
    const list = raw
      .map((s) => (typeof s === 'string' ? s.trim() : ''))
      .filter(Boolean)
    if (list.length > MAX_ROWS) throw bad(`${label} can have at most ${MAX_ROWS} rows`)
    for (const s of list) if (s.length > MAX_COND) throw bad(`a ${label} entry is too long (max ${MAX_COND})`)
    return list
  }
  const failure_list = cleanConditions(body.failure_list, 'failure_list')
  const constraint_list = cleanConditions(body.constraint_list, 'constraint_list')

  return { name, client_list, websocket_list, failure_list, constraint_list }
}

// --- operations -----------------------------------------------------------------

// Normalize a stored client_list row to the shape matching its client's CURRENT stateful mode —
// legacy intervalSeconds rows and rows saved before a stateful flip both surface in the new shape
// (the GET feeds the modal's edit form). Read-only: the file is only rewritten by an upsert.
function normalizeClientRow(row, statefulIds) {
  const { client, method } = row
  if (statefulIds.has(client)) {
    const instances = Number(row.instances)
    return {
      client,
      method,
      instances: Number.isInteger(instances) && instances >= 1 ? Math.min(instances, MAX_INSTANCES) : 1,
    }
  }
  const raw =
    row.requestsPerSecond != null
      ? Number(row.requestsPerSecond)
      : Number(row.intervalSeconds) > 0
        ? 1 / Number(row.intervalSeconds)
        : 1
  const rps = Number.isFinite(raw) ? Math.min(Math.max(raw, MIN_RPS), MAX_RPS) : 1
  return { client, method, requestsPerSecond: Math.round(rps * 100) / 100 }
}
// The manifest read can race a system teardown — degrade to "no stateful clients" rather than 500.
function statefulClientIds(system) {
  try {
    return new Set(
      readManifest(system)
        .nodes.filter((n) => n.type === 'client' && n.stateful)
        .map((n) => n.id),
    )
  } catch {
    return new Set()
  }
}

function listProcesses(system) {
  const { processes } = readProcesses(system)
  const statefulIds = statefulClientIds(system)
  const runsByProcess = latestRuns(system, processes.map((p) => p.id))
  const withRuns = processes.map((p) => ({
    ...p,
    client_list: (p.client_list || []).map((r) => normalizeClientRow(r, statefulIds)),
    lastRun: runsByProcess[p.id] || null,
  }))
  return { ok: true, processes: withRuns, run: runStatus(system) }
}

function upsertProcess(body) {
  if (!isValidSystem(body.system)) throw bad(`unknown system "${body.system}"`)
  const input = validateProcessInput(body.system, body)
  const data = readProcesses(body.system)
  const now = new Date().toISOString()
  const id = typeof body.id === 'string' && body.id ? body.id : ''

  if (id) {
    const i = data.processes.findIndex((p) => p && p.id === id)
    if (i < 0) throw notFound(`unknown process "${id}"`)
    const running = runs.get(body.system)
    if (running && running.id === id) throw conflict('stop the process before editing it')
    const prev = data.processes[i]
    const next = { ...prev, ...input, id, createdAt: prev.createdAt || now, updatedAt: now }
    data.processes[i] = next
    writeProcesses(body.system, data)
    return { ok: true, process: next }
  }

  const created = { id: randomUUID(), ...input, createdAt: now, updatedAt: now }
  data.processes.push(created)
  writeProcesses(body.system, data)
  return { ok: true, process: created }
}

function deleteProcess(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof id !== 'string' || !id) throw bad('id is required')
  const running = runs.get(system)
  if (running && running.id === id) throw conflict('stop the process before deleting it')
  const data = readProcesses(system)
  const next = data.processes.filter((p) => !(p && p.id === id))
  const removed = next.length !== data.processes.length
  writeProcesses(system, { processes: next })
  return { ok: true, removed }
}

function startRun(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof id !== 'string' || !id) throw bad('id is required')
  const proc = readProcesses(system).processes.find((p) => p && p.id === id)
  if (!proc) throw notFound(`unknown process "${id}"`)
  const duration = Number(body.duration_seconds)
  if (!Number.isInteger(duration) || duration < 1 || duration > MAX_DURATION) {
    throw bad(`duration_seconds must be a whole number between 1 and ${MAX_DURATION}`)
  }
  if (runs.has(system)) throw conflict('a process is already running in this system')

  // A GENEROUS self-healing backstop, NOT a hard deadline: the launched session enforces the real
  // duration itself and needs wall-clock time beyond it (reading files, authoring the orchestrator,
  // evaluating conditions, writing the report) — and may sit queued behind other edits before it
  // even starts. The timer's only job is to clear a permanently-stuck flag if the session dies.
  const backstopMs = Math.max(duration * 3, duration + 300) * 1000
  const timer = setTimeout(() => clearRun(system), backstopMs)
  timer.unref?.()
  runs.set(system, { id, name: proc.name, startedAt: Date.now(), durationSeconds: duration, timer })
  return { ok: true, run: runStatus(system) }
}

function stopRun(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const e = runs.get(system)
  // Only stop when no id is given (a blanket stop) or it matches the running run — so a lingering
  // old session can't stop a freshly started one.
  if (e && (!id || e.id === id)) clearRun(system)
  return { ok: true, run: runStatus(system) }
}

export default function endtoend() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  const wrap = (fn) => async (req, res) => {
    try {
      json(res, 200, await fn(await readJsonBody(req)))
    } catch (err) {
      json(res, err.statusCode || 500, { ok: false, error: err.message })
    }
  }
  return {
    name: 'endtoend',
    configureServer(server) {
      // Sub-routes first — Connect matches by prefix, so /api/endtoend would otherwise swallow these.
      server.middlewares.use('/api/endtoend/start', (req, res, next) =>
        req.method === 'POST' ? wrap(startRun)(req, res) : next(),
      )
      server.middlewares.use('/api/endtoend/stop', (req, res, next) =>
        req.method === 'POST' ? wrap(stopRun)(req, res) : next(),
      )
      server.middlewares.use('/api/endtoend', (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(res, 400, { ok: false, error: 'unknown system' })
            return json(res, 200, listProcesses(system))
          }
          if (req.method === 'POST') return wrap(upsertProcess)(req, res)
          if (req.method === 'DELETE') return wrap(deleteProcess)(req, res)
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })

      // Don't leave a stale "running" flag (with its backstop timer) after the dev server stops.
      server.httpServer?.on('close', () => {
        for (const system of [...runs.keys()]) clearRun(system)
      })
    },
  }
}
