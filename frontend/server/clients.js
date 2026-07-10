// Vite dev-server plugin: external "clients" — callers that live OUTSIDE the system.
// A client is just a manifest node (no container, no compose / nginx / prometheus /
// rebuild); the "real" behavior lives in its per-client functions (scenarios.js): a client
// owns named functions whose authored call steps fire real calls through the load balancer.
//
//   GET    /api/clients?system=<id>
//     -> { ok, clients: [{ id, stateful }] }   (manifest client nodes)
//   POST   /api/clients  { system, name, stateful? }
//     -> appends a `client` node to manifest.json (instant — no docker). Returns { ok, node }.
//   PATCH  /api/clients  { system, id, stateful }
//     -> flip a client's stateful mode on its manifest node. No docker. Returns { ok, node }.
//   DELETE /api/clients  { system, id }
//     -> drops the manifest node (and its edges) + its script/state file. No docker.
//   GET    /api/clients/state?system=<id>&id=<client>
//     -> { ok, state: { values, history }, stateful }   (a stateful client's accumulated store)
//   DELETE /api/clients/state  { system, id }
//     -> clears (deletes) that client's on-disk state file. Returns { ok, cleared }.
//
// A client is STATELESS by default (fire-and-forget: each function run is a one-shot python3
// subprocess that persists nothing — today's behavior). A STATEFUL client instead loads + saves a
// durable per-client store (clients/<module>.state.json) across runs; the runner enables it by
// setting LB_CLIENT_STATE for that subprocess (scenarios.js), and lbclient.py's `state` reads it.
//
// A client is a *caller*, not a callee: it serves nothing, so it has no endpoints, no
// gRPC, no metrics/health, and no container. Its functions (and their authored call steps)
// are owned by the scenarios plugin, keyed by this client's id.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem, nextClientPosition } from './systems.js'
import { bad, NAME_RE, readJsonBody } from './scaffold.js'
import {
  scaffoldClientScript,
  removeClientScript,
  clientStatePath,
  readClientState,
  clientModule,
  clientsDir,
} from './clientScript.js'

// A truthy stateful flag from a request body (accepts a boolean or the string "true").
function parseStateful(v) {
  return v === true || v === 'true'
}

function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
}

function findClient(manifest, id) {
  return manifest.nodes.find((n) => n.id === id && n.type === 'client')
}

function createClient(body) {
  const { system, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof name !== 'string' || !NAME_RE.test(name) || name.length > 40) {
    throw bad('name must be lowercase letters, digits and hyphens (start with a letter)')
  }
  const manifest = readManifest(system)
  if (manifest.nodes.some((n) => n.id === name)) {
    throw bad(`a node named "${name}" already exists in this system`)
  }
  const node = {
    id: name,
    label: name,
    type: 'client',
    origin: 'create-client',
    external: true,
    // Stateless (fire-and-forget) unless the user opts into a durable per-client store.
    stateful: parseStateful(body.stateful),
    position: nextClientPosition(manifest),
    metrics: [],
  }
  manifest.nodes.push(node)
  writeManifest(system, manifest)
  // A client's behavior is a real Python script it runs; scaffold it (and the shared lb
  // helper) now so the file always exists for authoring sessions and the runner.
  scaffoldClientScript(system, name)
  return { ok: true, node }
}

// Flip a client's stateful mode. Pure manifest edit (no docker). Toggling OFF leaves any existing
// state file inert on disk (the runner just stops setting LB_CLIENT_STATE) — clear it explicitly
// via DELETE /api/clients/state, or it's reused if the client is toggled back on.
function updateClient(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findClient(manifest, id)
  if (!node) throw bad(`"${id}" is not a client in this system`)
  node.stateful = parseStateful(body.stateful)
  writeManifest(system, manifest)
  return { ok: true, node }
}

// A stateful client's accumulated store (empty when it has none yet). `stateful` echoes the node's
// mode so the UI can tell "no state yet" from "not a stateful client".
function getClientState(system, id) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findClient(manifest, id)
  if (!node) throw bad(`"${id}" is not a client in this system`)
  return { ok: true, stateful: !!node.stateful, state: readClientState(system, id) }
}

// Clear a client's durable store (delete the file). Its next stateful run starts fresh. Also
// removes the per-instance stores (<module>.i<N>.state.json) an end-to-end run's stateful
// instance pool creates for instances 2..N (instance 1 uses the canonical store). Exact-module
// match, so clearing "frontend" can't eat "frontend-admin"'s files.
function clearClientState(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  if (!findClient(manifest, id)) throw bad(`"${id}" is not a client in this system`)
  fs.rmSync(clientStatePath(system, id), { force: true })
  const instanceRe = new RegExp(`^${clientModule(id)}\\.i\\d+\\.state\\.json$`)
  let entries = []
  try {
    entries = fs.readdirSync(clientsDir(system))
  } catch {
    /* no clients dir yet — nothing to clear */
  }
  for (const f of entries) {
    if (instanceRe.test(f)) fs.rmSync(path.join(clientsDir(system), f), { force: true })
  }
  return { ok: true, cleared: true }
}

function deleteClient(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findClient(manifest, id)
  if (!node) throw bad(`"${id}" is not a client in this system`)
  // A websocket pool client is tier-owned: it only goes away with the whole tier
  // (the remove.js cascade from the lb, which also cleans up its script + stats).
  if (node.origin === 'create-websockets') {
    throw bad(`"${id}" is part of the "${node.wsTier}" websocket tier — delete the whole websocket process from its load balancer "${node.wsTier}"`)
  }
  manifest.nodes = manifest.nodes.filter((n) => n.id !== id)
  manifest.edges = (manifest.edges || []).filter((e) => e.from !== id && e.to !== id)
  writeManifest(system, manifest)
  // Drop its python module (the shared lbclient.py stays).
  removeClientScript(system, id)
  return { ok: true, removed: id }
}

function listClients(system) {
  const manifest = readManifest(system)
  const clients = manifest.nodes
    .filter((n) => n.type === 'client')
    .map((n) => ({ id: n.id, stateful: !!n.stateful }))
  return { ok: true, clients }
}

export default function clients() {
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
    name: 'clients',
    configureServer(server) {
      // Mount the /state sub-route BEFORE /api/clients — Connect matches by path prefix, so the
      // broader mount would otherwise swallow it (same ordering the endtoend plugin relies on).
      server.middlewares.use('/api/clients/state', (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const q = new URL(req.url, 'http://localhost').searchParams
            return json(res, 200, getClientState(q.get('system'), q.get('id')))
          }
          if (req.method === 'DELETE') return wrap(clearClientState)(req, res)
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
      server.middlewares.use('/api/clients', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(res, 400, { ok: false, error: 'unknown system' })
            return json(res, 200, listClients(system))
          }
          if (req.method === 'POST') return wrap(createClient)(req, res)
          if (req.method === 'PATCH') return wrap(updateClient)(req, res)
          if (req.method === 'DELETE') return wrap(deleteClient)(req, res)
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
