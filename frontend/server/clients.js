// Vite dev-server plugin: external "clients" — callers that live OUTSIDE the system.
// A client is just a manifest node (no container, no compose / nginx / prometheus /
// rebuild); the "real" behavior lives in its per-client functions (scenarios.js): a client
// owns named functions whose authored call steps fire real calls through the load balancer.
//
//   GET    /api/clients?system=<id>
//     -> { ok, clients: [{ id }] }   (manifest client nodes)
//   POST   /api/clients  { system, name }
//     -> appends a `client` node to manifest.json (instant — no docker). Returns { ok, node }.
//   DELETE /api/clients  { system, id }
//     -> drops the manifest node (and its edges). No docker.
//
// A client is a *caller*, not a callee: it serves nothing, so it has no endpoints, no
// gRPC, no metrics/health, and no container. Its functions (and their authored call steps)
// are owned by the scenarios plugin, keyed by this client's id.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem, nextClientPosition } from './systems.js'
import { bad, NAME_RE, readJsonBody } from './scaffold.js'
import { scaffoldClientScript, removeClientScript } from './clientScript.js'
import { removeWsClientScript } from './websockets.js'

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

function deleteClient(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findClient(manifest, id)
  if (!node) throw bad(`"${id}" is not a client in this system`)
  manifest.nodes = manifest.nodes.filter((n) => n.id !== id)
  manifest.edges = (manifest.edges || []).filter((e) => e.from !== id && e.to !== id)
  writeManifest(system, manifest)
  // Drop its script: a websocket client's behavior is its host pool script in
  // ws-clients/; an HTTP client's is its python module (the shared lbclient.py stays).
  if (node.origin === 'create-websockets') removeWsClientScript(system, id)
  else removeClientScript(system, id)
  return { ok: true, removed: id }
}

function listClients(system) {
  const manifest = readManifest(system)
  const clients = manifest.nodes
    .filter((n) => n.type === 'client')
    .map((n) => ({ id: n.id }))
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
      server.middlewares.use('/api/clients', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(res, 400, { ok: false, error: 'unknown system' })
            return json(res, 200, listClients(system))
          }
          if (req.method === 'POST') return wrap(createClient)(req, res)
          if (req.method === 'DELETE') return wrap(deleteClient)(req, res)
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
