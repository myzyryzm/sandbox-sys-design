// Vite dev-server plugin: delete a service or database from the active system.
//
//   POST /api/delete  { system, id }
//
// The inverse of services.js / databases.js: it strips the component out of
// docker-compose.yml (and, for a service, nginx.conf), prometheus.yml and
// manifest.json, removes its folder, then reconciles the running stack with
// `docker compose up -d --remove-orphans` (which deletes the now-orphaned
// containers) — never ./start.sh, which would kill this dev server.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemsDir, systemDir, isValidSystem } from './systems.js'
import { removeWsClientScript } from './websockets.js'
import { removeClientScript } from './clientScript.js'

const pexec = promisify(execFile)

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

function validate(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof id !== 'string' || !/^[a-z][a-z0-9-]*$/.test(id)) throw bad('invalid id')
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node) throw bad(`no node "${id}" in this system`)
  // A linked token stream (e.g. an LLM worker's redis) is owned by its service and
  // cascades with it — it is never individually deletable. Checked before the generic
  // deletable test so the error points at the owner.
  if (node.streamOf) {
    throw bad(`"${id}" is the token stream of "${node.streamOf}" — delete "${node.streamOf}" instead (the stream cascades with it)`)
  }
  const deletable =
    node.type === 'service' ||
    node.type === 'service-lb' ||
    node.type === 'external_service' ||
    node.origin === 'create-database' ||
    node.origin === 'create-event-stream' ||
    node.origin === 'create-websockets'
  if (!deletable) throw bad(`"${id}" (${node.type}) cannot be deleted`)
  // A load-balanced service's instance can't be deleted on its own — the cluster is one
  // unit, managed from the service's Load Balancing tab (change the instance count, or
  // delete the service `<name>` itself, which cascades every instance).
  if (node.instanceOf) {
    throw bad(`"${id}" is an instance of the "${node.instanceOf}" load-balanced service — change the instance count in its Load Balancing tab, or delete "${node.instanceOf}"`)
  }
  const kind =
    node.origin === 'create-websockets'
      ? 'websocket'
      : node.origin === 'create-database'
        ? 'database'
        : node.origin === 'create-event-stream'
          ? 'event-stream'
          : 'service'
  // A websocket tier is one unit: servers, bus, presence and the client pool all
  // cascade from the lb (see cascadeChildIds). Only the lb is individually
  // deletable — nothing else can validate, so the cascade loop below never has
  // to re-check its children.
  if (kind === 'websocket' && node.wsRole !== 'lb') {
    throw bad(`"${id}" is part of the "${node.wsTier}" websocket tier — delete the whole websocket process from its load balancer "${node.wsTier}"`)
  }
  return { system, id, manifest, kind }
}

// Compose services a component owns. A service is just itself; a database or an
// event stream also owns its exporter / init sidecars (the exact names
// databases.js / eventstreams.js emit). In a websocket tier only the two redis
// nodes have exporters (no `-init` sidecars); the lb and servers are just
// themselves, and the client has no container at all.
function ownedServices(id, kind, node) {
  if (kind === 'websocket') {
    return node?.wsRole === 'bus' || node?.wsRole === 'presence' ? [id, `${id}-exporter`] : [id]
  }
  // A linked token stream (streamOf) is a redis with an exporter, no init sidecar.
  if (node?.streamOf) return [id, `${id}-exporter`]
  return kind === 'database' || kind === 'event-stream'
    ? [id, `${id}-exporter`, `${id}-init`]
    : [id]
}

function removeComposeServices(system, names) {
  const file = path.join(systemDir(system), 'docker-compose.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const removed = new Set(names)

  for (const name of names) {
    if (doc.hasIn(['services', name])) doc.deleteIn(['services', name])
  }

  // Scrub any `depends_on` references to the removed services from the remaining
  // ones — a dangling depends_on makes the whole compose project invalid (e.g.
  // lb/prometheus depend on service-1). Handles both list and map forms.
  const services = doc.get('services')
  for (const pair of services?.items || []) {
    const dep = pair.value?.get?.('depends_on')
    if (!dep?.items) continue
    for (let i = dep.items.length - 1; i >= 0; i--) {
      const it = dep.items[i]
      const name = it?.key !== undefined ? String(it.key.value ?? it.key) : String(it.value ?? it)
      if (removed.has(name)) dep.items.splice(i, 1)
    }
    if (dep.items.length === 0) pair.value.delete('depends_on')
  }

  fs.writeFileSync(file, doc.toString())
}

function removeNginxRoute(system, id) {
  const file = path.join(systemDir(system), 'nginx', 'nginx.conf')
  let conf = fs.readFileSync(file, 'utf8')
  conf = conf.replace(new RegExp(` *upstream ${id} \\{[^}]*\\}\\n`), '')
  conf = conf.replace(new RegExp(` *location /${id}/ \\{[\\s\\S]*?\\n *\\}\\n`), '')
  fs.writeFileSync(file, conf)
}

function removeScrapeJob(system, id) {
  const file = path.join(systemDir(system), 'prometheus', 'prometheus.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const sc = doc.get('scrape_configs')
  const i = sc?.items?.findIndex((it) => String(it.get('job_name')) === id) ?? -1
  if (i >= 0) sc.delete(i)
  fs.writeFileSync(file, doc.toString())
}

// Mutate the manifest in memory: drop the node, its edges, and any gRPC client
// targets that pointed at it. The caller writes the manifest once when done (so
// a cascade of removals is a single write).
function scrubManifestNode(manifest, id) {
  manifest.nodes = manifest.nodes.filter((n) => n.id !== id)
  manifest.edges = (manifest.edges || []).filter((e) => e.from !== id && e.to !== id)
  // A deleted service may have been a gRPC client target of others — drop it from
  // every remaining node's grpc.clients[].targets (and prune emptied clients) so
  // no edge points at a node that no longer exists.
  for (const node of manifest.nodes) {
    if (!node.grpc?.clients) continue
    node.grpc.clients = node.grpc.clients
      .map((c) => ({ ...c, targets: (c.targets || []).filter((t) => t !== id) }))
    if (!node.grpc.servers?.length && !node.grpc.clients.length && !node.grpc.overrides?.length) {
      delete node.grpc
    }
  }
}

function writeManifest(system, manifest) {
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
}

const kindOf = (node) =>
  node?.origin === 'create-websockets'
    ? 'websocket'
    : node?.origin === 'create-database'
      ? 'database'
      : node?.origin === 'create-event-stream'
        ? 'event-stream'
        : node?.origin === 'create-cdc'
          ? 'cdc'
          : 'service'

// A removed CDC worker was registered as a producer in its target streams'
// streams.json — scrub it so no stream lists a producer that no longer exists.
function scrubProducerFromStreams(system, cluster, producerId) {
  const file = path.join(systemDir(system), cluster, 'streams.json')
  let data
  try {
    data = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return
  }
  let changed = false
  for (const t of Array.isArray(data.topics) ? data.topics : []) {
    if (!Array.isArray(t.producers)) continue
    const before = t.producers.length
    t.producers = t.producers.filter((p) => p !== producerId)
    if (t.producers.length !== before) changed = true
  }
  if (changed) fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n')
}

// A removed service may have been a Kafka CONSUMER (a consumer function registered a group
// {groupId:"<service>-<name>", members:[service]} in a cluster's streams.json) — scrub any group
// it belongs to so no topic lists a member that no longer exists. Mirrors scrubProducerFromStreams.
function scrubConsumerFromStreams(system, cluster, memberId) {
  const file = path.join(systemDir(system), cluster, 'streams.json')
  let data
  try {
    data = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return
  }
  let changed = false
  for (const t of Array.isArray(data.topics) ? data.topics : []) {
    if (!Array.isArray(t.consumers)) continue
    const before = t.consumers.length
    t.consumers = t.consumers.filter((g) => !(g && Array.isArray(g.members) && g.members.includes(memberId)))
    if (t.consumers.length !== before) changed = true
  }
  if (changed) fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n')
}

// Drop every consumer function in consumers.json that references a removed node — either as the
// owning `service` or as the `cluster` it consumes. The streams.json groups + cluster->service
// edges are scrubbed separately (the edge by scrubManifestNode, the group by the function above
// or, for a deleted cluster, by removing its folder).
function pruneConsumers(system, removedIds) {
  const file = path.join(systemDir(system), 'consumers.json')
  let data
  try {
    data = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return
  }
  if (!Array.isArray(data.consumers)) return
  const before = data.consumers.length
  data.consumers = data.consumers.filter(
    (c) => c && !removedIds.has(c.service) && !removedIds.has(c.cluster),
  )
  if (data.consumers.length !== before) fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n')
}

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return null
  }
}

// The owned children a delete cascades to — read replicas (replicaOf) and the CDC
// worker (cdcOf) of a database. They are torn down alongside the target, so they are
// never themselves "dependents" that should block the delete.
function cascadeChildIds(manifest, id, kind, node) {
  const isPrimary = kind === 'database' && !node?.replicaOf
  const secondaryIds = isPrimary
    ? manifest.nodes.filter((n) => n.replicaOf === id).map((n) => n.id)
    : []
  const cdcWorkerIds = kind === 'database'
    ? manifest.nodes.filter((n) => n.cdcOf === id).map((n) => n.id)
    : []
  // Deleting a websocket tier's lb takes the whole tier with it: every node
  // carrying `wsTier: <lb-id>` (servers, bus + presence redis, the client).
  const wsChildIds = kind === 'websocket' && node?.wsRole === 'lb'
    ? manifest.nodes.filter((n) => n.wsTier === id).map((n) => n.id)
    : []
  // Deleting a load-balanced service's cluster entry (`service-lb`) takes its whole
  // group: every instance carrying `instanceOf: <id>`. They cascade, so an instance is
  // never a "dependent" that blocks the delete.
  const svcLbInstanceIds = node?.type === 'service-lb'
    ? manifest.nodes.filter((n) => n.instanceOf === id).map((n) => n.id)
    : []
  // A service's linked token stream (e.g. an LLM worker's redis) carries
  // `streamOf: <id>` and is torn down alongside it.
  const streamIds = manifest.nodes.filter((n) => n.streamOf === id).map((n) => n.id)
  return { secondaryIds, cdcWorkerIds, wsChildIds, svcLbInstanceIds, streamIds }
}

// Reverse-dependency lookup: which OTHER nodes still call/use `id`, so deleting it
// would leave them dangling. Reads the per-system registries directly (same style as
// scrubProducerFromStreams). Returns [{ node, label, via, detail, calls }]:
//   via 'http'     — an endpoint in endpoints.json lists `id` in its downstream
//   via 'grpc'     — a manifest node's grpc.clients[] targets `id`
//   via 'kafka'    — (id is a stream) a node produces to / consumes one of its topics
//   via 'scenario' — a client function step calls /<id>/…
// `calls` are the concrete api calls into `id` (empty for structural kafka refs). The
// target itself and any `cascadeIds` (children removed alongside) are excluded.
function findDependents(system, manifest, id, kind, cascadeIds = new Set()) {
  const dir = systemDir(system)
  const byId = new Map(manifest.nodes.map((n) => [n.id, n]))
  const labelOf = (nid) => byId.get(nid)?.label || nid
  const skip = (nid) => nid === id || cascadeIds.has(nid) || !byId.has(nid)
  const deps = []

  // HTTP — endpoints.json: { service: [{ method, path, alias, downstream, downstreamMethods }] }
  const endpoints = readJson(path.join(dir, 'endpoints.json'))
  if (endpoints && typeof endpoints === 'object') {
    for (const [service, list] of Object.entries(endpoints)) {
      if (skip(service) || !Array.isArray(list)) continue
      for (const e of list) {
        if (!e || !Array.isArray(e.downstream) || !e.downstream.includes(id)) continue
        const dm = e.downstreamMethods && typeof e.downstreamMethods === 'object' ? e.downstreamMethods[id] : null
        const calls = Array.isArray(dm) ? dm.filter((c) => typeof c === 'string') : []
        const detail = e.alias || `${e.method || '?'} ${e.path || ''}`.trim()
        deps.push({ node: service, label: labelOf(service), via: 'http', detail, calls })
      }
    }
  }

  // gRPC — a manifest node's grpc.clients[{ contract, targets }] aims at `id`.
  for (const n of manifest.nodes) {
    if (skip(n.id) || !n.grpc?.clients) continue
    for (const c of n.grpc.clients) {
      if (!Array.isArray(c?.targets) || !c.targets.includes(id)) continue
      const contract = c.contract || 'gRPC'
      deps.push({ node: n.id, label: labelOf(n.id), via: 'grpc', detail: `${contract} contract`, calls: [`${contract} (gRPC)`] })
    }
  }

  // Kafka — only when `id` is the stream node: read its own streams.json topics.
  if (kind === 'event-stream') {
    const streams = readJson(path.join(dir, id, 'streams.json'))
    for (const t of streams && Array.isArray(streams.topics) ? streams.topics : []) {
      const topic = t.id || t.name || 'topic'
      for (const p of Array.isArray(t.producers) ? t.producers : []) {
        if (skip(p)) continue
        deps.push({ node: p, label: labelOf(p), via: 'kafka', detail: `produces to topic ${topic}`, calls: [] })
      }
      for (const grp of Array.isArray(t.consumers) ? t.consumers : []) {
        for (const m of Array.isArray(grp?.members) ? grp.members : []) {
          if (skip(m)) continue
          const g = grp.groupId ? ` (group ${grp.groupId})` : ''
          deps.push({ node: m, label: labelOf(m), via: 'kafka', detail: `consumes topic ${topic}${g}`, calls: [] })
        }
      }
    }
  }

  // Scenario — scenarios.json functions[].steps[] whose path is /<id>/…
  const scenarios = readJson(path.join(dir, 'scenarios.json'))
  for (const f of scenarios && Array.isArray(scenarios.functions) ? scenarios.functions : []) {
    if (skip(f?.client)) continue
    for (const step of Array.isArray(f.steps) ? f.steps : []) {
      if (typeof step?.path !== 'string') continue
      const seg = step.path.replace(/^\//, '').split('/')[0]
      if (seg !== id) continue
      deps.push({ node: f.client, label: labelOf(f.client), via: 'scenario', detail: `function ${f.name}`, calls: [`${step.method || ''} ${step.path}`.trim()] })
    }
  }

  // Consumer function — consumers.json: a loop whose `downstream` calls/reads/writes `id`. Like an
  // endpoint's downstream, this is a real outbound dependency, so deleting `id` would break it.
  const consumers = readJson(path.join(dir, 'consumers.json'))
  for (const c of consumers && Array.isArray(consumers.consumers) ? consumers.consumers : []) {
    if (skip(c?.service) || !Array.isArray(c.downstream) || !c.downstream.includes(id)) continue
    deps.push({ node: c.service, label: labelOf(c.service), via: 'consumer', detail: `function ${c.name}`, calls: [] })
  }

  // WebSocket tier — every relay server publishes through the tier's bus and reads/
  // writes its presence cache, so deleting either redis strands the tier's servers.
  // (Deleting the tier's own lb passes: the servers are in its cascade set.)
  const target = byId.get(id)
  if (target?.wsRole === 'bus' || target?.wsRole === 'presence') {
    const what = target.wsRole === 'bus' ? 'pub/sub bus' : 'presence store'
    for (const n of manifest.nodes) {
      if (skip(n.id) || n.wsTier !== target.wsTier || n.wsRole !== 'server') continue
      deps.push({ node: n.id, label: labelOf(n.id), via: 'websocket', detail: `uses this redis as its ${what}`, calls: [] })
    }
  }

  return deps
}

// A secondary being removed: reconcile its primary so it stops expecting the
// standby — drop it from postgres synchronous_standby_names, or rs.remove the
// mongo member. Best-effort (the container is going away regardless).
async function reconcileSecondaryRemoval(system, node, manifest) {
  if (process.env.DELETE_SKIP_REBUILD === '1') return
  const primaryNode = manifest.nodes.find((n) => n.id === node.replicaOf)
  if (!primaryNode) return
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
  try {
    if (primaryNode.type === 'postgres' && node.replication === 'sync') {
      const names = manifest.nodes
        .filter((n) => n.replicaOf === primaryNode.id && n.replication === 'sync' && n.id !== node.id)
        .map((n) => `"${n.id}"`)
        .join(',')
      // ALTER SYSTEM can't run in a transaction block — two separate -c commands.
      const alter = `ALTER SYSTEM SET synchronous_standby_names = '${names}';`
      await pexec('docker', ['compose', '-f', compose, 'exec', '-T', primaryNode.id, 'psql', '-U', 'sandbox', '-d', 'postgres', '-c', alter, '-c', 'SELECT pg_reload_conf();'], opts)
    } else if (primaryNode.type === 'mongodb') {
      const js = `try { rs.remove("${node.id}:27017") } catch (e) { print(e) }`
      await pexec('docker', ['compose', '-f', compose, 'exec', '-T', primaryNode.id, 'mongosh', '--quiet', '--eval', js], opts)
    }
  } catch {
    /* best-effort */
  }
}

async function rebuild(system, kind) {
  if (process.env.DELETE_SKIP_REBUILD === '1') return '(rebuild skipped)'

  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
  let log = ''
  try {
    // Reconcile: --remove-orphans deletes the containers no longer in compose.
    const up = await pexec('docker', ['compose', '-f', compose, 'up', '-d', '--remove-orphans'], opts)
    log += up.stdout + up.stderr
    if (kind === 'service') {
      const ng = await pexec('docker', ['compose', '-f', compose, 'exec', '-T', 'lb', 'nginx', '-s', 'reload'], opts)
      log += ng.stdout + ng.stderr
    }
    const r = await pexec('docker', ['compose', '-f', compose, 'restart', 'prometheus'], opts)
    log += r.stdout + r.stderr
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
  return log
}

export async function handleDelete(body) {
  const { system, id, manifest, kind } = validate(body)
  const node = manifest.nodes.find((n) => n.id === id)

  // The owned children removed in the same cascade (read replicas that stream from a
  // primary, a database's CDC worker, a websocket lb's whole tier, a load-balanced
  // service's instances) — excluded from the dependency guard below.
  const { secondaryIds, cdcWorkerIds, wsChildIds, svcLbInstanceIds, streamIds } = cascadeChildIds(manifest, id, kind, node)

  // Block the delete while another node still depends on `id` — an endpoint's HTTP
  // downstream, a gRPC client target, a Kafka producer/consumer, or a client function
  // step. The user must remove those calls first; cascaded children don't count.
  const dependents = findDependents(system, manifest, id, kind, new Set([...secondaryIds, ...cdcWorkerIds, ...wsChildIds, ...svcLbInstanceIds, ...streamIds]))
  if (dependents.length) {
    const err = bad(`Cannot delete "${id}" — ${dependents.length} node(s) still depend on it; remove those calls first.`)
    err.dependents = dependents
    throw err
  }

  // Deleting a single secondary reconciles its primary (sync list / replica-set
  // member) before teardown; a primary instead cascades its replicas + CDC worker.
  if (node?.replicaOf) await reconcileSecondaryRemoval(system, node, manifest)
  // Kafka clusters this worker may have registered as a producer in (captured
  // before the loop scrubs nodes from the manifest).
  const kafkaIds = manifest.nodes.filter((n) => n.origin === 'create-event-stream').map((n) => n.id)

  // Remove the children first (workers, secondaries, a ws lb's tier, a load-balanced
  // service's instances), then the target, in one manifest write.
  const removedIds = new Set([...cdcWorkerIds, ...secondaryIds, ...wsChildIds, ...svcLbInstanceIds, ...streamIds, id])
  for (const rid of [...cdcWorkerIds, ...secondaryIds, ...wsChildIds, ...svcLbInstanceIds, ...streamIds, id]) {
    const rnode = manifest.nodes.find((n) => n.id === rid)
    const rkind = kindOf(rnode)
    removeComposeServices(system, ownedServices(rid, rkind, rnode))
    if (rkind === 'service') removeNginxRoute(system, rid)
    if (rkind === 'cdc') for (const k of kafkaIds) scrubProducerFromStreams(system, k, rid)
    // A removed service may consume topics on other clusters (consumer functions) — scrub its
    // consumer groups from each cluster's streams.json (the deleted node's own folder is removed
    // below, so a removed cluster's groups go with it).
    if (rkind === 'service') for (const k of kafkaIds) scrubConsumerFromStreams(system, k, rid)
    // A websocket client's host pool script lives in ws-clients/, not a node folder.
    // It may ALSO own an authorable HTTP-function script in clients/ (like any client) —
    // remove both, so a recreated same-name tier doesn't inherit stale functions.
    if (rnode?.wsRole === 'client') {
      removeWsClientScript(system, rid)
      removeClientScript(system, rid)
    }
    removeScrapeJob(system, rid)
    scrubManifestNode(manifest, rid)
    fs.rmSync(path.join(systemDir(system), rid), { recursive: true, force: true })
  }
  // A deleted websocket tier also owns the shared hooks dir its servers mount
  // (ws-shared/ — fixed name, one tier per system, like ws-clients/).
  if (kind === 'websocket' && node?.wsRole === 'lb') {
    fs.rmSync(path.join(systemDir(system), 'ws-shared'), { recursive: true, force: true })
  }
  // A deleted load-balanced service also owns its haproxy config folder (<name>-lb/).
  if (node?.type === 'service-lb') {
    fs.rmSync(path.join(systemDir(system), `${id}-lb`), { recursive: true, force: true })
  }
  // Drop consumer functions that referenced any removed node (as owner service or as cluster).
  pruneConsumers(system, removedIds)
  writeManifest(system, manifest)

  const log = await rebuild(system, kind)
  return { ok: true, removed: id, kind, cascaded: [...cdcWorkerIds, ...secondaryIds, ...wsChildIds, ...svcLbInstanceIds, ...streamIds], log }
}

// What still depends on `id`, for the GET probe and a fresh-manifest computation.
function dependentsFor(system, manifest, node) {
  const kind = kindOf(node)
  const { secondaryIds, cdcWorkerIds, wsChildIds, svcLbInstanceIds, streamIds } = cascadeChildIds(manifest, node.id, kind, node)
  return findDependents(system, manifest, node.id, kind, new Set([...secondaryIds, ...cdcWorkerIds, ...wsChildIds, ...svcLbInstanceIds, ...streamIds]))
}

export default function removeComponent() {
  return {
    name: 'remove-component',
    configureServer(server) {
      server.middlewares.use('/api/delete', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          const body = await readJsonBody(req)
          const result = await handleDelete(body)
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(result))
        } catch (err) {
          res.statusCode = err.statusCode || 500
          res.setHeader('Content-Type', 'application/json')
          // `dependents` rides along on a blocked-delete 400 so the modal can list them.
          res.end(JSON.stringify({ ok: false, error: err.message, dependents: err.dependents || [] }))
        }
      })

      // Read-only probe: who still depends on a node, so the Delete tab can warn (and
      // disable its button) before the user even tries. No docker, no writes.
      server.middlewares.use('/api/dependents', (req, res, next) => {
        if (req.method !== 'GET') return next()
        try {
          const q = new URLSearchParams((req.url.split('?')[1]) || '')
          const system = q.get('system')
          const id = q.get('id')
          if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
          const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
          const node = manifest.nodes.find((n) => n.id === id)
          const dependents = node ? dependentsFor(system, manifest, node) : []
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ ok: true, dependents }))
        } catch (err) {
          res.statusCode = err.statusCode || 500
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ ok: false, error: err.message, dependents: [] }))
        }
      })
    },
  }
}
