// Shared "worker replica group" reconciler — run a typed service as N instances under
// one service id, with NO load balancer. Extracted from customTypes/llmWorker.js so
// every custom type that scales this way (LLM workers, Kafka consumer groups, …)
// reconciles through ONE implementation: the base `<name>` stays a real serving
// container; instances `<name>-2..N` clone its compose def (same `build: ./<name>`,
// differing only by SERVICE_ID / ETCD_WORKER_ID), each with its own scrape job, a
// manifest node carrying `instanceOf`, and a plain nginx `/<id>/` route for
// control-plane polling. The base carries `replicas: { instances: [...] }` — the shape
// scaffold.js `serviceCodeContainers` / `resolveBuildTargets` already understand, so
// per-service rebuilds (endpoint/gRPC sessions) hit every member with no extra wiring.
//
// A type parameterizes the reconciler with a cfg object:
//   serviceType            — the manifest service_type marking group members
//   typeLabel              — human label for error messages ("LLM worker", …)
//   maxTotal               — max members (base + instances)
//   memberMetrics(id, isBase, baseNode) — the metric cards for a member (refreshed on
//                            every scale call so pre-existing groups self-heal)
//   instanceExtras(baseNode)            — extra fields for instance nodes (e.g. grpc)
//   instanceComment(baseId)             — compose comment on instance entries
//   onMembersChanged(system, baseNode, memberIds) — optional post-write hook, called
//                            with the full member list [base, ...instances] whenever a
//                            scale call runs (including no-ops, for self-healing) —
//                            e.g. the consumer-group type syncs streams.json members.
//
// Mechanical (no launched session), mirroring serviceLb.js minus the haproxy sidecar.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import {
  bad, HttpError, serviceHealth, addNginxRoute, ensureNginxRoute, removeNginxRoute,
  reloadNginx, loadCompose, saveCompose, composeServiceDef, setComposeService,
  removeComposeService, loadPrometheus, savePrometheus, addScrapeJobDoc,
  removeScrapeJobDoc, withEtcdWorkerId, withSystemLock, NAME_RE,
} from './scaffold.js'
import { isValidSystem, systemDir, repoRoot, systemsDir } from './systems.js'

const pexec = promisify(execFile)
const INSTANCE_PORT = 8000 // instances serve FastAPI / are scraped here
const SKIP_REBUILD = () => process.env.CREATE_SVC_SKIP_REBUILD === '1'

const readManifest = (system) => JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
const writeManifest = (system, manifest) =>
  fs.writeFileSync(path.join(systemDir(system), 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n')

export const instanceOrdinal = (id, base) => {
  const m = new RegExp(`^${base}-(\\d+)$`).exec(id)
  return m ? Number(m[1]) : 0
}
export const instanceId = (base, ord) => `${base}-${ord}`
export function instanceNodes(manifest, base) {
  return manifest.nodes
    .filter((n) => n.instanceOf === base)
    .sort((a, b) => instanceOrdinal(a.id, base) - instanceOrdinal(b.id, base))
}

// A group instance node: a service card carrying only the grouping back-link. It shares
// the base's build/config, keeps service_type so its diagram body + metric cards match
// the base's, and owns no endpoints (the diagram suppresses those for instanceOf nodes);
// its nginx route exists purely for control-plane state polling.
function groupInstanceNode(cfg, base, id, entryNode) {
  return {
    id,
    label: id,
    type: 'service',
    origin: 'create-custom-service',
    service_type: cfg.serviceType,
    instanceOf: base,
    position: { x: (entryNode.position?.x ?? 80) + 260, y: entryNode.position?.y ?? 80 },
    metrics: cfg.memberMetrics(id, false, entryNode),
    health: serviceHealth(id),
    ...(cfg.instanceExtras ? cfg.instanceExtras(entryNode) : {}),
  }
}

// Reject a derived instance id that collides with an existing node or folder.
function assertFreeInstanceId(system, manifest, id) {
  if (manifest.nodes.some((n) => n.id === id)) throw bad(`a node named "${id}" already exists in this system`)
  if (fs.existsSync(path.join(systemDir(system), id))) throw bad(`systems/${system}/${id}/ already exists`)
}

// Resolve + validate the group ENTRY node (a base of this type, never itself an instance).
function entryNodeOf(cfg, system, nodeId) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (!nodeId || !NAME_RE.test(nodeId)) throw bad('invalid node')
  const manifest = readManifest(system)
  const node = manifest.nodes.find((x) => x.id === nodeId && x.service_type === cfg.serviceType)
  if (!node) throw bad(`"${nodeId}" is not a ${cfg.typeLabel} in this system`)
  if (node.instanceOf) {
    throw bad(`"${nodeId}" is a ${cfg.typeLabel} instance — scale the group from its base "${node.instanceOf}"`)
  }
  return { manifest, node }
}

// Frontend-safe rebuild for a replica group: build the new instance images, bring the
// stack up (creating them, or removing orphaned instance containers on scale-down),
// recreate the lb when instance routes changed (reloadLb — after `up -d` so nginx can
// resolve the new upstream hostnames), and restart prometheus so appended/removed scrape
// jobs are picked up. No haproxy sidecar (there is no load balancer). NEVER ./start.sh.
export async function scaleRebuild(system, opts) {
  return withSystemLock(system, () => _scaleRebuildImpl(system, opts))
}

async function _scaleRebuildImpl(system, { buildNames = [], removeOrphans = false, reloadLb = false }) {
  if (SKIP_REBUILD()) return '(rebuild skipped)'
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 600_000, maxBuffer: 16 * 1024 * 1024 }
  const run = async (args) => {
    const r = await pexec('docker', ['compose', '-f', compose, ...args], opts)
    return r.stdout + r.stderr
  }
  let log = ''
  try {
    if (buildNames.length) log += await run(['build', ...buildNames])
    const upArgs = ['up', '-d']
    if (removeOrphans) upArgs.push('--remove-orphans')
    log += await run(upArgs)
    if (reloadLb) log += await reloadNginx(system)
    log += await run(['restart', 'prometheus'])
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
  return log
}

// The idempotent "set desired member count" op (total = base + instances). instances==1
// removes every replica; N>=2 adds/drops the highest-ordinal instances to reach N.
export async function setGroupReplicas(cfg, body) {
  const { system } = body
  const { manifest, node } = entryNodeOf(cfg, system, body.node)

  const target = Number(body.instances)
  if (!Number.isInteger(target) || target < 1 || target > cfg.maxTotal) {
    throw bad(`instances must be a whole number between 1 and ${cfg.maxTotal}`)
  }

  const current = instanceNodes(manifest, node.id)
  const currentTotal = 1 + current.length

  // Idempotent reconciliation — runs on EVERY scale call (including no-change) so
  // pre-existing groups self-heal: refresh each member's metric cards to the current
  // shape and make sure every instance has its control-plane nginx route.
  node.metrics = cfg.memberMetrics(node.id, true, node)
  for (const n of current) n.metrics = cfg.memberMetrics(n.id, false, node)
  let routesChanged = false
  for (const n of current) if (ensureNginxRoute(system, n.id)) routesChanged = true

  if (target === currentTotal) {
    writeManifest(system, manifest)
    cfg.onMembersChanged?.(system, node, [node.id, ...current.map((n) => n.id)])
    const log = routesChanged && !SKIP_REBUILD()
      ? await withSystemLock(system, () => reloadNginx(system))
      : '(no change)'
    return { ok: true, node, log }
  }

  const doc = loadCompose(system)
  const prom = loadPrometheus(system)
  let plan
  let memberIds

  if (target > currentTotal) {
    // scale up: base is ordinal 1, so added instances start at 2. Clone the base compose
    // def and override SERVICE_ID (build/config all shared). If the base is
    // etcd-registered, `withEtcdWorkerId` rewrites the cloned ETCD_WORKER_ID to this
    // instance so each member registers a distinct key instead of all sharing the base's.
    const app = composeServiceDef(doc, node.id) || { build: `./${node.id}` }
    const maxOrd = current.reduce((m, n) => Math.max(m, instanceOrdinal(n.id, node.id)), 1)
    const newIds = []
    for (let o = maxOrd + 1; currentTotal + newIds.length < target; o++) {
      const id = instanceId(node.id, o)
      assertFreeInstanceId(system, manifest, id)
      setComposeService(
        doc,
        id,
        withEtcdWorkerId({ ...app, build: `./${node.id}`, environment: { ...(app.environment || {}), SERVICE_ID: id } }, id),
        cfg.instanceComment(node.id),
      )
      addScrapeJobDoc(prom, id, `${id}:${INSTANCE_PORT}`, ` Instance of ${cfg.typeLabel} "${node.id}"`)
      manifest.nodes.push(groupInstanceNode(cfg, node.id, id, node))
      newIds.push(id)
    }
    // Route writes go to disk immediately (unlike the in-memory compose/prom/manifest
    // docs), so they happen only after every id has passed assertFreeInstanceId — a
    // mid-loop throw must not leave nginx pointing at containers that never exist.
    for (const id of newIds) addNginxRoute(system, id)
    node.replicas = { instances: [...current.map((n) => n.id), ...newIds] }
    memberIds = [node.id, ...node.replicas.instances]
    plan = { buildNames: newIds, removeOrphans: false, reloadLb: true }
  } else {
    // scale down: drop the highest ordinals until base + kept === target
    const keep = target - 1
    const drop = current.slice(keep)
    for (const n of drop) {
      removeComposeService(doc, n.id)
      removeScrapeJobDoc(prom, n.id)
      removeNginxRoute(system, n.id)
    }
    const dropIds = new Set(drop.map((n) => n.id))
    manifest.nodes = manifest.nodes.filter((n) => !dropIds.has(n.id))
    manifest.edges = (manifest.edges || []).filter((e) => !dropIds.has(e.from) && !dropIds.has(e.to))
    const remaining = current.slice(0, keep).map((n) => n.id)
    if (remaining.length) node.replicas = { instances: remaining }
    else delete node.replicas
    memberIds = [node.id, ...remaining]
    plan = { buildNames: [], removeOrphans: true, reloadLb: true }
  }

  saveCompose(system, doc)
  savePrometheus(system, prom)
  writeManifest(system, manifest)
  cfg.onMembersChanged?.(system, node, memberIds)

  const log = await scaleRebuild(system, plan)
  return { ok: true, node: readManifest(system).nodes.find((n) => n.id === node.id), log }
}
