// Vite dev-server plugin: redis TOPOLOGY — reconcile a `create-database` redis node
// between three REAL container shapes (the Topology tab of the redis edit modal):
//
//   standalone   one `<id>` container (the shape "Add database" creates)
//   replicated   `<id>` + N read replicas (`<id>-<n>`, replicaOf nodes — the same
//                shape replicas.js writes) + a 3-node Redis Sentinel (quorum 2)
//                monitoring the primary: `<id>-sentinel-1..3` containers, tracked
//                on the primary node as `node.sentinel` and drawn as member dots.
//   cluster      a real Redis Cluster: `<id>-1..M` member containers
//                (M = shards × (1 + replicasPerShard), 16384 hash slots formed by a
//                one-shot `<id>-cluster-init`), ONE manifest node keeping id `<id>`
//                with `node.redisCluster` (the etcd members-as-containers convention).
//                No bare `<id>` container exists in cluster mode.
//
//   GET  /api/redis/topology?system&id
//        -> { ok, mode, replicas: [{id, replication}], sentinel, cluster, limits }
//   POST /api/redis/topology { system, id, mode, replicas?, shards?, replicasPerShard? }
//        -> { ok, node, mode, warnings, log }   (desired-state reconcile)
//
// The reconcile is MECHANICAL (compose/prometheus/manifest splices + docker) — the
// judgment work of retrofitting writer/reader service code (Sentinel discovery,
// RedisCluster clients, WAIT write modes) is a launched session's job: the tab
// enqueues one with the sandbox-redis-topology skill after a successful apply.
// `sentinel` and `redisCluster` are mutually exclusive; converting into/out of
// cluster mode recreates the data-bearing containers (data cleared, keyspace seeds
// replayed by the init sidecar). All edits land on one loaded compose + prometheus
// doc and are saved once; docker runs once at the end under withSystemLock.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { repoRoot, systemsDir, systemDir, isValidSystem } from './systems.js'
import {
  loadCompose, saveCompose, setComposeService, removeComposeService,
  loadPrometheus, savePrometheus, removeScrapeJobDoc, withSystemLock,
} from './scaffold.js'
import {
  HttpError, bad, readJsonBody, HEALTH_RULES, buildRedis, redisSeedCommand,
} from './databases.js'
import { buildRedisReplica, nextReplicaId, replicaPosition } from './replicas.js'

const pexec = promisify(execFile)
const skipDocker = () => process.env.REDIS_TOPOLOGY_SKIP_REBUILD === '1'

const EXPORTER_IMAGE = 'oliver006/redis_exporter:v1.62.0'
const SENTINEL_PORT = 26379
const SENTINEL_SIZE = 3 // fixed: the smallest set with a meaningful quorum
const SENTINEL_QUORUM = 2
const SENTINEL_DOWN_AFTER_MS = 5000
const SENTINEL_FAILOVER_TIMEOUT_MS = 10000
export const LIMITS = { replicasMin: 1, replicasMax: 4, shardsMin: 3, shardsMax: 5, replicasPerShardMax: 2 }

const now = () => new Date().toISOString()

function loadManifest(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const file = path.join(systemDir(system), 'manifest.json')
  return { file, manifest: JSON.parse(fs.readFileSync(file, 'utf8')) }
}
const saveManifest = (file, manifest) =>
  fs.writeFileSync(file, JSON.stringify(manifest, null, 2) + '\n')

// Topology applies to a user-created redis PRIMARY only: custom-owned redis (an LLM
// worker's token stream) and the websocket bus/presence caches have owned lifecycles,
// and a replica's shape is derived from its primary.
function findTopologyRedis(manifest, id) {
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.type !== 'redis') throw bad(`"${id}" is not a redis node in this system`)
  if (node.origin !== 'create-database') {
    throw bad(`the topology of "${id}" is owned by its creating feature — only "Add database" redis nodes are reconfigurable`)
  }
  if (node.replicaOf) throw bad('topology is configured on the primary, not a read replica')
  if (id.length > 30) throw bad(`"${id}" is too long to derive member container names`)
  return node
}

const sentinelMemberNames = (id) =>
  Array.from({ length: SENTINEL_SIZE }, (_, i) => `${id}-sentinel-${i + 1}`)
const clusterMemberNames = (id, count) =>
  Array.from({ length: count }, (_, i) => `${id}-${i + 1}`)
const modeOf = (node) => (node.redisCluster ? 'cluster' : node.sentinel ? 'replicated' : 'standalone')
const replicaNodesOf = (manifest, id) => manifest.nodes.filter((n) => n.replicaOf === id)

function parseTarget(body) {
  const mode = body.mode
  if (!['standalone', 'replicated', 'cluster'].includes(mode)) {
    throw bad('mode must be "standalone", "replicated" or "cluster"')
  }
  if (mode === 'replicated') {
    const replicas = Number(body.replicas)
    if (!Number.isInteger(replicas) || replicas < LIMITS.replicasMin || replicas > LIMITS.replicasMax) {
      throw bad(`replicas must be an integer ${LIMITS.replicasMin}-${LIMITS.replicasMax}`)
    }
    return { mode, replicas }
  }
  if (mode === 'cluster') {
    const shards = Number(body.shards)
    const replicasPerShard = Number(body.replicasPerShard ?? 0)
    if (!Number.isInteger(shards) || shards < LIMITS.shardsMin || shards > LIMITS.shardsMax) {
      throw bad(`shards must be an integer ${LIMITS.shardsMin}-${LIMITS.shardsMax} (Redis Cluster needs at least 3 masters)`)
    }
    if (!Number.isInteger(replicasPerShard) || replicasPerShard < 0 || replicasPerShard > LIMITS.replicasPerShardMax) {
      throw bad(`replicasPerShard must be an integer 0-${LIMITS.replicasPerShardMax}`)
    }
    return { mode, shards, replicasPerShard }
  }
  return { mode }
}

// --- generated artifacts ---------------------------------------------------------

// Shared source config each sentinel COPIES into its container before starting:
// sentinel rewrites its config file at runtime (epochs, known-replicas, promoted
// master), so a shared writable bind mount would cross-contaminate the three and a
// single-file ro mount can't be written. Copying also sidesteps the macOS stale-
// inode single-file-mount problem (see scaffold.js reloadNginx). masterName == the
// node id, which is what clients pass to Sentinel.master_for().
function sentinelConf(id) {
  return [
    `port ${SENTINEL_PORT}`,
    'dir /tmp',
    'sentinel resolve-hostnames yes',
    'sentinel announce-hostnames yes',
    `sentinel monitor ${id} ${id} 6379 ${SENTINEL_QUORUM}`,
    `sentinel down-after-milliseconds ${id} ${SENTINEL_DOWN_AFTER_MS}`,
    `sentinel failover-timeout ${id} ${SENTINEL_FAILOVER_TIMEOUT_MS}`,
    `sentinel parallel-syncs ${id} 1`,
  ].join('\n') + '\n'
}

function sentinelServices(id) {
  const services = {}
  for (const m of sentinelMemberNames(id)) {
    services[m] = {
      image: 'redis:7-alpine',
      depends_on: [id],
      volumes: [`./${id}-sentinel/sentinel.conf:/sentinel-src/sentinel.conf:ro`],
      command: ['sh', '-c', 'cp /sentinel-src/sentinel.conf /tmp/sentinel.conf && exec redis-sentinel /tmp/sentinel.conf'],
      restart: 'unless-stopped',
    }
    services[`${m}-exporter`] = {
      image: EXPORTER_IMAGE,
      environment: { REDIS_ADDR: `redis://${m}:${SENTINEL_PORT}` },
      depends_on: [m],
    }
  }
  return services
}

// Cluster members announce their compose-DNS hostname (redis >= 7.0) so MOVED
// redirects and gossip survive container recreates; no data volume ON PURPOSE —
// a recreate is a clean re-bootstrap re-formed by the init sidecar (etcd precedent).
function clusterMemberService(m) {
  return {
    image: 'redis:7-alpine',
    command: [
      'redis-server', '--port', '6379',
      '--cluster-enabled', 'yes',
      '--cluster-config-file', 'nodes.conf',
      '--cluster-node-timeout', '5000',
      '--cluster-announce-hostname', m,
      '--cluster-preferred-endpoint-type', 'hostname',
    ],
    restart: 'unless-stopped',
  }
}

// One-shot cluster former + seeder (replaces the standalone `<id>-init`): wait for
// every member, form the cluster once, then replay the keyspace seeds cluster-aware
// (`redis-cli -c` follows MOVED). Self-healing: before a create it flushes/resets any
// LONE node (cluster_known_nodes:1 — e.g. a member container that previously ran with
// another role and kept keys), but never touches members of an already-formed cluster,
// so an `up -d` re-run during a degraded-cluster outage demo can't wipe data.
//
// NOTE this string lands in docker-compose.yml, which performs ${VAR}/$VAR
// interpolation — every shell variable MUST be written `$$var` to reach the container
// shell literally (compose collapses `$$` → `$`; same trap as the postgres replica
// entrypoint's `$$PGDATA` in replicas.js). Member names derive from the
// NAME_RE-validated node id and keyspace names are REDIS_KS_RE-validated — safe
// inside the generated script.
function clusterInitScript(members, replicasPerShard, keyspaces) {
  const first = members[0]
  return [
    'set -e',
    ...members.map((m) => `until redis-cli -h ${m} ping | grep -q PONG; do sleep 1; done`),
    `if ! redis-cli -h ${first} cluster info | grep -q cluster_state:ok; then`,
    `  for h in ${members.join(' ')}; do`,
    '    if redis-cli -h $$h cluster info | grep -q "cluster_known_nodes:1"; then',
    '      redis-cli -h $$h flushall || true',
    '      redis-cli -h $$h cluster reset hard || true',
    '    fi',
    '  done',
    `  redis-cli --cluster create ${members.map((m) => `${m}:6379`).join(' ')} --cluster-replicas ${replicasPerShard} --cluster-yes || true`,
    'fi',
    'i=0',
    `until redis-cli -h ${first} cluster info | grep -q cluster_state:ok; do`,
    '  i=$$((i+1)); if [ $$i -gt 60 ]; then echo "cluster failed to form"; exit 1; fi',
    '  sleep 2',
    'done',
    ...(keyspaces || []).map((e) => redisSeedCommand(`redis-cli -c -h ${first}`, e)),
  ].join('\n')
}

// --- metrics / health ------------------------------------------------------------

function standaloneShapes(node) {
  // buildRedis is the single source of the standalone service/metric/health shapes;
  // its regenerated `keyspaces` field is ignored (the node keeps its live entries).
  return buildRedis({ name: node.id, entities: node.keyspaces || [] })
}

function replicatedMetrics(id) {
  return [
    ...standaloneShapes({ id, keyspaces: [] }).metrics,
    { label: 'replicas', query: `redis_connected_slaves{job="${id}"}`, unit: '' },
    { label: 'sentinels', query: `sum(up{job="${id}-sentinel"}) or vector(0)`, unit: '' },
    // Flips to 0 after a sentinel failover (the old primary is down or demoted) —
    // the visible teaching signal of the failover drill.
    { label: 'is master', query: `sum(redis_instance_info{job="${id}",role="master"}) or vector(0)`, unit: '' },
  ]
}

function clusterMetrics(id) {
  return [
    { label: 'members up', query: `sum(up{job="${id}"}) or vector(0)`, unit: '' },
    { label: 'cluster ok', query: `min(redis_cluster_state{job="${id}"}) or vector(0)`, unit: '' },
    { label: 'ops/s', query: `sum(rate(redis_commands_processed_total{job="${id}"}[1m]))`, unit: '/s' },
    { label: 'keys', query: `sum(redis_db_keys{job="${id}"})`, unit: '' },
  ]
}

// Quorum-style cluster health: a dead member drops out of both series (min over the
// survivors stays 1, sum(up) shrinks -> yellow); losing a whole shard flips the
// survivors' cluster_state to 0 -> red; all members up and serving -> green.
function clusterHealth(id, memberCount) {
  return {
    query: `(min(redis_cluster_state{job="${id}"}) or vector(0)) * (sum(up{job="${id}"}) or vector(0))`,
    rules: [
      { color: 'red', when: 'value < 1' },
      { color: 'yellow', when: `value < ${memberCount}` },
      { color: 'green', when: `value >= ${memberCount}` },
    ],
  }
}

// --- prometheus doc helpers --------------------------------------------------------

function addScrapeJobMulti(prom, jobName, targets, comment) {
  removeScrapeJobDoc(prom, jobName)
  const node = prom.createNode({ job_name: jobName, static_configs: [{ targets }] })
  if (comment) node.commentBefore = comment
  prom.addIn(['scrape_configs'], node)
}

// --- reconcile steps (all pure doc/manifest edits; saved once by the handler) ------

function removeReplicaNode(doc, prom, manifest, rid) {
  removeComposeService(doc, rid)
  removeComposeService(doc, `${rid}-exporter`)
  removeScrapeJobDoc(prom, rid)
  manifest.nodes = manifest.nodes.filter((n) => n.id !== rid)
  manifest.edges = (manifest.edges || []).filter((e) => e.from !== rid && e.to !== rid)
}

function removeAllReplicas(doc, prom, manifest, id) {
  for (const r of replicaNodesOf(manifest, id)) removeReplicaNode(doc, prom, manifest, r.id)
}

function removeSentinel(system, doc, prom, node) {
  for (const m of node.sentinel?.members || sentinelMemberNames(node.id)) {
    removeComposeService(doc, m)
    removeComposeService(doc, `${m}-exporter`)
  }
  removeScrapeJobDoc(prom, `${node.id}-sentinel`)
  fs.rmSync(path.join(systemDir(system), `${node.id}-sentinel`), { recursive: true, force: true })
  delete node.sentinel
}

function removeClusterMembers(doc, prom, node) {
  for (const m of node.redisCluster?.members || []) {
    removeComposeService(doc, m)
    removeComposeService(doc, `${m}-exporter`)
  }
  removeComposeService(doc, `${node.id}-cluster-init`)
  removeScrapeJobDoc(prom, node.id)
  delete node.redisCluster
}

// Restore the standalone base (`<id>` + `<id>-init` seeder + `<id>-exporter` +
// single-target scrape job + standalone metrics/health) after leaving cluster mode.
function ensureStandaloneBase(doc, prom, node) {
  const built = standaloneShapes(node)
  let first = true
  for (const [svc, def] of Object.entries(built.services)) {
    setComposeService(doc, svc, def, first ? ` Redis "${node.id}" — restored by Redis topology` : undefined)
    first = false
  }
  addScrapeJobMulti(prom, node.id, [`${node.id}-exporter:9121`], ` Database "${node.id}" — added by Redis topology`)
  node.metrics = built.metrics
  node.health = built.health
}

// Reconcile the replicaOf secondaries to `count`: adopt whatever exists (however it
// was created), trim the highest ordinals, top up through the same builder the
// "Add read replica" flow uses — so a topology replica is indistinguishable.
function reconcileReplicas(doc, prom, manifest, node, count) {
  const id = node.id
  const ordinalRe = new RegExp(`^${id}-(\\d+)$`)
  const existing = () =>
    replicaNodesOf(manifest, id)
      .map((n) => ({ n, ord: Number(ordinalRe.exec(n.id)?.[1] || 0) }))
      .sort((a, b) => a.ord - b.ord)

  let current = existing()
  while (current.length > count) {
    removeReplicaNode(doc, prom, manifest, current[current.length - 1].n.id)
    current = existing()
  }
  while (current.length < count) {
    const { id: secondaryId, ordinal } = nextReplicaId(id, manifest)
    const built = buildRedisReplica({ secondaryId, primary: id })
    let first = true
    for (const [svc, def] of Object.entries(built.services)) {
      setComposeService(doc, svc, def, first ? ` Redis replica "${secondaryId}" — added by Redis topology` : undefined)
      first = false
    }
    addScrapeJobMulti(prom, secondaryId, [`${secondaryId}-exporter:9121`], ` Redis replica "${secondaryId}" — added by Redis topology`)
    manifest.nodes.push({
      id: secondaryId,
      label: `${secondaryId} (replica)`,
      type: 'redis',
      origin: 'create-database',
      role: 'secondary',
      replicaOf: id,
      replication: 'async',
      readonly: true,
      position: replicaPosition(node, ordinal),
      metrics: built.metrics,
      health: built.health,
    })
    current = existing()
  }
}

// Write sentinel.conf + the 3 sentinel services. Returns the sentinel container
// names to force-recreate when the conf CONTENT changed (compose can't detect a
// content-only change to a mounted file).
function ensureSentinel(system, doc, node) {
  const id = node.id
  const dir = path.join(systemDir(system), `${id}-sentinel`)
  const confPath = path.join(dir, 'sentinel.conf')
  const conf = sentinelConf(id)
  let prev = null
  try {
    prev = fs.readFileSync(confPath, 'utf8')
  } catch {
    /* first enable */
  }
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(confPath, conf)
  const members = sentinelMemberNames(id)
  let first = true
  for (const [svc, def] of Object.entries(sentinelServices(id))) {
    setComposeService(doc, svc, def, first ? ` Redis sentinel for "${id}" (${SENTINEL_SIZE} nodes, quorum ${SENTINEL_QUORUM}) — added by Redis topology` : undefined)
    first = false
  }
  node.sentinel = {
    size: SENTINEL_SIZE,
    quorum: SENTINEL_QUORUM,
    masterName: id,
    members,
    downAfterMs: SENTINEL_DOWN_AFTER_MS,
    failoverTimeoutMs: SENTINEL_FAILOVER_TIMEOUT_MS,
  }
  return prev !== null && prev !== conf ? members : []
}

// Per-writer WAIT configs that can't be satisfied by the new topology are kept (the
// Keyspaces tab owns them) but surfaced as warnings.
function waitWarnings(node, replicaCount) {
  const out = []
  for (const ks of node.keyspaces || []) {
    for (const [svc, wm] of Object.entries(ks.writeModes || {})) {
      if (wm.mode !== 'wait') continue
      if (replicaCount === 0) {
        out.push(`keyspace "${ks.name}" writer "${svc}" uses WAIT but this topology has no replicas — WAIT will always time out`)
      } else if (wm.numreplicas > replicaCount) {
        out.push(`keyspace "${ks.name}" writer "${svc}" WAITs for ${wm.numreplicas} ack(s) but only ${replicaCount} replica(s) can acknowledge`)
      }
    }
  }
  return out
}

// --- docker -----------------------------------------------------------------------

// One `up -d --remove-orphans` (drops containers whose services we removed), then
// force-recreate what needs fresh state, then reload prometheus. Serialized per
// system; never nest another lock-wrapped rebuild inside the handler.
//
// `resetMaster`: leaving replicated mode keeps the `<id>` container, but a past
// sentinel FAILOVER may have left its runtime role "replica of <promoted node>" —
// now a removed host, so it would sit read-only forever. REPLICAOF NO ONE restores
// it to a clean master in place (no data loss; no-op if it already is the master).
async function composeUp(system, recreate, { resetMaster = null, announceIp = null, stopFirst = [] } = {}) {
  if (skipDocker()) return '(rebuild skipped)'
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
  return withSystemLock(system, async () => {
    let log = ''
    try {
      const up = await pexec('docker', ['compose', '-f', compose, 'up', '-d', '--remove-orphans'], opts)
      log += up.stdout + up.stderr
      // The initial `up -d` also starts one-shot init services — a cluster init
      // kicked off there would race the member recreation below and pollute the
      // fresh containers with partial CLUSTER MEET/ADDSLOTS state. Stop it before
      // recreating; the recreate pass starts a fresh run AFTER the members (compose
      // honors depends_on start order within one up invocation).
      if (stopFirst.length) {
        const st = await pexec('docker', ['compose', '-f', compose, 'stop', ...stopFirst], opts)
        log += st.stdout + st.stderr
      }
      if (recreate.length) {
        const rc = await pexec('docker', ['compose', '-f', compose, 'up', '-d', '--force-recreate', ...recreate], opts)
        log += rc.stdout + rc.stderr
      }
      if (resetMaster) {
        try {
          const rm = await pexec('docker', ['compose', '-f', compose, 'exec', '-T', resetMaster, 'redis-cli', 'REPLICAOF', 'NO', 'ONE'], opts)
          log += rm.stdout + rm.stderr
        } catch (err) {
          log += `(warning: could not reset "${resetMaster}" to master: ${err.stderr || err.message})`
        }
      }
      if (announceIp) {
        // Runtime-only on purpose: baking it into the primary's compose command
        // would recreate the container (clearing its data) on entering replicated
        // mode. It only matters if the primary is later DEMOTED by a failover —
        // then its replica gossip advertises the hostname, not the container IP.
        try {
          const ai = await pexec('docker', ['compose', '-f', compose, 'exec', '-T', announceIp, 'redis-cli', 'CONFIG', 'SET', 'replica-announce-ip', announceIp], opts)
          log += ai.stdout + ai.stderr
        } catch (err) {
          log += `(warning: could not set replica-announce-ip on "${announceIp}": ${err.stderr || err.message})`
        }
      }
      const r = await pexec('docker', ['compose', '-f', compose, 'restart', 'prometheus'], opts)
      log += r.stdout + r.stderr
    } catch (err) {
      const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
      throw new HttpError(500, `docker compose failed:\n${detail}`)
    }
    return log
  })
}

// --- handlers ----------------------------------------------------------------------

function handleGet(system, id) {
  const { manifest } = loadManifest(system)
  const node = findTopologyRedis(manifest, id)
  return {
    ok: true,
    mode: modeOf(node),
    replicas: replicaNodesOf(manifest, id).map((n) => ({ id: n.id, replication: n.replication || 'async' })),
    sentinel: node.sentinel || null,
    cluster: node.redisCluster || null,
    limits: LIMITS,
  }
}

async function handleSet(body) {
  const system = body.system
  const { file, manifest } = loadManifest(system)
  const node = findTopologyRedis(manifest, body.id)
  const id = node.id
  const target = parseTarget(body)
  const currentMode = modeOf(node)
  const currentReplicas = replicaNodesOf(manifest, id).length

  // Idempotent Apply: an exact no-change request skips the docker roundtrip.
  // EXCEPT cluster→cluster with identical params — that is a deliberate RE-FORM
  // (force-recreate the members + re-run the init), the repair action for a
  // cluster that failed to bootstrap or was left degraded.
  if (
    (target.mode === 'standalone' && currentMode === 'standalone') ||
    (target.mode === 'replicated' && currentMode === 'replicated' && currentReplicas === target.replicas)
  ) {
    return { ok: true, node, mode: target.mode, warnings: [], log: '(no topology change)' }
  }

  const doc = loadCompose(system)
  const prom = loadPrometheus(system)
  const warnings = []
  let recreate = []

  // 1. Tear down what the target mode doesn't include.
  if (currentMode === 'cluster') {
    removeClusterMembers(doc, prom, node)
    warnings.push('leaving/re-forming Redis Cluster recreates the data-bearing containers — existing data is cleared and keyspace seeds are replayed')
    if (target.mode !== 'cluster') ensureStandaloneBase(doc, prom, node)
  }
  if (node.sentinel && target.mode !== 'replicated') removeSentinel(system, doc, prom, node)
  if (target.mode !== 'replicated') removeAllReplicas(doc, prom, manifest, id)

  // 2. Build the target mode.
  if (target.mode === 'replicated') {
    reconcileReplicas(doc, prom, manifest, node, target.replicas)
    recreate = ensureSentinel(system, doc, node)
    node.role = 'primary'
    node.metrics = replicatedMetrics(id)
    node.health = { query: `redis_up{job="${id}"}`, rules: HEALTH_RULES }
    warnings.push(...waitWarnings(node, target.replicas))
  } else if (target.mode === 'cluster') {
    const memberCount = target.shards * (1 + target.replicasPerShard)
    const members = clusterMemberNames(id, memberCount)
    for (const m of members) {
      // Own cluster/replica services were already removed above — anything left is foreign.
      if (manifest.nodes.some((n) => n.id === m) || doc.hasIn(['services', m])) {
        throw bad(`member name "${m}" collides with an existing service/node`)
      }
    }
    if (currentMode !== 'cluster') {
      removeComposeService(doc, id)
      removeComposeService(doc, `${id}-exporter`)
      removeComposeService(doc, `${id}-init`)
      warnings.push('converting to Redis Cluster recreates the data-bearing containers — existing data is cleared and keyspace seeds are replayed')
    }
    let first = true
    for (const m of members) {
      setComposeService(doc, m, clusterMemberService(m), first ? ` Redis cluster "${id}" (${target.shards} shards × ${1 + target.replicasPerShard}) — added by Redis topology` : undefined)
      setComposeService(doc, `${m}-exporter`, {
        image: EXPORTER_IMAGE,
        environment: { REDIS_ADDR: `redis://${m}:6379` },
        depends_on: [m],
      })
      first = false
    }
    setComposeService(doc, `${id}-cluster-init`, {
      image: 'redis:7-alpine',
      depends_on: [...members],
      restart: 'no',
      entrypoint: ['sh', '-c', clusterInitScript(members, target.replicasPerShard, node.keyspaces)],
    })
    addScrapeJobMulti(prom, id, members.map((m) => `${m}-exporter:9121`), ` Redis cluster "${id}" — managed by Redis topology`)
    node.redisCluster = { shards: target.shards, replicasPerShard: target.replicasPerShard, members }
    delete node.role
    node.metrics = clusterMetrics(id)
    node.health = clusterHealth(id, memberCount)
    // Always force-recreate every member + the init AFTER the plain `up -d`:
    // a member name may have existed before this edit with another role (an old
    // replica `<id>-1`, or a resize survivor with stale nodes.conf), and `up -d`
    // can race the init against the OLD container — `--cluster create` then refuses
    // ("node is not empty"). The explicit recreate pass runs members-then-init in
    // dependency order on clean, empty containers. (Fresh containers recreate in
    // seconds — determinism is worth it.)
    recreate = [...members, `${id}-cluster-init`]
    warnings.push(...waitWarnings(node, target.replicasPerShard))
  } else {
    // standalone: teardown above did the work; refresh the base shapes in place.
    const built = standaloneShapes(node)
    if (currentMode !== 'cluster') {
      node.metrics = built.metrics
      node.health = built.health
    }
    delete node.role
    warnings.push(...waitWarnings(node, 0))
  }

  saveCompose(system, doc)
  savePrometheus(system, prom)
  saveManifest(file, manifest)

  const log = await composeUp(system, recreate, {
    resetMaster: currentMode === 'replicated' && target.mode === 'standalone' ? id : null,
    announceIp: target.mode === 'replicated' ? id : null,
    stopFirst: target.mode === 'cluster' ? [`${id}-cluster-init`] : [],
  })
  return { ok: true, node, mode: target.mode, warnings, log }
}

// --- plugin -------------------------------------------------------------------------

export default function redisTopology() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  return {
    name: 'redis-topology',
    configureServer(server) {
      server.middlewares.use('/api/redis/topology', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const url = new URL(req.url, 'http://localhost')
            return json(res, 200, handleGet(url.searchParams.get('system'), url.searchParams.get('id')))
          }
          if (req.method === 'POST') {
            return json(res, 200, await handleSet(await readJsonBody(req)))
          }
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
