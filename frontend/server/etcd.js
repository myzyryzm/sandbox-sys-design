// Vite dev-server plugin: provision and manage the system's etcd cluster — real
// service discovery (leased-key registration + watch) on a real N-member Raft cluster.
//
//   POST /api/etcd  { system, name, size, heartbeatMs, electionMs, leaseTtlSeconds }
//     -> creates N etcd member containers (<name>-1..N, static bootstrap), a
//        prometheus scrape job (etcd serves /metrics natively on the client port),
//        a type:"etcd" manifest node carrying the cluster config, and the
//        systems/<id>/etcd.json registry, then brings the stack up. ONE etcd setup
//        per system: a second POST is a 409 (the Prometheus-node precedent).
//   GET  /api/etcd?system=<id>&id=<name>[&live=0]
//     -> { ok, cluster, keyspaces, memberStatus } — the etcd.json registry merged
//        with the LIVE cluster (per-member health/leader via `etcdctl endpoint
//        status`, current workers per keyspace via `etcdctl get --prefix`).
//        Probing is N docker execs, so `&live=0` skips it for an instant
//        registry-only paint (workers/memberStatus: null); a follow-up live=1
//        request fills them in — the eventstreams.js two-phase pattern.
//   PUT  /api/etcd  { system, id, size?, heartbeatMs?, electionMs?, leaseTtlSeconds? }
//     -> a TTL-only change is a pure etcd.json write (registration loops mount the
//        file and re-read it by mtime — no rebuild). Changing size or a Raft knob
//        rewrites the member set (+ a fresh --initial-cluster-token so the members
//        re-bootstrap cleanly) and force-recreates the cluster; leased keys are
//        ephemeral by design and every registration loop re-registers on reconnect.
//   POST /api/etcd/keyspace  { system, id, service, description?, conversationId? }
//     -> upsert the keyspace /services/<service>/ (identity = service; one keyspace
//        per service). MECHANICAL half only, mirroring consumers.js: the registry
//        entry (implemented:false, Claude flips it) + the compose edits on the
//        registering container(s) (mount etcd.json:ro, ETCD_WORKER_ID,
//        ETCD_ENDPOINTS). The lease+put+keepalive loop in the service's app.py is
//        authored by a launched Claude session (sandbox-etcd skill).
//   DELETE /api/etcd/keyspace { system, id, service }
//     -> remove the keyspace. 400 while other services still listen to it. Returns
//        { ok, removed, wasImplemented } so the frontend knows whether a session is
//        needed to strip the registration loop.
//   POST /api/etcd/listener  { system, id, keyspace, service, description?, conversationId? }
//     -> upsert a listener (identity = (keyspace, service)): registry entry +
//        ETCD_ENDPOINTS on the listener's container(s). The watch_prefix loop is
//        authored by a launched session, which flips implemented:true.
//   DELETE /api/etcd/listener { system, id, keyspace, service }
//     -> remove the listener entry (+ scrub ETCD_ENDPOINTS when the service has no
//        other etcd role). Returns { ok, removed, wasImplemented }.
//   POST /api/etcd/member  { system, id, member, action: "stop" | "start" }
//     -> kill/start one member container — the Cluster tab's quorum demo. `kill`
//        (not stop) like outage.js, so death is abrupt and failover is honest.
//
// Mirrors eventstreams.js / databases.js: comment-preserving YAML edits, strict
// whitelist validation (browser input only ever lands in generated files, never a
// shell arg), docker via execFile + arg arrays, frontend-safe rebuilds (never
// ./start.sh). etcd speaks gRPC, not HTTP-through-the-lb, so there is NO nginx
// route and no host ports — services reach it by container DNS (<name>-i:2379).
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { isMap, isSeq } from 'yaml'
import { repoRoot, systemsDir, systemDir, isValidSystem, nextNodePosition } from './systems.js'
import {
  HttpError, bad, readJsonBody, NAME_RE, addManifestNode,
  loadCompose, saveCompose, setComposeService, removeComposeService,
  loadPrometheus, savePrometheus, removeScrapeJobDoc,
} from './scaffold.js'
import { addComposeServices, addScrapeJob } from './databases.js'

const pexec = promisify(execFile)

// v3.5 pinned: the etcd_debugging_mvcc_* metrics the node's cards use are renamed
// in 3.6. The image is multi-arch (arm64 + amd64).
const ETCD_IMAGE = 'gcr.io/etcd-development/etcd:v3.5.21'
const SIZES = [3, 5, 7] // odd, so quorum math is meaningful
const HEARTBEAT_MIN = 10
const HEARTBEAT_MAX = 10_000
const ELECTION_MAX = 50_000
const ELECTION_FACTOR = 5 // election timeout must be >= 5x heartbeat or elections get spurious
const TTL_MIN = 2
const TTL_MAX = 3600
const MAX_DESC = 4000

const quorumOf = (size) => Math.floor(size / 2) + 1

// --- registry (systems/<id>/etcd.json) --------------------------------------------
//
// Top-level (not in a node folder) because registering services mount it read-only
// at /etcd/etcd.json — their keepalive loops re-read leaseTtlSeconds by mtime, which
// is what makes TTL live-editable with no rebuild. `implemented` flags are owned by
// the launched Claude sessions, exactly like consumers.json.

function etcdFile(system) {
  return path.join(systemDir(system), 'etcd.json')
}
// Tolerate an absent/garbled file (a system with no etcd yet).
function readEtcd(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(etcdFile(system), 'utf8'))
    return {
      cluster: raw?.cluster && typeof raw.cluster === 'object' ? raw.cluster : null,
      keyspaces: Array.isArray(raw?.keyspaces) ? raw.keyspaces : [],
    }
  } catch {
    return { cluster: null, keyspaces: [] }
  }
}
function writeEtcd(system, data) {
  fs.writeFileSync(etcdFile(system), JSON.stringify(data, null, 2) + '\n')
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

function findEtcdNode(manifest, id) {
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.origin !== 'create-etcd') throw bad(`"${id}" is not an etcd cluster in this system`)
  return node
}

// --- cluster shapes ----------------------------------------------------------------

const memberNames = (name, size) => Array.from({ length: size }, (_, i) => `${name}-${i + 1}`)
const endpointsCsv = (members) => members.map((m) => `${m}:2379`).join(',')

// One member's compose def. No data volume ON PURPOSE: leased registrations are
// ephemeral, so a recreate is a clean re-bootstrap (fresh --initial-cluster-token)
// and every registration loop re-puts its key. restart: unless-stopped keeps a
// crashed member retrying but leaves a deliberately-killed one (quorum demo) down.
function memberDef(member, members, { heartbeatMs, electionMs }, token) {
  return {
    image: ETCD_IMAGE,
    command: [
      '/usr/local/bin/etcd',
      `--name=${member}`,
      '--data-dir=/var/lib/etcd',
      '--listen-client-urls=http://0.0.0.0:2379',
      `--advertise-client-urls=http://${member}:2379`,
      '--listen-peer-urls=http://0.0.0.0:2380',
      `--initial-advertise-peer-urls=http://${member}:2380`,
      `--initial-cluster=${members.map((m) => `${m}=http://${m}:2380`).join(',')}`,
      '--initial-cluster-state=new',
      `--initial-cluster-token=${token}`,
      `--heartbeat-interval=${heartbeatMs}`,
      `--election-timeout=${electionMs}`,
    ],
    restart: 'unless-stopped',
  }
}

function buildMembers(name, size, cfg, token) {
  const members = memberNames(name, size)
  const services = {}
  for (const m of members) services[m] = memberDef(m, members, cfg, token)
  return services
}

function clusterMetrics(name) {
  return [
    { label: 'members up', query: `sum(up{job="${name}"}) or vector(0)`, unit: '' },
    { label: 'has leader', query: `min(etcd_server_has_leader{job="${name}"}) or vector(0)`, unit: '' },
    { label: 'leader changes', query: `max(etcd_server_leader_changes_seen_total{job="${name}"}) or vector(0)`, unit: '' },
    { label: 'keys', query: `max(etcd_debugging_mvcc_keys_total{job="${name}"}) or vector(0)`, unit: '' },
    { label: 'watchers', query: `sum(etcd_debugging_mvcc_watcher_total{job="${name}"}) or vector(0)`, unit: '' },
    { label: 'puts/s', query: `sum(rate(etcd_mvcc_put_total{job="${name}"}[1m])) or vector(0)`, unit: '/s' },
  ]
}

// Quorum-aware coloring (regenerated on every resize): red below quorum (writes
// are down), yellow quorate-but-degraded, green all members up.
function clusterHealth(name, size) {
  const quorum = quorumOf(size)
  return {
    query: `sum(up{job="${name}"}) or vector(0)`,
    rules: [
      { color: 'red', when: `value < ${quorum}` },
      { color: 'yellow', when: `value < ${size}` },
      { color: 'green', when: `value >= ${size}` },
    ],
  }
}

// --- compose env/volume edits on registrant/listener services -----------------------
//
// These mutate the parsed Document IN PLACE (setIn/addIn on paths) rather than
// replacing whole service defs, so each service's "added by …" commentBefore and
// any hand edits survive. environment may be map or `KEY=VAL` sequence form.

function setEnvVar(doc, svc, key, value) {
  const env = doc.getIn(['services', svc, 'environment'])
  if (isSeq(env)) {
    env.items = env.items.filter((it) => !String(it?.value ?? it).startsWith(`${key}=`))
    env.add(doc.createNode(`${key}=${value}`))
    return
  }
  doc.setIn(['services', svc, 'environment', key], value)
}

function removeEnvVar(doc, svc, key) {
  const env = doc.getIn(['services', svc, 'environment'])
  if (isSeq(env)) {
    env.items = env.items.filter((it) => !String(it?.value ?? it).startsWith(`${key}=`))
  } else if (isMap(env) && env.has(key)) {
    env.delete(key)
  }
  const after = doc.getIn(['services', svc, 'environment'])
  if ((isSeq(after) || isMap(after)) && after.items.length === 0) {
    doc.deleteIn(['services', svc, 'environment'])
  }
}

function addVolumeEntry(doc, svc, entry) {
  const vols = doc.getIn(['services', svc, 'volumes'])
  if (!isSeq(vols)) {
    doc.setIn(['services', svc, 'volumes'], doc.createNode([entry]))
    return
  }
  if (vols.items.some((it) => String(it?.value ?? it) === entry)) return
  vols.add(doc.createNode(entry))
}

function removeVolumeEntry(doc, svc, entry) {
  const vols = doc.getIn(['services', svc, 'volumes'])
  if (!isSeq(vols)) return
  vols.items = vols.items.filter((it) => String(it?.value ?? it) !== entry)
  if (vols.items.length === 0) doc.deleteIn(['services', svc, 'volumes'])
}

const ETCD_JSON_MOUNT = './etcd.json:/etcd/etcd.json:ro'

// The containers that actually run a service's code: a load-balanced service's
// instances (<svc>-1..N), or the service itself. Registration/watch loops live in
// those containers, so that's where the env vars (and the etcd.json mount) go.
function serviceContainers(svcNode) {
  return svcNode.svcLb?.instances?.length ? [...svcNode.svcLb.instances] : [svcNode.id]
}

// A service node that can carry etcd code: an internal plain service (not a
// cluster instance) or a service-lb entry. The id is also validated here because
// it doubles as the /services/<id>/ key-path segment.
function findRegistrableService(manifest, service) {
  const node = manifest.nodes.find(
    (n) => n.id === service &&
      ((n.type === 'service' && !n.instanceOf) || n.type === 'service-lb'),
  )
  if (!node) throw bad(`"${service}" is not an internal service in this system`)
  if (!NAME_RE.test(service)) throw bad(`invalid service name "${service}"`)
  return node
}

// --- validation ----------------------------------------------------------------------

function validateClusterCfg({ size, heartbeatMs, electionMs, leaseTtlSeconds }) {
  if (!SIZES.includes(size)) throw bad(`cluster size must be one of ${SIZES.join(', ')} (odd, so quorum is meaningful)`)
  if (!Number.isInteger(heartbeatMs) || heartbeatMs < HEARTBEAT_MIN || heartbeatMs > HEARTBEAT_MAX) {
    throw bad(`heartbeat interval must be ${HEARTBEAT_MIN}-${HEARTBEAT_MAX} ms`)
  }
  if (!Number.isInteger(electionMs) || electionMs > ELECTION_MAX) {
    throw bad(`election timeout must be an integer <= ${ELECTION_MAX} ms`)
  }
  if (electionMs < ELECTION_FACTOR * heartbeatMs) {
    throw bad(`election timeout must be at least ${ELECTION_FACTOR}x the heartbeat interval (${ELECTION_FACTOR * heartbeatMs} ms) or followers start spurious elections`)
  }
  if (!Number.isInteger(leaseTtlSeconds) || leaseTtlSeconds < TTL_MIN || leaseTtlSeconds > TTL_MAX) {
    throw bad(`lease TTL must be ${TTL_MIN}-${TTL_MAX} seconds`)
  }
}

// --- create ---------------------------------------------------------------------------

async function handleCreate(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const name = body.name
  if (typeof name !== 'string' || !NAME_RE.test(name) || name.length > 40) {
    throw bad('name must be lowercase letters, digits and hyphens (start with a letter)')
  }

  const manifest = readManifest(system)
  if (manifest.nodes.some((n) => n.type === 'etcd')) {
    throw new HttpError(409, 'etcd is already on the diagram (only one cluster is allowed).')
  }
  if (manifest.nodes.some((n) => n.id === name)) {
    throw bad(`a node named "${name}" already exists in this system`)
  }

  const cfg = {
    size: Number(body.size),
    heartbeatMs: Number(body.heartbeatMs ?? 100),
    electionMs: Number(body.electionMs ?? 1000),
    leaseTtlSeconds: Number(body.leaseTtlSeconds ?? 15),
  }
  validateClusterCfg(cfg)

  const members = memberNames(name, cfg.size)
  const doc = loadCompose(system)
  for (const m of members) {
    if (doc.hasIn(['services', m]) || manifest.nodes.some((n) => n.id === m)) {
      throw bad(`member name "${m}" collides with an existing service/node`)
    }
  }

  // 1. registry (mounted ro into registering services; TTL is read live from here)
  const now = new Date().toISOString()
  writeEtcd(system, {
    cluster: { id: name, ...cfg, createdAt: now, updatedAt: now },
    keyspaces: [],
  })

  // 2-4. compose members, prometheus scrape job, manifest node
  const token = `${name}-${Date.now()}`
  addComposeServices(system, buildMembers(name, cfg.size, cfg, token), name, 'etcd cluster', 'Add etcd')
  addScrapeJob(
    system,
    { job_name: name, static_configs: [{ targets: members.map((m) => `${m}:2379`) }] },
    name, 'Add etcd', 'etcd cluster',
  )
  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'etcd',
    origin: 'create-etcd',
    position: nextNodePosition(manifest),
    etcd: { ...cfg, quorum: quorumOf(cfg.size), members },
    metrics: clusterMetrics(name),
    health: clusterHealth(name, cfg.size),
  })

  // 5. bring the members up (image-only: no build) + load the scrape job
  const log = await composeUp(system, [])
  return { ok: true, node, log }
}

// up -d (+ optional force-recreate list) then restart prometheus. The escape hatch
// mirrors EVENT_STREAM_SKIP_REBUILD: validate file generation without pulling images.
async function composeUp(system, recreate, { removeOrphans = false } = {}) {
  if (process.env.ETCD_SKIP_REBUILD === '1') return '(rebuild skipped)'
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
  let log = ''
  try {
    const upArgs = ['compose', '-f', compose, 'up', '-d']
    if (removeOrphans) upArgs.push('--remove-orphans')
    const up = await pexec('docker', upArgs, opts)
    log += up.stdout + up.stderr
    if (recreate.length) {
      const rc = await pexec('docker', ['compose', '-f', compose, 'up', '-d', '--force-recreate', ...recreate], opts)
      log += rc.stdout + rc.stderr
    }
    const r = await pexec('docker', ['compose', '-f', compose, 'restart', 'prometheus'], opts)
    log += r.stdout + r.stderr
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
  return log
}

// --- live introspection ----------------------------------------------------------------

const EXEC_OPTS = { cwd: repoRoot, timeout: 20_000, maxBuffer: 8 * 1024 * 1024 }

function composeArgs(system, ...rest) {
  return ['compose', '-f', path.join(systemsDir, system, 'docker-compose.yml'), ...rest]
}

// Per-member status: exec `etcdctl endpoint status` INSIDE each member (its own
// 127.0.0.1:2379 — no --cluster, so this works even with quorum lost: the Status
// RPC is served locally). A member whose exec fails (container killed) is down.
// isLeader compares the member's own id to the leader id it reports (0 = no leader).
async function probeMembers(system, members) {
  const probes = members.map(async (m) => {
    try {
      const { stdout } = await pexec(
        'docker',
        composeArgs(system, 'exec', '-T', m, 'etcdctl', '--command-timeout=3s', 'endpoint', 'status', '-w', 'json'),
        EXEC_OPTS,
      )
      const st = JSON.parse(stdout)?.[0]?.Status
      const own = st?.header?.member_id
      return { id: m, healthy: true, isLeader: Boolean(own && st?.leader && own === st.leader) }
    } catch {
      return { id: m, healthy: false, isLeader: false }
    }
  })
  return Promise.all(probes)
}

// Every /services/... key currently on the cluster, via the first healthy member.
// --consistency=s (serializable, local read) so worker listing still works from a
// quorum-less minority. etcdctl -w json base64-encodes keys/values — decoded here.
async function probeWorkers(system, members, healthyIds) {
  const order = [...healthyIds, ...members.filter((m) => !healthyIds.includes(m))]
  for (const m of order) {
    try {
      const { stdout } = await pexec(
        'docker',
        composeArgs(system, 'exec', '-T', m, 'etcdctl', '--command-timeout=3s', 'get', '--prefix', '/services/', '-w', 'json', '--consistency=s'),
        EXEC_OPTS,
      )
      const kvs = JSON.parse(stdout)?.kvs || []
      return kvs.map((kv) => ({
        key: Buffer.from(kv.key, 'base64').toString('utf8'),
        value: Buffer.from(kv.value || '', 'base64').toString('utf8'),
      }))
    } catch {
      /* try the next member */
    }
  }
  return null
}

async function getCluster(system, id, { checkLive = true } = {}) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findEtcdNode(manifest, id)
  const { cluster, keyspaces } = readEtcd(system)
  const cfg = cluster || node.etcd || {}
  const size = cfg.size || node.etcd?.size || 3
  const members = node.etcd?.members || memberNames(id, size)

  let memberStatus = null
  let kvs = null
  if (checkLive) {
    memberStatus = await probeMembers(system, members)
    kvs = await probeWorkers(system, members, memberStatus.filter((m) => m.healthy).map((m) => m.id))
  }

  const quorum = quorumOf(size)
  return {
    ok: true,
    cluster: {
      id,
      size,
      quorum,
      tolerates: size - quorum,
      heartbeatMs: cfg.heartbeatMs,
      electionMs: cfg.electionMs,
      leaseTtlSeconds: cfg.leaseTtlSeconds,
      members,
    },
    keyspaces: keyspaces.map((ks) => ({
      ...ks,
      workers: kvs
        ? kvs
            .filter((kv) => kv.key.startsWith(ks.prefix))
            .map((kv) => ({ id: kv.key.slice(ks.prefix.length), value: kv.value }))
        : null,
    })),
    memberStatus,
  }
}

// --- reconfigure -------------------------------------------------------------------------

async function handleUpdate(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findEtcdNode(manifest, id)
  const data = readEtcd(system)
  const prev = data.cluster || { id, ...node.etcd }

  const cfg = {
    size: body.size !== undefined ? Number(body.size) : prev.size,
    heartbeatMs: body.heartbeatMs !== undefined ? Number(body.heartbeatMs) : prev.heartbeatMs,
    electionMs: body.electionMs !== undefined ? Number(body.electionMs) : prev.electionMs,
    leaseTtlSeconds: body.leaseTtlSeconds !== undefined ? Number(body.leaseTtlSeconds) : prev.leaseTtlSeconds,
  }
  validateClusterCfg(cfg)

  const raftChanged =
    cfg.size !== prev.size || cfg.heartbeatMs !== prev.heartbeatMs || cfg.electionMs !== prev.electionMs
  const now = new Date().toISOString()
  data.cluster = { ...prev, ...cfg, updatedAt: now }
  writeEtcd(system, data)

  // TTL-only: registration loops mount etcd.json and re-read it by mtime. Done.
  if (!raftChanged) {
    node.etcd = { ...node.etcd, ...cfg }
    writeManifest(system, manifest)
    return { ok: true, rebuilt: false, cluster: data.cluster }
  }

  // Member-set / Raft-knob change: rewrite the members with a fresh bootstrap token
  // (data dirs are ephemeral, so force-recreate re-bootstraps cleanly), repoint the
  // scrape job, refresh every registrant/listener's ETCD_ENDPOINTS, regenerate the
  // quorum-aware health rules, then recreate the cluster.
  const oldMembers = node.etcd?.members || memberNames(id, prev.size || 3)
  const members = memberNames(id, cfg.size)
  for (const m of members) {
    if (!oldMembers.includes(m) && manifest.nodes.some((n) => n.id === m)) {
      throw bad(`member name "${m}" collides with an existing node`)
    }
  }
  const token = `${id}-${Date.now()}`

  const doc = loadCompose(system)
  for (const m of oldMembers) if (!members.includes(m)) removeComposeService(doc, m)
  let first = true
  for (const m of members) {
    setComposeService(doc, m, memberDef(m, members, cfg, token),
      first && !oldMembers.includes(m) ? ` etcd cluster "${id}" — added by Add etcd` : undefined)
    first = false
  }
  // Endpoint lists baked into registrant/listener env go stale on resize.
  const endpoints = endpointsCsv(members)
  const touched = new Set()
  for (const ks of data.keyspaces) {
    const owner = manifest.nodes.find((n) => n.id === ks.service)
    if (owner) for (const c of serviceContainers(owner)) touched.add(c)
    for (const l of Array.isArray(ks.listeners) ? ks.listeners : []) {
      const ln = manifest.nodes.find((n) => n.id === l.service)
      if (ln) for (const c of serviceContainers(ln)) touched.add(c)
    }
  }
  for (const c of touched) if (doc.hasIn(['services', c])) setEnvVar(doc, c, 'ETCD_ENDPOINTS', endpoints)
  saveCompose(system, doc)

  const prom = loadPrometheus(system)
  removeScrapeJobDoc(prom, id)
  const job = prom.createNode({ job_name: id, static_configs: [{ targets: members.map((m) => `${m}:2379`) }] })
  job.commentBefore = ` etcd cluster "${id}" — added by Add etcd`
  prom.addIn(['scrape_configs'], job)
  savePrometheus(system, prom)

  node.etcd = { ...cfg, quorum: quorumOf(cfg.size), members }
  node.health = clusterHealth(id, cfg.size)
  writeManifest(system, manifest)

  const log = await composeUp(system, members, { removeOrphans: true })
  return { ok: true, rebuilt: true, cluster: data.cluster, log }
}

// --- keyspaces ----------------------------------------------------------------------------

function validateKeyspaceInput(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findEtcdNode(manifest, id)
  const svcNode = findRegistrableService(manifest, body.service)
  let description = typeof body.description === 'string' ? body.description : ''
  if (description.length > MAX_DESC) throw bad('description is too long')
  const conversationId = typeof body.conversationId === 'string' ? body.conversationId : ''
  return { system, id, manifest, node, svcNode, service: svcNode.id, description, conversationId }
}

function upsertKeyspace(input) {
  const { system, id, node, svcNode, service, conversationId } = input
  const data = readEtcd(system)
  const now = new Date().toISOString()
  const prefix = `/services/${service}/`
  const i = data.keyspaces.findIndex((k) => k && k.service === service)
  const prev = i >= 0 ? data.keyspaces[i] : null

  let description = input.description
  if (!prev && !description.trim()) {
    description = `Register each ${service} worker under ${prefix} as a leased key (value host:port) with a TTL keepalive, so listeners discover the live worker set.`
  }

  const snapshot = { at: now, description }
  let ks
  if (prev) {
    const history = Array.isArray(prev.history) ? prev.history : []
    ks = {
      ...prev,
      service,
      prefix,
      description,
      conversationId: conversationId || prev.conversationId || '',
      implemented: prev.implemented === true, // Claude owns this; an edit must not reset it
      updatedAt: now,
      history: [...history, snapshot],
    }
    data.keyspaces[i] = ks
  } else {
    ks = {
      service,
      prefix,
      description,
      implemented: false,
      conversationId: conversationId || '',
      createdAt: now,
      updatedAt: now,
      history: [snapshot],
      listeners: [],
    }
    data.keyspaces.push(ks)
  }
  writeEtcd(system, data)

  // Mechanical compose half (no rebuild here — the launched session's
  // `up -d --build <service>` applies it): each registering container gets the
  // etcd.json mount (live TTL), its worker identity, and the endpoint list.
  const members = node.etcd?.members || []
  const doc = loadCompose(system)
  serviceContainers(svcNode).forEach((c, idx) => {
    if (!doc.hasIn(['services', c])) return
    addVolumeEntry(doc, c, ETCD_JSON_MOUNT)
    // Instances are already named <service>-i; a plain service registers as <service>-1.
    setEnvVar(doc, c, 'ETCD_WORKER_ID', c === service ? `${service}-1` : c)
    setEnvVar(doc, c, 'ETCD_ENDPOINTS', endpointsCsv(members))
  })
  saveCompose(system, doc)

  return { ok: true, keyspace: ks }
}

// Does this service still need ETCD_ENDPOINTS (it registers or listens somewhere)?
function hasEtcdRole(keyspaces, service) {
  return keyspaces.some(
    (k) => k.service === service ||
      (Array.isArray(k.listeners) && k.listeners.some((l) => l && l.service === service)),
  )
}

function scrubServiceCompose(system, manifest, service, { workerId = false, endpoints = false, mount = false }) {
  const svcNode = manifest.nodes.find((n) => n.id === service)
  if (!svcNode) return
  const doc = loadCompose(system)
  for (const c of serviceContainers(svcNode)) {
    if (!doc.hasIn(['services', c])) continue
    if (workerId) removeEnvVar(doc, c, 'ETCD_WORKER_ID')
    if (endpoints) removeEnvVar(doc, c, 'ETCD_ENDPOINTS')
    if (mount) removeVolumeEntry(doc, c, ETCD_JSON_MOUNT)
  }
  saveCompose(system, doc)
}

function deleteKeyspace(body) {
  const { system, id, service } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  findEtcdNode(manifest, id)
  if (typeof service !== 'string' || !service) throw bad('service is required')

  const data = readEtcd(system)
  const gone = data.keyspaces.find((k) => k && k.service === service)
  const listeners = (gone && Array.isArray(gone.listeners) ? gone.listeners : []).map((l) => l.service)
  if (listeners.length) {
    throw bad(`cannot remove /services/${service}/ — still watched by: ${listeners.join(', ')}. Remove the listeners first.`)
  }
  data.keyspaces = data.keyspaces.filter((k) => !(k && k.service === service))
  const removed = Boolean(gone)
  writeEtcd(system, data)

  if (gone) {
    scrubServiceCompose(system, manifest, service, {
      workerId: true,
      // Keep ETCD_ENDPOINTS + the mount only if the service still listens elsewhere
      // (the mount is registrant-only, so it always goes; endpoints may stay).
      endpoints: !hasEtcdRole(data.keyspaces, service),
      mount: true,
    })
  }
  return { ok: true, removed, wasImplemented: gone?.implemented === true }
}

// --- listeners -----------------------------------------------------------------------------

function upsertListener(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findEtcdNode(manifest, id)
  const svcNode = findRegistrableService(manifest, body.service)
  const service = svcNode.id

  const data = readEtcd(system)
  const ks = data.keyspaces.find((k) => k && k.service === body.keyspace)
  if (!ks) throw bad(`no keyspace for "${body.keyspace}" on this cluster`)
  if (ks.service === service) throw bad(`${service} already owns ${ks.prefix} — a service doesn't listen to its own keyspace`)

  let description = typeof body.description === 'string' ? body.description : ''
  if (description.length > MAX_DESC) throw bad('description is too long')
  const conversationId = typeof body.conversationId === 'string' ? body.conversationId : ''

  const now = new Date().toISOString()
  if (!Array.isArray(ks.listeners)) ks.listeners = []
  const i = ks.listeners.findIndex((l) => l && l.service === service)
  const prev = i >= 0 ? ks.listeners[i] : null
  if (!prev && !description.trim()) {
    description = `Watch ${ks.prefix} and keep a live in-memory map of ${ks.service} workers (worker id -> host:port), updated by pushed etcd events.`
  }
  const snapshot = { at: now, description }
  let listener
  if (prev) {
    const history = Array.isArray(prev.history) ? prev.history : []
    listener = {
      ...prev,
      service,
      description,
      conversationId: conversationId || prev.conversationId || '',
      implemented: prev.implemented === true,
      updatedAt: now,
      history: [...history, snapshot],
    }
    ks.listeners[i] = listener
  } else {
    listener = {
      service,
      description,
      implemented: false,
      conversationId: conversationId || '',
      createdAt: now,
      updatedAt: now,
      history: [snapshot],
    }
    ks.listeners.push(listener)
  }
  ks.updatedAt = now
  writeEtcd(system, data)

  // Mechanical compose half: the watcher containers just need the endpoint list.
  const doc = loadCompose(system)
  for (const c of serviceContainers(svcNode)) {
    if (doc.hasIn(['services', c])) setEnvVar(doc, c, 'ETCD_ENDPOINTS', endpointsCsv(node.etcd?.members || []))
  }
  saveCompose(system, doc)

  return { ok: true, keyspace: ks.service, listener }
}

function deleteListener(body) {
  const { system, id, keyspace, service } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  findEtcdNode(manifest, id)
  if (typeof service !== 'string' || !service) throw bad('service is required')

  const data = readEtcd(system)
  const ks = data.keyspaces.find((k) => k && k.service === keyspace)
  if (!ks) throw bad(`no keyspace for "${keyspace}" on this cluster`)
  const gone = (Array.isArray(ks.listeners) ? ks.listeners : []).find((l) => l && l.service === service)
  ks.listeners = (Array.isArray(ks.listeners) ? ks.listeners : []).filter((l) => !(l && l.service === service))
  const removed = Boolean(gone)
  writeEtcd(system, data)

  if (gone && !hasEtcdRole(data.keyspaces, service)) {
    scrubServiceCompose(system, manifest, service, { endpoints: true })
  }
  return { ok: true, removed, wasImplemented: gone?.implemented === true }
}

// --- member stop/start (the quorum demo) ------------------------------------------------------

async function handleMember(body) {
  const { system, id, member, action } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findEtcdNode(manifest, id)
  if (!node.etcd?.members?.includes(member)) throw bad(`"${member}" is not a member of ${id}`)
  if (action !== 'stop' && action !== 'start') throw bad('action must be "stop" or "start"')

  const verb = action === 'stop' ? 'kill' : 'start'
  try {
    const { stdout, stderr } = await pexec('docker', composeArgs(system, verb, member), {
      cwd: repoRoot, timeout: 60_000, maxBuffer: 4 * 1024 * 1024,
    })
    return { ok: true, member, action, log: stdout + stderr }
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose ${verb} failed:\n${detail}`)
  }
}

// --- plugin -------------------------------------------------------------------------------------

export default function etcdPlugin() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  return {
    name: 'etcd',
    configureServer(server) {
      server.middlewares.use('/api/etcd', async (req, res, next) => {
        // Connect strips the mount prefix: /api/etcd/keyspace arrives as /keyspace.
        const url = new URL(req.url, 'http://localhost')
        const sub = url.pathname.replace(/\/$/, '')
        try {
          if (sub === '/keyspace') {
            if (req.method === 'POST') return json(res, 200, upsertKeyspace(validateKeyspaceInput(await readJsonBody(req))))
            if (req.method === 'DELETE') return json(res, 200, deleteKeyspace(await readJsonBody(req)))
            return next()
          }
          if (sub === '/listener') {
            if (req.method === 'POST') return json(res, 200, upsertListener(await readJsonBody(req)))
            if (req.method === 'DELETE') return json(res, 200, deleteListener(await readJsonBody(req)))
            return next()
          }
          if (sub === '/member') {
            if (req.method === 'POST') return json(res, 200, await handleMember(await readJsonBody(req)))
            return next()
          }
          if (sub === '') {
            if (req.method === 'POST') return json(res, 200, await handleCreate(await readJsonBody(req)))
            if (req.method === 'PUT') return json(res, 200, await handleUpdate(await readJsonBody(req)))
            if (req.method === 'GET') {
              return json(res, 200, await getCluster(
                url.searchParams.get('system'),
                url.searchParams.get('id'),
                { checkLive: url.searchParams.get('live') !== '0' },
              ))
            }
            return next()
          }
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
