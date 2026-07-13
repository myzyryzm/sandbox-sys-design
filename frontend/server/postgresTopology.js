// Vite dev-server plugin: postgres TOPOLOGY — reconcile a `create-database` postgres
// node between two REAL container shapes (the Topology tab of the postgres edit modal):
//
//   standalone   one `<id>` container (the shape "Add database" creates)
//   replicated   `<id>` + N streaming standbys (`<id>-<n>`, replicaOf nodes — the same
//                shape replicas.js writes) + ONE `<id>-failover` watcher container: the
//                postgres answer to redis Sentinel. Tracked on the primary node as
//                `node.postgresHa` and drawn as member dots.
//
//   GET  /api/postgres/topology?system&id
//        -> { ok, mode, replicas: [{id, replication}], ha, limits, warnings }
//   POST /api/postgres/topology { system, id, mode, replicas?, sync?, failover? }
//        -> { ok, node, mode, warnings, log }            (desired-state reconcile)
//   POST /api/postgres/failover { system, id, target }
//        -> promote `target` (a planned switchover: fence the old primary, then promote)
//   POST /api/postgres/rejoin   { system, id, member }
//        -> rebuild `member` as a standby of the LIVE primary (the post-failover repair)
//
// ENTERING REPLICATED MODE DOES NOT TOUCH `<id>`. The primary keeps its container and its
// data, and it doesn't even restart:
//   - the replication pg_hba line goes in live (`exec` + pg_reload_conf) and is ALSO
//     persisted as an initdb script for a from-scratch rebuild — prepPostgresPrimary(),
//   - synchronous_standby_names / synchronous_commit / wal_keep_size are all runtime
//     ALTER SYSTEM + reload, applied by the watcher.
// (postgres:16-alpine declares VOLUME on its data dir, so a member's data actually lives in
// an anonymous volume that survives a container recreate — see composeUp. Not relying on a
// recreate at all is still the better contract: it keeps `up -d` a no-op for the primary.)
//
// ROLES ARE RUNTIME, MEMBERSHIP IS MANIFEST. `replicaOf` means "member of <id>'s
// cluster", not "is currently a standby" — after a failover the live primary is a
// `<id>-<n>` container while `<id>` is still the manifest's cluster entry. This is the
// same fudge redisTopology.js makes with sentinel (see its `resetMaster` comment), and
// it is what keeps the diagram's cluster box + replica arrows working with no changes.
// The live role is a METRIC (`pg_ha_is_primary`), which is what the tab and the diagram
// dots actually read.
//
// The reconcile is MECHANICAL (compose/prometheus/manifest splices + docker) — the
// judgment work of retrofitting the services that USE the database (multi-host DSN with
// target_session_attrs, read routing) is a launched session's job: the tab enqueues one
// with the sandbox-postgres-topology skill after a successful apply.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { repoRoot, systemsDir, systemDir, isValidSystem } from './systems.js'
import {
  loadCompose, saveCompose, setComposeService, removeComposeService, composeServiceDef,
  loadPrometheus, savePrometheus, removeScrapeJobDoc, withSystemLock, cloneTemplate,
} from './scaffold.js'
import { HttpError, bad, readJsonBody, HEALTH_RULES } from './databases.js'
import {
  buildPostgresReplica, nextReplicaId, replicaPosition, prepPostgresPrimary,
} from './replicas.js'

const pexec = promisify(execFile)
const skipDocker = () => process.env.PG_TOPOLOGY_SKIP_REBUILD === '1'

const TEMPLATE_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'templates', 'pg-failover')
const TEMPLATE_FILES = ['Dockerfile', 'app.py', 'requirements.txt']
const WATCHER_PORT = 8000

export const LIMITS = {
  replicasMin: 1,
  replicasMax: 4,
  downAfterMsMin: 2000,
  downAfterMsMax: 60000,
  commitLevels: ['on', 'remote_write', 'remote_apply', 'local', 'off'],
}
const DEFAULT_DOWN_AFTER_MS = 5000

const watcherOf = (id) => `${id}-failover`
const composePath = (system) => path.join(systemsDir, system, 'docker-compose.yml')
const execOpts = () => ({ cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 })

function loadManifest(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const file = path.join(systemDir(system), 'manifest.json')
  return { file, manifest: JSON.parse(fs.readFileSync(file, 'utf8')) }
}
const saveManifest = (file, manifest) =>
  fs.writeFileSync(file, JSON.stringify(manifest, null, 2) + '\n')

// Topology applies to a user-created postgres PRIMARY only (the cluster ENTRY node).
function findTopologyPostgres(manifest, id) {
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.type !== 'postgres') throw bad(`"${id}" is not a postgres node in this system`)
  if (node.origin !== 'create-database') {
    throw bad(`the topology of "${id}" is owned by its creating feature — only "Add database" postgres nodes are reconfigurable`)
  }
  if (node.replicaOf) throw bad('topology is configured on the cluster entry, not a standby')
  if (id.length > 30) throw bad(`"${id}" is too long to derive member container names`)
  return node
}

const modeOf = (node) => (node.postgresHa ? 'replicated' : 'standalone')
const replicaNodesOf = (manifest, id) => manifest.nodes.filter((n) => n.replicaOf === id)
const dbNameOf = (id) => id.replace(/-/g, '_')

// The services whose CODE talks to this database — the ones a topology change obliges to
// switch to a multi-host DSN. Read from the registries that actually record it: an
// endpoint's `downstream` and a Kafka consumer function's `downstream`. (Manifest edges
// are not it — a db gets no edge at creation, so they are routinely empty.)
export function dependentServices(system, dbId) {
  const dir = systemDir(system)
  const out = new Set()
  const read = (f) => {
    try {
      return JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8'))
    } catch {
      return null
    }
  }
  const endpoints = read('endpoints.json') || {}
  for (const [svc, eps] of Object.entries(endpoints)) {
    for (const ep of eps || []) {
      if ((ep.downstream || []).includes(dbId)) out.add(svc)
    }
  }
  const consumers = read('consumers.json')?.consumers || []
  for (const c of consumers) {
    if ((c.downstream || []).includes(dbId)) out.add(c.service)
  }
  return [...out].sort()
}

// --- request parsing ----------------------------------------------------------------

function parseTarget(body, replicaIds) {
  const mode = body.mode
  if (!['standalone', 'replicated'].includes(mode)) {
    throw bad('mode must be "standalone" or "replicated"')
  }
  if (mode === 'standalone') return { mode }

  const replicas = Number(body.replicas)
  if (!Number.isInteger(replicas) || replicas < LIMITS.replicasMin || replicas > LIMITS.replicasMax) {
    throw bad(`replicas must be an integer ${LIMITS.replicasMin}-${LIMITS.replicasMax}`)
  }

  // The sync set is expressed as standby ORDINALS (1..replicas) so it stays meaningful
  // when the count changes — ids are derived, never trusted from the client.
  const sync = body.sync || {}
  const ordinals = Array.isArray(sync.standbys) ? sync.standbys.map(Number) : []
  for (const o of ordinals) {
    if (!Number.isInteger(o) || o < 1 || o > replicas) {
      throw bad(`sync standby ordinal ${o} is outside 1-${replicas}`)
    }
  }
  const method = sync.method === 'FIRST' ? 'FIRST' : 'ANY'
  const commitLevel = LIMITS.commitLevels.includes(sync.commitLevel) ? sync.commitLevel : 'on'
  let quorum = Number(sync.quorum ?? 1)
  if (!Number.isInteger(quorum) || quorum < 1) quorum = 1
  if (ordinals.length && quorum > ordinals.length) {
    throw bad(`quorum ${quorum} exceeds the ${ordinals.length} standby(s) marked synchronous`)
  }

  const fo = body.failover || {}
  const downAfterMs = Number(fo.downAfterMs ?? DEFAULT_DOWN_AFTER_MS)
  if (!Number.isInteger(downAfterMs) || downAfterMs < LIMITS.downAfterMsMin || downAfterMs > LIMITS.downAfterMsMax) {
    throw bad(`downAfterMs must be an integer ${LIMITS.downAfterMsMin}-${LIMITS.downAfterMsMax}`)
  }

  return {
    mode,
    replicas,
    sync: { method, quorum, commitLevel, ordinals: [...new Set(ordinals)].sort((a, b) => a - b) },
    failover: {
      enabled: fo.enabled !== false,
      autoDegrade: fo.autoDegrade !== false,
      downAfterMs,
    },
  }
}

// The primary's own postgres flags, read off its compose `command`
// (`['postgres','-c','wal_level=logical','-c','max_wal_senders=10', …]` — what cdc.js
// writes when CDC is enabled). Every member inherits them; see buildPostgresReplica.
function primaryPostgresSettings(doc, id) {
  const def = composeServiceDef(doc, id)
  const cmd = def?.command
  if (!Array.isArray(cmd)) return []
  const out = []
  for (let i = 0; i < cmd.length; i++) {
    if (cmd[i] === '-c' && typeof cmd[i + 1] === 'string') out.push(cmd[++i])
  }
  return out
}

// The flags the whole cluster runs with. Read from the entry node's compose `command`
// when it has one — but once `<id>` has itself been REJOINED as a standby its `command`
// is gone (a standby carries its flags inside its entrypoint), so the HA block keeps a
// durable copy. Without it, a second rejoin would rebuild a member with no wal_level.
function clusterSettings(doc, node) {
  const fromCompose = primaryPostgresSettings(doc, node.id)
  return fromCompose.length ? fromCompose : (node.postgresHa?.settings || [])
}

// --- generated artifacts --------------------------------------------------------------

// The watcher's live config. Mounted READ-ONLY and re-read by mtime, so changing the
// sync set / quorum / threshold is a file write + nothing else — no rebuild, no restart
// (the same contract etcd.json has for its lease TTL).
function writeHaConf(system, node, ha, syncStandbys) {
  const dir = path.join(systemDir(system), node.id)
  fs.mkdirSync(dir, { recursive: true })
  const conf = {
    primary: ha.primary,
    members: ha.members,
    enabled: ha.enabled,
    autoDegrade: ha.autoDegrade,
    downAfterMs: ha.downAfterMs,
    db: dbNameOf(node.id),
    user: 'sandbox',
    password: 'sandbox',
    sync: {
      method: ha.sync.method,
      quorum: ha.sync.quorum,
      commitLevel: ha.sync.commitLevel,
      standbys: syncStandbys,
    },
  }
  fs.writeFileSync(path.join(dir, 'ha.json'), JSON.stringify(conf, null, 2) + '\n')
}

// Re-derive ha.json from the manifest's CURRENT standbys. Called by remove.js when a
// standby is deleted out from under the topology (the Delete tab): the watcher is the
// single writer of synchronous_standby_names, so the way to tell it a member is gone is
// to rewrite its config, not to run a competing ALTER SYSTEM. The mount is re-read by
// mtime, so this needs no restart. Returns the watcher id, or null when there is no HA.
export function syncHaMembers(system, manifest, primaryNode, removedId = null) {
  const ha = primaryNode?.postgresHa
  if (!ha) return null
  const standbys = manifest.nodes.filter(
    (n) => n.replicaOf === primaryNode.id && n.id !== removedId,
  )
  const memberIds = [primaryNode.id, ...standbys.map((n) => n.id)]
  const syncIds = standbys.filter((n) => n.replication === 'sync').map((n) => n.id)
  ha.members = memberIds
  // A quorum larger than the surviving sync set would block every commit.
  if (ha.sync) {
    ha.sync.standbys = syncIds
    ha.sync.quorum = Math.max(1, Math.min(ha.sync.quorum || 1, syncIds.length || 1))
  }
  if (ha.primary === removedId) ha.primary = primaryNode.id
  writeHaConf(system, primaryNode, ha, syncIds)
  return ha.watcher || watcherOf(primaryNode.id)
}

function watcherService(id) {
  return {
    build: `./${watcherOf(id)}`,
    depends_on: [id],
    volumes: [`./${id}/ha.json:/ha.json:ro`],
    restart: 'unless-stopped',
  }
}

// --- metrics / health -------------------------------------------------------------------

function standaloneMetrics(id) {
  return [
    { label: 'connections', query: `sum(pg_stat_database_numbackends{job="${id}"})`, unit: '' },
    { label: 'commits/s', query: `sum(rate(pg_stat_database_xact_commit{job="${id}"}[1m]))`, unit: '/s' },
    { label: 'rows fetch/s', query: `sum(rate(pg_stat_database_tup_fetched{job="${id}"}[1m]))`, unit: '/s' },
  ]
}

// The watcher is the authority on roles (the exporter can't tell you who is primary), so
// the cluster-level cards read its pg_ha_* series.
function replicatedMetrics(id) {
  const w = watcherOf(id)
  return [
    { label: 'connections', query: `sum(pg_stat_database_numbackends{job="${id}"})`, unit: '' },
    { label: 'commits/s', query: `sum(rate(pg_stat_database_xact_commit{job="${id}"}[1m]))`, unit: '/s' },
    { label: 'members up', query: `sum(pg_ha_member_up{job="${w}"}) or vector(0)`, unit: '' },
    // Flips off `<id>` and onto a standby after a failover — the visible teaching signal.
    { label: 'is primary', query: `pg_ha_is_primary{job="${w}",member="${id}"} or vector(0)`, unit: '' },
    { label: 'sync acks', query: `pg_ha_sync_acking{job="${w}"} or vector(0)`, unit: '' },
  ]
}

// Quorum-ish health: no live primary anywhere -> red (the cluster cannot take writes);
// a primary but a missing member -> yellow; everything up -> green.
function replicatedHealth(id, memberCount) {
  const w = watcherOf(id)
  return {
    query: `(sum(pg_ha_is_primary{job="${w}"}) or vector(0)) * (sum(pg_ha_member_up{job="${w}"}) or vector(0))`,
    rules: [
      { color: 'red', when: 'value < 1' },
      { color: 'yellow', when: `value < ${memberCount}` },
      { color: 'green', when: `value >= ${memberCount}` },
    ],
  }
}

// --- prometheus doc helper ----------------------------------------------------------------

function addScrapeJobMulti(prom, jobName, targets, comment) {
  removeScrapeJobDoc(prom, jobName)
  const node = prom.createNode({ job_name: jobName, static_configs: [{ targets }] })
  if (comment) node.commentBefore = comment
  prom.addIn(['scrape_configs'], node)
}

// --- live probing (the backend's own view of who is primary) --------------------------------

// One `psql` per member. Used only by the ACTIONS (promote / rejoin / leaving replicated
// mode), never on the polling GET — the tab reads roles from Prometheus like the diagram.
async function liveRole(system, member) {
  try {
    const r = await pexec(
      'docker',
      ['compose', '-f', composePath(system), 'exec', '-T', member, 'psql', '-U', 'sandbox', '-d', 'postgres',
        '-tAc', "SELECT pg_is_in_recovery()::int || ':' || current_setting('default_transaction_read_only')"],
      execOpts(),
    )
    const [inRecovery, readOnly] = r.stdout.trim().split(':')
    return { up: true, standby: inRecovery === '1', fenced: readOnly === 'on' }
  } catch {
    return { up: false }
  }
}

// The member currently serving writes: up, out of recovery, and not fenced.
async function findLivePrimary(system, members) {
  for (const m of members) {
    const role = await liveRole(system, m)
    if (role.up && !role.standby && !role.fenced) return m
  }
  return null
}

async function psql(system, member, statements) {
  const args = ['compose', '-f', composePath(system), 'exec', '-T', member,
    'psql', '-U', 'sandbox', '-d', 'postgres']
  for (const s of statements) args.push('-c', s)
  const r = await pexec('docker', args, execOpts())
  return r.stdout + r.stderr
}

// --- reconcile steps (pure doc/manifest edits; saved once by the handler) --------------------

function removeReplicaNode(doc, prom, manifest, rid) {
  removeComposeService(doc, rid)
  removeComposeService(doc, `${rid}-exporter`)
  removeScrapeJobDoc(prom, rid)
  manifest.nodes = manifest.nodes.filter((n) => n.id !== rid)
  manifest.edges = (manifest.edges || []).filter((e) => e.from !== rid && e.to !== rid)
}

function removeWatcher(system, doc, prom, node) {
  removeComposeService(doc, watcherOf(node.id))
  removeScrapeJobDoc(prom, watcherOf(node.id))
  fs.rmSync(path.join(systemDir(system), watcherOf(node.id)), { recursive: true, force: true })
  fs.rmSync(path.join(systemDir(system), node.id, 'ha.json'), { force: true })
  delete node.postgresHa
}

// Reconcile the replicaOf standbys to `count`, marking each sync/async per the target.
// Tops up through the SAME builder the "Add read replica" flow uses, so a topology
// standby is indistinguishable from a hand-added one.
function reconcileReplicas(doc, prom, manifest, node, target) {
  const id = node.id
  const dbName = dbNameOf(id)
  const settings = clusterSettings(doc, node)
  const ordinalRe = new RegExp(`^${id}-(\\d+)$`)
  const existing = () =>
    replicaNodesOf(manifest, id)
      .map((n) => ({ n, ord: Number(ordinalRe.exec(n.id)?.[1] || 0) }))
      .sort((a, b) => a.ord - b.ord)

  let current = existing()
  while (current.length > target.replicas) {
    removeReplicaNode(doc, prom, manifest, current[current.length - 1].n.id)
    current = existing()
  }
  while (current.length < target.replicas) {
    const { id: secondaryId, ordinal } = nextReplicaId(id, manifest)
    const built = buildPostgresReplica({ secondaryId, primary: id, dbName, settings })
    let first = true
    for (const [svc, def] of Object.entries(built.services)) {
      setComposeService(doc, svc, def, first ? ` PostgreSQL standby "${secondaryId}" — added by Postgres topology` : undefined)
      first = false
    }
    addScrapeJobMulti(prom, secondaryId, [`${secondaryId}-exporter:9187`], ` PostgreSQL standby "${secondaryId}" — added by Postgres topology`)
    manifest.nodes.push({
      id: secondaryId,
      label: `${secondaryId} (replica)`,
      type: 'postgres',
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

  // Stamp the sync/async flag by ordinal (the id list the watcher is given).
  const syncIds = []
  for (const { n, ord } of existing()) {
    const isSync = target.sync.ordinals.includes(ord)
    n.replication = isSync ? 'sync' : 'async'
    if (isSync) syncIds.push(n.id)
  }
  return { memberIds: [id, ...existing().map(({ n }) => n.id)], syncIds }
}

function ensureWatcher(system, doc, prom, node, target, memberIds, syncIds, settings) {
  const wid = watcherOf(node.id)
  cloneTemplate(system, wid, TEMPLATE_DIR, TEMPLATE_FILES)
  // The configured primary is sticky: a past failover may have moved the LIVE primary to
  // a standby, and re-applying the topology must not silently claim `<id>` is primary
  // again (that is what Promote / Rejoin are for).
  const primary = node.postgresHa?.primary && memberIds.includes(node.postgresHa.primary)
    ? node.postgresHa.primary
    : node.id
  const ha = {
    enabled: target.failover.enabled,
    autoDegrade: target.failover.autoDegrade,
    downAfterMs: target.failover.downAfterMs,
    primary,
    members: memberIds,
    watcher: wid,
    sync: target.sync,
  }
  writeHaConf(system, node, ha, syncIds)
  setComposeService(doc, wid, watcherService(node.id),
    ` Postgres failover watcher for "${node.id}" — added by Postgres topology`)
  addScrapeJobMulti(prom, wid, [`${wid}:${WATCHER_PORT}`], ` Postgres failover watcher "${wid}"`)
  // Snapshot the entry node's ORIGINAL (initdb-shaped) compose def while it still has one.
  // A rejoin rewrites `<id>` into a standby, and there is no way to reconstruct the exact
  // def afterwards (its seed.sql / repl-hba mounts are appended by other flows) — so keep
  // a verbatim copy to restore from when the cluster is torn back down to standalone. The
  // YAML comment is snapshotted too: it is not part of toJSON(), and restoring without it
  // would leave a gratuitous diff on a file that is supposed to come back byte-identical.
  const fresh = composeServiceDef(doc, node.id)
  const keep = node.postgresHa?.entryDef
  const entryDef = keep || (fresh && !fresh.entrypoint ? fresh : null)
  const entryComment = node.postgresHa?.entryComment
    ?? (entryDef === fresh ? doc.getIn(['services', node.id], true)?.commentBefore : undefined)
  node.postgresHa = {
    ...ha,
    settings,
    dsn: clusterDsns(node.id, memberIds),
    ...(entryDef ? { entryDef } : {}),
    ...(entryComment != null ? { entryComment } : {}),
    sync: { ...target.sync, standbys: syncIds },
  }
  return wid
}

// Config errors the topology can't satisfy — surfaced, not silently accepted.
function syncWarnings(target, syncIds) {
  const out = []
  if (!syncIds.length) return out
  if (target.sync.quorum > syncIds.length) {
    out.push(`quorum ${target.sync.quorum} exceeds the ${syncIds.length} synchronous standby(s) — every commit would block`)
  }
  if (syncIds.length && target.sync.commitLevel === 'off') {
    out.push('synchronous_commit=off makes the synchronous standbys pointless — commits do not wait for anyone')
  }
  if (!target.failover.autoDegrade) {
    out.push('auto-degrade is off: if a synchronous standby dies, every write to the primary blocks until it returns (this is the stall you may be trying to demonstrate)')
  }
  return out
}

// --- docker --------------------------------------------------------------------------------

// `renew` = recreate these services AND throw away their data dir.
//
// postgres:16-alpine declares VOLUME /var/lib/postgresql/data, so every member has an
// ANONYMOUS volume that docker compose deliberately carries over to the new container on
// `--force-recreate`. That is why enabling replication never costs you the database. It is
// also why a rejoin has to opt out: the standby entrypoint only clones when the data dir is
// empty, so without `--renew-anon-volumes` a rejoining node would just boot its own stale
// data back up (and, if it was fenced, boot it back up read-only) instead of re-cloning
// from the live primary.
async function composeUp(system, { build = [], recreate = [], renew = [] } = {}) {
  if (skipDocker()) return '(rebuild skipped)'
  const compose = composePath(system)
  const opts = execOpts()
  return withSystemLock(system, async () => {
    let log = ''
    const run = async (args) => {
      const r = await pexec('docker', ['compose', '-f', compose, ...args], opts)
      log += r.stdout + r.stderr
    }
    try {
      if (build.length) await run(['build', ...build])
      await run(['up', '-d', '--remove-orphans'])
      if (recreate.length) await run(['up', '-d', '--force-recreate', ...recreate])
      if (renew.length) await run(['up', '-d', '--force-recreate', '--renew-anon-volumes', ...renew])
      await run(['restart', 'prometheus'])
    } catch (err) {
      const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
      throw new HttpError(500, `docker compose failed:\n${detail}`)
    }
    return log
  })
}

// --- handlers --------------------------------------------------------------------------------

// The connection strings the attached services should use. This is the postgres answer to
// "ask Sentinel who the master is": libpq tries each host and `target_session_attrs`
// decides which one it will settle on, so a failover needs no code change at all — only
// this DSN. Computed here so the retrofit session substitutes a string rather than
// assembling one (and so the tab can show the user exactly what will change).
export function clusterDsns(id, members) {
  const db = dbNameOf(id)
  const hosts = members.map((m) => `${m}:5432`).join(',')
  const base = `postgresql://sandbox:sandbox@${hosts}/${db}`
  return {
    readWrite: `${base}?target_session_attrs=read-write&connect_timeout=2`,
    // Prefers a standby but falls back to the primary if none is up, and spreads across
    // them — read scaling that degrades to "still works" rather than "fails".
    readOnly: `${base}?target_session_attrs=prefer-standby&load_balance_hosts=random&connect_timeout=2`,
  }
}

function handleGet(system, id) {
  const { manifest } = loadManifest(system)
  const node = findTopologyPostgres(manifest, id)
  const ordinalRe = new RegExp(`^${id}-(\\d+)$`)
  const members = node.postgresHa?.members || [id]
  return {
    ok: true,
    mode: modeOf(node),
    replicas: replicaNodesOf(manifest, id).map((n) => ({
      id: n.id,
      ordinal: Number(ordinalRe.exec(n.id)?.[1] || 0),
      replication: n.replication || 'async',
    })),
    ha: node.postgresHa || null,
    services: dependentServices(system, id),
    dsn: clusterDsns(id, members),
    limits: LIMITS,
  }
}

async function handleSet(body) {
  const system = body.system
  const { file, manifest } = loadManifest(system)
  const node = findTopologyPostgres(manifest, body.id)
  const id = node.id
  const target = parseTarget(body, replicaNodesOf(manifest, id).map((n) => n.id))
  const currentMode = modeOf(node)

  // BEFORE loadCompose, deliberately. prepPostgresPrimary splices the repl-hba initdb mount
  // onto the compose file ON DISK; a doc we had already loaded would be saved back over the
  // top of it and silently drop the mount. Nothing would look broken — the live `exec` half
  // of prepPostgresPrimary still opens pg_hba on the running container — right up until a
  // from-scratch rebuild, where the standbys would fail to authenticate.
  if (target.mode === 'replicated') await prepPostgresPrimary(system, id)

  const doc = loadCompose(system)
  const prom = loadPrometheus(system)
  const warnings = []
  const build = []
  const recreate = []

  if (target.mode === 'standalone') {
    // Leaving replicated mode deletes the standbys. If a failover has happened, the LIVE
    // data is on one of them — refuse rather than silently destroy it.
    if (currentMode === 'replicated') {
      const members = node.postgresHa?.members || [id]
      const live = await findLivePrimary(system, members)
      if (live && live !== id) {
        throw bad(
          `"${id}" is not currently the primary — "${live}" is (a failover happened), and it holds the live data. ` +
          `Rejoin "${id}" as a standby, let it catch up, then Promote it back before converting to standalone.`,
        )
      }
      // If a failover + rejoin left `<id>` with a STANDBY-shaped compose def, restore its
      // original initdb-shaped one — otherwise it would keep trying to stream from a member
      // we are about to delete. Its data comes along (the anonymous data volume survives the
      // recreate), and the guard above has already established that `<id>` is the live
      // primary, so it has no standby.signal to strip either.
      const entryDef = node.postgresHa?.entryDef
      if (composeServiceDef(doc, id)?.entrypoint && entryDef) {
        setComposeService(doc, id, entryDef, node.postgresHa.entryComment)
      }
      for (const r of replicaNodesOf(manifest, id)) removeReplicaNode(doc, prom, manifest, r.id)
      removeWatcher(system, doc, prom, node)
      // The replication-only artifacts prepPostgresPrimary added: the pg_hba initdb script
      // and its mount. Nothing streams from this node any more, so leaving them behind would
      // just be residue (and a standing "anyone may replicate" rule on a rebuilt container).
      const hba = `./${id}/repl-hba.sh:/docker-entrypoint-initdb.d/00-repl-hba.sh:ro`
      const vols = composeServiceDef(doc, id)?.volumes
      if (Array.isArray(vols) && vols.includes(hba)) {
        doc.setIn(['services', id, 'volumes'], doc.createNode(vols.filter((v) => v !== hba)))
      }
      fs.rmSync(path.join(systemDir(system), id, 'repl-hba.sh'), { force: true })
    }
    delete node.role
    node.metrics = standaloneMetrics(id)
    node.health = { query: `pg_up{job="${id}"}`, rules: HEALTH_RULES }
  } else {
    const settings = clusterSettings(doc, node)
    const { memberIds, syncIds } = reconcileReplicas(doc, prom, manifest, node, target)
    const wid = ensureWatcher(system, doc, prom, node, target, memberIds, syncIds, settings)
    build.push(wid)
    // The watcher must re-read ha.json from a clean start when the member set changed.
    recreate.push(wid)
    node.role = 'primary'
    node.metrics = replicatedMetrics(id)
    node.health = replicatedHealth(id, memberIds.length)
    warnings.push(...syncWarnings(target, syncIds))
  }

  saveCompose(system, doc)
  savePrometheus(system, prom)
  saveManifest(file, manifest)

  let log = await composeUp(system, { build, recreate })

  // Leaving replicated mode: the primary is still carrying the replication settings the
  // watcher gave it — and the watcher is now GONE. `synchronous_standby_names` naming a
  // standby we just deleted would block EVERY COMMIT forever (postgres waits for an ack
  // that can never arrive, and nothing is left to degrade it). Clear them here; also drop
  // any fence, so a node that was a fenced ex-primary comes back writable.
  if (target.mode === 'standalone' && currentMode === 'replicated' && !skipDocker()) {
    try {
      log += await psql(system, id, [
        'ALTER SYSTEM RESET synchronous_standby_names',
        'ALTER SYSTEM RESET synchronous_commit',
        'ALTER SYSTEM RESET default_transaction_read_only',
        'SELECT pg_reload_conf()',
      ])
    } catch (err) {
      warnings.push(`could not clear replication settings on "${id}" — if commits hang, run: ALTER SYSTEM RESET synchronous_standby_names; SELECT pg_reload_conf(); (${err.message})`)
    }
  }
  return {
    ok: true,
    node,
    mode: target.mode,
    warnings,
    log,
    services: dependentServices(system, id),
    dsn: clusterDsns(id, node.postgresHa?.members || [id]),
  }
}

// A planned switchover (or a manual promote after the watcher's auto-promote is off).
// ORDER MATTERS: fence the old primary FIRST. If we promoted first there would briefly be
// two writable primaries, and the watcher — which keeps the incumbent on a split brain —
// would fence the node we just promoted.
async function handleFailover(body) {
  const system = body.system
  const { file, manifest } = loadManifest(system)
  const node = findTopologyPostgres(manifest, body.id)
  if (!node.postgresHa) throw bad(`"${node.id}" is not replicated — enable a topology first`)
  const members = node.postgresHa.members || []
  const target = body.target
  if (!members.includes(target)) throw bad(`"${target}" is not a member of this cluster`)

  const role = await liveRole(system, target)
  if (!role.up) throw bad(`"${target}" is not reachable`)
  if (!role.standby) throw bad(`"${target}" is already the primary`)

  let log = ''
  const old = await findLivePrimary(system, members)
  if (old && old !== target) {
    // A fenced primary answers `SHOW transaction_read_only` = on, so libpq's
    // target_session_attrs=read-write skips it: writers move to the new primary with no
    // code change and no restart. Rejoin (below) is what makes it a real standby again.
    log += await psql(system, old, [
      'ALTER SYSTEM SET default_transaction_read_only = on',
      'SELECT pg_reload_conf()',
    ])
  }
  log += await psql(system, target, ['SELECT pg_promote(true, 60)'])

  // The watcher repoints the surviving standbys and re-applies the sync config within a
  // tick; we only record the new primary so a later Apply doesn't claim `<id>` is primary.
  node.postgresHa.primary = target
  writeHaConf(system, node, node.postgresHa, node.postgresHa.sync?.standbys || [])
  saveManifest(file, manifest)
  return { ok: true, primary: target, log }
}

// The post-failover repair: rebuild a member as a standby of the LIVE primary. Its data
// is stale by definition (it is a fenced ex-primary, or a node that fell out of the
// cluster), so recreating the container — which empties the data dir — is exactly right:
// the standby entrypoint then pg_basebackups a fresh copy from the live primary.
async function handleRejoin(body) {
  const system = body.system
  const { file, manifest } = loadManifest(system)
  const node = findTopologyPostgres(manifest, body.id)
  if (!node.postgresHa) throw bad(`"${node.id}" is not replicated — enable a topology first`)
  const members = node.postgresHa.members || []
  const member = body.member
  if (!members.includes(member)) throw bad(`"${member}" is not a member of this cluster`)

  const primary = await findLivePrimary(system, members.filter((m) => m !== member))
  if (!primary) throw bad('no live primary to rejoin — promote a healthy standby first')

  const doc = loadCompose(system)
  const built = buildPostgresReplica({
    secondaryId: member,
    primary,
    dbName: dbNameOf(node.id),
    settings: clusterSettings(doc, node),
  })
  const def = { ...built.services[member] }
  // NO depends_on. Roles rotate, so a `depends_on` here is a cycle waiting to happen:
  // rejoining `<id>` under `<id>-1` (which already depends_on `<id>`) makes compose refuse
  // to run AT ALL — "dependency cycle detected" breaks every service in the file, not just
  // these two. The standby entrypoint already waits for its primary with pg_isready before
  // base-backing up, which is the only ordering that actually matters.
  delete def.depends_on
  // Only the member's own service — its exporter already points at it and stays put.
  setComposeService(doc, member, def,
    ` PostgreSQL "${member}" — rebuilt as a standby of "${primary}" by Postgres topology`)
  saveCompose(system, doc)

  if (member !== node.id) {
    const n = manifest.nodes.find((x) => x.id === member)
    if (n) n.replication = n.replication || 'async'
  }
  saveManifest(file, manifest)

  // `renew`, not `recreate`: the point of a rejoin is to DISCARD this node's data dir and
  // clone a fresh one from the live primary. Its anonymous volume would otherwise survive
  // the recreate and the entrypoint would skip the clone entirely (see composeUp).
  const log = await composeUp(system, { renew: [member] })
  return { ok: true, member, primary, log }
}

// --- plugin ---------------------------------------------------------------------------------

export default function postgresTopology() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  const post = (handler) => async (req, res, next) => {
    try {
      if (req.method !== 'POST') return next()
      return json(res, 200, await handler(await readJsonBody(req)))
    } catch (err) {
      return json(res, err.statusCode || 500, { ok: false, error: err.message })
    }
  }
  return {
    name: 'postgres-topology',
    configureServer(server) {
      server.middlewares.use('/api/postgres/failover', post(handleFailover))
      server.middlewares.use('/api/postgres/rejoin', post(handleRejoin))
      server.middlewares.use('/api/postgres/topology', async (req, res, next) => {
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
