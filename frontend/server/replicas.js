// Vite dev-server plugin: add a read replica (secondary) to an existing database.
//
//   POST /api/db-replicas  { system, primary, mode }   (mode: "async" | "sync")
//
// Provisions a REAL streaming read-replica of an existing postgres / mongodb /
// redis database (object-store has no replica concept). The secondary:
//   - has id `<primary>-<N>` (next free ordinal),
//   - is a read-only standby that actually streams from the primary,
//   - gets its own exporter + Prometheus scrape job + manifest node, and
//   - records `replicaOf`/`replication`/`readonly` so the diagram draws the
//     primary↔secondary arrow and the dotted cluster box.
//
// This mirrors databases.js: a thin backend that does the docker work directly
// (the "Add read replica" modal button calls it). The sandbox-database skill
// documents the by-hand / Claude-session equivalent. All user input is
// validated against strict whitelists and only ever lands in generated files
// (compose entrypoints / SQL) — never in a shell argument we run.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemsDir, systemDir, isValidSystem } from './systems.js'
import {
  HttpError,
  bad,
  readJsonBody,
  HEALTH_RULES,
  addComposeServices,
  addScrapeJob,
} from './databases.js'

const pexec = promisify(execFile)
const skipDocker = () => process.env.CREATE_DB_SKIP_REBUILD === '1'

const NODE_W = 190 // keep secondaries near their primary (matches the diagram)
const DB_ID_RE = /^[a-z][a-z0-9-]*$/

// Engines that support replicas. object-store + dynamodb are excluded (no replica
// concept). Cassandra's "replica" is a second cluster node joining the ring (not a
// read-only standby) — see buildCassandraReplica / prepCassandraPrimary.
const ENGINE_LABEL = { postgres: 'PostgreSQL', mongodb: 'MongoDB', redis: 'Redis', cassandra: 'Cassandra' }

// ---------------------------------------------------------------------------
// Manifest + compose helpers
// ---------------------------------------------------------------------------

function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
}

const composePath = (system) => path.join(systemsDir, system, 'docker-compose.yml')
const execOpts = () => ({ cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 })

// `<primary>-<N>` with N = (max existing ordinal for this primary) + 1, so ids
// never collide even after a middle replica was deleted.
function nextReplicaId(primary, manifest) {
  const re = new RegExp(`^${primary}-(\\d+)$`)
  let max = 0
  for (const n of manifest.nodes) {
    const m = re.exec(n.id)
    if (m) max = Math.max(max, Number(m[1]))
  }
  const ordinal = max + 1
  return { id: `${primary}-${ordinal}`, ordinal }
}

// Lay the secondary out adjacent to its primary (a row beneath it) so the arrow
// stays short and the dotted box stays tight.
function replicaPosition(primaryNode, ordinal) {
  const px = primaryNode.position?.x ?? 80
  const py = primaryNode.position?.y ?? 80
  return { x: px + (ordinal - 1) * (NODE_W + 30), y: py + 170 }
}

// Add a read-only volume mount to an existing service, only if absent.
function addServiceVolume(system, service, mount) {
  const file = composePath(system)
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const vols = doc.getIn(['services', service, 'volumes'])
  const present = vols?.items?.some((it) => String(it.value ?? it) === mount)
  if (!present) doc.addIn(['services', service, 'volumes'], mount)
  fs.writeFileSync(file, doc.toString())
}

// Set a service's `command`, only if not already set (idempotent for mongo).
function setServiceCommand(system, service, command) {
  const file = composePath(system)
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  if (!doc.getIn(['services', service, 'command'])) {
    doc.setIn(['services', service, 'command'], doc.createNode(command))
    fs.writeFileSync(file, doc.toString())
    return true
  }
  return false
}

// ---------------------------------------------------------------------------
// Per-engine secondary builders (compose services + scrape + node fields)
// ---------------------------------------------------------------------------

function buildPostgresReplica({ secondaryId, primary, dbName }) {
  // A standby: wait for the primary, base-backup it once (which writes
  // standby.signal + recovery config), then run postgres — read-only by nature.
  // NOTE: this string lands in docker-compose.yml, which performs ${VAR}/$VAR
  // interpolation — so `$PGDATA` MUST be written `$$PGDATA` to reach the
  // container shell literally (compose collapses `$$` → `$`).
  const entrypoint = [
    'bash',
    '-c',
    [
      'set -e',
      `until pg_isready -h ${primary} -p 5432 -U sandbox; do echo "waiting for ${primary}"; sleep 2; done`,
      'if [ ! -f "$$PGDATA/standby.signal" ]; then',
      '  rm -rf "$$PGDATA"/* || true',
      `  pg_basebackup -h ${primary} -p 5432 -U sandbox -D "$$PGDATA" -Fp -Xs -R -P`,
      `  echo "primary_conninfo = 'host=${primary} port=5432 user=sandbox application_name=${secondaryId}'" >> "$$PGDATA/postgresql.auto.conf"`,
      'fi',
      // The docker volume mount point is 0755; postgres refuses anything looser
      // than 0700 for its data dir.
      'chmod 0700 "$$PGDATA"',
      'exec postgres -c hot_standby=on',
    ].join('\n'),
  ]
  return {
    services: {
      [secondaryId]: {
        image: 'postgres:16-alpine',
        user: 'postgres',
        depends_on: [primary],
        entrypoint,
      },
      [`${secondaryId}-exporter`]: {
        image: 'quay.io/prometheuscommunity/postgres-exporter:v0.16.0',
        environment: {
          DATA_SOURCE_NAME: `postgresql://sandbox:sandbox@${secondaryId}:5432/${dbName}?sslmode=disable`,
        },
        depends_on: [secondaryId],
      },
    },
    scrapeJob: { job_name: secondaryId, static_configs: [{ targets: [`${secondaryId}-exporter:9187`] }] },
    metrics: [
      { label: 'connections', query: `sum(pg_stat_database_numbackends{job="${secondaryId}"})`, unit: '' },
      { label: 'rows fetch/s', query: `sum(rate(pg_stat_database_tup_fetched{job="${secondaryId}"}[1m]))`, unit: '/s' },
      { label: 'repl lag', query: `pg_replication_lag{job="${secondaryId}"}`, unit: 's' },
    ],
    health: { query: `pg_up{job="${secondaryId}"}`, rules: HEALTH_RULES },
  }
}

function buildRedisReplica({ secondaryId, primary }) {
  return {
    services: {
      [secondaryId]: {
        image: 'redis:7-alpine',
        depends_on: [primary],
        // A replica is read-only by default; replicaof makes it follow the primary.
        command: ['redis-server', '--replicaof', primary, '6379', '--replica-read-only', 'yes'],
      },
      [`${secondaryId}-exporter`]: {
        image: 'oliver006/redis_exporter:v1.62.0',
        environment: { REDIS_ADDR: `redis://${secondaryId}:6379` },
        depends_on: [secondaryId],
      },
    },
    scrapeJob: { job_name: secondaryId, static_configs: [{ targets: [`${secondaryId}-exporter:9121`] }] },
    metrics: [
      { label: 'clients', query: `redis_connected_clients{job="${secondaryId}"}`, unit: '' },
      { label: 'ops/s', query: `sum(rate(redis_commands_processed_total{job="${secondaryId}"}[1m]))`, unit: '/s' },
      { label: 'link up', query: `redis_master_link_up{job="${secondaryId}"}`, unit: '' },
    ],
    health: { query: `redis_up{job="${secondaryId}"}`, rules: HEALTH_RULES },
  }
}

function buildMongoReplica({ secondaryId, primary }) {
  return {
    services: {
      [secondaryId]: {
        image: 'mongo:7',
        depends_on: [primary],
        command: ['mongod', '--replSet', 'rs0', '--bind_ip_all'],
      },
      [`${secondaryId}-exporter`]: {
        image: 'percona/mongodb_exporter:0.40',
        command: [`--mongodb.uri=mongodb://${secondaryId}:27017/?directConnection=true`, '--collect-all', '--compatible-mode'],
        depends_on: [secondaryId],
      },
    },
    scrapeJob: { job_name: secondaryId, static_configs: [{ targets: [`${secondaryId}-exporter:9216`] }] },
    metrics: [
      { label: 'repl state', query: `mongodb_mongod_replset_my_state{job="${secondaryId}"}`, unit: '' },
      { label: 'ops/s', query: `sum(rate(mongodb_ss_opcounters{job="${secondaryId}"}[1m]))`, unit: '/s' },
    ],
    health: { query: `mongodb_up{job="${secondaryId}"}`, rules: HEALTH_RULES },
  }
}

// A Cassandra "replica" is a second node that JOINS the primary's ring (via
// CASSANDRA_SEEDS + a shared cluster name), not a read-only standby — it accepts
// writes like any Cassandra node. It reuses the primary's custom exporter build
// context (created by buildCassandra) pointed at itself.
function buildCassandraReplica({ secondaryId, primary }) {
  return {
    services: {
      [secondaryId]: {
        image: 'cassandra:5',
        depends_on: [primary],
        environment: {
          CASSANDRA_CLUSTER_NAME: 'sandbox', // must match the primary to join its ring
          CASSANDRA_SEEDS: primary,
          MAX_HEAP_SIZE: '512M',
          HEAP_NEWSIZE: '128M',
        },
      },
      [`${secondaryId}-exporter`]: {
        build: `./${primary}/exporter`,
        depends_on: [secondaryId],
        environment: { CASSANDRA_HOST: secondaryId, CASSANDRA_PORT: '9042' },
      },
    },
    scrapeJob: { job_name: secondaryId, static_configs: [{ targets: [`${secondaryId}-exporter:9100`] }] },
    metrics: [
      { label: 'nodes seen', query: `cassandra_node_count{job="${secondaryId}"}`, unit: '' },
      { label: 'tables', query: `sum(cassandra_table_count{job="${secondaryId}"})`, unit: '' },
      { label: 'probe', query: `cassandra_probe_latency_seconds{job="${secondaryId}"}`, unit: 'ms', scale: 1000 },
    ],
    health: { query: `cassandra_up{job="${secondaryId}"}`, rules: HEALTH_RULES },
  }
}

const BUILDERS = {
  postgres: buildPostgresReplica,
  redis: buildRedisReplica,
  mongodb: buildMongoReplica,
  cassandra: buildCassandraReplica,
}

// ---------------------------------------------------------------------------
// Making the primary replication-ready (idempotent, engine-specific)
// ---------------------------------------------------------------------------

const PG_HBA_LINE = 'host replication all 0.0.0.0/0 trust'

// Postgres: allow replication connections from the compose network. Persist it
// as an initdb script (so a fresh rebuild reproduces it) AND apply it live to
// the already-running primary so we don't have to recreate it now.
async function prepPostgresPrimary(system, primary) {
  const dir = path.join(systemDir(system), primary)
  fs.mkdirSync(dir, { recursive: true })
  const script =
    '#!/bin/sh\n' +
    'set -e\n' +
    '# Allow read replicas to stream (added by Add read replica).\n' +
    `cat >> "$PGDATA/pg_hba.conf" <<'EOF'\n${PG_HBA_LINE}\nEOF\n`
  fs.writeFileSync(path.join(dir, 'repl-hba.sh'), script)
  addServiceVolume(system, primary, `./${primary}/repl-hba.sh:/docker-entrypoint-initdb.d/00-repl-hba.sh:ro`)

  if (skipDocker()) return ''
  // sh -c script below is a CONSTANT (no interpolation); `primary` is a discrete,
  // regex-validated exec arg, never shell-interpolated.
  const live =
    `grep -q "${PG_HBA_LINE}" "$PGDATA/pg_hba.conf" || echo "${PG_HBA_LINE}" >> "$PGDATA/pg_hba.conf"; ` +
    'psql -U sandbox -d postgres -c "SELECT pg_reload_conf();"'
  try {
    const r = await pexec(
      'docker',
      ['compose', '-f', composePath(system), 'exec', '-T', primary, 'sh', '-c', live],
      execOpts(),
    )
    return r.stdout + r.stderr
  } catch (err) {
    // Non-fatal: the persisted initdb script still makes a fresh primary ready.
    return `(warning: could not reload primary pg_hba live: ${err.stderr || err.message})`
  }
}

// Mongo: the first replica turns the standalone into a replica set — give the
// primary `--replSet rs0` (recreated below). Returns whether it changed.
function prepMongoPrimary(system, primary) {
  return setServiceCommand(system, primary, ['mongod', '--replSet', 'rs0', '--bind_ip_all'])
}

// Cassandra: raise the keyspace replication factor to 2 BEFORE the new node
// bootstraps, so it streams the existing data as it joins. Best-effort + live only;
// a from-scratch rebuild recreates the keyspace at RF=1 (init.cql), so re-adding a
// replica (or a manual ALTER + `nodetool repair`) is needed after such a rebuild.
async function prepCassandraPrimary(system, primary) {
  if (skipDocker()) return ''
  const ks = primary.replace(/-/g, '_')
  const cql = `ALTER KEYSPACE ${ks} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};`
  try {
    const r = await pexec(
      'docker',
      ['compose', '-f', composePath(system), 'exec', '-T', primary, 'cqlsh', '-e', cql],
      execOpts(),
    )
    return r.stdout + r.stderr
  } catch (err) {
    return `(warning: could not raise ${ks} replication factor: ${err.stderr || err.message})`
  }
}

// ---------------------------------------------------------------------------
// Rebuild + post-wiring (engine-specific service set, then replication wiring)
// ---------------------------------------------------------------------------

async function rebuildAndWire({ engine, system, primary, secondaryId, mode, manifest, mongoFirst }) {
  if (skipDocker()) return '(rebuild skipped)'
  const compose = composePath(system)
  const opts = execOpts()
  let log = ''
  const run = async (args) => {
    const r = await pexec('docker', ['compose', '-f', compose, ...args], opts)
    log += r.stdout + r.stderr
  }
  try {
    const sec = [secondaryId, `${secondaryId}-exporter`]
    // Mongo's first replica also (re)creates the primary so it runs with --replSet.
    const up = engine === 'mongodb' && mongoFirst ? [primary, ...sec] : sec
    await run(['up', '-d', ...up])

    if (engine === 'mongodb') {
      await wireMongo(compose, opts, primary, secondaryId).then((l) => (log += l))
    }
    if (engine === 'postgres' && mode === 'sync') {
      await setPostgresSyncStandbys(compose, opts, primary, manifest).then((l) => (log += l))
    }

    await run(['restart', 'prometheus'])
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
  return log
}

// Initiate the replica set if needed, wait for a writable primary, then add the
// new member as a priority-0 (never-elected) read-only secondary. `primary` /
// `secondaryId` are regex-validated and passed to mongosh via --eval (an arg).
async function wireMongo(compose, opts, primary, secondaryId) {
  const js = [
    `try { rs.status() } catch (e) { rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "${primary}:27017" }] }) }`,
    'for (var i = 0; i < 60; i++) { try { if (db.hello().isWritablePrimary) break } catch (e) {} sleep(1000) }',
    `try { rs.add({ host: "${secondaryId}:27017", priority: 0 }) } catch (e) { print(e) }`,
  ].join('\n')
  try {
    const r = await pexec(
      'docker',
      ['compose', '-f', compose, 'exec', '-T', primary, 'mongosh', '--quiet', '--eval', js],
      opts,
    )
    return r.stdout + r.stderr
  } catch (err) {
    return `(warning: mongo replica-set wiring: ${err.stderr || err.message})`
  }
}

// Recompute the primary's synchronous_standby_names from the manifest (all of
// its sync secondaries) and reload — so sync commits wait on those standbys.
async function setPostgresSyncStandbys(compose, opts, primary, manifest) {
  const names = manifest.nodes
    .filter((n) => n.replicaOf === primary && n.replication === 'sync')
    .map((n) => `"${n.id}"`)
    .join(',')
  // ALTER SYSTEM can't run inside a transaction block, so pass it and the reload
  // as two separate `-c` commands (psql runs each in its own transaction).
  const alter = `ALTER SYSTEM SET synchronous_standby_names = '${names}';`
  try {
    const r = await pexec(
      'docker',
      ['compose', '-f', compose, 'exec', '-T', primary, 'psql', '-U', 'sandbox', '-d', 'postgres', '-c', alter, '-c', 'SELECT pg_reload_conf();'],
      opts,
    )
    return r.stdout + r.stderr
  } catch (err) {
    return `(warning: could not set synchronous_standby_names: ${err.stderr || err.message})`
  }
}

// ---------------------------------------------------------------------------
// Request handling
// ---------------------------------------------------------------------------

function validate(body) {
  const { system, primary, mode = 'async' } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof primary !== 'string' || !DB_ID_RE.test(primary)) throw bad('invalid primary id')
  const manifest = readManifest(system)
  const primaryNode = manifest.nodes.find((n) => n.id === primary)
  if (!primaryNode) throw bad(`no node "${primary}" in this system`)
  if (primaryNode.origin !== 'create-database') throw bad(`"${primary}" is not a database`)
  if (primaryNode.replicaOf) throw bad(`"${primary}" is itself a replica — chained replication isn't supported`)
  const engine = primaryNode.type
  if (!ENGINE_LABEL[engine]) throw bad(`${ENGINE_LABEL[engine] || engine} databases don't support read replicas`)
  if (mode !== 'async' && mode !== 'sync') throw bad('mode must be "async" or "sync"')
  if (mode === 'sync' && engine !== 'postgres') throw bad('sync replication is only supported for postgres')
  return { system, primaryNode, engine, mode, manifest }
}

export async function handleCreateReplica(body) {
  const { system, primaryNode, engine, mode, manifest } = validate(body)
  const primary = primaryNode.id
  const { id: secondaryId, ordinal } = nextReplicaId(primary, manifest)
  if (manifest.nodes.some((n) => n.id === secondaryId)) {
    throw bad(`a node named "${secondaryId}" already exists`)
  }
  const dbName = primary.replace(/-/g, '_')
  const built = BUILDERS[engine]({ secondaryId, primary, dbName })

  // 1. Make the primary replication-ready (idempotent).
  let mongoFirst = false
  if (engine === 'postgres') await prepPostgresPrimary(system, primary)
  else if (engine === 'mongodb') mongoFirst = prepMongoPrimary(system, primary)
  else if (engine === 'cassandra') await prepCassandraPrimary(system, primary)

  // 2. Compose services + scrape job.
  addComposeServices(system, built.services, secondaryId, ENGINE_LABEL[engine], 'Add read replica')
  addScrapeJob(system, built.scrapeJob, secondaryId, 'Add read replica')

  // 3. Manifest: mark the primary, add the secondary node.
  primaryNode.role = 'primary'
  // Cassandra's secondary is a ring peer (accepts writes), not a read-only standby —
  // reflect that in the label/flags the diagram + Schema tab read.
  const isCassandra = engine === 'cassandra'
  const node = {
    id: secondaryId,
    // Engine is shown by the node `type` in the header's upper-right corner, so the
    // label is just the name + the meaningful qualifier (no engine prefix).
    label: `${secondaryId} (${isCassandra ? 'cluster node' : 'replica'})`,
    type: engine,
    origin: 'create-database',
    role: 'secondary',
    replicaOf: primary,
    replication: isCassandra ? 'peer' : mode,
    readonly: !isCassandra,
    position: replicaPosition(primaryNode, ordinal),
    metrics: built.metrics,
    health: built.health,
  }
  manifest.nodes.push(node)
  writeManifest(system, manifest)

  // 4. Bring up only the new services, wire replication, reload prometheus.
  const log = await rebuildAndWire({ engine, system, primary, secondaryId, mode, manifest, mongoFirst })
  return { ok: true, node, log }
}

export default function createReplica() {
  return {
    name: 'create-replica',
    configureServer(server) {
      server.middlewares.use('/api/db-replicas', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          const body = await readJsonBody(req)
          const result = await handleCreateReplica(body)
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(result))
        } catch (err) {
          res.statusCode = err.statusCode || 500
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ ok: false, error: err.message }))
        }
      })
    },
  }
}
