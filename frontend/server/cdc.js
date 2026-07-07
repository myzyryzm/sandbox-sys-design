// Vite dev-server plugin: Change Data Capture (CDC) for a database.
//
//   GET  /api/db-cdc?system=&id=   -> { ok, type, label, entities, rules, streams }
//   POST /api/db-cdc               { system, id, table, operations[], stream, topic }  add/update a rule
//   POST /api/db-cdc-remove        { system, id, table, stream, topic }                remove a rule
//
// A database's change events are captured by a REAL per-database worker container
// (`<db>-cdc`) and produced to a Kafka topic. There is no mock layer: the worker
// streams from postgres logical replication / mongo change streams and produces to
// the broker. This plugin does only the MECHANICAL scaffold; the engine-specific
// capture code is authored by a spawned Claude session via the `sandbox-database-cdc`
// skill (the frontend launches it when the worker must first be built).
//
// Rules live in systems/<id>/<db>/cdc.json (parallel to seeds.json), mounted
// read-only into the worker so edits survive a restart. Each rule is a flat
// { table, operations:[INSERT|UPDATE|DELETE], stream, topic }; identity for
// upsert/remove is the (table, stream, topic) tuple. The worker registers as a
// producer in each target stream's streams.json and the diagram draws
// db -> <db>-cdc -> <stream> from manifest edges.
//
// All user input is validated against the LIVE schema + manifest whitelists; docker
// runs via execFile arg arrays (never a shell string); only generated files are
// written — never ./start.sh, which would kill this dev server.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemDir, isValidSystem, nextNodePosition } from './systems.js'
import { HttpError, bad, readJsonBody, HEALTH_RULES, addComposeServices, addScrapeJob } from './databases.js'
import { getSchema } from './dbschema.js'

const pexec = promisify(execFile)
const skipDocker = () => process.env.CDC_SKIP_REBUILD === '1'

const CDC_ENGINES = new Set(['postgres', 'mongodb', 'dynamodb', 'cassandra'])
const CDC_DB_PORTS = { postgres: 5432, mongodb: 27017, dynamodb: 8000, cassandra: 9042 }
const CANON_OPS = ['INSERT', 'UPDATE', 'DELETE'] // canonical change-event order
const OPS = new Set(CANON_OPS)
const TOPIC_RE = /^[a-zA-Z0-9._-]+$/ // Kafka topic naming (also keeps it shell-safe)

const dbName = (id) => id.replace(/-/g, '_')
const workerOf = (id) => `${id}-cdc`
const composePath = (system) => path.join(systemDir(system), 'docker-compose.yml')
const execOpts = () => ({ cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 })

// ---------------------------------------------------------------------------
// Manifest / registry helpers
// ---------------------------------------------------------------------------

function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(path.join(systemDir(system), 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n')
}

// Resolve + validate a request to a real, CDC-capable database node.
function resolve(system, id) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = (manifest.nodes || []).find((n) => n.id === id)
  if (!node || node.origin !== 'create-database') throw bad(`"${id}" is not a database in this system`)
  if (node.replicaOf) throw bad('cannot configure CDC on a read replica')
  if (!CDC_ENGINES.has(node.type)) throw bad(`CDC is not supported for ${node.type}`)
  return { node, manifest }
}

const cdcPath = (system, id) => path.join(systemDir(system), id, 'cdc.json')

function readCdc(system, id) {
  try {
    const raw = JSON.parse(fs.readFileSync(cdcPath(system, id), 'utf8'))
    const rules = Array.isArray(raw.rules) ? raw.rules : []
    return {
      rules: rules
        .filter((r) => r && typeof r.table === 'string' && typeof r.stream === 'string' && typeof r.topic === 'string')
        .map((r) => ({
          table: r.table,
          operations: (Array.isArray(r.operations) ? r.operations : []).filter((o) => OPS.has(o)),
          stream: r.stream,
          topic: r.topic,
        })),
    }
  } catch {
    return { rules: [] }
  }
}

function writeCdc(system, id, cdc) {
  const dir = path.join(systemDir(system), id)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(cdcPath(system, id), JSON.stringify(cdc, null, 2) + '\n')
}

// --- target stream registry (streams.json) -----------------------------------

const streamsPath = (system, cluster) => path.join(systemDir(system), cluster, 'streams.json')

function readStreams(system, cluster) {
  try {
    const raw = JSON.parse(fs.readFileSync(streamsPath(system, cluster), 'utf8'))
    return { topics: Array.isArray(raw.topics) ? raw.topics : [] }
  } catch {
    return { topics: [] }
  }
}
function writeStreams(system, cluster, data) {
  const dir = path.join(systemDir(system), cluster)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(streamsPath(system, cluster), JSON.stringify(data, null, 2) + '\n')
}

function registerProducer(system, cluster, topic, producerId) {
  const data = readStreams(system, cluster)
  let t = data.topics.find((x) => x && x.id === topic)
  if (!t) {
    t = { id: topic, producers: [], consumers: [] }
    data.topics.push(t)
  }
  if (!Array.isArray(t.producers)) t.producers = []
  if (!t.producers.includes(producerId)) t.producers.push(producerId)
  writeStreams(system, cluster, data)
}
function deregisterProducer(system, cluster, topic, producerId) {
  const data = readStreams(system, cluster)
  const t = data.topics.find((x) => x && x.id === topic)
  if (!t) return
  t.producers = (t.producers || []).filter((p) => p !== producerId)
  writeStreams(system, cluster, data)
}
function deregisterProducerAll(system, cluster, producerId) {
  const data = readStreams(system, cluster)
  let changed = false
  for (const t of data.topics || []) {
    const before = (t.producers || []).length
    t.producers = (t.producers || []).filter((p) => p !== producerId)
    if (t.producers.length !== before) changed = true
  }
  if (changed) writeStreams(system, cluster, data)
}

// --- manifest edges ----------------------------------------------------------

function addEdge(manifest, from, to) {
  manifest.edges = manifest.edges || []
  if (!manifest.edges.some((e) => e.from === from && e.to === to)) manifest.edges.push({ from, to })
}
function removeEdge(manifest, from, to) {
  manifest.edges = (manifest.edges || []).filter((e) => !(e.from === from && e.to === to))
}

// ---------------------------------------------------------------------------
// Compose helpers (comment-preserving)
// ---------------------------------------------------------------------------

// Set a service's `command`, only if not already set (idempotent — a mongo db
// already converted to a replica set for a read replica keeps its command).
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

// Add a dependency to a service's depends_on list, only if absent.
function addServiceDependsOn(system, service, dep) {
  const file = composePath(system)
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const p = ['services', service, 'depends_on']
  const cur = doc.getIn(p)
  const arr = cur && typeof cur.toJSON === 'function' ? cur.toJSON() : Array.isArray(cur) ? cur : []
  if (!arr.includes(dep)) {
    doc.addIn(p, doc.createNode(dep))
    fs.writeFileSync(file, doc.toString())
  }
}

function removeComposeService(system, name) {
  const file = composePath(system)
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  if (doc.hasIn(['services', name])) doc.deleteIn(['services', name])
  fs.writeFileSync(file, doc.toString())
}

function removeScrapeJob(system, jobName) {
  const file = path.join(systemDir(system), 'prometheus', 'prometheus.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const sc = doc.get('scrape_configs')
  const i = sc?.items?.findIndex((it) => String(it.get('job_name')) === jobName) ?? -1
  if (i >= 0) sc.delete(i)
  fs.writeFileSync(file, doc.toString())
}

// ---------------------------------------------------------------------------
// Worker shape (compose service + scrape job + manifest node)
// ---------------------------------------------------------------------------

const pgSlot = (dbId) => `${dbName(dbId)}_cdc` // valid pg identifier; backend drops it on teardown

function workerService(engine, dbId, streams) {
  const environment = {
    CDC_ENGINE: engine,
    CDC_DB_HOST: dbId,
    CDC_DB_PORT: CDC_DB_PORTS[engine],
    CDC_DB_NAME: dbName(dbId), // postgres db / mongo db / cassandra keyspace (unused by dynamodb)
    CDC_DB_USER: 'sandbox',
    CDC_DB_PASSWORD: 'sandbox',
  }
  // The worker must name its logical replication slot CDC_PG_SLOT so the backend
  // can drop it on teardown (an orphan slot makes postgres retain WAL forever).
  if (engine === 'postgres') environment.CDC_PG_SLOT = pgSlot(dbId)
  // DynamoDB Streams: the worker tails via boto3, so it needs the endpoint + creds.
  if (engine === 'dynamodb') {
    environment.DDB_ENDPOINT = `http://${dbId}:8000`
    environment.AWS_ACCESS_KEY_ID = 'sandbox'
    environment.AWS_SECRET_ACCESS_KEY = 'sandbox'
    environment.AWS_DEFAULT_REGION = 'us-east-1'
  }
  return {
    build: `./${workerOf(dbId)}`,
    depends_on: [dbId, ...streams],
    environment,
    // cdc.json is mounted read-only so the worker reads its rules on (re)start.
    volumes: [`./${dbId}/cdc.json:/cdc.json:ro`],
    restart: 'unless-stopped',
  }
}

function workerMetrics(wid) {
  return [
    { label: 'captured/s', query: `sum(rate(cdc_events_captured_total{job="${wid}"}[1m])) or vector(0)`, unit: '/s' },
    { label: 'produced/s', query: `sum(rate(cdc_events_produced_total{job="${wid}"}[1m])) or vector(0)`, unit: '/s' },
    { label: 'errors/s', query: `sum(rate(cdc_errors_total{job="${wid}"}[1m])) or vector(0)`, unit: '/s' },
  ]
}

function scaffoldWorker(system, manifest, dbId, engine, streams) {
  const wid = workerOf(dbId)
  addComposeServices(system, { [wid]: workerService(engine, dbId, streams) }, wid, 'CDC', 'Add CDC')
  addScrapeJob(system, { job_name: wid, static_configs: [{ targets: [`${wid}:8000`] }] }, wid, 'Add CDC')
  manifest.nodes.push({
    id: wid,
    label: wid,
    type: 'cdc',
    origin: 'create-cdc',
    cdcOf: dbId,
    position: nextNodePosition(manifest),
    metrics: workerMetrics(wid),
    health: { query: `up{job="${wid}"}`, rules: HEALTH_RULES },
  })
  addEdge(manifest, dbId, wid)
  for (const s of streams) addEdge(manifest, wid, s)
  writeManifest(system, manifest)
  // Empty dir — the skill session authors the Dockerfile/app.py/requirements and
  // builds it. The backend never builds the worker.
  fs.mkdirSync(path.join(systemDir(system), wid), { recursive: true })
}

// ---------------------------------------------------------------------------
// Docker (frontend-safe; never ./start.sh)
// ---------------------------------------------------------------------------

async function run(system, args) {
  try {
    const r = await pexec('docker', ['compose', '-f', composePath(system), ...args], execOpts())
    return r.stdout + r.stderr
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
}

// Make a running worker re-read its mounted cdc.json. Best-effort: if the worker
// hasn't been built yet (its authoring session is still in flight), the new rule is
// already in cdc.json and the pending build will pick it up — so a failed restart is
// just a warning, never a 500. Never `up` here (that could try to build an empty dir).
async function restartWorker(system, wid) {
  try {
    await pexec('docker', ['compose', '-f', composePath(system), 'restart', wid], execOpts())
    return null
  } catch (err) {
    return `restart ${wid} (it may not be built yet): ${err.stderr || err.message}`
  }
}

// Initiate the single-node replica set mongo change streams require. Idempotent:
// skips if the set is already up (e.g. a read replica already converted it).
async function initMongoReplSet(system, dbId) {
  const js = [
    `try { rs.status() } catch (e) { rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "${dbId}:27017" }] }) }`,
    'for (var i = 0; i < 60; i++) { try { if (db.hello().isWritablePrimary) break } catch (e) {} sleep(1000) }',
  ].join('\n')
  try {
    await pexec(
      'docker',
      ['compose', '-f', composePath(system), 'exec', '-T', dbId, 'mongosh', '--quiet', '--eval', js],
      execOpts(),
    )
    return null
  } catch (err) {
    return `mongo replica-set init: ${err.stderr || err.message}`
  }
}

// Drop a postgres logical replication slot once its worker container is gone (an
// idle slot makes the primary retain WAL forever). Best-effort: the db may be down,
// or the slot may not exist. Run AFTER the worker is removed so the slot is inactive.
async function dropPgSlot(system, dbId) {
  const slot = pgSlot(dbId)
  const sql = `SELECT pg_drop_replication_slot('${slot}') FROM pg_replication_slots WHERE slot_name = '${slot}';`
  try {
    await pexec(
      'docker',
      ['compose', '-f', composePath(system), 'exec', '-T', '-e', 'PGPASSWORD=sandbox', dbId,
        'psql', '-U', 'sandbox', '-d', dbName(dbId), '-w', '-c', sql],
      execOpts(),
    )
    return null
  } catch (err) {
    return `drop replication slot "${slot}": ${err.stderr || err.message}`
  }
}

// Create the target topic on the live broker (auto-create is OFF, so a producer to
// a missing topic fails). Best-effort: the broker may be momentarily unreachable,
// and the worker also ensures the topic at runtime.
async function ensureTopic(system, cluster, topic) {
  try {
    await pexec(
      'docker',
      ['compose', '-f', composePath(system), 'exec', '-T', cluster,
        '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', `${cluster}:9092`,
        '--create', '--if-not-exists', '--topic', topic, '--partitions', '1', '--replication-factor', '1'],
      execOpts(),
    )
    return null
  } catch (err) {
    return `create topic "${topic}" on ${cluster}: ${err.stderr || err.message}`
  }
}

// ---------------------------------------------------------------------------
// Request handlers
// ---------------------------------------------------------------------------

async function handleGet(system, id) {
  const { node, manifest } = resolve(system, id)
  const schema = await getSchema(system, id) // throws 502 if the container is down
  const streams = (manifest.nodes || [])
    .filter((n) => n.origin === 'create-event-stream')
    .map((n) => ({ id: n.id, topics: readStreams(system, n.id).topics.map((t) => t.id) }))
  return { ok: true, type: node.type, label: node.label, entities: schema.entities, rules: readCdc(system, id).rules, streams }
}

async function handleAdd(body) {
  const { system, id } = body
  const { node, manifest } = resolve(system, id)
  const engine = node.type

  // --- validate the rule against the live schema + manifest --------------------
  const table = String(body.table || '')
  const rawOps = Array.isArray(body.operations) ? body.operations.map((o) => String(o).toUpperCase()) : []
  const operations = CANON_OPS.filter((o) => rawOps.includes(o))
  if (!operations.length) throw bad('select at least one operation (INSERT / UPDATE / DELETE)')
  for (const o of rawOps) if (!OPS.has(o)) throw bad(`invalid operation "${o}"`)

  const schema = await getSchema(system, id)
  if (!schema.entities.find((e) => e.name === table)) {
    throw bad(`unknown ${engine === 'mongodb' ? 'collection' : 'table'} "${table}"`)
  }

  const stream = String(body.stream || '')
  const streamNode = manifest.nodes.find((n) => n.id === stream && n.origin === 'create-event-stream')
  if (!streamNode) throw bad(`"${stream}" is not an event stream in this system`)

  const topic = String(body.topic || '')
  if (!TOPIC_RE.test(topic) || topic.length > 100) throw bad(`invalid topic "${topic}"`)

  const wid = workerOf(id)
  const workerExists = manifest.nodes.some((n) => n.id === wid)

  // --- upsert the rule (identity = table+stream+topic) ------------------------
  const cdc = readCdc(system, id)
  const existing = cdc.rules.find((r) => r.table === table && r.stream === stream && r.topic === topic)
  if (existing) existing.operations = operations
  else cdc.rules.push({ table, operations, stream, topic })
  writeCdc(system, id, cdc)

  // --- the worker is a producer of this topic --------------------------------
  registerProducer(system, stream, topic, wid)

  const warnings = []

  if (!workerExists) {
    // FIRST RULE: enable the engine for CDC (postgres/mongodb only), scaffold the worker
    // (NOT built here — the spawned skill session authors + builds it). DynamoDB already
    // has per-table Streams (set at table creation) and Cassandra uses a polling worker,
    // so neither mutates or recreates the db container.
    const dbCommandChanged = engine === 'postgres' || engine === 'mongodb'
    if (engine === 'postgres') {
      setServiceCommand(system, id, ['postgres', '-c', 'wal_level=logical', '-c', 'max_wal_senders=10', '-c', 'max_replication_slots=10'])
    } else if (engine === 'mongodb') {
      setServiceCommand(system, id, ['mongod', '--replSet', 'rs0', '--bind_ip_all'])
    }
    const streams = [...new Set(cdc.rules.map((r) => r.stream))]
    scaffoldWorker(system, manifest, id, engine, streams)

    if (!skipDocker()) {
      if (dbCommandChanged) await run(system, ['up', '-d', id]) // recreate db with the new command
      if (engine === 'mongodb') {
        const w = await initMongoReplSet(system, id)
        if (w) warnings.push(w)
      }
      const t = await ensureTopic(system, stream, topic)
      if (t) warnings.push(t)
      await run(system, ['restart', 'prometheus']) // load the worker scrape job
    }
    return { ok: true, rules: cdc.rules, needsWorker: true, workerId: wid, warnings }
  }

  // WORKER EXISTS: ensure the worker depends on / draws to this stream, then make
  // it re-read cdc.json. No Claude session — pure registry + restart.
  const newStream = !(manifest.edges || []).some((e) => e.from === wid && e.to === stream)
  if (newStream) {
    addServiceDependsOn(system, wid, stream)
    addEdge(manifest, wid, stream)
    writeManifest(system, manifest)
  }
  if (!skipDocker()) {
    const t = await ensureTopic(system, stream, topic)
    if (t) warnings.push(t)
    const w = await restartWorker(system, wid)
    if (w) warnings.push(w)
  }
  return { ok: true, rules: cdc.rules, needsWorker: false, workerId: wid, warnings }
}

async function handleRemove(body) {
  const { system, id } = body
  const { node, manifest } = resolve(system, id)
  const engine = node.type
  const table = String(body.table || '')
  const stream = String(body.stream || '')
  const topic = String(body.topic || '')
  const wid = workerOf(id)

  const cdc = readCdc(system, id)
  const before = cdc.rules.length
  cdc.rules = cdc.rules.filter((r) => !(r.table === table && r.stream === stream && r.topic === topic))
  if (cdc.rules.length === before) throw bad('no such CDC rule')
  writeCdc(system, id, cdc)

  if (cdc.rules.length === 0) {
    // LAST RULE: tear the worker down (engine enablement is left in place — reverting
    // wal_level / the replica set is risky and unnecessary).
    removeComposeService(system, wid)
    removeScrapeJob(system, wid)
    manifest.nodes = manifest.nodes.filter((n) => n.id !== wid)
    manifest.edges = (manifest.edges || []).filter((e) => e.from !== wid && e.to !== wid)
    writeManifest(system, manifest)
    for (const n of manifest.nodes.filter((n) => n.origin === 'create-event-stream')) {
      deregisterProducerAll(system, n.id, wid)
    }
    fs.rmSync(path.join(systemDir(system), wid), { recursive: true, force: true })
    const warnings = []
    if (!skipDocker()) {
      await run(system, ['up', '-d', '--remove-orphans']) // worker container gone → slot inactive
      if (engine === 'postgres') {
        const w = await dropPgSlot(system, id)
        if (w) warnings.push(w)
      }
      await run(system, ['restart', 'prometheus'])
    }
    return { ok: true, rules: [], removed: true, warnings }
  }

  // Not the last rule: drop the producer entry / worker→stream edge if nothing else
  // still uses them, then restart the worker so it re-reads cdc.json.
  if (!cdc.rules.some((r) => r.stream === stream && r.topic === topic)) {
    deregisterProducer(system, stream, topic, wid)
  }
  if (!cdc.rules.some((r) => r.stream === stream)) {
    removeEdge(manifest, wid, stream)
    writeManifest(system, manifest)
  }
  const warnings = []
  if (!skipDocker()) {
    const w = await restartWorker(system, wid)
    if (w) warnings.push(w)
  }
  return { ok: true, rules: cdc.rules, removed: false, warnings }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

export default function cdc() {
  return {
    name: 'cdc',
    configureServer(server) {
      const send = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }

      server.middlewares.use('/api/db-cdc-remove', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          send(res, 200, await handleRemove(await readJsonBody(req)))
        } catch (err) {
          send(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })

      server.middlewares.use('/api/db-cdc', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const url = new URL(req.url, 'http://localhost')
            return send(res, 200, await handleGet(url.searchParams.get('system'), url.searchParams.get('id')))
          }
          if (req.method === 'POST') return send(res, 200, await handleAdd(await readJsonBody(req)))
          return next()
        } catch (err) {
          send(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
