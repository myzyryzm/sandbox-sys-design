// Vite dev-server plugin: provision an event stream (Kafka) into the active system.
//
//   POST /api/event-streams  { system, type, name, topics? }
//     -> creates a single-broker Kafka (KRaft) + a kafka-exporter, a prometheus
//        scrape job, a manifest node, and a streams.json topic registry, then
//        rebuilds the stack.
//   GET  /api/event-stream?system=<id>&id=<cluster>[&live=0]
//     -> { ok, type, label, topics: [{ id, live, producers, consumers }] }
//        Topics/producers/consumers come from the streams.json registry, merged
//        with the broker's LIVE topic list (so a `live` flag and any broker-only
//        topics surface) — the same registry-⊕-live shape endpoints.js uses.
//        Probing the broker is a slow `docker compose exec` (~6s JVM boot), so
//        `&live=0` skips it for an instant registry-only paint (`live: null`); the
//        frontend fetches that first, then upgrades with the authoritative live=1.
//
// Mirrors databases.js / services.js: same plugin shape, comment-preserving YAML
// edits via the `yaml` Document API, strict whitelist validation (user input only
// ever lands in generated files, never a shell arg), and a frontend-safe
// `docker compose` rebuild — never ./start.sh, which would kill this dev server.
//
// Kafka speaks a binary protocol, not HTTP, so there is NO nginx route (unlike a
// service). Producers/consumers aren't something a broker tracks, so they live in
// the registry and are what the diagram's producer→cluster / cluster→consumer
// edges are drawn from.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemsDir, systemDir, isValidSystem, nextNodePosition } from './systems.js'
import { readModelsFile } from './models.js'

const pexec = promisify(execFile)

class HttpError extends Error {
  constructor(status, message) {
    super(message)
    this.statusCode = status
  }
}
const bad = (msg) => new HttpError(400, msg)

const NAME_RE = /^[a-z][a-z0-9-]*$/ // node id == compose service == folder name
const TOPIC_RE = /^[a-zA-Z0-9._-]+$/ // Kafka topic naming (also keeps it shell-safe)
const MODEL_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/ // mirrors MODEL_NAME_RE in models.js
const PARTITIONS_MAX = 64

// A topic's declared partition count: a clamped positive integer. Absent/garbage
// (incl. every pre-partitioning streams.json) means 1 — Kafka's own default here.
function normPartitions(raw) {
  const n = Number(raw)
  if (!Number.isInteger(n) || n < 1) return 1
  return Math.min(PARTITIONS_MAX, n)
}

// Health rule shared by every node: red when the target is down, green up.
const HEALTH_RULES = [
  { color: 'red', when: 'value < 1' },
  { color: 'green', when: 'value >= 1' },
]

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > 1_000_000) reject(bad('request body too large'))
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

// ---------------------------------------------------------------------------
// Per-type templates. Each returns the compose services to splice in, the
// prometheus scrape job, the manifest node metrics/health, and the node type.
// Only Kafka today; the dropdown (and this map) are ready for more engines.
// ---------------------------------------------------------------------------

function buildKafka({ name, topics }) {
  // One-shot sidecar: wait for the broker, then create each declared topic. Same
  // shape as the redis/minio `-init` seeders in databases.js. Topic ids are
  // regex-validated, so this generated shell string can't be injected into.
  const creates = topics
    .map((t) => `/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${name}:9092 --create --if-not-exists --topic ${t.id} --partitions ${normPartitions(t.partitions)} --replication-factor 1`)
    .join('\n')
  const seed = [
    'set -e',
    `until /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${name}:9092 --list >/dev/null 2>&1; do sleep 2; done`,
    creates,
  ].join('\n')

  return {
    nodeType: 'kafka',
    label: 'Kafka',
    services: {
      [name]: {
        image: 'apache/kafka:3.8.0',
        environment: {
          KAFKA_NODE_ID: 1,
          KAFKA_PROCESS_ROLES: 'broker,controller',
          KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093',
          KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://${name}:9092`,
          KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
          KAFKA_CONTROLLER_QUORUM_VOTERS: `1@${name}:9093`,
          KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1,
          KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1,
          KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1,
          KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0,
          KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
        },
      },
      [`${name}-exporter`]: {
        image: 'danielqsj/kafka-exporter:latest',
        command: [`--kafka.server=${name}:9092`],
        depends_on: [name],
        // The exporter exits (255) if the broker isn't accepting connections
        // yet at startup — depends_on only waits for the container to start,
        // not for Kafka to be ready. Restart so it retries until the broker is
        // up (and recovers if the broker later restarts).
        restart: 'unless-stopped',
      },
      [`${name}-init`]: {
        image: 'apache/kafka:3.8.0',
        depends_on: [name],
        restart: 'no',
        entrypoint: ['sh', '-c', seed],
      },
    },
    scrapeJob: { job_name: name, static_configs: [{ targets: [`${name}-exporter:9308`] }] },
    metrics: [
      // Internal Kafka topics (e.g. __consumer_offsets, 50 partitions by default) are
      // excluded with topic!~"__.*" so these counts reflect only user-declared topics.
      { label: 'topics', query: `count(count by (topic)(kafka_topic_partitions{job="${name}",topic!~"__.*"})) or vector(0)`, unit: '' },
      { label: 'partitions', query: `sum(kafka_topic_partitions{job="${name}",topic!~"__.*"}) or vector(0)`, unit: '' },
      { label: 'msgs/s', query: `sum(rate(kafka_topic_partition_current_offset{job="${name}",topic!~"__.*"}[1m])) or vector(0)`, unit: '/s' },
      { label: 'lag', query: `sum(kafka_consumergroup_lag{job="${name}",topic!~"__.*"}) or vector(0)`, unit: '' },
    ],
    health: { query: `up{job="${name}"}`, rules: HEALTH_RULES },
  }
}

const TYPES = {
  kafka: { label: 'Kafka', build: buildKafka },
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

function validate(body) {
  const { system, type, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const spec = TYPES[type]
  if (!spec) throw bad(`unknown event stream type "${type}"`)
  if (typeof name !== 'string' || !NAME_RE.test(name) || name.length > 40) {
    throw bad('name must be lowercase letters, digits and hyphens (start with a letter)')
  }

  const dir = systemDir(system)
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8'))
  if (manifest.nodes.some((n) => n.id === name)) {
    throw bad(`a node named "${name}" already exists in this system`)
  }
  if (fs.existsSync(path.join(dir, name))) throw bad(`systems/${system}/${name}/ already exists`)

  const rawTopics = Array.isArray(body.topics) ? body.topics : []
  const seen = new Set()
  const topics = []
  for (const raw of rawTopics) {
    const id = typeof raw === 'string' ? raw.trim() : (raw && raw.id) || ''
    if (!id) continue
    if (!TOPIC_RE.test(id) || id.length > 100) throw bad(`invalid topic id "${id}"`)
    if (seen.has(id)) throw bad(`duplicate topic "${id}"`)
    seen.add(id)
    topics.push({ id, partitions: normPartitions(raw && raw.partitions), producers: [], consumers: [] })
  }

  return { system, type, spec, name, manifest, topics }
}

// ---------------------------------------------------------------------------
// File mutations (comment-preserving, mirroring databases.js)
// ---------------------------------------------------------------------------

function addComposeServices(system, services, name, label) {
  const file = path.join(systemDir(system), 'docker-compose.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  let first = true
  for (const [svc, def] of Object.entries(services)) {
    const node = doc.createNode(def)
    if (first) {
      node.commentBefore = ` ${label} "${name}" — added by Add event stream`
      first = false
    }
    doc.setIn(['services', svc], node)
  }
  fs.writeFileSync(file, doc.toString())
}

function addScrapeJob(system, scrapeJob, name) {
  const file = path.join(systemDir(system), 'prometheus', 'prometheus.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const node = doc.createNode(scrapeJob)
  node.commentBefore = ` Event stream "${name}" — added by Add event stream`
  doc.addIn(['scrape_configs'], node)
  fs.writeFileSync(file, doc.toString())
}

function addManifestNode(system, manifest, built, name) {
  const node = {
    id: name,
    // The engine (Kafka) is shown by the node `type` in the header's upper-right
    // corner, so the label is just the name — prefixing it would duplicate the type.
    label: name,
    type: built.nodeType,
    origin: 'create-event-stream',
    position: nextNodePosition(manifest),
    metrics: built.metrics,
    health: built.health,
  }
  manifest.nodes.push(node)
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
  return node
}

async function rebuild(system) {
  // Escape hatch for tests/CI: validate file generation without pulling images.
  if (process.env.EVENT_STREAM_SKIP_REBUILD === '1') return '(rebuild skipped)'

  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
  let log = ''
  try {
    // up -d (no --build) leaves existing services running and only creates the
    // new ones. Restart prometheus to pick up the appended scrape job (its
    // mounted config doesn't reload on `up` since its definition is unchanged).
    const up = await pexec('docker', ['compose', '-f', compose, 'up', '-d'], opts)
    log += up.stdout + up.stderr
    const r = await pexec('docker', ['compose', '-f', compose, 'restart', 'prometheus'], opts)
    log += r.stdout + r.stderr
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
  return log
}

export async function handleCreate(body) {
  const { system, spec, name, manifest, topics } = validate(body)
  const built = spec.build({ name, topics })

  // 1. topic registry (the diagram + read-only modal read this)
  const dir = systemDir(system)
  fs.mkdirSync(path.join(dir, name), { recursive: true })
  fs.writeFileSync(
    path.join(dir, name, 'streams.json'),
    JSON.stringify({ topics }, null, 2) + '\n',
  )

  // 2-4. compose, prometheus, manifest
  addComposeServices(system, built.services, name, spec.label)
  addScrapeJob(system, built.scrapeJob, name)
  const node = addManifestNode(system, manifest, built, name)

  // 5. rebuild (frontend-safe)
  const log = await rebuild(system)
  return { ok: true, node, log }
}

// ---------------------------------------------------------------------------
// Read-only introspection: GET /api/event-stream
// ---------------------------------------------------------------------------

// The declared topics from systems/<id>/<cluster>/streams.json. Tolerates an
// absent/garbled file (a freshly-made cluster with no registry yet -> no topics).
function loadRegistry(system, id) {
  const file = path.join(systemDir(system), id, 'streams.json')
  let raw
  try {
    raw = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return new Map()
  }
  const map = new Map()
  for (const t of Array.isArray(raw?.topics) ? raw.topics : []) {
    if (!t || typeof t.id !== 'string') continue
    const producers = (Array.isArray(t.producers) ? t.producers : []).filter((p) => typeof p === 'string')
    const consumers = (Array.isArray(t.consumers) ? t.consumers : [])
      .filter((c) => c && typeof c.groupId === 'string')
      .map((c) => ({
        groupId: c.groupId,
        members: (Array.isArray(c.members) ? c.members : []).filter((m) => typeof m === 'string'),
      }))
    // Optional message-schema contract: a model-bank reference + whether it's enforced in
    // producer/consumer code. Absent fields mean "no schema / documented-only".
    const schemaModel = typeof t.schemaModel === 'string' ? t.schemaModel : ''
    const enforceSchema = t.enforceSchema === true
    map.set(t.id, { id: t.id, partitions: normPartitions(t.partitions), producers, consumers, schemaModel, enforceSchema })
  }
  return map
}

// The cluster-level "pause consumers" flag from streams.json (top-level boolean,
// omitted when false). Tolerates an absent/garbled file. Read live by pause-aware
// consumer loops (see the sandbox-event-stream skill) so toggling needs no rebuild.
function loadClusterPaused(system, id) {
  const file = path.join(systemDir(system), id, 'streams.json')
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))?.consumersPaused === true
  } catch {
    return false
  }
}

// Topics that actually exist on the running broker right now. Internal Kafka
// topics (e.g. __consumer_offsets) are hidden. Returns null if the broker can't
// be reached, so the caller can fall back to registry-only (live:false).
async function liveTopics(system, id) {
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  try {
    const { stdout } = await pexec(
      'docker',
      ['compose', '-f', compose, 'exec', '-T', id,
        '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', `${id}:9092`, '--list'],
      { cwd: repoRoot, timeout: 15_000, maxBuffer: 4 * 1024 * 1024 },
    )
    return new Set(
      stdout.split('\n').map((s) => s.trim()).filter((s) => s && !s.startsWith('__')),
    )
  } catch {
    return null
  }
}

async function getStreams(system, id, { checkLive = true } = {}) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.origin !== 'create-event-stream') {
    throw bad(`"${id}" is not an event stream in this system`)
  }

  const registry = loadRegistry(system, id)
  // Liveness is a slow `docker compose exec` — the broker's kafka-topics.sh is a
  // ~6s JVM boot, so probing it on every modal open made topics seem to "not load".
  // When the caller only wants a fast first paint (?live=0) we skip the probe and
  // report `live: null` (unknown); a follow-up ?live=1 request fills the flags in.
  const live = checkLive ? await liveTopics(system, id) : null // Set | null
  const order = []
  // Registry topics first (declared order), annotated with live presence
  // (null = not probed yet, so the frontend shows no pending/live badge).
  for (const [tid, meta] of registry) {
    order.push({ ...meta, live: checkLive ? Boolean(live && live.has(tid)) : null })
  }
  // Broker topics not in the registry (created out-of-band) — show them too.
  // Their partition count is unknown without a per-topic --describe probe: null.
  if (live) {
    for (const tid of live) {
      if (!registry.has(tid)) order.push({ id: tid, partitions: null, producers: [], consumers: [], schemaModel: '', enforceSchema: false, live: true })
    }
  }
  order.sort((a, b) => a.id.localeCompare(b.id))
  return {
    ok: true,
    type: node.type,
    label: node.label,
    consumersPaused: loadClusterPaused(system, id),
    topics: order,
  }
}

// Set (or clear) a topic's message-schema contract in the registry. `schemaModel` is a
// model-bank reference (validated to exist, like an endpoint's requestModel/responseModel);
// `enforceSchema` records whether producers/consumers validate against it at runtime. This
// is a pure registry edit (no docker rebuild) — the actual runtime-validation code changes
// (when enforceSchema flips on for a topic that already has producers/consumers) are a
// judgment task delegated to a launched sandbox-event-stream session, not done here. The
// raw file is mutated in place so producers/consumers are preserved verbatim.
function setTopicSchema(system, id, topic, schemaModel, enforceSchema) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.origin !== 'create-event-stream') {
    throw bad(`"${id}" is not an event stream in this system`)
  }
  if (typeof topic !== 'string' || !topic) throw bad('topic is required')

  const model = typeof schemaModel === 'string' ? schemaModel.trim() : ''
  if (model) {
    if (!MODEL_NAME_RE.test(model) || model.length > 60) throw bad(`invalid model name "${model}"`)
    if (!readModelsFile(system).models.some((m) => m && m.name === model)) {
      throw bad(`model "${model}" is not defined in the models bank`)
    }
  }
  const enforce = enforceSchema === true

  const file = path.join(systemDir(system), id, 'streams.json')
  let raw
  try {
    raw = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    throw bad(`no topic registry for "${id}"`)
  }
  const t = (Array.isArray(raw?.topics) ? raw.topics : []).find((x) => x && x.id === topic)
  if (!t) throw bad(`topic "${topic}" not found on "${id}"`)
  // Keep the entry tidy: store the fields only when meaningful (absent == none / not enforced).
  if (model) t.schemaModel = model
  else delete t.schemaModel
  if (enforce) t.enforceSchema = true
  else delete t.enforceSchema
  fs.writeFileSync(file, JSON.stringify(raw, null, 2) + '\n')
  return { id: t.id, schemaModel: model, enforceSchema: enforce }
}

// Grow a topic's partition count on the LIVE broker, then persist it in the
// registry. Kafka can only ever ADD partitions (`--alter --partitions N` rejects a
// decrease), so this is increase-only by validation too. Mechanical — no rebuild;
// consumer groups on the topic rebalance onto the new partitions automatically.
async function alterTopicPartitions(system, id, topic, partitions) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.origin !== 'create-event-stream') {
    throw bad(`"${id}" is not an event stream in this system`)
  }
  if (typeof topic !== 'string' || !TOPIC_RE.test(topic)) throw bad(`invalid topic "${topic}"`)

  const file = path.join(systemDir(system), id, 'streams.json')
  let raw
  try {
    raw = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    throw bad(`no topic registry for "${id}"`)
  }
  const t = (Array.isArray(raw?.topics) ? raw.topics : []).find((x) => x && x.id === topic)
  if (!t) throw bad(`topic "${topic}" not found on "${id}"`)

  const current = normPartitions(t.partitions)
  const n = Number(partitions)
  if (!Number.isInteger(n) || n <= current) {
    throw bad(`partitions must be an integer greater than the current ${current} (Kafka cannot shrink a topic)`)
  }
  if (n > PARTITIONS_MAX) throw bad(`partitions must be at most ${PARTITIONS_MAX}`)

  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  try {
    await pexec(
      'docker',
      ['compose', '-f', compose, 'exec', '-T', id,
        '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', `${id}:9092`,
        '--alter', '--topic', topic, '--partitions', String(n)],
      { cwd: repoRoot, timeout: 60_000, maxBuffer: 4 * 1024 * 1024 },
    )
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `kafka-topics --alter failed:\n${detail}`)
  }

  t.partitions = n
  fs.writeFileSync(file, JSON.stringify(raw, null, 2) + '\n')
  return { id: t.id, partitions: n }
}

// Set (or clear) the cluster-level "pause consumers" flag in streams.json. Like
// setTopicSchema this is a pure registry edit — no docker rebuild. Pause-aware
// consumer loops mount this file and re-read it by mtime, so they stop/resume
// fetching within one poll cycle (see the sandbox-event-stream skill). The flag is
// stored only when true to keep the file tidy (absent == not paused).
function setClusterPause(system, id, paused) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.origin !== 'create-event-stream') {
    throw bad(`"${id}" is not an event stream in this system`)
  }
  if (typeof paused !== 'boolean') throw bad('consumersPaused must be a boolean')

  const file = path.join(systemDir(system), id, 'streams.json')
  let raw
  try {
    raw = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    throw bad(`no topic registry for "${id}"`)
  }
  if (paused) raw.consumersPaused = true
  else delete raw.consumersPaused
  fs.writeFileSync(file, JSON.stringify(raw, null, 2) + '\n')
  return { consumersPaused: paused }
}

// Every event-stream cluster in the system whose consumers are currently paused.
// Registry-only (no broker probe) so the diagram can poll it cheaply, like outages.
function listPausedClusters(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  return manifest.nodes
    .filter((n) => n.origin === 'create-event-stream' && loadClusterPaused(system, n.id))
    .map((n) => n.id)
}

export default function eventStreams() {
  return {
    name: 'event-streams',
    configureServer(server) {
      // POST /api/event-streams — create a cluster.
      server.middlewares.use('/api/event-streams', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          const body = await readJsonBody(req)
          const result = await handleCreate(body)
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(result))
        } catch (err) {
          res.statusCode = err.statusCode || 500
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ ok: false, error: err.message }))
        }
      })

      // GET  /api/event-stream — read-only topics + producers/consumers.
      // POST /api/event-stream — set a topic's message schema (registry write, no rebuild).
      server.middlewares.use('/api/event-stream', async (req, res, next) => {
        const json = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }
        if (req.method === 'GET') {
          try {
            const url = new URL(req.url, 'http://localhost')
            // ?live=0 → fast registry-only paint (skip the slow broker probe).
            const checkLive = url.searchParams.get('live') !== '0'
            const result = await getStreams(
              url.searchParams.get('system'),
              url.searchParams.get('id'),
              { checkLive },
            )
            return json(200, result)
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
        }
        if (req.method === 'POST') {
          try {
            const body = await readJsonBody(req)
            // Three cluster registry writes share this route: a per-topic schema set
            // (carries `topic`, no `partitions`), a per-topic partition grow (carries
            // `topic` + `partitions` — the only one that touches the live broker), and
            // the cluster-level consumers-pause toggle (no `topic`, carries a boolean
            // `consumersPaused`). None needs a docker rebuild.
            if (body.topic === undefined && typeof body.consumersPaused === 'boolean') {
              const result = setClusterPause(body.system, body.id, body.consumersPaused)
              return json(200, { ok: true, ...result })
            }
            if (body.topic !== undefined && body.partitions !== undefined) {
              const topic = await alterTopicPartitions(body.system, body.id, body.topic, body.partitions)
              return json(200, { ok: true, topic })
            }
            const topic = setTopicSchema(body.system, body.id, body.topic, body.schemaModel, body.enforceSchema)
            return json(200, { ok: true, topic })
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
        }
        return next()
      })

      // GET /api/consumer-pause?system=<id> — the set of clusters whose consumers are
      // paused. Registry-only (no broker probe) so the diagram can poll it cheaply.
      server.middlewares.use('/api/consumer-pause', async (req, res, next) => {
        if (req.method !== 'GET') return next()
        const json = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }
        try {
          const url = new URL(req.url, 'http://localhost')
          return json(200, { ok: true, paused: listPausedClusters(url.searchParams.get('system')) })
        } catch (err) {
          return json(err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
