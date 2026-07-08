// Custom service type: LLM Worker — simulated LLM inference with continuous batching.
//
// "Add service" → "LLM Worker" creates ONE worker service (FastAPI + a Worker gRPC
// server) AND its linked redis "<name>-stream" (+ exporter) in a single onAdd: the
// worker streams every generated token to redis streams (tokens:<user_message_id>),
// so the two are provisioned — and torn down (streamOf cascade in remove.js) —
// together. The Worker contract is registered into the grpc bank (direct-write,
// identical to modal-authored ones).
//
// Live tunables (ttl_seconds / chat_db / max_active) live in systems/<id>/<name>/
// worker.json, bind-mounted read-only and mtime-polled by the container — the config
// route writes the file IN PLACE (single-file bind mounts pin their inode on macOS
// Docker Desktop; tmp+rename would detach the mount) and needs no rebuild. The
// on_cache_evict hook registry (hook.json) is pure metadata: the Edit tab launches a
// Claude session (sandbox-llm-worker skill) that authors <name>/hooks.py and restarts
// the worker; the session owns `implemented`.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  bad, serviceMetrics, serviceHealth, cloneTemplate, addComposeService,
  addNginxRoute, addScrapeJob, addManifestNode, rebuild, NAME_RE,
} from '../scaffold.js'
import { addComposeServices, addScrapeJob as addDbScrapeJob, HEALTH_RULES } from '../databases.js'
import { installContracts } from '../grpcInstall.js'
import { isValidSystem, systemDir, nextNodePosition } from '../systems.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const TPL = path.join(__dirname, '..', 'templates', 'llm-worker')
const WORKER_DIR = path.join(TPL, 'worker')
const GRPC_DIR = path.join(TPL, 'grpc')
const SERVICE_FILES = ['app.py', 'model.py', 'hooks.py', 'requirements.txt', 'Dockerfile']

const CONFIG_DEFAULTS = { ttl_seconds: 30, chat_db: null, max_active: 5 }
const TTL_MAX = 60
const MAX_ACTIVE_MAX = 32
const LB = 'http://localhost:8080' // the system's load balancer (compose maps 8080:80)
const SKIP_REBUILD = () => process.env.CREATE_SVC_SKIP_REBUILD === '1'

const read = (dir, f) => fs.readFileSync(path.join(dir, f), 'utf8')
const readManifest = (system) => JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
const readJsonFile = (file, fallback) => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return fallback
  }
}

// The fixed contract this type owns, with provenance text recorded in the bank.
function contractSpecs() {
  return [
    {
      contract: 'Worker',
      proto: read(GRPC_DIR, 'Worker.proto'),
      servicer: read(GRPC_DIR, 'Worker_servicer.py'),
      source: 'llm_worker',
      instruction:
        'Simulated LLM inference worker. AddPrompt admits a UserMessage (id + content ' +
        'mandatory; chat/message optional) into the continuous-batching loop — prompts are ' +
        'tokenized a-z → 0-25, generated tokens stream to the linked redis (stream key ' +
        'tokens:<id>, END token 26 last), and finished sequences keep their KV caches in a ' +
        'TTL prefix cache keyed by chat. GetStatus reports whether the worker has space.',
    },
  ]
}

// The linked redis node's metric cards (same PromQL shape the database flow writes).
function redisMetrics(name) {
  return [
    { label: 'clients', query: `redis_connected_clients{job="${name}"}`, unit: '' },
    { label: 'ops/s', query: `sum(rate(redis_commands_processed_total{job="${name}"}[1m]))`, unit: '/s' },
    { label: 'keys', query: `sum(redis_db_keys{job="${name}"})`, unit: '' },
  ]
}

// The worker's cards: the standard service HTTP set + the inference-specific ones.
function workerMetrics(name) {
  return [
    ...serviceMetrics(name),
    {
      label: 'tokens/s',
      query: `sum(rate(llm_tokens_streamed_total{job="${name}"}[1m])) or vector(0)`,
      unit: '/s',
    },
    { label: 'batch', query: `sum(llm_active_sequences{job="${name}"}) or vector(0)`, unit: '' },
  ]
}

// ---------------------------------------------------------------------------
// Create the worker + its linked redis (the add-service "onAdd")
// ---------------------------------------------------------------------------
async function onAdd({ system, name, manifest }) {
  const streamName = `${name}-stream`

  // 1. Guards for everything onAdd derives beyond validateCreate's <name> checks —
  //    all before any write.
  if (!NAME_RE.test(streamName) || streamName.length > 50) throw bad('service name too long')
  const ids = new Set((manifest.nodes || []).map((n) => n.id))
  for (const derived of [streamName, `${streamName}-exporter`]) {
    if (ids.has(derived)) throw bad(`"${derived}" already exists in this system`)
  }
  if (fs.existsSync(path.join(systemDir(system), streamName))) {
    throw bad(`folder "${streamName}" already exists in this system`)
  }
  const registry = readJsonFile(path.join(systemDir(system), 'grpc', '_registry.json'), { contracts: {} })
  const existing = registry.contracts?.Worker
  if (existing && existing.source !== 'llm_worker') {
    throw bad('this system already has a "Worker" gRPC contract from another source')
  }

  // 2. Install the Worker contract into the bank (direct-write + protoc generate).
  await installContracts(system, contractSpecs())

  // 3. Scaffold the worker (template + live-config + hook registry + compose +
  //    nginx + prometheus).
  cloneTemplate(system, name, WORKER_DIR, SERVICE_FILES)
  fs.writeFileSync(
    path.join(systemDir(system), name, 'worker.json'),
    JSON.stringify(CONFIG_DEFAULTS, null, 2) + '\n',
  )
  fs.writeFileSync(
    path.join(systemDir(system), name, 'hook.json'),
    JSON.stringify(
      { description: '', implemented: false, conversationId: '', createdAt: new Date().toISOString(), history: [] },
      null,
      2,
    ) + '\n',
  )
  addComposeService(
    system,
    name,
    {
      build: `./${name}`,
      environment: { SERVICE_ID: name, REDIS_HOST: streamName },
      volumes: [
        './grpc:/app/grpc_pkg:ro',
        './manifest.json:/manifest.json:ro',
        `./${name}/worker.json:/config/worker.json:ro`, // live tunables (mtime-polled)
        `./${name}/hooks.py:/app/hooks.py:ro`, // on_cache_evict (restart to apply)
      ],
      depends_on: [streamName],
    },
    ` LLM Worker "${name}" — custom service type`,
  )
  addNginxRoute(system, name)
  addScrapeJob(system, name, 8000, ` LLM Worker "${name}" — custom service type`)

  // 4. The linked redis + exporter (the token stream the worker XADDs into). Same
  //    compose/scrape shape the database flow writes, but owned by this worker
  //    (streamOf) so deletion cascades with it.
  addComposeServices(
    system,
    {
      [streamName]: { image: 'redis:7-alpine' },
      [`${streamName}-exporter`]: {
        image: 'oliver006/redis_exporter:v1.62.0',
        environment: { REDIS_ADDR: `redis://${streamName}:6379` },
        depends_on: [streamName],
      },
    },
    streamName,
    'Redis token stream',
    'Add service (LLM Worker)',
  )
  addDbScrapeJob(
    system,
    { job_name: streamName, static_configs: [{ targets: [`${streamName}-exporter:9121`] }] },
    streamName,
    'Add service (LLM Worker)',
    'Redis',
  )

  // 5. Manifest: worker node + linked redis node + the worker→redis edge. The edge
  //    is pushed before the addManifestNode calls persist the manifest, so one write
  //    lands all three.
  manifest.edges = manifest.edges || []
  manifest.edges.push({ from: name, to: streamName, origin: 'create-custom-service' })
  const position = nextNodePosition(manifest)
  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'service',
    origin: 'create-custom-service',
    service_type: 'llm_worker',
    position,
    metrics: workerMetrics(name),
    health: serviceHealth(name),
    grpc: { servers: ['Worker'], clients: [], overrides: [] },
    llm: { stream: streamName },
  })
  addManifestNode(system, manifest, {
    id: streamName,
    label: streamName,
    type: 'redis',
    origin: 'create-custom-service',
    streamOf: name,
    position: { x: position.x + 300, y: position.y },
    metrics: redisMetrics(streamName),
    health: { query: `redis_up{job="${streamName}"}`, rules: HEALTH_RULES },
  })

  const log = SKIP_REBUILD() ? '(rebuild skipped)' : await rebuild(system, name)
  return { ok: true, node, log }
}

// ---------------------------------------------------------------------------
// Control routes (mounted by customServices.js with a { json, readJsonBody } ctx)
// ---------------------------------------------------------------------------
const fail = (ctx, res, err) => ctx.json(res, err.statusCode || 500, { ok: false, error: err.message })

// Resolve + validate an llm_worker node from query/body params.
function workerNode(system, node) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (!node || !NAME_RE.test(node)) throw bad('invalid node')
  const manifest = readManifest(system)
  const n = manifest.nodes.find((x) => x.id === node && x.service_type === 'llm_worker')
  if (!n) throw bad(`"${node}" is not an LLM worker in this system`)
  return { manifest, node: n }
}

const configFile = (system, node) => path.join(systemDir(system), node, 'worker.json')
const hookFile = (system, node) => path.join(systemDir(system), node, 'hook.json')

// Aggregate every worker's live /llm/state + its on-disk hook/config registries into
// one node-keyed map the tab + diagram read (hook/config stay readable even while the
// container is down or still building).
async function handleState(req, res, _next, ctx) {
  if (req.method !== 'GET') return ctx.json(res, 405, { ok: false, error: 'GET only' })
  try {
    const url = new URL(req.url, 'http://localhost')
    const system = url.searchParams.get('system')
    if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
    const manifest = readManifest(system)
    const workers = manifest.nodes.filter((n) => n.service_type === 'llm_worker')
    const nodes = {}
    await Promise.all(
      workers.map(async (w) => {
        let live = null
        try {
          const r = await fetch(`${LB}/${w.id}/llm/state`)
          if (r.ok) {
            const s = await r.json()
            if (s.ok) live = s
          }
        } catch {
          /* worker not reachable yet (still building) */
        }
        nodes[w.id] = {
          live,
          config: readJsonFile(configFile(system, w.id), { ...CONFIG_DEFAULTS }),
          hook: readJsonFile(hookFile(system, w.id), { description: '', implemented: false, conversationId: '' }),
        }
      }),
    )
    ctx.json(res, 200, { ok: true, nodes })
  } catch (err) {
    fail(ctx, res, err)
  }
}

// GET reads the live-config file; POST validates + rewrites it IN PLACE (the container
// mtime-polls the bind mount — no rebuild, no restart).
async function handleConfig(req, res, _next, ctx) {
  try {
    if (req.method === 'GET') {
      const url = new URL(req.url, 'http://localhost')
      const system = url.searchParams.get('system')
      const node = url.searchParams.get('node')
      workerNode(system, node)
      return ctx.json(res, 200, { ok: true, config: readJsonFile(configFile(system, node), { ...CONFIG_DEFAULTS }) })
    }
    if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'GET or POST only' })
    const body = await ctx.readJsonBody(req)
    const { system, node } = body
    const { manifest } = workerNode(system, node)

    const ttl = Number(body.ttl_seconds)
    if (!Number.isInteger(ttl) || ttl < 0 || ttl > TTL_MAX) {
      throw bad(`ttl_seconds must be an integer 0-${TTL_MAX} (0 disables caching)`)
    }
    const maxActive = Number(body.max_active)
    if (!Number.isInteger(maxActive) || maxActive < 1 || maxActive > MAX_ACTIVE_MAX) {
      throw bad(`max_active must be an integer 1-${MAX_ACTIVE_MAX}`)
    }
    let chatDb = body.chat_db ?? null
    if (chatDb !== null) {
      const db = manifest.nodes.find((n) => n.id === chatDb && n.type === 'postgres')
      if (!db) throw bad(`chat_db must be a postgres node in this system (or null)`)
      chatDb = db.id
    }

    const config = { ttl_seconds: ttl, chat_db: chatDb, max_active: maxActive }
    fs.writeFileSync(configFile(system, node), JSON.stringify(config, null, 2) + '\n')
    ctx.json(res, 200, { ok: true, config })
  } catch (err) {
    fail(ctx, res, err)
  }
}

// Upsert the on_cache_evict hook registry entry. Pure metadata write — the Edit tab
// launches the authoring session itself (edit queue); that session owns `implemented`.
async function handleHook(req, res, _next, ctx) {
  try {
    if (req.method === 'GET') {
      const url = new URL(req.url, 'http://localhost')
      const system = url.searchParams.get('system')
      const node = url.searchParams.get('node')
      workerNode(system, node)
      return ctx.json(res, 200, {
        ok: true,
        hook: readJsonFile(hookFile(system, node), { description: '', implemented: false, conversationId: '' }),
      })
    }
    if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'GET or POST only' })
    const body = await ctx.readJsonBody(req)
    const { system, node } = body
    workerNode(system, node)
    const description = typeof body.description === 'string' ? body.description.trim() : ''
    if (!description) throw bad('description required')
    const conversationId = typeof body.conversationId === 'string' ? body.conversationId : ''

    const now = new Date().toISOString()
    const prev = readJsonFile(hookFile(system, node), {})
    const history = Array.isArray(prev.history) ? prev.history : []
    const hook = {
      description,
      // The launched session owns this; a description edit must not reset it.
      implemented: prev.implemented === true,
      conversationId: conversationId || prev.conversationId || '',
      createdAt: prev.createdAt || now,
      updatedAt: now,
      history: [...history, { at: now, description }],
    }
    fs.writeFileSync(hookFile(system, node), JSON.stringify(hook, null, 2) + '\n')
    ctx.json(res, 200, { ok: true, hook })
  } catch (err) {
    fail(ctx, res, err)
  }
}

export default {
  serviceType: 'llm_worker',
  displayName: 'LLM Worker',
  description:
    'Simulated LLM inference: gRPC AddPrompt/GetStatus, continuous batching with a TTL prefix cache, tokens streamed to a linked redis created with it.',
  onAdd,
  routes: [
    { path: '/api/custom/llm-worker/state', handler: handleState },
    { path: '/api/custom/llm-worker/config', handler: handleConfig },
    { path: '/api/custom/llm-worker/hook', handler: handleHook },
  ],
}
