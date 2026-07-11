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
//
// onAdd also creates the group's scaler sidecar `<name>-scaler` (templates/llm-scaler/):
// a real container that polls every member's /llm/state, computes batch utilization
// (active sequences / total max_active) and derives a desired worker count from
// systems/<id>/<name>/scaler.json — a live-mounted policy (mtime-polled; edits apply
// with no rebuild). It NEVER touches docker: the shared autoscale apply loop
// (autoscale.js, registered via onServerStart) polls each scaler's /state through the
// lb and applies changes with the same idempotent setGroupReplicas the manual Scaling
// tab uses — docker stays host-side, the container stays unprivileged.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  bad, serviceMetrics, serviceHealth, cloneTemplate, addComposeService,
  addNginxRoute, addScrapeJob, addManifestNode, NAME_RE,
} from '../scaffold.js'
import { setGroupReplicas, scaleRebuild } from '../replicaGroup.js'
import { startAutoscaleLoop } from '../autoscale.js'
import { addComposeServices, addScrapeJob as addDbScrapeJob, HEALTH_RULES } from '../databases.js'
import { installContracts } from '../grpcInstall.js'
import { isValidSystem, systemDir, nextNodePosition } from '../systems.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const TPL = path.join(__dirname, '..', 'templates', 'llm-worker')
const WORKER_DIR = path.join(TPL, 'worker')
const GRPC_DIR = path.join(TPL, 'grpc')
const SCALER_TPL = path.join(__dirname, '..', 'templates', 'llm-scaler')
const SERVICE_FILES = ['app.py', 'model.py', 'hooks.py', 'requirements.txt', 'Dockerfile']
const SCALER_FILES = ['app.py', 'requirements.txt', 'Dockerfile']

const CONFIG_DEFAULTS = { ttl_seconds: 30, chat_db: null, max_active: 5 }
const TTL_MAX = 60
const MAX_ACTIVE_MAX = 32
const MAX_WORKERS = 8 // total workers in a group (base + instances)
const LB = 'http://localhost:8080' // the system's load balancer (compose maps 8080:80)
const SKIP_REBUILD = () => process.env.CREATE_SVC_SKIP_REBUILD === '1'

// Scaling-policy defaults written into <base>/scaler.json at creation. Utilization is
// batch occupancy: sum(active sequences) / sum(max_active) over reachable workers.
const POLICY_DEFAULTS = {
  enabled: true,
  min: 1,
  max: MAX_WORKERS,
  scale_up_util: 0.8,
  scale_down_util: 0.3,
  up_stable_seconds: 15,
  down_stable_seconds: 60,
  cooldown_seconds: 90,
}
const POLICY_LIMITS = { seconds: 86_400 }

const scalerIdOf = (base) => `${base}-scaler`
const policyFile = (system, base) => path.join(systemDir(system), base, 'scaler.json')

const read = (dir, f) => fs.readFileSync(path.join(dir, f), 'utf8')
const readManifest = (system) => JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
const writeManifest = (system, manifest) =>
  fs.writeFileSync(path.join(systemDir(system), 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n')
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
    { label: 'cached', query: `sum(llm_cached_prefixes{job="${name}"}) or vector(0)`, unit: '' },
  ]
}

// The scaler node's cards — its own exported gauges (utilization shown as a %).
function scalerMetrics(id) {
  return [
    { label: 'util', query: `llm_worker_utilization{job="${id}"} or vector(0)`, unit: '%', scale: 100 },
    { label: 'desired', query: `llm_worker_desired_replicas{job="${id}"} or vector(0)`, unit: '' },
    { label: 'workers', query: `llm_worker_members{job="${id}"} or vector(0)`, unit: '' },
  ]
}

// ---------------------------------------------------------------------------
// Create the worker + its linked redis (the add-service "onAdd")
// ---------------------------------------------------------------------------
async function onAdd({ system, name, manifest }) {
  const streamName = `${name}-stream`
  const scalerId = scalerIdOf(name)

  // 1. Guards for everything onAdd derives beyond validateCreate's <name> checks —
  //    all before any write.
  for (const derived of [streamName, scalerId]) {
    if (!NAME_RE.test(derived) || derived.length > 50) throw bad('service name too long')
  }
  const ids = new Set((manifest.nodes || []).map((n) => n.id))
  for (const derived of [streamName, `${streamName}-exporter`, scalerId]) {
    if (ids.has(derived)) throw bad(`"${derived}" already exists in this system`)
  }
  for (const derived of [streamName, scalerId]) {
    if (fs.existsSync(path.join(systemDir(system), derived))) {
      throw bad(`folder "${derived}" already exists in this system`)
    }
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

  // 5. Scaffold the scaler: its own template + live policy file + compose + nginx
  //    (control-plane /state through the lb) + scrape job. It discovers the member
  //    set from the live-mounted manifest (safe: every backend manifest write is an
  //    in-place writeFileSync, so the bind-mounted inode never detaches).
  cloneTemplate(system, scalerId, SCALER_TPL, SCALER_FILES)
  fs.writeFileSync(policyFile(system, name), JSON.stringify(POLICY_DEFAULTS, null, 2) + '\n')
  addComposeService(
    system,
    scalerId,
    {
      build: `./${scalerId}`,
      environment: { SERVICE_ID: scalerId, BASE: name, SYSTEM_ID: system },
      volumes: [
        `./${name}/scaler.json:/config/scaler.json:ro`, // live policy (mtime-polled)
        './manifest.json:/manifest.json:ro', // member discovery (base + instances)
      ],
      depends_on: [name],
    },
    ` Scaler for LLM worker "${name}" — watches batch utilization, drives autoscaling`,
  )
  addNginxRoute(system, scalerId)
  addScrapeJob(system, scalerId, 8000, ` Scaler for LLM worker "${name}"`)

  // 6. Manifest: worker node + linked redis node + scaler node + the worker→redis
  //    edge. The edge is pushed before the addManifestNode calls persist the
  //    manifest, so one write lands everything.
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
  // The worker's redis contract is fixed (runs:started announcements + per-run
  // tokens:<id> streams), so the stream node is born with those keyspaces declared —
  // typed KEY rows on the diagram, managed afterwards in its Keyspaces tab. Readers
  // (e.g. a persistence reader group) are declared/scanned in later, not known here.
  const ksNow = new Date().toISOString()
  const streamKeyspace = (ks) => ({
    ...ks,
    writers: [name],
    readers: [],
    verified: true,
    origin: 'user',
    suggestedWriters: [],
    suggestedReaders: [],
    createdAt: ksNow,
    updatedAt: ksNow,
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
    keyspaces: [
      streamKeyspace({ name: 'runs:started', match: 'exact', type: 'stream', shorthand: 'announce' }),
      streamKeyspace({ name: 'tokens:', match: 'prefix', type: 'stream' }),
    ],
  })
  addManifestNode(system, manifest, {
    id: scalerId,
    label: scalerId,
    type: 'service',
    origin: 'create-custom-service',
    service_type: 'llm_scaler',
    scalerOf: name,
    position: { x: position.x + 300, y: position.y + 160 },
    metrics: scalerMetrics(scalerId),
    health: serviceHealth(scalerId),
  })

  // 7. One locked rebuild for both built containers (worker + scaler; `up -d` also
  //    starts the redis pair), lb reload for the new routes, prometheus restart for
  //    the new scrape jobs.
  const log = SKIP_REBUILD()
    ? '(rebuild skipped)'
    : await scaleRebuild(system, { buildNames: [name, scalerId], reloadLb: true })
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
    // Every group member is polled — base AND instances (each has a plain nginx
    // `/<id>/` route so its /llm/state is reachable through the lb; see setReplicas).
    // config/hook always come from the BASE's folder: instances share the base's
    // worker.json / hook.json bind mounts and have no folder of their own.
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
        const baseId = w.instanceOf || w.id
        nodes[w.id] = {
          live,
          config: readJsonFile(configFile(system, baseId), { ...CONFIG_DEFAULTS }),
          hook: readJsonFile(hookFile(system, baseId), { description: '', implemented: false, conversationId: '' }),
        }
      }),
    )
    // Each base also gets its scaler's live /state + the on-disk policy (readable even
    // while the scaler is down), and the scaler node gets its own entry — the Scaling
    // tab and the scaler card's diagram body read these.
    await Promise.all(
      workers.filter((w) => !w.instanceOf).map(async (b) => {
        const scalerId = scalerIdOf(b.id)
        let live = null
        try {
          const r = await fetch(`${LB}/${scalerId}/state`, { signal: AbortSignal.timeout(3000) })
          if (r.ok) {
            const s = await r.json()
            // The lb only serves the ACTIVE system — guard against a same-named
            // scaler in a different system answering.
            if (s && s.base === b.id && (!s.system || s.system === system)) live = s
          }
        } catch {
          /* scaler not reachable (system inactive or still building) */
        }
        nodes[b.id] = {
          ...(nodes[b.id] || {}),
          scaler: live,
          policy: readJsonFile(policyFile(system, b.id), null),
        }
        if (manifest.nodes.some((n) => n.id === scalerId)) nodes[scalerId] = { live }
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

// Resolve + validate the group BASE (policy lives in the base's folder; instances
// share it and have none of their own).
function baseWorkerNode(system, node) {
  const r = workerNode(system, node)
  if (r.node.instanceOf) throw bad(`"${node}" is an instance — edit the policy on its base "${r.node.instanceOf}"`)
  return r
}

// GET reads the live policy file; POST validates + rewrites it IN PLACE (the scaler
// mtime-polls the bind mount — no rebuild, no restart).
async function handlePolicy(req, res, _next, ctx) {
  try {
    if (req.method === 'GET') {
      const url = new URL(req.url, 'http://localhost')
      const system = url.searchParams.get('system')
      const node = url.searchParams.get('node')
      baseWorkerNode(system, node)
      return ctx.json(res, 200, { ok: true, policy: readJsonFile(policyFile(system, node), { ...POLICY_DEFAULTS }) })
    }
    if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'GET or POST only' })
    const body = await ctx.readJsonBody(req)
    const { system, node } = body
    baseWorkerNode(system, node)

    const int = (v, lbl, lo, hi) => {
      const n = Number(v)
      if (!Number.isInteger(n) || n < lo || n > hi) throw bad(`${lbl} must be an integer ${lo}-${hi}`)
      return n
    }
    const min = int(body.min, 'min', 1, MAX_WORKERS)
    const max = int(body.max, 'max', 1, MAX_WORKERS)
    if (min > max) throw bad('min must be ≤ max')
    const upUtil = Number(body.scale_up_util)
    if (!Number.isFinite(upUtil) || upUtil <= 0 || upUtil > 1) throw bad('scale_up_util must be a number in (0, 1]')
    const downUtil = Number(body.scale_down_util)
    if (!Number.isFinite(downUtil) || downUtil < 0 || downUtil >= 1) throw bad('scale_down_util must be a number in [0, 1)')
    if (downUtil >= upUtil) throw bad('scale_down_util must be below scale_up_util')
    const policy = {
      enabled: body.enabled !== false,
      min,
      max,
      scale_up_util: upUtil,
      scale_down_util: downUtil,
      up_stable_seconds: int(body.up_stable_seconds, 'up_stable_seconds', 0, POLICY_LIMITS.seconds),
      down_stable_seconds: int(body.down_stable_seconds, 'down_stable_seconds', 0, POLICY_LIMITS.seconds),
      cooldown_seconds: int(body.cooldown_seconds, 'cooldown_seconds', 0, POLICY_LIMITS.seconds),
    }
    // In place, never tmp+rename — single-file bind mounts pin their inode.
    fs.writeFileSync(policyFile(system, node), JSON.stringify(policy, null, 2) + '\n')
    ctx.json(res, 200, { ok: true, policy })
  } catch (err) {
    fail(ctx, res, err)
  }
}

// ---------------------------------------------------------------------------
// Replica scaling — run the worker as N instances under one service id, with NO
// load balancer, via the shared replica-group reconciler (replicaGroup.js). The
// base `<name>` stays a real serving worker; instances `<name>-2..N` clone its
// build/config/hook/redis-stream (differing only by SERVICE_ID). The DATA plane
// stays gRPC-only with caller-side forwarding (`<name>-i:50051` — see the
// entry+instanceOf expansion in the sandbox-grpc-attach skill); each instance
// also gets a plain nginx `/<id>/` route so the CONTROL plane (/llm/state,
// polled by handleState for the diagram) can reach it through the lb.
// Mechanical (no launched session), mirroring serviceLb.js minus the haproxy sidecar.
// ---------------------------------------------------------------------------

const REPLICA_CFG = {
  serviceType: 'llm_worker',
  typeLabel: 'LLM worker',
  maxTotal: MAX_WORKERS,
  memberMetrics: (id) => workerMetrics(id),
  instanceExtras: () => ({ grpc: { servers: ['Worker'], clients: [], overrides: [] } }),
  instanceComment: (base) =>
    ` Instance of LLM worker "${base}" — request traffic over gRPC; nginx route is control-plane only`,
}

// The idempotent "set desired worker count" op (total = base + instances). instances==1
// removes every replica; N>=2 adds/drops the highest-ordinal instances to reach N.
export async function setReplicas(body) {
  return setGroupReplicas(REPLICA_CFG, body)
}

async function handleScale(req, res, _next, ctx) {
  try {
    if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'POST only' })
    const body = await ctx.readJsonBody(req)
    ctx.json(res, 200, await setReplicas(body))
  } catch (err) {
    fail(ctx, res, err)
  }
}

// The autoscale APPLY loop lives in the shared autoscale.js (one interval serves
// every worker-group type); this type just registers its identity guard — the lb
// only serves the active system, so a same-named scaler from another system is
// rejected by its base id + SYSTEM_ID.
function onServerStart(server) {
  startAutoscaleLoop(server, {
    tag: 'llm-worker',
    disabledEnv: 'LLM_AUTOSCALE_DISABLED',
    replicaCfg: REPLICA_CFG,
    scalerIdOf,
    matchesBase: (state, base, system) => state.base === base.id && (!state.system || state.system === system),
  })
}

export default {
  serviceType: 'llm_worker',
  displayName: 'LLM Worker',
  description:
    'Simulated LLM inference: gRPC AddPrompt/GetStatus, continuous batching with a TTL prefix cache, tokens streamed to a linked redis created with it, and a scaler sidecar driving utilization-based autoscaling.',
  onAdd,
  onServerStart,
  routes: [
    { path: '/api/custom/llm-worker/state', handler: handleState },
    { path: '/api/custom/llm-worker/config', handler: handleConfig },
    { path: '/api/custom/llm-worker/hook', handler: handleHook },
    { path: '/api/custom/llm-worker/policy', handler: handlePolicy },
    { path: '/api/custom/llm-worker/scale', handler: handleScale },
  ],
}
