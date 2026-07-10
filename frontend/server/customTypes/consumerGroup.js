// Custom service type: Kafka Consumer Group — a brand-new service created FROM the
// Consumers tab (hidden from the generic add-service modal: creation needs a cluster +
// topic + function context that only that tab has).
//
// "Define a consumer" on a Kafka node creates ONE consuming service (plain FastAPI
// template — the poll loop is authored by a launched sandbox-event-stream session,
// exactly like consumers on ordinary services) AND its scaler sidecar `<name>-scaler`
// in a single onAdd. The service scales through the shared replica-group reconciler
// (replicaGroup.js): N member containers under one id, all running the same authored
// loop with the same Kafka group id, so the broker itself rebalances the topic's
// partitions across them — that's the feature.
//
// The scaler is a real container (templates/consumer-scaler/) that watches the group
// on the broker (lag, live members, per-member partition assignments) and computes a
// desired replica count from systems/<id>/<base>/scaler.json — a live-mounted policy
// (mtime-polled; edits apply with no rebuild). It NEVER touches docker: the shared
// autoscale apply loop (autoscale.js, registered via onServerStart) polls each
// scaler's /state through the lb and applies changes with the same idempotent
// setGroupReplicas the manual Scaling tab uses — docker stays host-side, the
// container stays unprivileged.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  bad, serviceMetrics, serviceHealth, cloneTemplate, addComposeService, addNginxRoute,
  addScrapeJob, addManifestNode, NAME_RE,
} from '../scaffold.js'
import { setGroupReplicas, scaleRebuild } from '../replicaGroup.js'
import { startAutoscaleLoop } from '../autoscale.js'
import { validateInput as validateConsumerInput, upsertConsumer } from '../consumers.js'
import { isValidSystem, systemDir, nextNodePosition } from '../systems.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const SERVICE_TPL = path.join(__dirname, '..', 'templates', 'service')
const SCALER_TPL = path.join(__dirname, '..', 'templates', 'consumer-scaler')
const SERVICE_FILES = ['app.py', 'requirements.txt', 'Dockerfile']

const MAX_MEMBERS = 8 // total members in a group (base + instances)
const LB = 'http://localhost:8080' // the system's load balancer (compose maps 8080:80)
const SKIP_REBUILD = () => process.env.CREATE_SVC_SKIP_REBUILD === '1'

// Scaling-policy defaults written into <base>/scaler.json at creation. groupId rides
// in the same live-mounted file so a group-id edit reaches the running scaler with no
// rebuild (consumers.js syncs it on upsert/rename).
const POLICY_DEFAULTS = {
  enabled: true,
  min: 1,
  max: MAX_MEMBERS,
  scale_up_lag: 1000,
  scale_down_lag: 100,
  up_stable_seconds: 15,
  down_stable_seconds: 60,
  cooldown_seconds: 90,
}
const POLICY_LIMITS = { lag: 10_000_000, seconds: 86_400 }

const readManifest = (system) => JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
const scalerIdOf = (base) => `${base}-scaler`
const policyFile = (system, base) => path.join(systemDir(system), base, 'scaler.json')
const readJsonFile = (file, fallback) => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return fallback
  }
}

// The group-lag card the BASE carries next to its HTTP cards (must stay in step with
// groupLagMetric in consumers.js, which refreshes it when the group id is edited).
function groupLagMetric(cluster, groupId) {
  return {
    label: 'lag',
    query: `sum(kafka_consumergroup_lag{job="${cluster}",consumergroup="${groupId}"}) or vector(0)`,
    unit: '',
  }
}

// The scaler node's cards — its own exported gauges.
function scalerMetrics(id) {
  return [
    { label: 'lag', query: `consumer_group_lag_total{job="${id}"} or vector(0)`, unit: '' },
    { label: 'desired', query: `consumer_group_desired_replicas{job="${id}"} or vector(0)`, unit: '' },
    { label: 'members', query: `consumer_group_members{job="${id}"} or vector(0)`, unit: '' },
  ]
}

// Rewrite the group's `members` in the cluster's streams.json to the full container
// list — the replica reconciler calls this on every scale so the registry (what the
// Topics tab shows and what delete-guards scan) always names every real member.
function syncGroupMembers(system, baseNode, memberIds) {
  const cluster = baseNode.consumerGroup?.cluster
  const groupId = baseNode.consumerGroup?.groupId
  if (!cluster || !groupId) return
  const file = path.join(systemDir(system), cluster, 'streams.json')
  const raw = readJsonFile(file, null)
  if (!raw || !Array.isArray(raw.topics)) return
  let changed = false
  for (const t of raw.topics) {
    for (const g of (t && Array.isArray(t.consumers) ? t.consumers : [])) {
      if (g && g.groupId === groupId && JSON.stringify(g.members) !== JSON.stringify(memberIds)) {
        g.members = [...memberIds]
        changed = true
      }
    }
  }
  if (changed) fs.writeFileSync(file, JSON.stringify(raw, null, 2) + '\n')
}

const REPLICA_CFG = {
  serviceType: 'consumer_group',
  typeLabel: 'consumer group service',
  maxTotal: MAX_MEMBERS,
  // The base carries the group-lag card; instances just the HTTP set. Every member
  // runs the same authored loop, so partitions spread across them automatically.
  memberMetrics: (id, isBase, baseNode) =>
    isBase && baseNode.consumerGroup
      ? [...serviceMetrics(id), groupLagMetric(baseNode.consumerGroup.cluster, baseNode.consumerGroup.groupId)]
      : serviceMetrics(id),
  instanceComment: (base) =>
    ` Instance of consumer group service "${base}" — same Kafka group id; the broker rebalances partitions across members`,
  onMembersChanged: syncGroupMembers,
}

// ---------------------------------------------------------------------------
// Create the consuming service + its scaler (the add-service "onAdd")
// ---------------------------------------------------------------------------
async function onAdd({ system, name, manifest, options }) {
  const opts = options || {}
  const cluster = opts.cluster
  const scalerId = scalerIdOf(name)

  // 1. Pre-validate the WHOLE spec before any write: the consumer-function fields
  //    (cluster, topic, function name, group id, poll rate) through the same
  //    validator ordinary consumers use — minus the service-node check, since the
  //    service is what we're about to create — plus the derived scaler id.
  const spec = validateConsumerInput(
    {
      system,
      service: name,
      name: opts.fn,
      cluster,
      topic: opts.topic,
      pollRate: opts.pollRate,
      groupId: opts.groupId,
      description: opts.description,
      conversationId: opts.conversationId,
    },
    { requireServiceNode: false },
  )
  if (!NAME_RE.test(scalerId) || scalerId.length > 50) throw bad('service name too long')
  if (manifest.nodes.some((n) => n.id === scalerId)) throw bad(`"${scalerId}" already exists in this system`)
  if (fs.existsSync(path.join(systemDir(system), scalerId))) {
    throw bad(`folder "${scalerId}" already exists in this system`)
  }

  // 2. Scaffold the consuming service: plain FastAPI template (the poll loop comes
  //    from the launched session), with the cluster's streams.json pause-flag mount
  //    and SERVICE_ID pre-wired — the session and every cloned replica rely on both
  //    (SERVICE_ID doubles as the member's Kafka client_id so the diagram can map
  //    members to partitions).
  cloneTemplate(system, name, SERVICE_TPL, SERVICE_FILES)
  addComposeService(
    system,
    name,
    {
      build: `./${name}`,
      environment: { SERVICE_ID: name },
      volumes: [`./${cluster}/streams.json:/streams/${cluster}.json:ro`],
      depends_on: [cluster],
    },
    ` Consumer group "${spec.groupId}" service "${name}" — custom service type`,
  )
  addNginxRoute(system, name)
  addScrapeJob(system, name, 8000, ` Consumer group service "${name}" — custom service type`)

  // 3. Scaffold the scaler: its own template + live policy file + compose + nginx
  //    (control-plane /state through the lb) + scrape job.
  cloneTemplate(system, scalerId, SCALER_TPL, SERVICE_FILES)
  fs.writeFileSync(
    policyFile(system, name),
    JSON.stringify({ groupId: spec.groupId, ...POLICY_DEFAULTS }, null, 2) + '\n',
  )
  addComposeService(
    system,
    scalerId,
    {
      build: `./${scalerId}`,
      environment: { CLUSTER: cluster, SERVICE_ID: scalerId },
      volumes: [
        `./${name}/scaler.json:/config/scaler.json:ro`, // live policy (mtime-polled)
        `./${cluster}/streams.json:/streams/${cluster}.json:ro`, // topic discovery + pause flag
      ],
      depends_on: [cluster],
    },
    ` Scaler for consumer group service "${name}" — watches lag, drives autoscaling`,
  )
  addNginxRoute(system, scalerId)
  addScrapeJob(system, scalerId, 8000, ` Scaler for consumer group service "${name}"`)

  // 4. Manifest: the consuming service (group identity on the node) + the scaler
  //    (scalerOf back-link drives its rendering, drag and delete-cascade).
  const position = nextNodePosition(manifest)
  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'service',
    origin: 'create-custom-service',
    service_type: 'consumer_group',
    consumerGroup: { cluster, groupId: spec.groupId },
    position,
    metrics: [...serviceMetrics(name), groupLagMetric(cluster, spec.groupId)],
    health: serviceHealth(name),
  })
  addManifestNode(system, manifest, {
    id: scalerId,
    label: scalerId,
    type: 'service',
    origin: 'create-custom-service',
    service_type: 'consumer_scaler',
    scalerOf: name,
    position: { x: position.x + 300, y: position.y },
    metrics: scalerMetrics(scalerId),
    health: serviceHealth(scalerId),
  })

  // 5. Register the consumer function through the SAME path ordinary consumers use
  //    (consumers.json record + streams.json group + consume edge; the node now
  //    exists on disk, so the full validation passes).
  upsertConsumer(validateConsumerInput({
    system,
    service: name,
    name: opts.fn,
    cluster,
    topic: opts.topic,
    pollRate: opts.pollRate,
    groupId: spec.groupId,
    description: opts.description,
    conversationId: opts.conversationId,
  }))

  // 6. One locked rebuild for both containers (service + scaler), lb reload for the
  //    two new routes, prometheus restart for the two new scrape jobs.
  const log = SKIP_REBUILD()
    ? '(rebuild skipped)'
    : await scaleRebuild(system, { buildNames: [name, scalerId], reloadLb: true })
  return { ok: true, node, log }
}

// ---------------------------------------------------------------------------
// Control routes (mounted by customServices.js with a { json, readJsonBody } ctx)
// ---------------------------------------------------------------------------
const fail = (ctx, res, err) => ctx.json(res, err.statusCode || 500, { ok: false, error: err.message })

// Resolve + validate a consumer_group BASE node from query/body params.
function baseNode(system, node) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (!node || !NAME_RE.test(node)) throw bad('invalid node')
  const manifest = readManifest(system)
  const n = manifest.nodes.find((x) => x.id === node && x.service_type === 'consumer_group' && !x.instanceOf)
  if (!n) throw bad(`"${node}" is not a consumer group service in this system`)
  return { manifest, node: n }
}

async function handleScale(req, res, _next, ctx) {
  try {
    if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'POST only' })
    const body = await ctx.readJsonBody(req)
    ctx.json(res, 200, await setGroupReplicas(REPLICA_CFG, body))
  } catch (err) {
    fail(ctx, res, err)
  }
}

// GET reads the live policy file; POST validates + rewrites it IN PLACE (the scaler
// mtime-polls the bind mount — no rebuild, no restart). groupId is NOT client-settable
// here: it syncs from the consumer registry (consumers.js) only.
async function handlePolicy(req, res, _next, ctx) {
  try {
    if (req.method === 'GET') {
      const url = new URL(req.url, 'http://localhost')
      const system = url.searchParams.get('system')
      const node = url.searchParams.get('node')
      baseNode(system, node)
      return ctx.json(res, 200, { ok: true, policy: readJsonFile(policyFile(system, node), { ...POLICY_DEFAULTS }) })
    }
    if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'GET or POST only' })
    const body = await ctx.readJsonBody(req)
    const { system, node } = body
    baseNode(system, node)

    const int = (v, lbl, lo, hi) => {
      const n = Number(v)
      if (!Number.isInteger(n) || n < lo || n > hi) throw bad(`${lbl} must be an integer ${lo}-${hi}`)
      return n
    }
    const min = int(body.min, 'min', 1, MAX_MEMBERS)
    const max = int(body.max, 'max', 1, MAX_MEMBERS)
    if (min > max) throw bad('min must be ≤ max')
    const upLag = int(body.scale_up_lag, 'scale_up_lag', 1, POLICY_LIMITS.lag)
    const downLag = int(body.scale_down_lag, 'scale_down_lag', 0, POLICY_LIMITS.lag)
    if (downLag >= upLag) throw bad('scale_down_lag must be below scale_up_lag')
    const policy = {
      groupId: readJsonFile(policyFile(system, node), {}).groupId || '',
      enabled: body.enabled !== false,
      min,
      max,
      scale_up_lag: upLag,
      scale_down_lag: downLag,
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

// Aggregate every group's scaler /state + on-disk policy into one node-keyed map the
// Scaling tab + diagram read: nodes[<base>] = { live, policy }, nodes[<member>] =
// { partitions } (matched by the member's Kafka client_id == its SERVICE_ID == its
// node id), nodes[<base>-scaler] = { live }.
async function handleState(req, res, _next, ctx) {
  if (req.method !== 'GET') return ctx.json(res, 405, { ok: false, error: 'GET only' })
  try {
    const url = new URL(req.url, 'http://localhost')
    const system = url.searchParams.get('system')
    if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
    const manifest = readManifest(system)
    const bases = manifest.nodes.filter((n) => n.service_type === 'consumer_group' && !n.instanceOf)
    const nodes = {}
    await Promise.all(
      bases.map(async (b) => {
        const scalerId = scalerIdOf(b.id)
        let live = null
        try {
          const r = await fetch(`${LB}/${scalerId}/state`, { signal: AbortSignal.timeout(3000) })
          if (r.ok) {
            const s = await r.json()
            // The lb only serves the ACTIVE system — guard against a same-named
            // scaler in a different system answering.
            if (s && s.group === b.consumerGroup?.groupId) live = s
          }
        } catch {
          /* scaler not reachable (system inactive or still building) */
        }
        nodes[b.id] = { live, policy: readJsonFile(policyFile(system, b.id), null) }
        if (manifest.nodes.some((n) => n.id === scalerId)) nodes[scalerId] = { live }
        if (live?.members) {
          for (const m of live.members) {
            if (m.clientId && manifest.nodes.some((n) => n.id === m.clientId)) {
              nodes[m.clientId] = { ...(nodes[m.clientId] || {}), partitions: m.partitions }
            }
          }
        }
      }),
    )
    ctx.json(res, 200, { ok: true, nodes })
  } catch (err) {
    fail(ctx, res, err)
  }
}

// The autoscale APPLY loop lives in the shared autoscale.js (one interval serves
// every worker-group type); this type just registers its identity guard — the lb
// only serves the active system, so a same-named scaler from another system is
// rejected by its group id.
function onServerStart(server) {
  startAutoscaleLoop(server, {
    tag: 'consumer-group',
    disabledEnv: 'CONSUMER_AUTOSCALE_DISABLED',
    replicaCfg: REPLICA_CFG,
    scalerIdOf,
    matchesBase: (state, base) => state.group === base.consumerGroup?.groupId,
  })
}

export default {
  serviceType: 'consumer_group',
  displayName: 'Kafka Consumer Group',
  description:
    'A consuming service created from the Consumers tab: N member containers share one Kafka group id (the broker rebalances partitions across them), with a real scaler container driving lag-based autoscaling.',
  hidden: true, // created from the Consumers tab, not the add-service modal
  onAdd,
  onServerStart,
  routes: [
    { path: '/api/custom/consumer-group/scale', handler: handleScale },
    { path: '/api/custom/consumer-group/policy', handler: handlePolicy },
    { path: '/api/custom/consumer-group/state', handler: handleState },
  ],
}
