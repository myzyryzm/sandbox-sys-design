// Custom service type: LLM Persistence Readers — created FROM the llm-worker node's
// Persistence tab (hidden from the generic add-service modal: creation needs a worker
// + a persist target that only that tab has).
//
// "Add persistence readers" creates ONE reading service (plain FastAPI template — the
// XREADGROUP poll/persist loop is authored by a launched sandbox-llm-persistence
// session) that forms a redis consumer group on the worker's `runs:started`
// announcement stream: each member claims announcements ({run_id: <user_message_id>}),
// accumulates that run's typed token stream (tokens:<run_id>, entries
// {type: token|done|error, text}), and once the run finishes writes the accumulated
// output to the configured database table/field — or per the freeform spec when the
// user chose a specialized implementation. Scaling is MANUAL (Readers tab member
// count) through the shared replica-group reconciler: all members share the one
// READER_GROUP, so redis divides announcements across them, one reader per run. No
// scaler sidecar (unlike consumer_group / llm_worker).
//
// The registry is systems/<id>/persistence.json — consumers.json conventions: the
// backend writes the entry with implemented:false, the launched session authors the
// loop and flips implemented:true. Edited live (no rebuild); the db target is fixed
// at creation (delete + re-add to change it, like node names).
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  bad, serviceMetrics, serviceHealth, cloneTemplate, addComposeService, addNginxRoute,
  addScrapeJob, addManifestNode, NAME_RE,
} from '../scaffold.js'
import { setGroupReplicas, scaleRebuild } from '../replicaGroup.js'
import { isValidSystem, systemDir, nextNodePosition } from '../systems.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const SERVICE_TPL = path.join(__dirname, '..', 'templates', 'service')
const SERVICE_FILES = ['app.py', 'requirements.txt', 'Dockerfile']

const MAX_MEMBERS = 8 // total members in a group (base + instances)
const ANNOUNCE_STREAM = 'runs:started' // where accepted AddPrompts announce runs
const LB = 'http://localhost:8080' // the system's load balancer (compose maps 8080:80)
const SKIP_REBUILD = () => process.env.CREATE_SVC_SKIP_REBUILD === '1'
const DB_TYPES = new Set(['postgres', 'mongodb']) // structured persist targets

const readManifest = (system) => JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
const registryFile = (system) => path.join(systemDir(system), 'persistence.json')

function readReaders(system) {
  try {
    const data = JSON.parse(fs.readFileSync(registryFile(system), 'utf8'))
    return Array.isArray(data.readers) ? data : { readers: [] }
  } catch {
    return { readers: [] }
  }
}
const writeReaders = (system, data) =>
  fs.writeFileSync(registryFile(system), JSON.stringify(data, null, 2) + '\n')

// The persist cards next to the HTTP set. The session's authored loop must export
// persistence_runs_total{status=complete|partial|failed} and persistence_active_runs
// (the metrics contract in the sandbox-llm-persistence skill); `or vector(0)` keeps
// the cards rendering 0 until it does. The BASE aggregates across every member's
// scrape job; an instance card shows only its own.
function persistenceMetrics(id, isBase, baseId) {
  const sel = isBase ? `job=~"${baseId}(-\\d+)?"` : `job="${id}"`
  return [
    { label: 'persisted', query: `sum(persistence_runs_total{${sel}}) or vector(0)`, unit: '' },
    { label: 'active', query: `sum(persistence_active_runs{${sel}}) or vector(0)`, unit: '' },
  ]
}

// Keep the registry entry's `members` naming every real container — the replica
// reconciler calls this on every scale (including no-ops) so the Readers tab and
// the delete-guard scan always see the live member list.
function syncMembers(system, baseNode, memberIds) {
  const data = readReaders(system)
  const entry = data.readers.find((r) => r && r.service === baseNode.id)
  if (!entry) return
  if (JSON.stringify(entry.members) !== JSON.stringify(memberIds)) {
    entry.members = [...memberIds]
    writeReaders(system, data)
  }
}

const REPLICA_CFG = {
  serviceType: 'persistence_reader',
  typeLabel: 'persistence reader group',
  maxTotal: MAX_MEMBERS,
  // Every member runs the same authored loop with the same READER_GROUP; its
  // XREADGROUP consumer name is its SERVICE_ID (overridden per instance by the
  // reconciler), so announcements divide across members automatically.
  memberMetrics: (id, isBase, baseNode) => [
    ...serviceMetrics(id),
    ...persistenceMetrics(id, isBase, baseNode.id),
  ],
  instanceComment: (base) =>
    ` Instance of persistence readers "${base}" — same redis consumer group; runs:started announcements divide across members`,
  onMembersChanged: syncMembers,
}

const trimmed = (v) => (typeof v === 'string' ? v.trim() : '')
const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_.-]*$/ // table / field names (pg + mongo)

// ---------------------------------------------------------------------------
// Create the reading service (the add-service "onAdd")
// ---------------------------------------------------------------------------
async function onAdd({ system, name, manifest, options }) {
  const opts = options || {}

  // 1. Pre-validate the WHOLE spec before any write. The worker must be a group
  //    BASE with a linked stream; the persist target is either db+table+field
  //    (structured) or a freeform spec (specialized implementation).
  const worker = manifest.nodes.find(
    (n) => n.id === opts.worker && n.service_type === 'llm_worker' && !n.instanceOf,
  )
  if (!worker) throw bad(`"${opts.worker || ''}" is not an LLM worker in this system`)
  const stream = worker.llm?.stream
  if (!stream || !manifest.nodes.some((n) => n.id === stream)) {
    throw bad(`worker "${worker.id}" has no linked token-stream redis`)
  }
  const description = trimmed(opts.description)
  if (!description) throw bad('description is required')
  if (description.length > 4000) throw bad('description too long (max 4000 chars)')
  const freeform = trimmed(opts.freeform)
  if (freeform.length > 4000) throw bad('freeform spec too long (max 4000 chars)')
  const db = trimmed(opts.db)
  const table = trimmed(opts.table)
  const field = trimmed(opts.field)
  if (!freeform) {
    const dbNode = manifest.nodes.find((n) => n.id === db && n.origin === 'create-database')
    if (!dbNode || !DB_TYPES.has(dbNode.type)) {
      throw bad('pick a postgres/mongodb database (or provide a freeform spec)')
    }
    if (!IDENT_RE.test(table)) throw bad('pick the table/collection to write to')
    if (!IDENT_RE.test(field)) throw bad('pick the field/column to write the output to')
  }
  const conversationId = trimmed(opts.conversationId)
  if (conversationId.length > 64) throw bad('invalid conversationId')

  // 2. Scaffold the reading service: plain FastAPI template (the poll/persist loop
  //    comes from the launched session) with the group identity pre-wired in env —
  //    SERVICE_ID doubles as the member's XREADGROUP consumer name, so every cloned
  //    replica claims runs under its own name with no code change.
  cloneTemplate(system, name, SERVICE_TPL, SERVICE_FILES)
  addComposeService(
    system,
    name,
    {
      build: `./${name}`,
      environment: {
        SERVICE_ID: name,
        REDIS_HOST: stream,
        ANNOUNCE_STREAM,
        READER_GROUP: name,
        ...(freeform ? {} : { DB_NODE: db }),
      },
      depends_on: [stream, ...(freeform ? [] : [db])],
    },
    ` Persistence readers "${name}" — consume ${ANNOUNCE_STREAM} on "${stream}", persist finished runs`,
  )
  addNginxRoute(system, name)
  addScrapeJob(system, name, 8000, ` Persistence readers "${name}" — custom service type`)

  // 3. Registry entry (persistence.json — consumers.json conventions: the backend
  //    owns the spec, the launched session owns `implemented`).
  const now = new Date().toISOString()
  const data = readReaders(system)
  data.readers = data.readers.filter((r) => r && r.service !== name)
  data.readers.push({
    service: name,
    worker: worker.id,
    stream,
    announce: ANNOUNCE_STREAM,
    group: name,
    db: freeform ? null : db,
    table: freeform ? '' : table,
    field: freeform ? '' : field,
    freeform,
    description,
    members: [name],
    implemented: false,
    conversationId,
    createdAt: now,
    updatedAt: now,
    history: [{ at: now, description, db: freeform ? null : db, table, field, freeform }],
  })
  writeReaders(system, data)

  // 4. Manifest: the reader node (group identity + persist target on the node) and
  //    its consume/persist edges. Edges are pushed before addManifestNode persists
  //    the manifest, so one write lands everything (llmWorker.js convention).
  manifest.edges = manifest.edges || []
  manifest.edges.push({ from: name, to: stream, origin: 'create-custom-service' })
  if (!freeform) manifest.edges.push({ from: name, to: db, origin: 'create-custom-service' })
  const position = nextNodePosition(manifest)
  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'service',
    origin: 'create-custom-service',
    service_type: 'persistence_reader',
    persistence: {
      worker: worker.id,
      stream,
      announce: ANNOUNCE_STREAM,
      group: name,
      db: freeform ? null : db,
      table: freeform ? '' : table,
      field: freeform ? '' : field,
    },
    position,
    metrics: [...serviceMetrics(name), ...persistenceMetrics(name, true, name)],
    health: serviceHealth(name),
  })

  // 5. One locked rebuild for the new container, lb reload for the new route,
  //    prometheus restart for the new scrape job.
  const log = SKIP_REBUILD()
    ? '(rebuild skipped)'
    : await scaleRebuild(system, { buildNames: [name], reloadLb: true })
  return { ok: true, node, log }
}

// ---------------------------------------------------------------------------
// Control routes (mounted by customServices.js with a { json, readJsonBody } ctx)
// ---------------------------------------------------------------------------
const fail = (ctx, res, err) => ctx.json(res, err.statusCode || 500, { ok: false, error: err.message })

// Resolve + validate a persistence_reader BASE node from query/body params.
function baseNode(system, node) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (!node || !NAME_RE.test(node)) throw bad('invalid node')
  const manifest = readManifest(system)
  const n = manifest.nodes.find((x) => x.id === node && x.service_type === 'persistence_reader' && !x.instanceOf)
  if (!n) throw bad(`"${node}" is not a persistence reader group in this system`)
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

// Aggregate every group's registry entry + members' live /reader/state into one
// node-keyed map the tabs + diagram read: nodes[<base>] = { registry, live },
// nodes[<member>] = { live }. /reader/state is authored by the session (404 on the
// bare template) — best-effort with a short timeout, like the other type states.
async function handleState(req, res, _next, ctx) {
  if (req.method !== 'GET') return ctx.json(res, 405, { ok: false, error: 'GET only' })
  try {
    const url = new URL(req.url, 'http://localhost')
    const system = url.searchParams.get('system')
    if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
    const manifest = readManifest(system)
    const registry = readReaders(system)
    const members = manifest.nodes.filter((n) => n.service_type === 'persistence_reader')
    const nodes = {}
    await Promise.all(
      members.map(async (m) => {
        let live = null
        try {
          const r = await fetch(`${LB}/${m.id}/reader/state`, { signal: AbortSignal.timeout(3000) })
          if (r.ok) {
            const s = await r.json()
            const base = m.instanceOf || m.id
            // The lb only serves the ACTIVE system — guard against a same-named
            // reader in a different system answering.
            if (s && s.group === base) live = s
          }
        } catch {
          /* reader not reachable (system inactive, still building, or loop not authored yet) */
        }
        nodes[m.id] = m.instanceOf
          ? { live }
          : { live, registry: registry.readers.find((e) => e && e.service === m.id) || null }
      }),
    )
    ctx.json(res, 200, { ok: true, nodes })
  } catch (err) {
    fail(ctx, res, err)
  }
}

// Registry-only edit — description / table / field / freeform / conversationId, and
// `implemented` (the launched session flips it; preserved unless explicitly sent).
// The db target is fixed at creation (compose env + depends_on point at it): delete
// + re-add the readers to change it. Pure JSON + manifest block, no rebuild.
async function handleUpdate(req, res, _next, ctx) {
  try {
    if (req.method !== 'POST') return ctx.json(res, 405, { ok: false, error: 'POST only' })
    const body = await ctx.readJsonBody(req)
    const { system } = body
    const { manifest, node } = baseNode(system, body.node)

    const data = readReaders(system)
    const entry = data.readers.find((r) => r && r.service === node.id)
    if (!entry) throw bad(`no persistence entry for "${node.id}"`)
    if (body.db !== undefined && trimmed(body.db) !== (entry.db || '')) {
      throw bad('the database target is fixed at creation — delete and re-add the readers to change it')
    }

    let changed = false
    const setStr = (key, v, max, re) => {
      if (v === undefined) return
      const s = trimmed(v)
      if (s.length > max) throw bad(`${key} too long (max ${max} chars)`)
      if (s && re && !re.test(s)) throw bad(`invalid ${key}`)
      if (s !== (entry[key] || '')) {
        entry[key] = s
        changed = true
      }
    }
    setStr('description', body.description, 4000)
    setStr('freeform', body.freeform, 4000)
    setStr('conversationId', body.conversationId, 64)
    if (entry.db) {
      // structured target: table/field stay editable (the re-author session honors them)
      setStr('table', body.table, 100, IDENT_RE)
      setStr('field', body.field, 100, IDENT_RE)
    }
    if (typeof body.implemented === 'boolean' && body.implemented !== entry.implemented) {
      entry.implemented = body.implemented
      changed = true
    }
    if (changed) {
      entry.updatedAt = new Date().toISOString()
      entry.history = Array.isArray(entry.history) ? entry.history : []
      entry.history.push({
        at: entry.updatedAt,
        description: entry.description,
        db: entry.db,
        table: entry.table,
        field: entry.field,
        freeform: entry.freeform,
      })
      writeReaders(system, data)
      // Keep the manifest block (what the diagram shows) in step with the registry.
      if (node.persistence) {
        node.persistence.table = entry.table
        node.persistence.field = entry.field
        fs.writeFileSync(
          path.join(systemDir(system), 'manifest.json'),
          JSON.stringify(manifest, null, 2) + '\n',
        )
      }
    }
    ctx.json(res, 200, { ok: true, reader: entry })
  } catch (err) {
    fail(ctx, res, err)
  }
}

export default {
  serviceType: 'persistence_reader',
  displayName: 'LLM Persistence Readers',
  description:
    'A reading service created from the LLM worker\'s Persistence tab: N member containers form a redis consumer group on the worker\'s runs:started stream, each claiming announced runs, accumulating their token streams, and persisting the finished output to a chosen database table/field.',
  hidden: true, // created from the llm-worker Persistence tab, not the add-service modal
  onAdd,
  routes: [
    { path: '/api/custom/persistence-reader/scale', handler: handleScale },
    { path: '/api/custom/persistence-reader/state', handler: handleState },
    { path: '/api/custom/persistence-reader/update', handler: handleUpdate },
  ],
}
