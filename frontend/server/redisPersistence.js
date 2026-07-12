// Vite dev-server plugin: redis persistence (RDB snapshots + AOF) for a
// user-created redis primary.
//
//   GET  /api/redis/persistence?system=<id>&id=<redisId>
//     -> { ok, persistence|null, defaults, limits, mode, targets, status, statusError? }
//   POST /api/redis/persistence         { system, id, persistence: { rdb, aof } }
//   POST /api/redis/persistence         { system, id, reset: true }
//     -> { ok, persistence|null, targets, warnings, log }
//   POST /api/redis/persistence/action  { system, id, action: 'bgsave'|'bgrewriteaof' }
//     -> { ok, results: [{ target, ok, message }] }
//
// The settings live as a `persistence` block on the manifest redis node (absent =
// redis image defaults, exactly like a keyspace's absent writeModes). An apply is
// TWO mechanically independent halves:
//   1. durable — bake the flags into the compose `command:` of every data-bearing
//      container (primary + replicas, or every cluster member), so any future
//      recreate — including a Topology reshape, whose builders re-read the block —
//      keeps the policy;
//   2. live — `redis-cli CONFIG SET` on each running container (atomic multi-param,
//      redis >= 7), so the change takes effect with NO restart and no data loss.
// Consequence of the sandbox's no-data-volume contract: after an apply the running
// container's create-time definition differs from compose, so the next untargeted
// `docker compose up -d` (a topology apply) recreates it once — clean, as always.
//
// No node metric is added on purpose: topology transitions rewrite node.metrics
// wholesale, so an appended row would silently vanish. The exporter already ships
// redis_rdb_last_save_timestamp_seconds if a PromQL reader ever wants it; the tab's
// status readout (INFO persistence) covers the live view.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'
import { loadCompose, saveCompose, withSystemLock } from './scaffold.js'
import {
  HttpError, bad, readJsonBody,
  REDIS_PERSISTENCE_DEFAULTS, REDIS_PERSISTENCE_LIMITS, redisPersistenceFlags,
} from './databases.js'
import { composeExec } from './dbschema.js'

const now = () => new Date().toISOString()

function loadManifest(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const file = path.join(systemDir(system), 'manifest.json')
  return { file, manifest: JSON.parse(fs.readFileSync(file, 'utf8')) }
}
const saveManifest = (file, manifest) =>
  fs.writeFileSync(file, JSON.stringify(manifest, null, 2) + '\n')

// Same ownership gate as the Topology tab (findTopologyRedis): custom-owned redis
// (an LLM worker's token stream) and the websocket bus/presence caches have owned
// lifecycles, and a replica's persistence mirrors its primary's block.
function findPersistenceRedis(manifest, id) {
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.type !== 'redis') throw bad(`"${id}" is not a redis node in this system`)
  if (node.origin !== 'create-database') {
    throw bad(`the persistence of "${id}" is owned by its creating feature — only "Add database" redis nodes are configurable`)
  }
  if (node.replicaOf) throw bad('persistence is configured on the primary, not a read replica')
  return node
}

// Every DATA-bearing container of the node's current topology — never sentinels
// (no dataset), never exporters or init sidecars.
function dataTargets(manifest, node) {
  if (node.redisCluster?.members?.length) return [...node.redisCluster.members]
  return [node.id, ...manifest.nodes.filter((n) => n.replicaOf === node.id).map((n) => n.id)]
}

const modeOf = (node) => (node.redisCluster ? 'cluster' : node.sentinel ? 'replicated' : 'standalone')

// --- validation --------------------------------------------------------------------

function normalizePersistence(raw) {
  if (!raw || typeof raw !== 'object') throw bad('persistence must be an object { rdb, aof }')
  const L = REDIS_PERSISTENCE_LIMITS
  const rdbIn = raw.rdb || {}
  const aofIn = raw.aof || {}

  if (typeof rdbIn.enabled !== 'boolean') throw bad('rdb.enabled must be true or false')
  if (!Array.isArray(rdbIn.rules)) throw bad('rdb.rules must be an array of { seconds, changes }')
  if (rdbIn.rules.length > L.maxRules) throw bad(`rdb.rules: at most ${L.maxRules} save rules`)
  const seen = new Set()
  const rules = rdbIn.rules.map((r) => {
    const seconds = Number(r?.seconds)
    const changes = Number(r?.changes)
    if (!Number.isInteger(seconds) || seconds < L.secondsMin || seconds > L.secondsMax) {
      throw bad(`save rule seconds must be an integer ${L.secondsMin}-${L.secondsMax}`)
    }
    if (!Number.isInteger(changes) || changes < L.changesMin || changes > L.changesMax) {
      throw bad(`save rule changes must be an integer ${L.changesMin}-${L.changesMax}`)
    }
    if (seen.has(seconds)) throw bad(`duplicate ${seconds}s save rule — redis keeps one change threshold per time window`)
    seen.add(seconds)
    return { seconds, changes }
  })
  if (rdbIn.enabled && rules.length === 0) {
    throw bad('RDB needs at least one save rule ("snapshot after N seconds if ≥ C changes") — or disable it')
  }

  if (typeof aofIn.enabled !== 'boolean') throw bad('aof.enabled must be true or false')
  const fsync = aofIn.fsync ?? REDIS_PERSISTENCE_DEFAULTS.aof.fsync
  if (!L.fsync.includes(fsync)) throw bad(`aof.fsync must be one of: ${L.fsync.join(', ')}`)
  const rewritePercent = Number(aofIn.rewritePercent ?? REDIS_PERSISTENCE_DEFAULTS.aof.rewritePercent)
  if (!Number.isInteger(rewritePercent) || rewritePercent < L.rewritePercentMin || rewritePercent > L.rewritePercentMax) {
    throw bad(`aof.rewritePercent must be an integer ${L.rewritePercentMin}-${L.rewritePercentMax} (0 disables auto-rewrite)`)
  }
  const rewriteMinMb = Number(aofIn.rewriteMinMb ?? REDIS_PERSISTENCE_DEFAULTS.aof.rewriteMinMb)
  if (!Number.isInteger(rewriteMinMb) || rewriteMinMb < L.rewriteMinMbMin || rewriteMinMb > L.rewriteMinMbMax) {
    throw bad(`aof.rewriteMinMb must be an integer ${L.rewriteMinMbMin}-${L.rewriteMinMbMax}`)
  }

  return {
    rdb: { enabled: rdbIn.enabled, rules },
    aof: { enabled: aofIn.enabled, fsync, rewritePercent, rewriteMinMb },
  }
}

// --- compose command surgery ---------------------------------------------------------

// The closed set of flags this feature owns in a redis command array. Each is
// stripped together with its value tokens (everything up to the next `--` token) —
// deterministic because only redisPersistenceFlags ever writes them.
const OWNED_FLAGS = new Set([
  '--save', '--appendonly', '--appendfsync',
  '--auto-aof-rewrite-percentage', '--auto-aof-rewrite-min-size',
])

function stripPersistenceFlags(cmd) {
  const out = []
  for (let i = 0; i < cmd.length; i++) {
    if (OWNED_FLAGS.has(String(cmd[i]))) {
      while (i + 1 < cmd.length && !String(cmd[i + 1] ?? '').startsWith('--')) i++
      continue
    }
    out.push(cmd[i])
  }
  return out
}

// Rewrite ONE service's command in place (never the whole def — image/depends_on/
// volumes and YAML comments stay untouched). A bare standalone service gains
// `command: [redis-server, ...flags]`; a reset that strips the command back down to
// a lone `redis-server` deletes the key, restoring the original bare shape.
function setServiceCommand(doc, svc, persistence, warnings) {
  if (!doc.hasIn(['services', svc])) {
    warnings.push(`compose service "${svc}" not found — skipped`)
    return
  }
  const cur = doc.getIn(['services', svc, 'command'])
  const base = stripPersistenceFlags(cur ? cur.toJSON() : ['redis-server'])
  const flags = redisPersistenceFlags(persistence)
  if (!flags.length && base.length === 1 && base[0] === 'redis-server') {
    if (cur) doc.deleteIn(['services', svc, 'command'])
    return
  }
  doc.setIn(['services', svc, 'command'], doc.createNode([...base, ...flags]))
}

// --- live apply / status --------------------------------------------------------------

// One atomic multi-param CONFIG SET (redis >= 7). On reset the image defaults are
// sent explicitly — the running server can't re-read a config file it never had.
function configSetArgv(persistence) {
  const p = persistence || REDIS_PERSISTENCE_DEFAULTS
  const save = p.rdb.enabled ? p.rdb.rules.map((r) => `${r.seconds} ${r.changes}`).join(' ') : ''
  return [
    'redis-cli', 'CONFIG', 'SET',
    'save', save,
    'appendonly', p.aof.enabled ? 'yes' : 'no',
    'appendfsync', p.aof.fsync,
    'auto-aof-rewrite-percentage', String(p.aof.rewritePercent),
    'auto-aof-rewrite-min-size', `${p.aof.rewriteMinMb}mb`,
  ]
}

const INFO_FIELDS = new Set([
  'rdb_last_save_time', 'rdb_changes_since_last_save',
  'rdb_bgsave_in_progress', 'rdb_last_bgsave_status',
  'aof_enabled', 'aof_rewrite_in_progress',
  'aof_last_bgrewrite_status', 'aof_last_write_status',
])

async function readStatus(system, target) {
  const info = await composeExec(system, target, { envFlags: [], argv: ['redis-cli', 'INFO', 'persistence'] })
  const status = {}
  for (const line of info.stdout.split('\n')) {
    if (line.startsWith('#')) continue
    const i = line.indexOf(':')
    if (i < 1) continue
    const key = line.slice(0, i).trim()
    if (!INFO_FIELDS.has(key)) continue
    const value = line.slice(i + 1).trim()
    status[key] = /^-?\d+$/.test(value) ? Number(value) : value
  }
  // What the server is ACTUALLY running right now (shows drift while a container
  // created before the last apply hasn't been recreated yet).
  const cfg = await composeExec(system, target, { envFlags: [], argv: ['redis-cli', 'CONFIG', 'GET', 'save', 'appendonly', 'appendfsync'] })
  const lines = cfg.stdout.split('\n').map((l) => l.replace(/\r$/, ''))
  const live = {}
  for (let i = 0; i + 1 < lines.length; i += 2) {
    if (lines[i]) live[lines[i]] = lines[i + 1]
  }
  return { target, ...status, live }
}

// --- handlers ---------------------------------------------------------------------------

async function handleGet(system, id) {
  const { manifest } = loadManifest(system)
  const node = findPersistenceRedis(manifest, id)
  const targets = dataTargets(manifest, node)
  let status = null
  let statusError = null
  for (const t of targets) {
    try {
      status = await readStatus(system, t)
      break
    } catch (err) {
      statusError = `"${t}": ${(err.stderr || err.message || 'unreachable').trim().split('\n')[0]}`
    }
  }
  return {
    ok: true,
    persistence: node.persistence || null,
    defaults: REDIS_PERSISTENCE_DEFAULTS,
    limits: REDIS_PERSISTENCE_LIMITS,
    mode: modeOf(node),
    targets,
    status,
    ...(status ? {} : { statusError: statusError || 'no data container is running' }),
  }
}

async function handleSet(body) {
  const system = body.system
  const { file, manifest } = loadManifest(system)
  const node = findPersistenceRedis(manifest, body.id)
  const reset = body.reset === true
  const persistence = reset ? null : normalizePersistence(body.persistence)

  const warnings = []
  if (persistence && !persistence.rdb.enabled && !persistence.aof.enabled) {
    warnings.push('RDB and AOF are both off — a restart of this container loses every key')
  }
  if (persistence?.aof.fsync === 'always') {
    warnings.push('appendfsync "always" fsyncs on every write — maximum durability, significant write latency')
  }

  // Durable half: compose commands + the manifest block, one parse/write each.
  const targets = dataTargets(manifest, node)
  const doc = loadCompose(system)
  for (const t of targets) setServiceCommand(doc, t, persistence, warnings)
  saveCompose(system, doc)
  if (reset) delete node.persistence
  else node.persistence = { ...persistence, updatedAt: now() }
  saveManifest(file, manifest)

  // Live half: best-effort per container — a down container just keeps the baked
  // flags for its next recreate. Serialized with the rebuild paths; no nesting.
  const argv = configSetArgv(persistence)
  const log = await withSystemLock(system, async () => {
    let out = ''
    for (const t of targets) {
      try {
        const r = await composeExec(system, t, { envFlags: [], argv })
        out += `${t}: ${r.stdout.trim() || 'OK'}\n`
      } catch (err) {
        warnings.push(`"${t}" is not reachable — settings are saved to compose and apply on its next recreate`)
        out += `${t}: unreachable\n`
      }
    }
    return out
  })

  return { ok: true, persistence: node.persistence || null, targets, warnings, log }
}

async function handleAction(body) {
  const system = body.system
  const { manifest } = loadManifest(system)
  const node = findPersistenceRedis(manifest, body.id)
  if (!['bgsave', 'bgrewriteaof'].includes(body.action)) {
    throw bad('action must be "bgsave" or "bgrewriteaof"')
  }
  const argv = ['redis-cli', body.action === 'bgsave' ? 'BGSAVE' : 'BGREWRITEAOF']
  const targets = dataTargets(manifest, node)
  const results = await withSystemLock(system, async () => {
    const out = []
    for (const t of targets) {
      try {
        const r = await composeExec(system, t, { envFlags: [], argv })
        out.push({ target: t, ok: true, message: r.stdout.trim() })
      } catch (err) {
        out.push({ target: t, ok: false, message: (err.stderr || err.message || 'unreachable').trim().split('\n')[0] })
      }
    }
    return out
  })
  if (!results.some((r) => r.ok)) {
    throw new HttpError(409, `no data container of "${node.id}" is reachable — is the stack running?`)
  }
  return { ok: true, results }
}

// --- plugin -------------------------------------------------------------------------------

export default function redisPersistence() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  return {
    name: 'redis-persistence',
    configureServer(server) {
      server.middlewares.use('/api/redis/persistence', async (req, res, next) => {
        // Connect strips the mount prefix: /api/redis/persistence/action -> /action.
        const url = new URL(req.url, 'http://localhost')
        const sub = url.pathname.replace(/\/$/, '')
        try {
          if (sub === '/action') {
            if (req.method === 'POST') return json(res, 200, await handleAction(await readJsonBody(req)))
            return next()
          }
          if (sub === '') {
            if (req.method === 'GET') {
              return json(res, 200, await handleGet(url.searchParams.get('system'), url.searchParams.get('id')))
            }
            if (req.method === 'POST') return json(res, 200, await handleSet(await readJsonBody(req)))
          }
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
