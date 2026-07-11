// Vite dev-server plugin: redis KEYSPACES — the persisted key-namespace config of a
// `type:"redis"` manifest node (ANY origin: create-database, an LLM worker's token
// stream, a websocket tier's bus/presence cache).
//
// A keyspace names a key (or key prefix), its expected redis TYPE, an optional
// display/reference shorthand, and the services that write/read it:
//   { name, match: 'prefix'|'exact', type: string|list|set|hash|zset|stream|geo,
//     shorthand?, writers: [nodeIds], readers: [nodeIds],
//     writeModes?: { [writerId]: { mode:'wait', numreplicas, timeoutMs, implemented, updatedAt } },
//     verified, origin: 'user'|'scan', suggestedWriters: [], suggestedReaders: [],
//     observedType?, lastScanAt?, createdAt, updatedAt }
//
// writeModes is the per-WRITER write acknowledgement mode: absent = async (fire and
// forget — the default, never stored); 'wait' = pseudo-synchronous replication via
// the WAIT command (`r.wait(numreplicas, timeoutMs)` after each write, blocking
// until numreplicas replicas ack or the timeout elapses). `implemented` is owned by
// the scan, which greps the writer's source for an actual WAIT call.
//
// Entries live ON the manifest node (`node.keyspaces`) like the grpc/resilience
// blocks: the 3s manifest poll already delivers them to the diagram (typed KEY rows
// + the writer → redis → reader click-trace), and launched sessions see them in the
// inlined manifest — so services can reference a keyspace by its shorthand. Every
// route below is a live JSON edit; none of them rebuilds a container.
//
//   GET    /api/redis/keyspaces?system&id  -> { ok, keyspaces }
//   POST   /api/redis/keyspace             { system, id, keyspace, prevName? }  upsert
//   DELETE /api/redis/keyspace             { system, id, name }
//   POST   /api/redis/keyspace/verify      { system, id, name }       flips verified:true
//   POST   /api/redis/keyspace/suggestion  { system, id, name, service, role, action }
//   POST   /api/redis/scan                 { system, id } -> { ok, report, keyspaces }
//
// The scan reads the LIVE container (SCAN + TYPE via one `redis-cli EVAL` of a
// constant Lua script — execFile arg arrays, nothing interpolated): live keys that
// match no declared keyspace are ADDED as `verified:false` entries the user can
// Verify away; declared-vs-observed type drift is reported. A source grep over the
// system's code-bearing services then SUGGESTS writers/readers per keyspace, which
// the user accepts or dismisses in the Keyspaces tab.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'
import {
  HttpError, bad, readJsonBody,
  REDIS_KS_RE, REDIS_SHORTHAND_RE, REDIS_KS_TYPES,
} from './databases.js'
import { composeExec } from './dbschema.js'

const now = () => new Date().toISOString()

// TYPE reports geo data as zset (GEO is a zset encoding), so a declared geo
// keyspace observing zset is not drift.
const typesCompatible = (declared, observed) =>
  declared === observed || (declared === 'geo' && observed === 'zset')

function loadManifest(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const file = path.join(systemDir(system), 'manifest.json')
  return { file, manifest: JSON.parse(fs.readFileSync(file, 'utf8')) }
}

const saveManifest = (file, manifest) =>
  fs.writeFileSync(file, JSON.stringify(manifest, null, 2) + '\n')

// Keyspaces live on a redis PRIMARY; a read replica mirrors the primary's data,
// so its config stays on the primary node.
function redisNodeOf(manifest, id) {
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.type !== 'redis') throw bad(`"${id}" is not a redis node in this system`)
  if (node.replicaOf) throw bad('keyspaces are configured on the primary, not a read replica')
  return node
}

// Nodes that may be declared (or suggested) as a keyspace's writers/readers: the
// ones carrying real code under systems/<sys>/<nodeId>/. Group instances are
// excluded — a load-balanced cluster's ENTRY owns the code.
const isCodeBearing = (n) =>
  (n.type === 'service' && !n.instanceOf) || n.type === 'service-lb' || n.type === 'websocket-server'

function normalizeKeyspace(raw, manifest) {
  if (!raw || typeof raw !== 'object') throw bad('missing keyspace')
  const name = typeof raw.name === 'string' ? raw.name.trim() : ''
  if (!REDIS_KS_RE.test(name)) {
    throw bad('keyspace name must be 1-128 chars of letters, digits, "_", ".", ":" or "-" (starting alphanumeric)')
  }
  const match = raw.match === 'exact' || raw.match === 'prefix' ? raw.match : null
  if (!match) throw bad('match must be "prefix" or "exact"')
  if (!REDIS_KS_TYPES.has(raw.type)) throw bad(`invalid keyspace type "${raw.type}"`)
  const shorthand = typeof raw.shorthand === 'string' ? raw.shorthand.trim() : ''
  if (shorthand && !REDIS_SHORTHAND_RE.test(shorthand)) {
    throw bad('shorthand must be 1-32 chars of letters, digits, "_" or "-" (starting with a letter)')
  }
  const roles = {}
  for (const role of ['writers', 'readers']) {
    const seen = new Set()
    for (const id of Array.isArray(raw[role]) ? raw[role] : []) {
      const n = manifest.nodes.find((m) => m.id === id)
      if (!n || !isCodeBearing(n)) throw bad(`"${id}" is not a service in this system`)
      seen.add(id)
    }
    roles[role] = [...seen]
  }
  // Per-writer write modes: only 'wait' entries are kept (async is the unstored
  // default), and only for declared writers — a key whose writer was just removed
  // is silently dropped rather than rejected (the tab re-submits the full map).
  const writeModes = {}
  if (raw.writeModes && typeof raw.writeModes === 'object' && !Array.isArray(raw.writeModes)) {
    for (const [svc, wm] of Object.entries(raw.writeModes)) {
      if (!roles.writers.includes(svc) || !wm || typeof wm !== 'object') continue
      if (wm.mode !== 'wait') continue
      const numreplicas = Number(wm.numreplicas)
      const timeoutMs = Number(wm.timeoutMs)
      if (!Number.isInteger(numreplicas) || numreplicas < 1 || numreplicas > 9) {
        throw bad(`WAIT numreplicas for "${svc}" must be an integer 1-9`)
      }
      if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 60000) {
        throw bad(`WAIT timeout for "${svc}" must be an integer 0-60000 ms`)
      }
      writeModes[svc] = { mode: 'wait', numreplicas, timeoutMs }
    }
  }
  return { name, match, type: raw.type, shorthand, ...roles, writeModes }
}

// --- registry mutations (upsert / delete / verify / suggestion) ---------------

function upsertKeyspace(body) {
  const { file, manifest } = loadManifest(body.system)
  const node = redisNodeOf(manifest, body.id)
  const ks = normalizeKeyspace(body.keyspace, manifest)
  const list = node.keyspaces || []
  const identity = typeof body.prevName === 'string' && body.prevName ? body.prevName : ks.name
  const idx = list.findIndex((k) => k.name === identity)
  if (body.prevName && idx < 0) throw bad(`no keyspace named "${body.prevName}" on "${node.id}"`)
  if (list.some((k, i) => i !== idx && k.name === ks.name)) {
    throw bad(`a keyspace named "${ks.name}" already exists on "${node.id}"`)
  }
  if (ks.shorthand) {
    const taken = list.find((k, i) => i !== idx && k.shorthand === ks.shorthand)
    if (taken) throw bad(`shorthand "${ks.shorthand}" is already used by "${taken.name}"`)
  }
  const prev = idx >= 0 ? list[idx] : null
  const ts = now()
  // The submitted writeModes map is authoritative (the tab always re-submits the
  // full map; an absent writer = back to async). An unchanged wait entry keeps its
  // scan-owned `implemented`; a parameter change resets it — the writer's code no
  // longer matches what's declared.
  const writeModes = {}
  for (const [svc, wm] of Object.entries(ks.writeModes)) {
    const prevWm = prev?.writeModes?.[svc]
    writeModes[svc] =
      prevWm && prevWm.numreplicas === wm.numreplicas && prevWm.timeoutMs === wm.timeoutMs
        ? prevWm
        : { ...wm, implemented: false, updatedAt: ts }
  }
  const entry = {
    name: ks.name,
    match: ks.match,
    type: ks.type,
    ...(ks.shorthand ? { shorthand: ks.shorthand } : {}),
    writers: ks.writers,
    readers: ks.readers,
    ...(Object.keys(writeModes).length ? { writeModes } : {}),
    // Verification is separate state — an edit must not flip it either way. A
    // brand-new user-declared keyspace IS the declaration, so it starts verified;
    // only scan-discovered entries wait for the explicit Verify click.
    verified: prev ? prev.verified === true : true,
    origin: prev?.origin || 'user',
    // Declaring a service by hand consumes any matching pending suggestion.
    suggestedWriters: (prev?.suggestedWriters || []).filter((s) => !ks.writers.includes(s)),
    suggestedReaders: (prev?.suggestedReaders || []).filter((s) => !ks.readers.includes(s)),
    ...(prev?.observedType ? { observedType: prev.observedType } : {}),
    ...(prev?.lastScanAt ? { lastScanAt: prev.lastScanAt } : {}),
    createdAt: prev?.createdAt || ts,
    updatedAt: ts,
  }
  if (idx >= 0) list[idx] = entry
  else list.push(entry)
  node.keyspaces = list
  saveManifest(file, manifest)
  return { ok: true, keyspace: entry }
}

function deleteKeyspace(body) {
  const { file, manifest } = loadManifest(body.system)
  const node = redisNodeOf(manifest, body.id)
  const list = node.keyspaces || []
  const idx = list.findIndex((k) => k.name === body.name)
  if (idx < 0) throw bad(`no keyspace named "${body.name}" on "${node.id}"`)
  list.splice(idx, 1)
  if (list.length) node.keyspaces = list
  else delete node.keyspaces
  saveManifest(file, manifest)
  return { ok: true }
}

function verifyKeyspace(body) {
  const { file, manifest } = loadManifest(body.system)
  const node = redisNodeOf(manifest, body.id)
  const entry = (node.keyspaces || []).find((k) => k.name === body.name)
  if (!entry) throw bad(`no keyspace named "${body.name}" on "${node.id}"`)
  entry.verified = true
  entry.updatedAt = now()
  saveManifest(file, manifest)
  return { ok: true, keyspace: entry }
}

function handleSuggestion(body) {
  const { file, manifest } = loadManifest(body.system)
  const node = redisNodeOf(manifest, body.id)
  const entry = (node.keyspaces || []).find((k) => k.name === body.name)
  if (!entry) throw bad(`no keyspace named "${body.name}" on "${node.id}"`)
  if (body.role !== 'writer' && body.role !== 'reader') throw bad('role must be "writer" or "reader"')
  if (body.action !== 'accept' && body.action !== 'dismiss') throw bad('action must be "accept" or "dismiss"')
  const suggestedKey = body.role === 'writer' ? 'suggestedWriters' : 'suggestedReaders'
  const declaredKey = body.role === 'writer' ? 'writers' : 'readers'
  const svc = body.service
  if (!(entry[suggestedKey] || []).includes(svc)) {
    throw bad(`"${svc}" is not a suggested ${body.role} of "${body.name}"`)
  }
  entry[suggestedKey] = entry[suggestedKey].filter((s) => s !== svc)
  if (body.action === 'accept' && !(entry[declaredKey] || []).includes(svc)) {
    entry[declaredKey] = [...(entry[declaredKey] || []), svc]
  }
  entry.updatedAt = now()
  saveManifest(file, manifest)
  return { ok: true, keyspace: entry }
}

// --- the live scan -------------------------------------------------------------

// One constant Lua script walks the whole keyspace server-side (SCAN + TYPE) and
// returns one "key\ttype" line per key — a single exec instead of N+1 round trips.
// Constant string, nothing interpolated: injection-proof by construction.
const SCAN_LUA =
  "local out={} local cur='0' " +
  "repeat local r=redis.call('SCAN',cur,'COUNT',500) cur=r[1] " +
  "for i=1,#r[2] do local k=r[2][i] out[#out+1]=k..'\\t'..redis.call('TYPE',k)['ok'] end " +
  "until cur=='0' return out"

async function scanLiveKeys(system, node) {
  // In cluster mode (Topology tab) there is no `<node.id>` container — the keys
  // live sharded across the member containers, so every reachable member is
  // scanned and the lines merged (replicas mirror their shard master's keys; the
  // by-key dedupe collapses them). Standalone/replicated scans the primary alone.
  const targets = node.redisCluster?.members?.length ? node.redisCluster.members : [node.id]
  const reachable = []
  let detail = ''
  for (const target of targets) {
    try {
      const ping = await composeExec(system, target, { envFlags: [], argv: ['redis-cli', 'PING'] })
      if (!/PONG/.test(ping.stdout)) throw new Error(ping.stdout.trim() || 'no PONG')
      reachable.push(target)
    } catch (err) {
      detail = `${err.stdout || ''}${err.stderr || ''}`.trim() || err.message
    }
  }
  if (!reachable.length) {
    throw new HttpError(
      409,
      `redis container "${targets[0]}" is not reachable (${detail}). Start it with ` +
        `docker compose -f systems/${system}/docker-compose.yml up -d ${targets[0]} — verifying ` +
        'needs the system running (an empty keyspace may just mean no writer has run yet).',
    )
  }
  const byKey = new Map()
  for (const target of reachable) {
    const { stdout } = await composeExec(system, target, {
      envFlags: [],
      argv: ['redis-cli', 'EVAL', SCAN_LUA, '0'],
    })
    for (const line of stdout.split('\n')) {
      const t = line.indexOf('\t')
      if (t < 0) continue
      const key = line.slice(0, t)
      const liveType = line.slice(t + 1).trim()
      if (key && liveType && !byKey.has(key)) byKey.set(key, { key, liveType })
    }
  }
  return [...byKey.values()]
}

// Exact match beats any prefix; among prefixes the longest declared name wins
// (so `tokens:done:` shadows `tokens:` for keys under both).
function matchEntry(list, key) {
  let best = null
  for (const ks of list) {
    if (ks.match === 'exact') {
      if (key === ks.name) return ks
    } else if (key.startsWith(ks.name)) {
      if (!best || ks.name.length > best.name.length) best = ks
    }
  }
  return best
}

// Unmatched live keys become candidate entries: grouped by the prefix through the
// first ':' (`session:abc` -> prefix `session:`); colon-less keys are exact entries.
function groupUnmatched(unmatched) {
  const groups = new Map()
  for (const { key, liveType } of unmatched) {
    const c = key.indexOf(':')
    const name = c >= 0 ? key.slice(0, c + 1) : key
    if (!groups.has(name)) {
      groups.set(name, { name, match: c >= 0 ? 'prefix' : 'exact', types: new Map(), count: 0 })
    }
    const g = groups.get(name)
    g.count += 1
    g.types.set(liveType, (g.types.get(liveType) || 0) + 1)
  }
  return [...groups.values()]
}

const dominantType = (types) => [...types.entries()].sort((a, b) => b[1] - a[1])[0][0]

// --- writer/reader suggestions via source grep ----------------------------------

// redis-py snake_case and ioredis/node-redis (incl. camelCase) mutators vs readers.
// Case-insensitive so `hset`/`hSet` both hit. Consuming pops (LPOP/BLPOP…) count as
// READS — that's the consumer side of a list queue. Loose on purpose: these only
// ever produce SUGGESTIONS a human accepts, so a `dict.get(` false positive costs
// one dismiss click.
const WRITE_CALL_RE =
  /\.\s*(getset|setnx|setex|psetex|mset|set|append|incrbyfloat|incrby|incr|decrby|decr|hsetnx|hmset|hset|hdel|hincrbyfloat|hincrby|zadd|zincrby|zrem|sadd|srem|smove|lpushx|rpushx|lpush|rpush|lset|linsert|xadd|geoadd|del|unlink|expire|pexpire|publish)\s*\(/i
const READ_CALL_RE =
  /\.\s*(mget|getrange|get|strlen|hgetall|hmget|hget|hkeys|hvals|hlen|zrangebyscore|zrevrange|zrange|zscore|zcard|zrank|smembers|sismember|scard|srandmember|lrange|lindex|llen|blpop|brpop|lpop|rpop|xreadgroup|xread|xrevrange|xrange|xlen|georadius|geosearch|geopos|geodist|subscribe|psubscribe)\s*\(/i
// A WAIT call: redis-py `r.wait(n, t)` / node-redis `client.wait(n, t)`, or the
// generic escape hatch `execute_command('WAIT', ...)`. `.wait(<digit>` keeps
// `threading.Event().wait(timeout=5)`-style false positives rare; like the verb
// regexes above, this only drives a BADGE, so a miss costs one wrong pill.
const WAIT_CALL_RE = /\.\s*wait\s*\(\s*\d|execute_command\(\s*['"]wait['"]/i

// A node's greppable source: *.py / *.js / *.mjs directly under systems/<sys>/<id>/
// (dir name == node id — the repo invariant), plus ws-shared/ for websocket relays
// (the shared hooks execute on every server). Missing dirs (e.g. nodes with no
// code folder) and oversized files are skipped silently.
function sourceFilesFor(system, node) {
  const dirs = [path.join(systemDir(system), node.id)]
  if (node.type === 'websocket-server') dirs.push(path.join(systemDir(system), 'ws-shared'))
  const files = []
  for (const dir of dirs) {
    let names = []
    try {
      names = fs.readdirSync(dir)
    } catch {
      continue
    }
    for (const f of names) {
      if (!/\.(py|js|mjs)$/.test(f)) continue
      const p = path.join(dir, f)
      try {
        if (!fs.statSync(p).isFile() || fs.statSync(p).size > 256 * 1024) continue
        files.push(fs.readFileSync(p, 'utf8'))
      } catch {
        /* unreadable — skip */
      }
    }
  }
  return files
}

// Suggest node as writer/reader of a keyspace iff one of its source files both
// mentions the keyspace name AND calls a redis verb of that direction. Returns the
// per-keyspace deltas this scan produced (already merged into the entries).
function suggestRoles(system, manifest, list) {
  const deltas = new Map()
  for (const node of manifest.nodes.filter(isCodeBearing)) {
    const files = sourceFilesFor(system, node)
    if (!files.length) continue
    for (const ks of list) {
      const hits = files.filter((text) => text.includes(ks.name))
      if (!hits.length) continue
      const roles = [
        ['suggestedWriters', 'writers', hits.some((t) => WRITE_CALL_RE.test(t))],
        ['suggestedReaders', 'readers', hits.some((t) => READ_CALL_RE.test(t))],
      ]
      for (const [suggestedKey, declaredKey, hit] of roles) {
        if (!hit) continue
        if ((ks[declaredKey] || []).includes(node.id)) continue
        if ((ks[suggestedKey] || []).includes(node.id)) continue
        ks[suggestedKey] = [...(ks[suggestedKey] || []), node.id]
        if (!deltas.has(ks.name)) {
          deltas.set(ks.name, { name: ks.name, suggestedWriters: [], suggestedReaders: [] })
        }
        deltas.get(ks.name)[suggestedKey].push(node.id)
      }
    }
  }
  return [...deltas.values()]
}

async function handleScan(body) {
  const { file, manifest } = loadManifest(body.system)
  const node = redisNodeOf(manifest, body.id)
  const live = await scanLiveKeys(body.system, node)

  const list = node.keyspaces || []
  const notes = []
  const perEntry = new Map(list.map((k) => [k.name, { count: 0, types: new Map() }]))
  const unmatched = []
  for (const item of live) {
    const entry = matchEntry(list, item.key)
    if (!entry) {
      unmatched.push(item)
      continue
    }
    const acc = perEntry.get(entry.name)
    acc.count += 1
    acc.types.set(item.liveType, (acc.types.get(item.liveType) || 0) + 1)
  }

  const ts = now()
  const matched = []
  const unseen = []
  for (const entry of list) {
    const acc = perEntry.get(entry.name)
    if (!acc || !acc.count) {
      unseen.push(entry.name)
      // Nothing live to compare against — drop a stale observation so an old
      // mismatch pill can't outlive the keys that caused it.
      delete entry.observedType
      entry.lastScanAt = ts
      continue
    }
    entry.observedType = dominantType(acc.types)
    entry.lastScanAt = ts
    const mismatch = ![...acc.types.keys()].every((t) => typesCompatible(entry.type, t))
    matched.push({ name: entry.name, keyCount: acc.count, observedType: entry.observedType, mismatch })
    if (acc.types.size > 1) {
      notes.push(`keys matching "${entry.name}" have mixed live types (${[...acc.types.keys()].join(', ')})`)
    }
  }

  const added = []
  for (const g of groupUnmatched(unmatched)) {
    if (!REDIS_KS_RE.test(g.name)) {
      notes.push(`skipped live namespace "${g.name}" — unsupported characters in the key name`)
      continue
    }
    const type = dominantType(g.types)
    list.push({
      name: g.name,
      match: g.match,
      type,
      writers: [],
      readers: [],
      verified: false,
      origin: 'scan',
      suggestedWriters: [],
      suggestedReaders: [],
      observedType: type,
      lastScanAt: ts,
      createdAt: ts,
      updatedAt: ts,
    })
    added.push({ name: g.name, match: g.match, type, keyCount: g.count })
    if (g.types.size > 1) {
      notes.push(`keys under "${g.name}" have mixed live types (${[...g.types.keys()].join(', ')}) — recorded ${type}`)
    }
  }

  const suggestions = suggestRoles(body.system, manifest, list)

  // Verify each wait-mode writer actually implements the WAIT call it declares:
  // same source-grep the suggestions use, but demanding BOTH the keyspace name and
  // a WAIT invocation somewhere in the writer's code.
  const waitChecks = []
  for (const ks of list) {
    for (const [svc, wm] of Object.entries(ks.writeModes || {})) {
      if (wm.mode !== 'wait') continue
      const writerNode = manifest.nodes.find((n) => n.id === svc)
      if (!writerNode) continue
      const files = sourceFilesFor(body.system, writerNode)
      const implemented =
        files.some((t) => t.includes(ks.name)) && files.some((t) => WAIT_CALL_RE.test(t))
      if (wm.implemented !== implemented) {
        wm.implemented = implemented
        wm.updatedAt = ts
      }
      waitChecks.push({ name: ks.name, writer: svc, implemented })
      if (!implemented) {
        notes.push(`writer "${svc}" declares WAIT on "${ks.name}" but no WAIT call was found in its source`)
      }
    }
  }

  if (live.length === 0) {
    notes.push(
      '0 live keys — writers may not have run yet, and pub/sub channels never appear in SCAN (a pure message bus scans empty).',
    )
  }

  if (list.length) node.keyspaces = list
  saveManifest(file, manifest)
  return {
    ok: true,
    report: { scannedKeys: live.length, matched, unseen, added, suggestions, waitChecks, notes },
    keyspaces: list,
  }
}

// --- plugin ---------------------------------------------------------------------

export default function redisKeyspaces() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  return {
    name: 'redis-keyspaces',
    configureServer(server) {
      server.middlewares.use('/api/redis', async (req, res, next) => {
        // Connect strips the mount prefix: /api/redis/keyspace arrives as /keyspace.
        const url = new URL(req.url, 'http://localhost')
        const sub = url.pathname.replace(/\/$/, '')
        try {
          if (sub === '/keyspaces') {
            if (req.method === 'GET') {
              const { manifest } = loadManifest(url.searchParams.get('system'))
              const node = redisNodeOf(manifest, url.searchParams.get('id'))
              return json(res, 200, { ok: true, keyspaces: node.keyspaces || [] })
            }
            return next()
          }
          if (sub === '/keyspace') {
            if (req.method === 'POST') return json(res, 200, upsertKeyspace(await readJsonBody(req)))
            if (req.method === 'DELETE') return json(res, 200, deleteKeyspace(await readJsonBody(req)))
            return next()
          }
          if (sub === '/keyspace/verify') {
            if (req.method === 'POST') return json(res, 200, verifyKeyspace(await readJsonBody(req)))
            return next()
          }
          if (sub === '/keyspace/suggestion') {
            if (req.method === 'POST') return json(res, 200, handleSuggestion(await readJsonBody(req)))
            return next()
          }
          if (sub === '/scan') {
            if (req.method === 'POST') return json(res, 200, await handleScan(await readJsonBody(req)))
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
