// Vite dev-server plugin: provision a WebSocket tier into the active system.
//
//   POST /api/websockets  { system, name, servers?, algorithm?, bus?, presence? }
//     -> creates, in one mechanical shot (no launched session — the server code is a
//        deterministic template):
//          <name>-lb          haproxy L4 (mode tcp) load balancer, host port 8090,
//                             algorithm leastconn|roundrobin|source, native
//                             prometheus exporter on :8405
//          <name>-server-1..N node.js `ws` relay servers (build: templates/websocket/server),
//                             each with prom-client metrics on :9100
//          <name>-bus         redis pub/sub bus (cross-server routing) + exporter
//          <name>-presence    redis presence cache (clientId -> serverId) + exporter
//          <name>-client      a container-less `client` manifest node whose behavior is
//                             a host-run node script (ws-clients/<name>-client.mjs)
//        plus the scrape jobs, manifest nodes/edges, and the tier registry
//        systems/<id>/<name>-lb/websockets.json (source of truth for haproxy.cfg).
//   GET  /api/websockets?system=<id>
//     -> { ok, tier, stats, clientMethods } — the registry contents (or tier:null; one
//        tier per system today), the client pool's last-run delivery stats (from
//        ws-clients/<client>.stats.json, written by the pool script itself on every
//        run regardless of driver — or null when it has never run), and the static
//        descriptor of the pool client's two BUILT-IN methods (spawnAndSend /
//        onReceive: names, args with defaults+bounds, summaries). The methods are not
//        editable or deletable and only end-to-end processes invoke them — the UI
//        renders them read-only.
//   POST /api/websockets/methods  { system, method, text, conversationId? }
//     -> appends a description ENTRY to one of the tier's two SHARED server methods
//        (onMessage / onSend) in the registry and marks it implemented:false. Pure
//        registry write (plus an idempotent ws-shared/ + compose-mount backfill for
//        tiers that predate shared methods) — the actual hook code in
//        ws-shared/hooks.js is authored by a launched Claude session
//        (sandbox-websocket skill), which restarts the servers and flips
//        implemented back to true (the consumers.json contract: Claude owns it).
//   POST /api/websockets/run  { system, client, count?, durationSeconds?, rate? }
//     -> spawns `node ws-clients/<client>.mjs --count N --duration S --rate R` on the
//        host and parses its trailing `__WS_RESULTS__ <json>` line (the ws twin of
//        scenarios.js' __LB_RESULTS__ runner).
//
// Mirrors eventstreams.js / databases.js: same plugin shape, comment-preserving YAML
// edits, strict whitelist validation (user input only ever lands in generated files,
// never a shell arg), and a frontend-safe `docker compose` rebuild — never ./start.sh.
//
// The lb speaks raw TCP (L4), not HTTP, so there is NO nginx route: ws clients reach
// it directly on the published host port. Tier membership lives on the manifest nodes
// (`wsTier: "<lb-id>"`, `wsRole: lb|server|bus|presence|client`), the same convention
// as replicaOf / cdcOf. The tier is one unit: deleting the lb cascades every wsTier
// member (remove.js), and every non-lb member is individually delete-BLOCKED — the
// whole websocket process only goes away via its load balancer.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemsDir, systemDir, isValidSystem, nextClientPosition } from './systems.js'
import { HttpError, bad, NAME_RE, readJsonBody, cloneTemplate } from './scaffold.js'
import { addComposeServices, addScrapeJob } from './databases.js'

const pexec = promisify(execFile)

const TEMPLATE_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'templates', 'websocket')
const SERVER_FILES = ['Dockerfile', 'package.json', 'server.js']

// The tier's ONE shared hooks file (fixed dir name like ws-clients/ — one tier per
// system). Bind-mounted read-only into every server container, so authoring a hook
// is a single file edit + `docker compose restart` of the servers — no image
// rebuild. A DIRECTORY mount, not a file mount: editors that write via rename would
// leave a file mount pinned to the stale inode.
const SHARED_DIR = 'ws-shared'
const SHARED_MOUNT = `./${SHARED_DIR}:/app/shared:ro`

// One websocket tier per system today, so the ports are fixed. The registry records
// them anyway so a multi-tier port allocator can come later without a shape change.
const HOST_PORT = 8090 // host -> lb (the existing nginx lb owns 8080)
const WS_PORT = 8080 // lb -> servers + in-container ws listener (never published)
const STATS_PORT = 8405 // haproxy's own prometheus exporter (scraped over the docker net)
const METRICS_PORT = 9100 // each server's prom-client /metrics

const ALGORITHMS = ['leastconn', 'roundrobin', 'source']
const RESULT_SENTINEL = '__WS_RESULTS__'

// Pool-run bounds: each pool client is one host fd, and the macOS default soft
// ulimit is often 256 — cap well under it (bigger pools: see the sandbox-websocket
// skill's ulimit note).
const MAX_POOL = 200
const MAX_DURATION_S = 120
const MAX_RATE = 20

// The pool client's BUILT-IN methods — a static descriptor, not a registry: the
// script is generated from one template, so its behavior is fixed. Served on
// GET /api/websockets so the UI has one source of truth for names/args/defaults.
// Neither method is editable or deletable, and neither can be run from the UI —
// only end-to-end processes invoke them (via their websocket pool rows).
const CLIENT_METHODS = [
  {
    name: 'spawnAndSend',
    builtin: true,
    summary:
      'spawn N pool clients that connect through the L4 load balancer; each sends messages to random peers at the given rate for the duration, then reports delivery stats',
    args: [
      { name: 'count', type: 'number', default: 5, min: 1, max: MAX_POOL },
      { name: 'durationSeconds', type: 'number', default: 10, min: 1, max: MAX_DURATION_S },
      { name: 'rate', type: 'number', default: 1, min: 1, max: MAX_RATE },
    ],
  },
  {
    name: 'onReceive',
    builtin: true,
    summary:
      "the 'message' handler: dedupes by msgId (a repeat only bumps the duplicates counter), records the delivery, and measures latency from the sender's sentAt timestamp",
    args: [{ name: 'message', type: 'json' }],
  },
]

// The two SHARED server methods every relay runs from ws-shared/hooks.js: onMessage
// (a frame is received from a client) and onSend (a payload is delivered back to a
// client). The BASE behavior lives in server.js and is fixed — description entries
// only ADD side effects, and the hook code is authored by a launched Claude session
// (sandbox-websocket skill). This plugin does the mechanical registry half; Claude
// owns `implemented` (set true after it writes the hook + restarts the servers).
const METHOD_NAMES = ['onMessage', 'onSend']
const MAX_METHOD_TEXT = 4000
const BASE_METHOD_TEXT = {
  onMessage:
    'Base (fixed): a client frame arrives; ws_messages_received_total is bumped, the frame is parsed and routed to msg.to — delivered directly when the target is connected to this server, otherwise looked up in the presence cache and published to the owning server\'s bus channel; unknown/offline targets are dropped.',
  onSend:
    'Base (fixed): a payload is delivered to a locally connected client over its websocket (locally-routed AND bus-arriving frames both funnel through here), bumping the delivered/latency metrics; a gone connection means the message is dropped.',
}
function defaultMethods(now = new Date().toISOString()) {
  return Object.fromEntries(
    METHOD_NAMES.map((m) => [
      m,
      { base: BASE_METHOD_TEXT[m], entries: [], implemented: true, conversationId: '', updatedAt: now },
    ]),
  )
}

// Health rule shared by every node: red when the target is down, green up.
const HEALTH_RULES = [
  { color: 'red', when: 'value < 1' },
  { color: 'green', when: 'value >= 1' },
]

function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
}

// The host-run client pool script (ws twin of clientScript.js' clients/<module>.py —
// exported so remove.js can clean it up on tier delete). The script leaves its last
// run's report next to itself as <id>.stats.json (see the template's finish()).
export function wsClientScriptPath(system, id) {
  return path.join(systemDir(system), 'ws-clients', `${id}.mjs`)
}
export function wsClientStatsPath(system, id) {
  return path.join(systemDir(system), 'ws-clients', `${id}.stats.json`)
}
function readClientStats(system, id) {
  try {
    return JSON.parse(fs.readFileSync(wsClientStatsPath(system, id), 'utf8'))
  } catch {
    return null
  }
}
export function removeWsClientScript(system, id) {
  try {
    fs.rmSync(wsClientScriptPath(system, id), { force: true })
    // stats too, or the empty-dir check below never fires and a recreated
    // same-name tier would resurrect a stale last-run report
    fs.rmSync(wsClientStatsPath(system, id), { force: true })
    const dir = path.join(systemDir(system), 'ws-clients')
    if (fs.existsSync(dir) && fs.readdirSync(dir).length === 0) fs.rmdirSync(dir)
  } catch {
    /* nothing to clean up */
  }
}

// ---------------------------------------------------------------------------
// The tier registry: systems/<id>/<lb>/websockets.json — the durable source of
// truth haproxy.cfg is rendered from.
// ---------------------------------------------------------------------------

function registryPath(system, lbId) {
  return path.join(systemDir(system), lbId, 'websockets.json')
}
export function readTierRegistry(system, lbId) {
  try {
    return JSON.parse(fs.readFileSync(registryPath(system, lbId), 'utf8'))
  } catch {
    return null
  }
}
function writeTierRegistry(system, reg) {
  fs.writeFileSync(registryPath(system, reg.lb), JSON.stringify(reg, null, 2) + '\n')
}

// The shared hooks file all servers mount — created with the tier, backfilled for
// tiers that predate shared methods, never clobbered once it has authored code.
function ensureSharedHooksFile(system) {
  const dir = path.join(systemDir(system), SHARED_DIR)
  fs.mkdirSync(dir, { recursive: true })
  const dest = path.join(dir, 'hooks.js')
  if (!fs.existsSync(dest)) fs.copyFileSync(path.join(TEMPLATE_DIR, 'shared', 'hooks.js'), dest)
}

// haproxy.cfg is rendered from the registry (never hand-edited server lines): an L4
// (mode tcp) frontend on the ws port, the selectable balance algorithm, one plain
// tcp-connect health-checked `server` line per relay, and haproxy's native
// prometheus exporter on its own HTTP frontend (never in the tcp path). Timeouts
// must outlive the servers' 30s heartbeat or idle-but-alive connections get reaped.
// The `resolvers docker` section makes haproxy re-resolve server hostnames through
// Docker's embedded DNS at runtime — without it, hostnames resolve once at startup,
// so any recreated server container (whose IP changed) stays DOWN until an lb
// restart. `init-addr libc,none` lets the lb boot even if a server isn't up yet.
function renderHaproxyCfg(reg) {
  const serverLines = reg.servers
    .map((s) => `    server ${s} ${s}:${reg.wsPort} check resolvers docker init-addr libc,none`)
    .join('\n')
  return `# L4 (tcp) load balancer for websocket tier "${reg.lb}" — generated by Add WebSockets.
# Regenerated from websockets.json when the tier changes; do not hand-edit the server lines.
global
    log stdout format raw local0

resolvers docker
    nameserver dns1 127.0.0.11:53
    resolve_retries 3
    timeout resolve 1s
    timeout retry   1s
    hold valid      10s
    hold obsolete   10s

defaults
    mode tcp
    log global
    timeout connect 5s
    timeout client  75s
    timeout server  75s
    timeout tunnel  1h

frontend ws_in
    bind *:${reg.wsPort}
    default_backend ws_servers

backend ws_servers
    balance ${reg.algorithm}
    option tcp-check
${serverLines}

# haproxy's built-in prometheus exporter, on its own HTTP frontend.
frontend stats
    mode http
    bind *:${reg.statsPort}
    http-request use-service prometheus-exporter if { path /metrics }
`
}
function writeHaproxyCfg(system, reg) {
  fs.writeFileSync(path.join(systemDir(system), reg.lb, 'haproxy.cfg'), renderHaproxyCfg(reg))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

function validate(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)

  const name = typeof body.name === 'string' ? body.name : ''
  // <name>-server-N etc. must stay comfortably inside NAME_RE's practical 40-char cap.
  if (!NAME_RE.test(name) || name.length > 20) {
    throw bad('name must be lowercase letters, digits and hyphens (start with a letter, max 20 chars)')
  }

  const servers = body.servers === undefined ? 2 : Number(body.servers)
  if (!Number.isInteger(servers) || servers < 1 || servers > 8) {
    throw bad('servers must be a whole number between 1 and 8')
  }

  const algorithm = body.algorithm === undefined ? 'leastconn' : body.algorithm
  if (!ALGORITHMS.includes(algorithm)) {
    throw bad(`algorithm must be one of: ${ALGORITHMS.join(', ')}`)
  }

  // Forward-compat selectors: redis is the only pub/sub + presence engine today.
  const busEngine = body.bus === undefined ? 'redis' : body.bus
  const presenceEngine = body.presence === undefined ? 'redis' : body.presence
  if (busEngine !== 'redis') throw bad('redis is the only pub/sub engine available today')
  if (presenceEngine !== 'redis') throw bad('redis is the only presence store available today')

  const manifest = readManifest(system)
  if (manifest.nodes.some((n) => n.origin === 'create-websockets')) {
    throw bad('this system already has a websocket tier — delete its load balancer first')
  }

  const ids = {
    lb: `${name}-lb`,
    servers: Array.from({ length: servers }, (_, i) => `${name}-server-${i + 1}`),
    bus: `${name}-bus`,
    presence: `${name}-presence`,
    client: `${name}-client`,
  }
  const all = [ids.lb, ...ids.servers, ids.bus, ids.presence, ids.client]

  // Primary ids are manifest-checked; exporter names are derived, so also check the
  // compose doc directly (guards against a hand-added compose-only service).
  const composeSrc = fs.readFileSync(path.join(systemDir(system), 'docker-compose.yml'), 'utf8')
  for (const id of all) {
    if (manifest.nodes.some((n) => n.id === id)) {
      throw bad(`a node named "${id}" already exists in this system`)
    }
    if (fs.existsSync(path.join(systemDir(system), id))) {
      throw bad(`systems/${system}/${id}/ already exists`)
    }
    for (const svc of [id, `${id}-exporter`]) {
      if (new RegExp(`^  ${svc}:`, 'm').test(composeSrc)) {
        throw bad(`a compose service named "${svc}" already exists in this system`)
      }
    }
  }

  return { system, name, servers, algorithm, manifest, ids }
}

// ---------------------------------------------------------------------------
// Build bundle (compose services + scrape jobs), mirroring buildKafka/buildRedis
// ---------------------------------------------------------------------------

// A tier redis (bus or presence): engine + exporter, no `-init` seeder — the
// servers create every key/channel they need at runtime.
function wsRedisServices(name) {
  return {
    [name]: { image: 'redis:7-alpine' },
    [`${name}-exporter`]: {
      image: 'oliver006/redis_exporter:v1.62.0',
      environment: { REDIS_ADDR: `redis://${name}:6379` },
      depends_on: [name],
    },
  }
}

function buildTier(ids, algorithm) {
  const services = {
    [ids.lb]: {
      image: 'haproxy:3.0-alpine',
      ports: [`${HOST_PORT}:${WS_PORT}`],
      volumes: [`./${ids.lb}/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro`],
      depends_on: [...ids.servers],
    },
  }
  for (const sid of ids.servers) {
    services[sid] = {
      build: `./${sid}`,
      environment: {
        SERVER_ID: sid,
        WS_PORT: WS_PORT,
        METRICS_PORT: METRICS_PORT,
        BUS_REDIS_URL: `redis://${ids.bus}:6379`,
        PRESENCE_REDIS_URL: `redis://${ids.presence}:6379`,
        HEARTBEAT_MS: 30000,
        PRESENCE_TTL_S: 60,
      },
      // every server mounts the tier's ONE shared hooks file (see SHARED_DIR)
      volumes: [SHARED_MOUNT],
      depends_on: [ids.bus, ids.presence],
      // depends_on only waits for the redis CONTAINERS to start, not for redis to
      // accept connections — a relay that exhausts its connect retries exits, so
      // restart until the bus/presence are reachable (same pattern as the kafka
      // exporter in eventstreams.js).
      restart: 'unless-stopped',
    }
  }
  Object.assign(services, wsRedisServices(ids.bus), wsRedisServices(ids.presence))

  const scrapeJobs = [
    { job_name: ids.lb, metrics_path: '/metrics', static_configs: [{ targets: [`${ids.lb}:${STATS_PORT}`] }] },
    ...ids.servers.map((sid) => ({ job_name: sid, static_configs: [{ targets: [`${sid}:${METRICS_PORT}`] }] })),
    { job_name: ids.bus, static_configs: [{ targets: [`${ids.bus}-exporter:9121`] }] },
    { job_name: ids.presence, static_configs: [{ targets: [`${ids.presence}-exporter:9121`] }] },
  ]

  return { services, scrapeJobs, algorithm }
}

// ---------------------------------------------------------------------------
// Manifest nodes + edges
// ---------------------------------------------------------------------------

function redisMetrics(name) {
  return [
    { label: 'clients', query: `redis_connected_clients{job="${name}"}`, unit: '' },
    { label: 'ops/s', query: `sum(rate(redis_commands_processed_total{job="${name}"}[1m]))`, unit: '/s' },
    { label: 'keys', query: `sum(redis_db_keys{job="${name}"})`, unit: '' },
  ]
}

function tierNodes(manifest, ids) {
  // Tier-owned layout (the generic grid packs 180px rows; this tier reads better as
  // columns): lb -> servers column -> redis column, dropped below existing internal
  // nodes; the client goes in the standard external-left client column.
  const internals = manifest.nodes.filter((n) => !n.external)
  const baseY = Math.max(0, ...internals.map((n) => n.position?.y || 0)) + 220
  const upHealth = (name) => ({ query: `up{job="${name}"}`, rules: HEALTH_RULES })

  const nodes = []
  nodes.push({
    id: ids.lb,
    label: ids.lb,
    type: 'websocket-lb',
    origin: 'create-websockets',
    wsRole: 'lb',
    position: { x: 80, y: baseY + Math.floor(((ids.servers.length - 1) * 180) / 2) },
    metrics: [
      { label: 'sessions', query: `sum(haproxy_backend_current_sessions{job="${ids.lb}",proxy="ws_servers"}) or vector(0)`, unit: '' },
      { label: 'conns/s', query: `sum(rate(haproxy_frontend_connections_total{job="${ids.lb}",proxy="ws_in"}[1m])) or vector(0)`, unit: '/s' },
      { label: 'servers up', query: `count(haproxy_server_status{job="${ids.lb}",proxy="ws_servers",state="UP"} == 1) or vector(0)`, unit: '' },
    ],
    health: upHealth(ids.lb),
  })
  ids.servers.forEach((sid, i) => {
    nodes.push({
      id: sid,
      label: sid,
      type: 'websocket-server',
      origin: 'create-websockets',
      wsRole: 'server',
      wsTier: ids.lb,
      position: { x: 380, y: baseY + i * 180 },
      metrics: [
        { label: 'ws conns', query: `ws_connections{job="${sid}"}`, unit: '' },
        { label: 'lb sessions', query: `haproxy_server_current_sessions{job="${ids.lb}",proxy="ws_servers",server="${sid}"}`, unit: '' },
        { label: 'msgs in/s', query: `sum(rate(ws_messages_received_total{job="${sid}"}[1m])) or vector(0)`, unit: '/s' },
        { label: 'local/s', query: `sum(rate(ws_messages_delivered_local_total{job="${sid}"}[1m])) or vector(0)`, unit: '/s' },
        { label: 'remote/s', query: `sum(rate(ws_messages_routed_remote_total{job="${sid}"}[1m])) or vector(0)`, unit: '/s' },
      ],
      health: upHealth(sid),
    })
  })
  nodes.push({
    id: ids.bus,
    label: ids.bus,
    type: 'redis',
    origin: 'create-websockets',
    wsRole: 'bus',
    wsTier: ids.lb,
    position: { x: 680, y: baseY },
    metrics: redisMetrics(ids.bus),
    health: { query: `redis_up{job="${ids.bus}"}`, rules: HEALTH_RULES },
  })
  nodes.push({
    id: ids.presence,
    label: ids.presence,
    type: 'redis',
    origin: 'create-websockets',
    wsRole: 'presence',
    wsTier: ids.lb,
    position: { x: 680, y: baseY + 180 },
    metrics: redisMetrics(ids.presence),
    health: { query: `redis_up{job="${ids.presence}"}`, rules: HEALTH_RULES },
  })
  nodes.push({
    id: ids.client,
    label: ids.client,
    type: 'client',
    origin: 'create-websockets',
    wsRole: 'client',
    wsTier: ids.lb,
    external: true,
    position: nextClientPosition(manifest),
    metrics: [],
  })

  const edges = [
    { from: ids.client, to: ids.lb },
    ...ids.servers.map((sid) => ({ from: ids.lb, to: sid })),
    ...ids.servers.map((sid) => ({ from: sid, to: ids.bus })),
    ...ids.servers.map((sid) => ({ from: sid, to: ids.presence })),
  ]
  return { nodes, edges }
}

// ---------------------------------------------------------------------------
// Rebuild (frontend-safe — NEVER ./start.sh)
// ---------------------------------------------------------------------------

async function rebuild(system, buildNames) {
  // Escape hatch for tests/CI: validate file generation without building images.
  if (process.env.WEBSOCKETS_SKIP_REBUILD === '1') return '(rebuild skipped)'

  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  // 600s: the first server build pulls node:22-alpine and npm-installs.
  const opts = { cwd: repoRoot, timeout: 600_000, maxBuffer: 16 * 1024 * 1024 }
  let log = ''
  try {
    // Build ONLY the new server images (a bare `up -d --build` would rebuild every
    // build: service in the system), then `up -d` creates the new containers and
    // leaves the rest running. No nginx reload — the tier has no nginx route.
    const b = await pexec('docker', ['compose', '-f', compose, 'build', ...buildNames], opts)
    log += b.stdout + b.stderr
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

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

export async function handleCreate(body) {
  const { system, servers, algorithm, manifest, ids } = validate(body)

  // 1. the tier registry + rendered haproxy.cfg (in the lb's folder, like a
  //    cluster's streams.json — removed with the folder on tier delete)
  const reg = {
    lb: ids.lb,
    algorithm,
    hostPort: HOST_PORT,
    wsPort: WS_PORT,
    statsPort: STATS_PORT,
    metricsPort: METRICS_PORT,
    servers: [...ids.servers],
    bus: ids.bus,
    presence: ids.presence,
    client: ids.client,
    methods: defaultMethods(),
  }
  fs.mkdirSync(path.join(systemDir(system), ids.lb), { recursive: true })
  writeTierRegistry(system, reg)
  writeHaproxyCfg(system, reg)

  // 2. per-server folders from the template (identical files — per-instance
  //    identity is env-only, like download-coordinator workers), plus the ONE
  //    shared hooks file they all bind-mount
  ensureSharedHooksFile(system)
  for (const sid of ids.servers) {
    cloneTemplate(system, sid, path.join(TEMPLATE_DIR, 'server'), SERVER_FILES)
  }

  // 3. compose + prometheus (comment-preserving)
  const built = buildTier(ids, algorithm)
  addComposeServices(system, built.services, ids.lb, 'WebSocket tier', 'Add WebSockets')
  for (const job of built.scrapeJobs) {
    addScrapeJob(system, job, job.job_name, 'Add WebSockets', 'WebSocket tier')
  }

  // 4. manifest nodes + edges, one write
  const { nodes, edges } = tierNodes(manifest, ids)
  manifest.nodes.push(...nodes)
  manifest.edges = [...(manifest.edges || []), ...edges]
  writeManifest(system, manifest)

  // 5. the host-run client pool script
  const tmpl = fs.readFileSync(path.join(TEMPLATE_DIR, 'client.mjs.tmpl'), 'utf8')
  const filled = tmpl
    .split('__CLIENT__').join(ids.client)
    .split('__WS_URL__').join(`ws://localhost:${HOST_PORT}`)
  fs.mkdirSync(path.join(systemDir(system), 'ws-clients'), { recursive: true })
  fs.writeFileSync(wsClientScriptPath(system, ids.client), filled)
  // a hand-deleted tier can leave a same-name stats file behind — a fresh tier
  // must not start life showing another tier's last run
  fs.rmSync(wsClientStatsPath(system, ids.client), { force: true })

  // 6. rebuild (frontend-safe)
  const log = await rebuild(system, ids.servers)
  return { ok: true, nodes, log }
}

// ---------------------------------------------------------------------------
// Read: the system's tier (or null)
// ---------------------------------------------------------------------------

function getTier(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const lb = manifest.nodes.find((n) => n.origin === 'create-websockets' && n.wsRole === 'lb')
  if (!lb) return { ok: true, tier: null }
  const tier = readTierRegistry(system, lb.id)
  // Tiers created before shared methods existed: default the block in the RESPONSE
  // only (no write on GET) so the UI always sees both methods.
  if (tier && !tier.methods) tier.methods = defaultMethods()
  const clientId =
    tier?.client ||
    manifest.nodes.find((n) => n.origin === 'create-websockets' && n.wsRole === 'client')?.id
  return {
    ok: true,
    tier,
    stats: clientId ? readClientStats(system, clientId) : null,
    clientMethods: CLIENT_METHODS,
  }
}

// ---------------------------------------------------------------------------
// Shared methods: append a description entry (the mechanical half — the hook
// code itself is authored by a launched Claude session, like consumers.js)
// ---------------------------------------------------------------------------

// A tier created before shared methods existed lacks ws-shared/ and the compose
// mounts — backfill both, idempotently and comment-preserving. server.js itself is
// NOT patched mechanically (it may be hand-customized); the launched session adds
// the hook loader per the skill's pre-migration escape when needed.
function ensureSharedScaffold(system, reg) {
  ensureSharedHooksFile(system)
  const file = path.join(systemDir(system), 'docker-compose.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  let changed = false
  for (const sid of reg.servers) {
    if (!doc.hasIn(['services', sid])) continue
    const volumes = doc.getIn(['services', sid, 'volumes'])
    if (volumes?.items?.some((i) => String(i?.value ?? i) === SHARED_MOUNT)) continue
    if (volumes) volumes.add(doc.createNode(SHARED_MOUNT))
    else doc.setIn(['services', sid, 'volumes'], doc.createNode([SHARED_MOUNT]))
    changed = true
  }
  if (changed) fs.writeFileSync(file, doc.toString())
}

function addMethodEntry(body) {
  const { system, method, conversationId } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  // whitelist (also keeps arbitrary keys out of the registry object)
  if (!METHOD_NAMES.includes(method)) {
    throw bad(`method must be one of: ${METHOD_NAMES.join(', ')}`)
  }
  const text = typeof body.text === 'string' ? body.text.trim() : ''
  if (!text) throw bad('text is required — describe the behavior to add')
  if (text.length > MAX_METHOD_TEXT) {
    throw bad(`text must be at most ${MAX_METHOD_TEXT} characters`)
  }

  const manifest = readManifest(system)
  const lb = manifest.nodes.find((n) => n.origin === 'create-websockets' && n.wsRole === 'lb')
  if (!lb) throw bad(`system "${system}" has no websocket tier`)
  const reg = readTierRegistry(system, lb.id)
  if (!reg) throw bad(`websocket tier registry for "${lb.id}" is missing`)

  if (!reg.methods) reg.methods = defaultMethods() // lazy-migrate older registries
  const m = reg.methods[method]
  const now = new Date().toISOString()
  m.entries = [...(m.entries || []), { at: now, text }]
  m.implemented = false // Claude owns this; set true after it writes the hook + restarts
  m.conversationId = conversationId || m.conversationId || ''
  m.updatedAt = now

  ensureSharedScaffold(system, reg)
  writeTierRegistry(system, reg)
  return { ok: true, lb: reg.lb, method, methods: reg.methods }
}

// ---------------------------------------------------------------------------
// Run a client pool (the ws twin of scenarios.js runFunction)
// ---------------------------------------------------------------------------

function lastLine(text) {
  const lines = String(text || '')
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
  return lines[lines.length - 1] || ''
}

function parseRunResults(stdout) {
  const lines = String(stdout || '').split('\n')
  for (let i = lines.length - 1; i >= 0; i--) {
    if (lines[i].startsWith(RESULT_SENTINEL + ' ')) {
      try {
        return JSON.parse(lines[i].slice(RESULT_SENTINEL.length + 1))
      } catch {
        return null
      }
    }
  }
  return null
}

async function runPool(body) {
  const { system, client } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = manifest.nodes.find(
    (n) => n.id === client && n.type === 'client' && n.origin === 'create-websockets',
  )
  if (!node) throw bad(`"${client}" is not a websocket client in this system`)

  const count = body.count === undefined ? 5 : Number(body.count)
  if (!Number.isInteger(count) || count < 1 || count > MAX_POOL) {
    throw bad(`count must be a whole number between 1 and ${MAX_POOL}`)
  }
  const duration = body.durationSeconds === undefined ? 10 : Number(body.durationSeconds)
  if (!Number.isInteger(duration) || duration < 1 || duration > MAX_DURATION_S) {
    throw bad(`durationSeconds must be a whole number between 1 and ${MAX_DURATION_S}`)
  }
  const rate = body.rate === undefined ? 1 : Number(body.rate)
  if (!Number.isInteger(rate) || rate < 1 || rate > MAX_RATE) {
    throw bad(`rate must be a whole number between 1 and ${MAX_RATE}`)
  }

  const scriptPath = wsClientScriptPath(system, client)
  if (!fs.existsSync(scriptPath)) {
    throw bad(`client "${client}" has no pool script (ws-clients/${client}.mjs is missing)`)
  }

  let stdout = ''
  let scriptError = null
  try {
    const r = await pexec(
      'node',
      [scriptPath, '--count', String(count), '--duration', String(duration), '--rate', String(rate)],
      // grace + duration + drain + backstop margin (see the template's timings)
      { cwd: systemDir(system), timeout: (duration + 30) * 1000, maxBuffer: 8 * 1024 * 1024 },
    )
    stdout = r.stdout || ''
  } catch (err) {
    if (err.code === 'ENOENT') {
      throw bad('node was not found on the host — install Node.js 22+ to run websocket client pools')
    }
    stdout = err.stdout || ''
    scriptError = err.killed ? 'the pool script timed out' : lastLine(err.stderr) || err.message
  }

  const results = parseRunResults(stdout)
  if (results == null) {
    throw bad(scriptError || 'the pool script produced no results — check ws-clients/' + client + '.mjs')
  }
  return { ok: true, results }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

export default function websockets() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  return {
    name: 'websockets',
    configureServer(server) {
      server.middlewares.use('/api/websockets', async (req, res, next) => {
        try {
          const url = new URL(req.url, 'http://localhost')
          if (url.pathname === '/methods' && req.method === 'POST') {
            return json(res, 200, addMethodEntry(await readJsonBody(req)))
          }
          if (url.pathname === '/run' && req.method === 'POST') {
            return json(res, 200, await runPool(await readJsonBody(req)))
          }
          if (url.pathname === '/' || url.pathname === '') {
            if (req.method === 'GET') {
              return json(res, 200, getTier(url.searchParams.get('system')))
            }
            if (req.method === 'POST') {
              return json(res, 200, await handleCreate(await readJsonBody(req)))
            }
          }
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
