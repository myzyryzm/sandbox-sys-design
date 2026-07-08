// Shared scaffolding primitives for creating sandbox service nodes.
//
// Extracted from services.js so BOTH the generic "Add service" flow and the
// custom-service-type mechanism (customTypes/) build services through one set of
// primitives — composing, not forking. A custom service type's onAdd uses these to
// clone a template, splice docker-compose / nginx / prometheus, append a manifest
// node, and rebuild, exactly like a generic service — varying only which template it
// clones and the node's extra fields (service_type, grpc, role config).
//
// All edits are comment-preserving YAML / marker-based text splices, mirroring the
// originals so generated files keep reading like the hand-authored ones. The rebuild
// is the frontend-safe `docker compose` path — NEVER ./start.sh, which would tear
// down the dev server this code runs inside.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemsDir, systemDir } from './systems.js'

const pexec = promisify(execFile)

export class HttpError extends Error {
  constructor(status, message) {
    super(message)
    this.statusCode = status
  }
}
export const bad = (msg) => new HttpError(400, msg)

// A valid compose service name / nginx location / prometheus job — and a valid node id.
export const NAME_RE = /^[a-z][a-z0-9-]*$/

export function readJsonBody(req, limit = 100_000) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > limit) reject(bad('request body too large'))
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

// The generic FastAPI metric set (req/s, p95, in-flight, error%), scoped to a
// service's own scrape job. Any service built from the hand-instrumented template
// exports these counters, so generic services AND custom typed services reuse it.
export function serviceMetrics(name) {
  const j = `{job="${name}"}`
  return [
    { label: 'req/s', query: `sum(rate(http_requests_total${j}[1m]))`, unit: '/s' },
    { label: 'p95', query: `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket${j}[1m])) by (le)) * 1000`, unit: 'ms' },
    { label: 'in-flight', query: `sum(http_requests_in_flight${j})`, unit: '' },
    { label: 'errors', query: `(sum(rate(http_requests_total{job="${name}",status=~"5.."}[1m])) or vector(0)) / clamp_min(sum(rate(http_requests_total${j}[1m])), 0.0001)`, unit: '%', scale: 100 },
  ]
}

// The standard service health block: green when its scrape target is up, red when down.
export function serviceHealth(name) {
  return {
    query: `up{job="${name}"}`,
    rules: [
      { color: 'red', when: 'value < 1' },
      { color: 'green', when: 'value >= 1' },
    ],
  }
}

// Copy a fixed list of files from a template dir into systems/<id>/<name>/, returning
// the created service directory.
export function cloneTemplate(system, name, templateDir, files) {
  const destDir = path.join(systemDir(system), name)
  fs.mkdirSync(destDir, { recursive: true })
  for (const f of files) {
    fs.copyFileSync(path.join(templateDir, f), path.join(destDir, f))
  }
  return destDir
}

// Splice a compose service entry (comment-preserving). `service` is the YAML value —
// e.g. { build: './<name>' } for a generic service, or a richer object with
// volumes/environment for a typed service that needs durable state.
export function addComposeService(system, name, service, comment) {
  const file = path.join(systemDir(system), 'docker-compose.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const node = doc.createNode(service)
  if (comment) node.commentBefore = comment
  doc.setIn(['services', name], node)
  fs.writeFileSync(file, doc.toString())
}

export function addScrapeJob(system, name, port = 8000, comment) {
  const file = path.join(systemDir(system), 'prometheus', 'prometheus.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const node = doc.createNode({ job_name: name, static_configs: [{ targets: [`${name}:${port}`] }] })
  node.commentBefore = comment || ` Service "${name}"`
  doc.addIn(['scrape_configs'], node)
  fs.writeFileSync(file, doc.toString())
}

// Insert a `/<name>/` route into nginx.conf at the marker comments laid down by the
// base system. The upstream gives the replica seam; the location strips the prefix
// (trailing slash on proxy_pass) so /<name>/health -> the service's /health.
export function addNginxRoute(system, name, port = 8000) {
  const file = path.join(systemDir(system), 'nginx', 'nginx.conf')
  let conf = fs.readFileSync(file, 'utf8')

  const upstream = `    upstream ${name} { server ${name}:${port}; }\n    # === end upstreams ===`
  const location =
    `        location /${name}/ {\n` +
    `            proxy_pass http://${name}/;\n` +
    `            proxy_set_header Host              $host;\n` +
    `            proxy_set_header X-Real-IP         $remote_addr;\n` +
    `            proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;\n` +
    `            proxy_set_header X-Forwarded-Proto $scheme;\n` +
    `        }\n        # === end locations ===`

  if (!conf.includes('# === end upstreams ===') || !conf.includes('# === end locations ===')) {
    throw new HttpError(500, 'nginx.conf is missing the insertion markers')
  }
  conf = conf.replace('    # === end upstreams ===', upstream)
  conf = conf.replace('        # === end locations ===', location)
  fs.writeFileSync(file, conf)
}

// Internal-route blocking: deny EXTERNAL calls (those arrive through the lb) to endpoints
// the user marked internal, while leaving service-to-service calls untouched — those talk
// container-to-container (http://<service>:8000) and never traverse the lb. It's an nginx
// `map` that sets $internal_block=1 for "<METHOD> <uri>" pairs matching an internal route,
// plus a single server-level guard `if ($internal_block) { return 403; }`. The map + guard
// are injected once (idempotent) the first time a route is blocked; the rule lines between
// the `# === internal blocks ===` markers are regenerated wholesale from the registry, so
// the file always reflects exactly the current internal set.

// Escape regex metacharacters in a literal path segment.
const escapeReSeg = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

// One map line: a case-sensitive regex over "<METHOD> <lbPath>". A {param} segment becomes
// a single-segment wildcard and a trailing slash is tolerated, so /svc/items/{id} blocks
// GET /svc/items/42 and /svc/items/42/ alike.
function internalBlockLine(method, lbPath) {
  const re = lbPath
    .split('/')
    .map((seg) => (/^\{[^/]+\}$/.test(seg) ? '[^/]+' : escapeReSeg(seg)))
    .join('/')
  return `        "~^${method} ${re}/?$" 1;`
}

// The (empty) map scaffold, in http context. Carries its own rule markers.
function internalMapBlock() {
  return (
    '    # === internal route blocks: deny external (lb) calls to endpoints marked internal ===\n' +
    '    # (service-to-service calls bypass the lb, so they keep working)\n' +
    '    map "$request_method $uri" $internal_block {\n' +
    '        default 0;\n' +
    '        # === internal blocks ===\n' +
    '        # === end internal blocks ===\n' +
    '    }\n' +
    '    # === end internal route blocks ==='
  )
}

const INTERNAL_GUARD =
  '        # Deny external calls to routes marked internal (see the $internal_block map above).\n' +
  '        if ($internal_block) { return 403; }'

// Rewrite nginx.conf so exactly `rules` (an array of { method, lbPath }) are blocked at the
// lb. Injects the map + guard the first time (idempotent) and regenerates the rule lines.
// Returns true if the file content changed. Does NOT reload — call reloadNginx() after.
export function setInternalRoutes(system, rules) {
  const file = path.join(systemDir(system), 'nginx', 'nginx.conf')
  const before = fs.readFileSync(file, 'utf8')
  let conf = before

  // Inject the map scaffold once (after the upstreams) so $internal_block is defined.
  if (!conf.includes('# === internal route blocks')) {
    if (!conf.includes('# === end upstreams ===')) {
      throw new HttpError(500, 'nginx.conf is missing the upstreams marker')
    }
    conf = conf.replace('    # === end upstreams ===', (m) => `${m}\n\n${internalMapBlock()}`)
  }
  // Inject the guard once (right after `listen 80;`).
  if (!conf.includes('if ($internal_block)')) {
    if (!/\n[ \t]*listen[ \t]+80;/.test(conf)) {
      throw new HttpError(500, 'nginx.conf is missing the `listen 80;` line')
    }
    conf = conf.replace(/(\n[ \t]*listen[ \t]+80;)/, (m) => `${m}\n\n${INTERNAL_GUARD}`)
  }
  // Regenerate the rule lines between the internal-blocks markers (function replacement so
  // the `$` in the generated regexes is never treated as a replacement pattern).
  const body = rules.map((r) => internalBlockLine(r.method, r.lbPath)).join('\n')
  conf = conf.replace(
    /        # === internal blocks ===\n[\s\S]*?        # === end internal blocks ===/,
    () => `        # === internal blocks ===\n${body ? body + '\n' : ''}        # === end internal blocks ===`,
  )

  if (conf === before) return false
  fs.writeFileSync(file, conf)
  return true
}

// Reload the lb's nginx config (no image rebuild) — used after an internal-route change.
// Recreates the lb rather than `nginx -s reload`: nginx.conf is a single-file bind mount,
// and on macOS Docker Desktop the container pins a stale inode, so a reload would re-read
// the OLD config and the change would silently not take effect. Recreating re-resolves the
// mount to the current file; `nginx -t` then validates the fresh container and surfaces
// nginx's own output if the new config is invalid.
export async function reloadNginx(system) {
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 60_000, maxBuffer: 16 * 1024 * 1024 }
  const run = (...args) => pexec('docker', ['compose', '-f', compose, ...args], opts)
  try {
    const up = await run('up', '-d', '--force-recreate', 'lb')
    const t = await run('exec', '-T', 'lb', 'nginx', '-t')
    return up.stdout + up.stderr + t.stdout + t.stderr
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `nginx reload failed:\n${detail}`)
  }
}

// Append a fully-formed node object to manifest.json and persist. The caller builds
// the node (id/label/type/origin/metrics/health + any extra fields like service_type,
// grpc, role config), so this stays generic.
export function addManifestNode(system, manifest, node) {
  manifest.nodes.push(node)
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
  return node
}

// ---------------------------------------------------------------------------
// compose / prometheus DOCUMENT splices (comment-preserving)
//
// Lower-level than addComposeService/addScrapeJob: these operate on a parsed `yaml`
// Document so a caller making SEVERAL edits at once (add N instances, drop one, repoint
// a target) parses + writes the file a single time. addComposeService/addScrapeJob stay
// the single-edit convenience wrappers. Shared by serviceLb.js (haproxy clusters) and
// customTypes/llmWorker.js (worker replica groups) — don't fork these; compose them.
// ---------------------------------------------------------------------------

export const composePath = (system) => path.join(systemDir(system), 'docker-compose.yml')
export const prometheusPath = (system) => path.join(systemDir(system), 'prometheus', 'prometheus.yml')

export function loadCompose(system) {
  return parseDocument(fs.readFileSync(composePath(system), 'utf8'))
}
export function saveCompose(system, doc) {
  fs.writeFileSync(composePath(system), doc.toString())
}

// The plain-JS form of an existing compose service (so a caller can clone its
// build/env/volumes onto sibling services), or null if it isn't defined.
export function composeServiceDef(doc, name) {
  const node = doc.getIn(['services', name])
  return node ? node.toJSON() : null
}

export function setComposeService(doc, name, def, comment) {
  const node = doc.createNode(def)
  if (comment) node.commentBefore = comment
  doc.setIn(['services', name], node)
}

export function removeComposeService(doc, name) {
  if (doc.hasIn(['services', name])) doc.deleteIn(['services', name])
  // Scrub dangling depends_on references to the removed service (list or map form).
  const services = doc.get('services')
  for (const pair of services?.items || []) {
    const dep = pair.value?.get?.('depends_on')
    if (!dep?.items) continue
    for (let i = dep.items.length - 1; i >= 0; i--) {
      const it = dep.items[i]
      const n = it?.key !== undefined ? String(it.key.value ?? it.key) : String(it.value ?? it)
      if (n === name) dep.items.splice(i, 1)
    }
    if (dep.items.length === 0) pair.value.delete('depends_on')
  }
}

export function loadPrometheus(system) {
  return parseDocument(fs.readFileSync(prometheusPath(system), 'utf8'))
}
export function savePrometheus(system, doc) {
  fs.writeFileSync(prometheusPath(system), doc.toString())
}
export function findScrapeJob(doc, jobName) {
  const sc = doc.get('scrape_configs')
  return sc?.items?.findIndex((it) => String(it.get('job_name')) === jobName) ?? -1
}
export function removeScrapeJobDoc(doc, jobName) {
  const sc = doc.get('scrape_configs')
  const i = findScrapeJob(doc, jobName)
  if (i >= 0) sc.delete(i)
}
export function addScrapeJobDoc(doc, jobName, target, comment) {
  const node = doc.createNode({ job_name: jobName, static_configs: [{ targets: [target] }] })
  if (comment) node.commentBefore = comment
  doc.addIn(['scrape_configs'], node)
}
// Repoint an existing job's single target (e.g. move `<name>` between its FastAPI port
// and an exporter port).
export function setScrapeJobTarget(doc, jobName, target) {
  const i = findScrapeJob(doc, jobName)
  if (i < 0) return
  doc.setIn(['scrape_configs', i, 'static_configs', 0, 'targets', 0], target)
}

// A load-balanced service's code lives in its instances (<name>-1..N), not in <name>
// itself — which is now an haproxy sidecar with no build context. So `rebuild(system,
// '<name>')` (called by every per-service flow: endpoint, grpc, …) must build the
// INSTANCES and recreate the sidecar, not try to `docker compose build <name>` (which
// would fail on the image-only haproxy service). A plain service resolves to itself.
//
// A worker replica group (customTypes/llmWorker.js — N instances sharing one id with NO
// load balancer) is the mirror image: `<name>` IS a real serving container, and its
// instances `<name>-2..N` all `build: ./<name>`, so a per-service edit must rebuild the
// base AND every instance (no sidecar to recreate).
function resolveBuildTargets(system, name) {
  try {
    const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
    const node = manifest.nodes.find((n) => n.id === name)
    if (node?.svcLb?.instances?.length) {
      return { buildNames: [...node.svcLb.instances], recreate: [name] }
    }
    if (node?.replicas?.instances?.length) {
      return { buildNames: [name, ...node.replicas.instances], recreate: [] }
    }
  } catch {
    /* fall back to the plain single-service path */
  }
  return { buildNames: [name], recreate: [] }
}

// Frontend-safe rebuild: build just <name> (or, for a load-balanced service, its
// instances), bring the stack up (creating the new container, leaving the rest
// running), recreate the lb so it picks up the new /<name>/ route, and restart
// prometheus so the appended scrape job is picked up. NEVER ./start.sh.
export async function rebuild(system, name) {
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
  const { buildNames, recreate } = resolveBuildTargets(system, name)
  let log = ''
  try {
    const b = await pexec('docker', ['compose', '-f', compose, 'build', ...buildNames], opts)
    log += b.stdout + b.stderr
    const up = await pexec('docker', ['compose', '-f', compose, 'up', '-d'], opts)
    log += up.stdout + up.stderr
    // Recreate a load-balanced service's sidecar so it re-reads its config and points
    // at freshly-built instances.
    for (const svc of recreate) {
      const rc = await pexec('docker', ['compose', '-f', compose, 'up', '-d', '--force-recreate', svc], opts)
      log += rc.stdout + rc.stderr
    }
    // Recreate the lb rather than `nginx -s reload`: nginx.conf is a single-file bind
    // mount, and on macOS Docker Desktop the container pins a stale inode, so a reload
    // re-reads the OLD config (route never lands, or worse, a truncated file fails to
    // load). Recreating re-resolves the mount to the current nginx.conf; `nginx -t`
    // then confirms the fresh container came up with a valid config (and surfaces the
    // error if it didn't).
    const ng = await pexec('docker', ['compose', '-f', compose, 'up', '-d', '--force-recreate', 'lb'], opts)
    log += ng.stdout + ng.stderr
    const ngt = await pexec('docker', ['compose', '-f', compose, 'exec', '-T', 'lb', 'nginx', '-t'], opts)
    log += ngt.stdout + ngt.stderr
    const r = await pexec('docker', ['compose', '-f', compose, 'restart', 'prometheus'], opts)
    log += r.stdout + r.stderr
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
  return log
}
