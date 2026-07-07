// Vite dev-server plugin: discover the live, routable endpoints of a system.
//
//   GET /api/endpoints?system=<id>
//     -> { ok, endpoints: [{ service, method, path, protocol, downstream }] }
//
// Each service is reachable through the LB at /<service-id>/, so we read its
// FastAPI /openapi.json through the LB (localhost:8080) and prefix every path
// with the service id. The result is exactly what the load balancer can route —
// e.g. { service: "service-1", method: "GET", path: "/service-1/health" }.
//
// /metrics is omitted (that's Prometheus's, scraped directly, not an LB route);
// /health and any custom endpoints are included.
//
// On top of live discovery we merge per-endpoint metadata from an optional
// per-system registry, systems/<id>/endpoints.json (a map of service id -> list
// of { method, path, protocol, downstream, ... }). That registry is the canonical
// home for endpoint facts the running container's OpenAPI can't carry — most
// importantly `downstream` (the node ids this endpoint calls), which drives the
// lifecycle trace on the diagram. Registry endpoints not yet served by the
// container still surface here (e.g. an endpoint whose implementation is pending).
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'
import { setInternalRoutes, reloadNginx } from './scaffold.js'
import { renameStepPaths } from './scenarios.js'

const LB_BASE = 'http://localhost:8080'
const HTTP_METHODS = new Set(['get', 'post', 'put', 'patch', 'delete', 'head', 'options'])
const SKIP_PATHS = new Set(['/metrics'])
const DEFAULT_PROTOCOL = 'http'

// Nodes that host HTTP endpoints: in-system services and external services (the
// latter simulate third-party APIs our services call out to). Both are reachable
// through the LB at /<id>/ and own an app.py, so the discover/add/remove flow is
// identical — the only difference is that an external service's endpoints are kept
// off the LB's advertised surface (see endpointPolicy on the frontend).
// A load-balanced service keeps owning its endpoints under its `<name>` id even after
// it becomes the cluster entry (`type:'service-lb'`): `/<name>/…` still routes through
// the haproxy sidecar to a real instance, so discovery + add/edit/delete work exactly
// as before. Its instances (`instanceOf` set) are NOT independent endpoint hosts —
// they serve the same routes but are never addressed individually.
const ENDPOINT_HOST_TYPES = new Set(['service', 'external_service', 'service-lb'])
const isEndpointHost = (n) => ENDPOINT_HOST_TYPES.has(n.type) && !n.instanceOf

async function fetchJson(url, ms = 2500) {
  const ctrl = new AbortController()
  const t = setTimeout(() => ctrl.abort(), ms)
  try {
    const res = await fetch(url, { signal: ctrl.signal })
    if (!res.ok) return null
    return await res.json()
  } catch {
    return null
  } finally {
    clearTimeout(t)
  }
}

// Load the optional endpoint registry, tolerating an absent or malformed file —
// a system without one simply has no extra metadata. Returns a map keyed
// `${service} ${METHOD} ${localPath}` -> record, for O(1) lookup during merge.
function loadRegistry(system, nodeIds) {
  const file = path.join(systemDir(system), 'endpoints.json')
  let raw
  try {
    raw = JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return new Map()
  }
  const map = new Map()
  for (const [service, list] of Object.entries(raw || {})) {
    if (!Array.isArray(list)) continue
    for (const e of list) {
      if (!e || typeof e.method !== 'string' || typeof e.path !== 'string') continue
      const method = e.method.toUpperCase()
      const protocol = typeof e.protocol === 'string' ? e.protocol : DEFAULT_PROTOCOL
      // Only keep downstream ids that are real nodes in this system, so the
      // diagram never tries to draw a trace edge to a node that doesn't exist.
      const downstream = (Array.isArray(e.downstream) ? e.downstream : []).filter((d) =>
        nodeIds.has(d),
      )
      const isObj = (v) => v && typeof v === 'object' && !Array.isArray(v)
      // A brief, per-downstream connection description (node id -> text), authored by
      // Claude. Keep only entries for nodes still in `downstream` so a removed node
      // can't leave a stale label, and only string values.
      const dd = isObj(e.downstreamDescriptions) ? e.downstreamDescriptions : {}
      const downstreamDescriptions = Object.fromEntries(
        downstream.filter((d) => typeof dd[d] === 'string').map((d) => [d, dd[d]]),
      )
      // The specific methods this endpoint calls on each downstream node (node id ->
      // ["METHOD /path", …], service-local paths), authored by Claude. Keep only entries
      // for nodes still in `downstream`, with non-empty string call entries — this is what
      // lets the diagram light up the exact called method rows on the services it reaches.
      const dm = isObj(e.downstreamMethods) ? e.downstreamMethods : {}
      const downstreamMethods = Object.fromEntries(
        downstream
          .filter((d) => Array.isArray(dm[d]))
          .map((d) => [d, dm[d].filter((s) => typeof s === 'string' && s.trim()).map((s) => s.trim())])
          .filter(([, calls]) => calls.length),
      )
      // Carry the editable spec (request/response/description), the function-name
      // alias, the saved update history, and the Claude session id all the way
      // through to the frontend — the endpoint modal needs them to pre-fill the
      // edit form, list the alias, and show the read-only update history.
      map.set(`${service} ${method} ${e.path}`, {
        service,
        method,
        path: e.path,
        protocol,
        downstream,
        downstreamDescriptions,
        downstreamMethods,
        alias: typeof e.alias === 'string' ? e.alias : '',
        request: isObj(e.request) ? e.request : {},
        response: isObj(e.response) ? e.response : {},
        // Optional reference to a model in the bank (systems/<id>/models.json); when
        // set it supersedes the inline request/response schema for that field.
        requestModel: typeof e.requestModel === 'string' ? e.requestModel : '',
        responseModel: typeof e.responseModel === 'string' ? e.responseModel : '',
        description: typeof e.description === 'string' ? e.description : '',
        conversationId: typeof e.conversationId === 'string' ? e.conversationId : null,
        history: Array.isArray(e.history) ? e.history : [],
        // User-set "internal" flag: drop this route from the load balancer's advertised
        // surface (it stays served for service-to-service calls). Honored by
        // endpointPolicy on the frontend.
        internal: e.internal === true,
      })
    }
  }
  return map
}

async function discover(system) {
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const services = manifest.nodes.filter(isEndpointHost).map((n) => n.id)
  const nodeIds = new Set(manifest.nodes.map((n) => n.id))
  const registry = loadRegistry(system, nodeIds)

  const endpoints = []
  const seen = new Set() // `${service} ${METHOD} ${localPath}` — dedupe live vs registry-only
  await Promise.all(
    services.map(async (id) => {
      const spec = await fetchJson(`${LB_BASE}/${id}/openapi.json`)
      const paths = spec?.paths || {}
      for (const [p, item] of Object.entries(paths)) {
        if (SKIP_PATHS.has(p)) continue
        for (const method of Object.keys(item)) {
          if (!HTTP_METHODS.has(method.toLowerCase())) continue
          const m = method.toUpperCase()
          const key = `${id} ${m} ${p}`
          seen.add(key)
          const meta = registry.get(key)
          endpoints.push({
            service: id,
            method: m,
            path: `/${id}${p}`,
            protocol: meta?.protocol || DEFAULT_PROTOCOL,
            downstream: meta?.downstream || [],
            downstreamDescriptions: meta?.downstreamDescriptions || {},
            downstreamMethods: meta?.downstreamMethods || {},
            alias: meta?.alias || '',
            request: meta?.request || {},
            response: meta?.response || {},
            requestModel: meta?.requestModel || '',
            responseModel: meta?.responseModel || '',
            description: meta?.description || '',
            conversationId: meta?.conversationId || null,
            history: meta?.history || [],
            internal: meta?.internal === true,
            // Served by the running container right now — deleting only the
            // registry entry won't remove it (it's re-discovered here), so the
            // UI must rebuild the service to truly delete it.
            live: true,
          })
        }
      }
    }),
  )

  // Registry endpoints the container isn't serving yet (implementation pending)
  // still belong on the diagram, prefixed like the live ones.
  for (const [key, meta] of registry) {
    if (seen.has(key)) continue
    if (!services.includes(meta.service)) continue
    if (SKIP_PATHS.has(meta.path)) continue
    endpoints.push({
      service: meta.service,
      method: meta.method,
      path: `/${meta.service}${meta.path}`,
      protocol: meta.protocol,
      downstream: meta.downstream,
      downstreamDescriptions: meta.downstreamDescriptions,
      downstreamMethods: meta.downstreamMethods,
      alias: meta.alias,
      request: meta.request,
      response: meta.response,
      requestModel: meta.requestModel,
      responseModel: meta.responseModel,
      description: meta.description,
      conversationId: meta.conversationId,
      history: meta.history,
      internal: meta.internal === true,
      live: false, // pending: only in the registry, no running route yet
    })
  }

  endpoints.sort((a, b) => a.path.localeCompare(b.path) || a.method.localeCompare(b.method))
  return endpoints
}

// ---------------------------------------------------------------------------
// POST /api/endpoints — write/update an endpoint record in the registry.
//
// This is the persistence half of the "add endpoint" flow: the modal generates
// a `conversationId` (the Claude session id it's about to launch), POSTs the
// endpoint spec here, and we upsert it into systems/<id>/endpoints.json so the
// endpoint shows immediately (as a pending endpoint) and the session id survives
// even if the terminal is closed. Claude then implements the handler and fills
// `downstream`. No docker rebuild here — that's Claude's job.
// ---------------------------------------------------------------------------

class HttpError extends Error {
  constructor(status, message) {
    super(message)
    this.statusCode = status
  }
}
const bad = (msg) => new HttpError(400, msg)

const METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'])
const PROTOCOLS = new Set(['http', 'https'])
const PATH_RE = /^\/[A-Za-z0-9._~\-/{}]*$/ // braces allow path params, e.g. /items/{item_id}
// An endpoint's optional function-name alias — a code-style identifier. Unique
// within a service (two services may reuse the same name); see validateEndpoint.
const ALIAS_RE = /^[A-Za-z_][A-Za-z0-9_]*$/

// Read systems/<id>/endpoints.json as a raw { service: [records] } map, tolerating
// an absent/garbled file. Shared by the alias-uniqueness check and the upsert.
function readRegistryFile(system) {
  const file = path.join(systemDir(system), 'endpoints.json')
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8')) || {}
  } catch {
    return {}
  }
}

// Built-in operational routes that are never part of a service's external client
// surface and must never be edited or deleted through the endpoint API: /health
// (the diagram's liveness check) and /resilience/state (the resilience wrapper's
// in-memory read). Matched on the service-local path, with or without a trailing
// slash. Type-specific internal routes (e.g. a custom service type's control plane)
// are locked client-side via the endpointPolicy registry, not hardcoded here.
const PROTECTED_PATHS = new Set(['/health', '/health/', '/resilience/state', '/resilience/state/'])
const isProtectedPath = (p) => typeof p === 'string' && PROTECTED_PATHS.has(p)
const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > 200_000) reject(bad('request body too large'))
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

// A schema is an optional flat object mapping field name -> type name (strings).
function validateSchema(value, label) {
  if (value == null) return {}
  if (typeof value !== 'object' || Array.isArray(value)) throw bad(`${label} must be a JSON object`)
  const out = {}
  for (const [k, v] of Object.entries(value)) {
    if (typeof v !== 'string') throw bad(`${label}.${k} must be a string type name`)
    out[k] = v
  }
  return out
}

// The set of model names defined in this system's models bank
// (systems/<id>/models.json), used to validate an endpoint's optional
// requestModel/responseModel reference. Tolerates an absent/garbled file.
function modelNames(system) {
  const file = path.join(systemDir(system), 'models.json')
  try {
    const raw = JSON.parse(fs.readFileSync(file, 'utf8'))
    const list = Array.isArray(raw?.models) ? raw.models : []
    return new Set(list.map((m) => m && m.name).filter(Boolean))
  } catch {
    return new Set()
  }
}

// An optional reference to a model in the bank. Empty -> ''. Otherwise it must name
// a model that actually exists, so an endpoint can never point at an undefined type.
function validateModelRef(system, value, label) {
  if (value == null || value === '') return ''
  if (typeof value !== 'string') throw bad(`${label} must be a model name`)
  if (!modelNames(system).has(value)) {
    throw bad(`unknown model "${value}" for ${label} — define it in the models bank first`)
  }
  return value
}

function validateEndpoint(body) {
  const { system, service } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)

  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const nodeIds = new Set(manifest.nodes.map((n) => n.id))
  const isService = manifest.nodes.some((n) => n.id === service && isEndpointHost(n))
  if (!isService) throw bad(`"${service}" is not a service or external service in this system`)

  const method = String(body.method || '').toUpperCase()
  if (!METHODS.has(method)) throw bad(`invalid method "${body.method}"`)

  const reqPath = body.path
  if (typeof reqPath !== 'string' || !PATH_RE.test(reqPath)) {
    throw bad('path must start with "/" and use url-safe characters')
  }
  if (isProtectedPath(reqPath)) throw bad(`"${reqPath}" is a built-in route and cannot be modified`)

  const protocol = body.protocol || 'http'
  if (!PROTOCOLS.has(protocol)) throw bad(`invalid protocol "${body.protocol}"`)

  // Required function-name alias. It must be a valid identifier and unique among
  // the *other* endpoints of this service (the one being edited — same method+path —
  // may keep its own alias).
  const alias = typeof body.alias === 'string' ? body.alias.trim() : ''
  if (!alias) throw bad('function name is required')
  if (!ALIAS_RE.test(alias) || alias.length > 60) {
    throw bad('function name must start with a letter or underscore and use only letters, digits and underscores')
  }
  {
    const list = readRegistryFile(system)[service]
    const clash = Array.isArray(list)
      ? list.find((e) => e && e.alias === alias && !(e.method === method && e.path === reqPath))
      : null
    if (clash) throw bad(`function name "${alias}" is already used by ${clash.method} ${clash.path} in this service`)
  }

  // Optional request/response model references — a model name from the bank, or ''.
  // When set, the inline request/response schema for that field is left empty.
  const requestModel = validateModelRef(system, body.requestModel, 'requestModel')
  const responseModel = validateModelRef(system, body.responseModel, 'responseModel')

  const description = typeof body.description === 'string' ? body.description : ''
  if (description.length > 4000) throw bad('description is too long')

  const conversationId = body.conversationId
  if (typeof conversationId !== 'string' || !UUID_RE.test(conversationId)) {
    throw bad('conversationId must be a UUID')
  }

  // `downstream` — like the downstreamDescriptions/downstreamMethods maps — is Claude-managed
  // connection metadata, edited directly in endpoints.json after a route is implemented; it is
  // NOT part of the spec the modal POSTs. Only honor a downstream sent explicitly; when the body
  // omits it, leave it OFF the record so the upsert PRESERVES the existing value on edit instead
  // of wiping a populated trace to []. (For a brand-new endpoint there's no prior value, so it
  // simply starts absent until Claude fills it in.)
  const downstream = Array.isArray(body.downstream)
    ? body.downstream.filter((d) => nodeIds.has(d))
    : undefined

  return {
    system,
    service,
    record: {
      method,
      path: reqPath,
      protocol,
      alias,
      request: validateSchema(body.request, 'request'),
      response: validateSchema(body.response, 'response'),
      requestModel,
      responseModel,
      description,
      ...(downstream !== undefined ? { downstream } : {}),
      conversationId,
    },
  }
}

// Upsert the record under its service, replacing any entry with the same
// method+path. Tolerates a missing/garbled file by starting from {}.
//
// Every save also appends a snapshot of the submitted spec to the endpoint's
// `history` (a server-authoritative, append-only list) so the modal can show the
// read-only changelog of "what this endpoint was created/updated with" over time.
// History is keyed to method+path; a normal upsert (POST) never changes the path, so
// the trail stays put. A path/alias rename (PUT, renameEndpoint) preserves the trail by
// mutating in place and appends a snapshot too — so `path` is recorded in EVERY snapshot
// (both writers) and the changelog can show a `path: /old -> /new` row.
function historySnapshot(record) {
  return {
    at: new Date().toISOString(),
    alias: record.alias || '',
    path: record.path || '',
    request: record.request || {},
    response: record.response || {},
    requestModel: record.requestModel || '',
    responseModel: record.responseModel || '',
    description: record.description || '',
  }
}

function upsertEndpoint(system, service, record) {
  const file = path.join(systemDir(system), 'endpoints.json')
  const registry = readRegistryFile(system)
  if (!Array.isArray(registry[service])) registry[service] = []
  const list = registry[service]
  const i = list.findIndex((e) => e.method === record.method && e.path === record.path)
  const prevHistory = i >= 0 && Array.isArray(list[i].history) ? list[i].history : []
  const withHistory = { ...record, history: [...prevHistory, historySnapshot(record)] }
  if (i >= 0) list[i] = { ...list[i], ...withHistory }
  else list.push(withHistory)
  fs.writeFileSync(file, JSON.stringify(registry, null, 2) + '\n')
}

// Remove an endpoint record by service + method + path. Returns true if one was
// removed. Drops an emptied service key to keep the file tidy. Note: this only
// removes the registry metadata; if a service actually serves the route, that
// handler still lives in the service's code (a separate, Claude-driven change).
function removeEndpoint(body) {
  const { system, service } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  if (!manifest.nodes.some((n) => n.id === service && isEndpointHost(n))) {
    throw bad(`"${service}" is not a service or external service in this system`)
  }
  const method = String(body.method || '').toUpperCase()
  const reqPath = body.path
  if (!METHODS.has(method) || typeof reqPath !== 'string') throw bad('method and path are required')
  if (isProtectedPath(reqPath)) throw bad(`"${reqPath}" is a built-in route and cannot be deleted`)

  const file = path.join(systemDir(system), 'endpoints.json')
  let registry = {}
  try {
    registry = JSON.parse(fs.readFileSync(file, 'utf8')) || {}
  } catch {
    registry = {}
  }
  const list = Array.isArray(registry[service]) ? registry[service] : []
  const gone = list.find((e) => e.method === method && e.path === reqPath)
  const next = list.filter((e) => !(e.method === method && e.path === reqPath))
  const removed = next.length !== list.length
  if (next.length) registry[service] = next
  else delete registry[service]
  fs.writeFileSync(file, JSON.stringify(registry, null, 2) + '\n')
  // Report whether the removed route was internal, so the caller can drop its nginx block.
  return { removed, wasInternal: gone?.internal === true }
}

// Flip an endpoint's `internal` flag in the registry. Marking a route internal drops it
// from the load balancer's advertised surface (the diagram's LB node) while leaving it
// served for service-to-service calls — a pure metadata edit, so NO docker rebuild and no
// Claude session: the frontend's endpoint poll picks it up. If the route is live but has
// no registry entry yet (e.g. a seed route never touched through the modal), a minimal
// record is created to carry the flag. Returns the new value.
function setInternal(body) {
  const { system, service } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  if (!manifest.nodes.some((n) => n.id === service && isEndpointHost(n))) {
    throw bad(`"${service}" is not a service or external service in this system`)
  }
  const method = String(body.method || '').toUpperCase()
  const reqPath = body.path
  if (!METHODS.has(method)) throw bad(`invalid method "${body.method}"`)
  if (typeof reqPath !== 'string' || !PATH_RE.test(reqPath)) {
    throw bad('path must start with "/" and use url-safe characters')
  }
  // Built-in operational routes are already internal-by-policy and locked — don't let
  // the flag be toggled on them.
  if (isProtectedPath(reqPath)) throw bad(`"${reqPath}" is a built-in route and cannot be modified`)
  const internal = body.internal === true

  const file = path.join(systemDir(system), 'endpoints.json')
  const registry = readRegistryFile(system)
  if (!Array.isArray(registry[service])) registry[service] = []
  const list = registry[service]
  const i = list.findIndex((e) => e.method === method && e.path === reqPath)
  if (i >= 0) list[i] = { ...list[i], internal }
  else list.push({ method, path: reqPath, protocol: 'http', internal })
  fs.writeFileSync(file, JSON.stringify(registry, null, 2) + '\n')
  return { internal }
}

// Every internal route across the system, as nginx-blockable { method, lbPath } rules.
// The registry is the source of truth for the internal flag; each service-local path is
// prefixed with /<service> to get the path the load balancer sees. Built-in operational
// routes (/health, /resilience/state) are never blocked.
function internalRules(system) {
  const registry = readRegistryFile(system)
  const rules = []
  for (const [service, list] of Object.entries(registry)) {
    if (!Array.isArray(list)) continue
    for (const e of list) {
      if (!e || e.internal !== true) continue
      if (typeof e.method !== 'string' || typeof e.path !== 'string') continue
      if (isProtectedPath(e.path)) continue
      rules.push({ method: e.method.toUpperCase(), lbPath: `/${service}${e.path}` })
    }
  }
  return rules
}

// Regenerate the lb's internal-route blocks from the registry and hot-reload nginx (no
// rebuild). The nginx.conf rewrite always happens (so it's correct on next start); the
// reload is what can fail when the system isn't running — the caller decides whether that
// failure is fatal. Returns the reload log on success; throws on reload failure.
async function syncInternalNginx(system) {
  setInternalRoutes(system, internalRules(system))
  return reloadNginx(system)
}

// ---------------------------------------------------------------------------
// PUT /api/endpoints — RENAME an endpoint's path and/or alias, cascading the
// change so everything that references it stays consistent. The mechanical half
// runs here (no docker rebuild): mutate the owner record in place (keeping its
// history), rewrite every other endpoint's `downstreamMethods` reference to this
// route, rewrite matching client-function step paths, and re-sync nginx internal
// blocks if an internal route's path moved. The code half — the owner's route
// decorator + each caller's call URL in app.py, then the rebuild — is delegated to
// a Claude session by the modal, exactly like add/edit. ALIAS lives only in the
// registry/diagram, so an alias-only rename is purely this mechanical pass.
// ---------------------------------------------------------------------------

// Parse a downstreamMethods entry "METHOD ref" (ref = service-local path | LB-prefixed
// path | alias; the method token is optional). Mirrors the diagram matcher in
// SystemDiagram.jsx so the cascade rewrites exactly what the diagram would highlight.
function splitMethodRef(entry) {
  const s = String(entry).trim()
  const sp = s.indexOf(' ')
  if (sp < 0) return { method: '', ref: s }
  return { method: s.slice(0, sp).toUpperCase(), ref: s.slice(sp + 1).trim() }
}

// Does a single downstreamMethods entry refer to <method> on <ownerService> at this
// service-local path or alias? Whole-ref equality (never substring), method-gated — so
// `/payment/webhook` never matches `/payment/webhook/2a`.
function entryRefersTo(entry, ownerService, method, localPath, alias) {
  if (typeof entry !== 'string') return false
  const { method: em, ref } = splitMethodRef(entry)
  if (em && em !== method) return false
  return ref === localPath || ref === `/${ownerService}${localPath}` || (!!alias && ref === alias)
}

function listRefersToRoute(entries, ownerService, method, localPath, alias) {
  return Array.isArray(entries) && entries.some((e) => entryRefersTo(e, ownerService, method, localPath, alias))
}

// Rewrite, across the whole registry, every `downstreamMethods[ownerService]` entry that
// refers to this route — preserving the authored form (service-local -> new service-local,
// LB-prefixed -> new LB-prefixed, alias -> new alias). An alias-only rename leaves path-form
// entries untouched, and vice-versa.
function cascadeDownstreamMethods(registry, ownerService, method, { oldPath, newPath, oldAlias, newAlias }) {
  for (const eps of Object.values(registry)) {
    if (!Array.isArray(eps)) continue
    for (const e of eps) {
      const arr = e?.downstreamMethods?.[ownerService]
      if (!Array.isArray(arr)) continue
      for (let k = 0; k < arr.length; k++) {
        if (typeof arr[k] !== 'string') continue
        const { method: em, ref } = splitMethodRef(arr[k])
        if (em && em !== method) continue
        const prefix = em ? `${em} ` : ''
        if (ref === oldPath) arr[k] = `${prefix}${newPath}`
        else if (ref === `/${ownerService}${oldPath}`) arr[k] = `${prefix}/${ownerService}${newPath}`
        else if (oldAlias && ref === oldAlias) arr[k] = `${prefix}${newAlias}`
      }
    }
  }
}

function renameEndpoint(body) {
  const { system, service } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  if (!manifest.nodes.some((n) => n.id === service && isEndpointHost(n))) {
    throw bad(`"${service}" is not a service or external service in this system`)
  }

  const method = String(body.method || '').toUpperCase()
  if (!METHODS.has(method)) throw bad(`invalid method "${body.method}"`)

  const oldPath = body.oldPath
  if (typeof oldPath !== 'string') throw bad('oldPath is required')
  const newPath = body.newPath
  if (typeof newPath !== 'string' || !PATH_RE.test(newPath)) {
    throw bad('path must start with "/" and use url-safe characters')
  }
  if (isProtectedPath(oldPath) || isProtectedPath(newPath)) {
    throw bad('built-in routes (e.g. /health) cannot be renamed')
  }

  const newAlias = typeof body.newAlias === 'string' ? body.newAlias.trim() : ''
  if (!newAlias) throw bad('function name is required')
  if (!ALIAS_RE.test(newAlias) || newAlias.length > 60) {
    throw bad('function name must start with a letter or underscore and use only letters, digits and underscores')
  }

  const registry = readRegistryFile(system)
  const list = Array.isArray(registry[service]) ? registry[service] : null
  const i = list ? list.findIndex((e) => e.method === method && e.path === oldPath) : -1
  if (i < 0) throw bad(`no ${method} ${oldPath} endpoint on "${service}" to rename`)
  const record = list[i]
  const oldAlias = typeof record.alias === 'string' ? record.alias : ''

  const pathChanged = newPath !== oldPath
  const aliasChanged = newAlias !== oldAlias

  // New path must not collide with another route on this service; new alias must be unique
  // among the OTHER endpoints (the renamed record itself is excluded by index).
  if (pathChanged && list.some((e, j) => j !== i && e.method === method && e.path === newPath)) {
    throw bad(`${method} ${newPath} already exists on "${service}"`)
  }
  {
    const clash = list.find((e, j) => j !== i && e && e.alias === newAlias)
    if (clash) throw bad(`function name "${newAlias}" is already used by ${clash.method} ${clash.path} in this service`)
  }

  // Optional spec fields carried through (the modal sends the full new record, so a concurrent
  // schema/description edit lands in the same write). Anything not sent keeps the current value.
  const protocol = PROTOCOLS.has(body.protocol) ? body.protocol : record.protocol || 'http'
  const requestModel = validateModelRef(system, body.requestModel ?? record.requestModel, 'requestModel')
  const responseModel = validateModelRef(system, body.responseModel ?? record.responseModel, 'responseModel')
  const request = body.request != null ? validateSchema(body.request, 'request') : record.request || {}
  const response = body.response != null ? validateSchema(body.response, 'response') : record.response || {}
  const description = typeof body.description === 'string' ? body.description : record.description || ''
  if (description.length > 4000) throw bad('description is too long')
  const conversationId =
    typeof body.conversationId === 'string' && UUID_RE.test(body.conversationId)
      ? body.conversationId
      : record.conversationId || null

  // Mutate in place — keeps the array slot AND the history trail; append one snapshot
  // (the identity change is the point, so we record it even if nothing else changed).
  const updated = {
    ...record,
    method,
    path: newPath,
    protocol,
    alias: newAlias,
    request,
    response,
    requestModel,
    responseModel,
    description,
    conversationId,
  }
  updated.history = [...(Array.isArray(record.history) ? record.history : []), historySnapshot(updated)]
  list[i] = updated

  if (pathChanged || aliasChanged) {
    cascadeDownstreamMethods(registry, service, method, { oldPath, newPath, oldAlias, newAlias })
  }
  fs.writeFileSync(path.join(systemDir(system), 'endpoints.json'), JSON.stringify(registry, null, 2) + '\n')

  // Client-function step paths reference the LB path, so a path rename rewrites them too.
  let scenarioWarnings = []
  if (pathChanged) {
    try {
      scenarioWarnings = renameStepPaths(system, service, method, oldPath, newPath).warnings || []
    } catch {
      /* scenarios bank is optional/garbled — skip */
    }
  }

  // Callers = every other service whose downstreamMethods now points at this route (after the
  // cascade). The modal seeds these into the rebuild session so caller app.py call URLs follow.
  const callers = []
  for (const [svc, eps] of Object.entries(registry)) {
    if (svc === service || !Array.isArray(eps)) continue
    if (eps.some((e) => listRefersToRoute(e?.downstreamMethods?.[service], service, method, newPath, newAlias))) {
      callers.push(svc)
    }
  }

  return {
    pathChanged,
    aliasChanged,
    internal: updated.internal === true,
    affectedServices: [service, ...callers],
    callers,
    scenarioWarnings,
  }
}

export default function endpoints() {
  return {
    name: 'endpoints',
    configureServer(server) {
      server.middlewares.use('/api/endpoints', async (req, res, next) => {
        const json = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }

        if (req.method === 'GET') {
          try {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(400, { ok: false, error: 'unknown system' })
            return json(200, { ok: true, endpoints: await discover(system) })
          } catch (err) {
            return json(500, { ok: false, error: err.message })
          }
        }

        if (req.method === 'POST') {
          try {
            const body = await readJsonBody(req)
            const { system, service, record } = validateEndpoint(body)
            upsertEndpoint(system, service, record)
            return json(200, { ok: true, endpoint: { service, ...record } })
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
        }

        // PUT renames an endpoint's path and/or alias and cascades the change (registry,
        // downstreamMethods, client-function steps, internal nginx blocks). No rebuild here —
        // the modal launches a Claude session to move the owner's decorator + caller call URLs.
        if (req.method === 'PUT') {
          let body
          let result
          try {
            body = await readJsonBody(req)
            result = renameEndpoint(body)
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
          // Only an internal route whose path moved needs its nginx block regenerated.
          if (result.pathChanged && result.internal) {
            try {
              await syncInternalNginx(body.system)
            } catch {
              /* nginx.conf is rewritten; the live reload just couldn't run (system down) */
            }
          }
          return json(200, { ok: true, ...result })
        }

        if (req.method === 'DELETE') {
          try {
            const body = await readJsonBody(req)
            const { removed, wasInternal } = removeEndpoint(body)
            // Removing an internal route must drop its nginx block too — best-effort
            // reload, since the registry/config are already correct regardless.
            if (wasInternal) {
              try {
                await syncInternalNginx(body.system)
              } catch {
                /* nginx.conf is updated; the live reload just couldn't run (system down) */
              }
            }
            return json(200, { ok: true, removed })
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
        }

        // PATCH toggles only the `internal` flag: update the registry, regenerate the lb's
        // block list, and hot-reload nginx (no rebuild). External calls to an internal
        // route get 403; service-to-service calls bypass the lb and are unaffected.
        if (req.method === 'PATCH') {
          let body
          let result
          try {
            body = await readJsonBody(req)
            result = setInternal(body) // registry write (validates input first)
            setInternalRoutes(body.system, internalRules(body.system)) // rewrite nginx.conf
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
          try {
            await reloadNginx(body.system)
            return json(200, { ok: true, ...result })
          } catch (err) {
            // The flag and nginx.conf are persisted; only the live reload failed (most
            // likely the system isn't running). Surface it without rolling back the save.
            return json(200, { ok: true, ...result, nginxReloaded: false, warning: err.message })
          }
        }

        return next()
      })
    },
  }
}
