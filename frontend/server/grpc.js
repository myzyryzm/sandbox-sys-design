// Vite dev-server plugin: the gRPC contract bank + server-only attach.
//
// The bank is pure SHAPE (mirrors models.js): contracts are created/edited as
// data (form field maps or an uploaded .proto) and the backend does the codegen
// itself — proto synthesis/splice via grpcProto.js + real protoc in a throwaway
// container — with NO Claude session. Behavior lives elsewhere: per-method
// `description`s written from the service Edit modal drive the servicer that a
// launched sandbox-grpc-attach session authors for the ONE service that serves
// (owns) the contract. Client wiring (manifest grpc.clients) is written by the
// flows that make a service call a contract, not by these routes.
//
//   GET    /api/grpc-contracts?system=<id>
//     -> { ok, contracts: [{ name, source, methods, conversationId, createdAt,
//          server, servers, clients }] }  (owner/clients joined from the manifest)
//   POST   /api/grpc-contracts   — CREATE only (409 if the name exists):
//     { system, contract, methods:[{name, request, response, responseStreaming}] }
//       -> synthesize the .proto from the field maps, protoc, persist proto+pb2.
//     { system, protoFile }
//       -> validate a complete .proto (structural checks + real protoc), persist
//          it verbatim + the generated pb2 bindings, register every rpc method.
//     No session is launched: a new contract has no owner and no behavior yet.
//   POST   /api/grpc-apply       — batch-apply STAGED edits to existing contracts:
//     { system, changes:[ {contract, kind:'methods', upserts, deletes}
//                       | {contract, kind:'replace-proto', protoFile}
//                       | {contract, kind:'delete'} ] }
//       -> two-phase: every changed proto compiles in one protoc run first
//          (nothing persisted on any failure), then protos+pb2+registry are
//          written and deletions scrubbed. Returns the affected services
//          (impact) so the modal can launch ONE propagation session.
//   DELETE /api/grpc-contracts   { system, contract }  — kept for compat.
//   GET    /api/grpc-service?system=<id>&id=<service>
//     -> { ok, grpc, contracts }  (contracts annotated with owner + descriptions)
//   POST   /api/grpc-attach      { system, service, contract, descriptions?,
//                                  conversationId? }
//     -> server-only: ONE owner per contract (409 if another service serves it).
//        Writes the node's manifest grpc.servers + the registry's per-method
//        descriptions; the servicer/wiring is the launched session's job.
//   POST   /api/grpc-detach      { system, service, contract }
//     -> 409 while another service still dials this one for the contract;
//        otherwise scrubs servers/overrides (the unwire session is launched by
//        the modal).
//   POST   /api/grpc-descriptions { system, contract, method, description,
//                                   conversationId? }
//     -> pure registry write (the modal sends the full, joined description).
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'
import { HttpError, bad, readJsonBody } from './scaffold.js'
import {
  parseRpcMethods,
  runProtocKeep,
  spliceUploadedProto,
  stripProtoComments,
  synthesizeFormProto,
} from './grpcProto.js'

// PascalCase for a proto service (contract) and its RPC methods.
const CONTRACT_RE = /^[A-Z][A-Za-z0-9]*$/
const METHOD_RE = /^[A-Z][A-Za-z0-9]*$/
// snake_case-ish proto field names.
const FIELD_RE = /^[a-z][a-z0-9_]*$/
// Scalar proto types the form emits; `repeated <scalar>` is also allowed.
const SCALAR_TYPES = new Set([
  'double', 'float', 'int32', 'int64', 'uint32', 'uint64', 'sint32', 'sint64',
  'fixed32', 'fixed64', 'sfixed32', 'sfixed64', 'bool', 'string', 'bytes',
])

const grpcDir = (system) => path.join(systemDir(system), 'grpc')
const registryFile = (system) => path.join(grpcDir(system), '_registry.json')
const manifestFile = (system) => path.join(systemDir(system), 'manifest.json')

function readManifest(system) {
  return JSON.parse(fs.readFileSync(manifestFile(system), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(manifestFile(system), JSON.stringify(manifest, null, 2) + '\n')
}

// Tolerates a missing/garbled registry — a system with no contracts yet.
function readRegistry(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(registryFile(system), 'utf8'))
    return raw && typeof raw.contracts === 'object' && raw.contracts ? raw : { contracts: {} }
  } catch {
    return { contracts: {} }
  }
}
function writeRegistry(system, registry) {
  fs.mkdirSync(grpcDir(system), { recursive: true })
  fs.writeFileSync(registryFile(system), JSON.stringify(registry, null, 2) + '\n')
}

// A flat { field_name: type } map, where type is a scalar or `repeated <scalar>`.
function validateFields(value, label) {
  if (value == null) return {}
  if (typeof value !== 'object' || Array.isArray(value)) throw bad(`${label} must be a JSON object`)
  const out = {}
  for (const [k, v] of Object.entries(value)) {
    if (!FIELD_RE.test(k)) throw bad(`${label} field name "${k}" must be snake_case`)
    if (typeof v !== 'string') throw bad(`${label}.${k} must be a proto type name`)
    const base = v.startsWith('repeated ') ? v.slice('repeated '.length) : v
    if (!SCALAR_TYPES.has(base)) throw bad(`${label}.${k} has unknown proto type "${v}"`)
    out[k] = v
  }
  return out
}

// ---------------------------------------------------------------------------
// Method records
// ---------------------------------------------------------------------------

const isScalarMap = (map) =>
  Object.values(map || {}).every((v) => {
    const base = typeof v === 'string' && v.startsWith('repeated ') ? v.slice('repeated '.length) : v
    return SCALAR_TYPES.has(base)
  })

// Can this method's messages be regenerated from its field maps? Explicit flag
// wins; legacy records infer: non-empty, all-scalar maps. (Upload-born methods
// have empty maps; llm-app's `repeated WorkerEndpoint` is non-scalar — both
// correctly classify as locked.)
function inferFormAuthored(m) {
  if (typeof m.formAuthored === 'boolean') return m.formAuthored
  const hasFields = Object.keys(m.request || {}).length + Object.keys(m.response || {}).length > 0
  return hasFields && isScalarMap(m.request) && isScalarMap(m.response)
}

// The normalized method shape the API returns (and the modal round-trips).
function publicMethod(m) {
  return {
    name: m.name,
    request: m.request || {},
    response: m.response || {},
    requestType: m.requestType || `${m.name}Request`,
    responseType: m.responseType || `${m.name}Reply`,
    requestStreaming: !!m.requestStreaming,
    responseStreaming: !!m.responseStreaming,
    formAuthored: inferFormAuthored(m),
    description: typeof m.description === 'string' ? m.description : '',
    conversationId: m.conversationId || null,
    // Append-only changelog of description updates (oldest-first); the UI shows
    // it newest-first under each method. Missing on legacy records → [].
    history: Array.isArray(m.history) ? m.history : [],
  }
}

// Validate one form-authored method upsert from the modal.
function normalizeUpsert(u) {
  if (!u || typeof u.name !== 'string' || !METHOD_RE.test(u.name)) {
    throw bad('method name must be PascalCase (e.g. GetChunk)')
  }
  const typeName = (v, fallback) => (typeof v === 'string' && /^[A-Za-z_]\w*$/.test(v) ? v : fallback)
  return {
    name: u.name,
    request: validateFields(u.request, `${u.name}.request`),
    response: validateFields(u.response, `${u.name}.response`),
    requestType: typeName(u.requestType, `${u.name}Request`),
    responseType: typeName(u.responseType, `${u.name}Reply`),
    requestStreaming: !!u.requestStreaming,
    responseStreaming: !!u.responseStreaming,
    formAuthored: true,
    description: '',
    conversationId: null,
    history: [],
  }
}

// ---------------------------------------------------------------------------
// Manifest joins (owner + clients — the bank's "usage" data)
// ---------------------------------------------------------------------------

function joinAttachments(manifest, contract) {
  const servers = []
  const clients = []
  for (const n of manifest.nodes) {
    if (n.grpc?.servers?.includes(contract)) servers.push(n.id)
    const c = (n.grpc?.clients || []).find((c) => c.contract === contract)
    if (c) clients.push({ service: n.id, targets: Array.isArray(c.targets) ? c.targets : [] })
  }
  return { servers, clients }
}

function contractView(manifest, name, c) {
  const { servers, clients } = joinAttachments(manifest, name)
  return {
    name,
    source: c.source || 'form',
    methods: (Array.isArray(c.methods) ? c.methods : []).map(publicMethod),
    conversationId: c.conversationId || null,
    createdAt: c.createdAt || null,
    server: servers[0] || null,
    servers,
    clients,
  }
}

function listContracts(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const { contracts } = readRegistry(system)
  const manifest = readManifest(system)
  const out = Object.entries(contracts).map(([name, c]) => contractView(manifest, name, c))
  out.sort((a, b) => a.name.localeCompare(b.name))
  return { ok: true, contracts: out }
}

// ---------------------------------------------------------------------------
// Codegen (mechanical: temp dir -> one protoc run -> copy in only on success)
// ---------------------------------------------------------------------------

async function generateInto(system, protoTexts /* Map<contract, protoText> */) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'grpc-gen-'))
  try {
    for (const [c, text] of protoTexts) fs.writeFileSync(path.join(tmp, `${c}.proto`), text)
    await runProtocKeep(tmp, [...protoTexts.keys()].map((c) => `${c}.proto`))
    fs.mkdirSync(grpcDir(system), { recursive: true })
    for (const [c] of protoTexts) {
      for (const f of [`${c}.proto`, `${c}_pb2.py`, `${c}_pb2_grpc.py`]) {
        const src = path.join(tmp, f)
        if (fs.existsSync(src)) fs.copyFileSync(src, path.join(grpcDir(system), f))
      }
    }
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true })
  }
}

// ---------------------------------------------------------------------------
// Create (new contracts only — edits go through /api/grpc-apply)
// ---------------------------------------------------------------------------

// Structural completeness checks for a whole .proto (cheap, before docker),
// on a comment-stripped copy. Returns the service (contract) name.
function validateUploadStructure(protoFile) {
  if (typeof protoFile !== 'string' || !protoFile.trim()) throw bad('protoFile (the .proto text) is required')
  if (protoFile.length > 200_000) throw bad('proto file is too large')
  const src = stripProtoComments(protoFile)
  if (!/\bsyntax\s*=\s*["']proto3["']\s*;/.test(src)) {
    throw bad('the .proto must declare syntax = "proto3";')
  }
  if (/^\s*import\s+/m.test(src)) {
    throw bad('uploaded contracts must be self-contained — remove the import statement(s)')
  }
  const serviceNames = [...src.matchAll(/\bservice\s+([A-Za-z_]\w*)\s*\{/g)].map((m) => m[1])
  if (serviceNames.length === 0) throw bad('no service defined — a contract needs exactly one `service`')
  if (serviceNames.length > 1) {
    throw bad(`found ${serviceNames.length} services (${serviceNames.join(', ')}) — a contract is exactly one service per file`)
  }
  const contract = serviceNames[0]
  if (!CONTRACT_RE.test(contract)) throw bad(`service name "${contract}" must be PascalCase (e.g. ChunkTransfer)`)
  const methods = parseRpcMethods(protoFile)
  if (methods.length === 0) throw bad(`service ${contract} has no rpc methods`)
  return { contract, methods }
}

async function createContract(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const registry = readRegistry(system)

  // Upload path: the whole .proto is authoritative.
  if (typeof body.protoFile === 'string') {
    const { contract, methods } = validateUploadStructure(body.protoFile)
    if (registry.contracts[contract]) {
      throw new HttpError(409, `contract "${contract}" already exists — edit it in the bank (Review & save) instead`)
    }
    await generateInto(system, new Map([[contract, body.protoFile]]))
    registry.contracts[contract] = {
      source: 'upload',
      methods: methods.map((m) => ({ ...m, formAuthored: false, description: '', conversationId: null, history: [] })),
      conversationId: null,
      createdAt: new Date().toISOString(),
    }
    writeRegistry(system, registry)
    return { ok: true, contract, methods: registry.contracts[contract].methods.map(publicMethod) }
  }

  // Form path: synthesize the proto from the field maps.
  const contract = body.contract
  if (typeof contract !== 'string' || !CONTRACT_RE.test(contract)) {
    throw bad('contract must be PascalCase (e.g. ChunkTransfer)')
  }
  if (registry.contracts[contract]) {
    throw new HttpError(409, `contract "${contract}" already exists — edit it in the bank (Review & save) instead`)
  }
  const methods = (Array.isArray(body.methods) ? body.methods : []).map(normalizeUpsert)
  if (!methods.length) throw bad('at least one method is required')
  const names = new Set()
  for (const m of methods) {
    if (names.has(m.name)) throw bad(`duplicate method "${m.name}"`)
    names.add(m.name)
  }
  const proto = synthesizeFormProto(system, contract, methods, null)
  await generateInto(system, new Map([[contract, proto]]))
  registry.contracts[contract] = {
    source: 'form',
    methods,
    conversationId: null,
    createdAt: new Date().toISOString(),
  }
  writeRegistry(system, registry)
  return { ok: true, contract, methods: methods.map(publicMethod) }
}

// ---------------------------------------------------------------------------
// Apply (staged batch — the model-bank "Review & save" analog)
// ---------------------------------------------------------------------------

async function applyChanges(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const changes = Array.isArray(body.changes) ? body.changes : []
  if (!changes.length) throw bad('changes is required')

  const registry = readRegistry(system)
  const manifest = readManifest(system)

  const protoTexts = new Map() // contract -> new proto text
  const nextMethods = new Map() // contract -> desired registry methods
  const becameUpload = new Set() // replace-proto flips source
  const deletions = []

  for (const ch of changes) {
    const name = ch?.contract
    if (typeof name !== 'string' || !CONTRACT_RE.test(name)) throw bad('invalid contract in changes')
    if (protoTexts.has(name) || deletions.includes(name)) throw bad(`duplicate change for "${name}"`)
    const existing = registry.contracts[name]
    if (!existing) throw bad(`contract "${name}" does not exist`)

    if (ch.kind === 'delete') {
      deletions.push(name)
      continue
    }

    if (ch.kind === 'replace-proto') {
      const { contract: uploadedName, methods: parsed } = validateUploadStructure(ch.protoFile)
      if (uploadedName !== name) {
        throw bad(`the uploaded proto defines service "${uploadedName}", not "${name}" — upload it as a new contract instead`)
      }
      const prevByName = new Map((existing.methods || []).map((m) => [m.name, m]))
      nextMethods.set(name, parsed.map((m) => ({
        ...m,
        formAuthored: false,
        description: prevByName.get(m.name)?.description || '',
        conversationId: prevByName.get(m.name)?.conversationId || null,
        history: prevByName.get(m.name)?.history || [],
      })))
      protoTexts.set(name, ch.protoFile)
      becameUpload.add(name)
      continue
    }

    if (ch.kind !== 'methods') throw bad(`unknown change kind "${ch?.kind}"`)
    const upserts = (Array.isArray(ch.upserts) ? ch.upserts : []).map(normalizeUpsert)
    const methodDeletes = (Array.isArray(ch.deletes) ? ch.deletes : []).filter((n) => typeof n === 'string')
    if (!upserts.length && !methodDeletes.length) throw bad(`the change for "${name}" is empty`)

    const prev = Array.isArray(existing.methods) ? existing.methods : []
    const prevByName = new Map(prev.map((m) => [m.name, m]))
    for (const u of upserts) {
      const old = prevByName.get(u.name)
      if (!old) continue
      if (!inferFormAuthored(old)) {
        throw bad(`method "${u.name}" of ${name} came from an uploaded .proto — delete it or re-upload the contract to reshape it`)
      }
      // An edit keeps the method's behavior text, changelog, session and message names.
      u.description = typeof old.description === 'string' ? old.description : ''
      u.conversationId = old.conversationId || null
      u.history = Array.isArray(old.history) ? old.history : []
      if (old.requestType) u.requestType = old.requestType
      if (old.responseType) u.responseType = old.responseType
    }
    const deleteSet = new Set(methodDeletes)
    const desired = prev.filter((m) => !deleteSet.has(m.name)).map((m) => ({ ...m }))
    for (const u of upserts) {
      const i = desired.findIndex((m) => m.name === u.name)
      if (i >= 0) desired[i] = u
      else desired.push(u)
    }
    if (!desired.length) throw bad(`"${name}" would have no methods left — delete the contract instead`)

    const protoPath = path.join(grpcDir(system), `${name}.proto`)
    const existingText = fs.existsSync(protoPath) ? fs.readFileSync(protoPath, 'utf8') : null
    let text
    if (!existing.source || existing.source === 'form') {
      text = synthesizeFormProto(
        system, name,
        desired.map((m) => ({ ...m, formAuthored: inferFormAuthored(m) })),
        existingText,
      )
    } else {
      // upload/custom: the on-disk proto is authoritative — splice it.
      if (!existingText) throw bad(`${name}.proto is missing on disk — re-upload the contract`)
      const lockedNames = new Set(prev.filter((m) => !inferFormAuthored(m)).map((m) => m.name))
      text = spliceUploadedProto(existingText, { upserts, deletes: methodDeletes, lockedNames })
    }
    protoTexts.set(name, text)
    nextMethods.set(name, desired)
  }

  // Impact is computed BEFORE persisting (deleted contracts are still attached
  // here) — it's what the modal's single propagation session works from.
  const changedNames = [...new Set([...protoTexts.keys(), ...deletions])]
  const impact = { owners: [], clients: [] }
  for (const name of changedNames) {
    const { servers, clients } = joinAttachments(manifest, name)
    for (const s of servers) impact.owners.push({ contract: name, service: s })
    for (const c of clients) impact.clients.push({ contract: name, service: c.service })
  }

  // Phase 1: every changed proto must compile (one protoc run; throws = nothing
  // persisted). Phase 2: copy protos+pb2 in, write the registry, run deletions.
  if (protoTexts.size) await generateInto(system, protoTexts)
  for (const [name, methods] of nextMethods) {
    registry.contracts[name] = {
      ...registry.contracts[name],
      methods,
      ...(becameUpload.has(name) ? { source: 'upload' } : {}),
    }
  }
  writeRegistry(system, registry)
  for (const name of deletions) deleteContract({ system, contract: name })

  return { ok: true, applied: changedNames, impact }
}

// ---------------------------------------------------------------------------
// Delete (registry + generated files + manifest scrub)
// ---------------------------------------------------------------------------

function deleteContract(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const contract = body.contract
  if (typeof contract !== 'string' || !CONTRACT_RE.test(contract)) throw bad('invalid contract')

  // 1. registry
  const registry = readRegistry(system)
  delete registry.contracts[contract]
  writeRegistry(system, registry)

  // 2. generated files (shared)
  for (const suffix of ['.proto', '_pb2.py', '_pb2_grpc.py', '_servicer.py']) {
    fs.rmSync(path.join(grpcDir(system), `${contract}${suffix}`), { force: true })
  }

  // 3. scrub every service's grpc block + any legacy per-service override file
  const manifest = readManifest(system)
  for (const node of manifest.nodes) {
    if (!node.grpc) continue
    const g = node.grpc
    g.servers = (g.servers || []).filter((c) => c !== contract)
    g.clients = (g.clients || []).filter((c) => c.contract !== contract)
    g.overrides = (g.overrides || []).filter((c) => c !== contract)
    if (!g.servers.length && !g.clients.length && !g.overrides.length) delete node.grpc
    fs.rmSync(path.join(systemDir(system), node.id, 'grpc', `${contract}_servicer_override.py`), { force: true })
  }
  writeManifest(system, manifest)
  return { ok: true, removed: contract }
}

// ---------------------------------------------------------------------------
// Per-service view + server-only attach / detach / descriptions
// ---------------------------------------------------------------------------

// The node kinds that can serve a contract (NodeEditModal offers the tab for both).
function findServiceNode(manifest, id) {
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || (node.type !== 'service' && node.type !== 'service-lb')) {
    throw bad(`"${id}" is not a service in this system`)
  }
  return node
}

function getService(system, id) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const node = findServiceNode(manifest, id)
  const { contracts } = readRegistry(system)
  const out = Object.entries(contracts).map(([name, c]) => contractView(manifest, name, c))
  out.sort((a, b) => a.name.localeCompare(b.name))
  return {
    ok: true,
    grpc: node.grpc || { servers: [], clients: [], overrides: [] },
    contracts: out,
  }
}

function attachContract(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const service = body.service
  const node = findServiceNode(manifest, service)

  const contract = body.contract
  if (typeof contract !== 'string' || !CONTRACT_RE.test(contract)) throw bad('invalid contract')
  const registry = readRegistry(system)
  const entry = registry.contracts[contract]
  if (!entry) throw bad(`contract "${contract}" does not exist`)

  // One serving service per contract (endpoint-like ownership). Custom types
  // install multi-server blocks directly and never pass through this route.
  const owner = manifest.nodes.find((n) => n.id !== service && n.grpc?.servers?.includes(contract))
  if (owner) {
    throw new HttpError(409, `"${contract}" is already served by "${owner.id}" — detach it there first`)
  }

  const descriptions =
    body.descriptions && typeof body.descriptions === 'object' && !Array.isArray(body.descriptions)
      ? body.descriptions
      : {}
  for (const [m, text] of Object.entries(descriptions)) {
    if (typeof text !== 'string' || text.length > 8000) throw bad(`description for "${m}" is invalid or too long`)
  }

  const g = node.grpc || { servers: [], clients: [], overrides: [] }
  g.servers = g.servers || []
  g.clients = g.clients || []
  g.overrides = g.overrides || []
  if (!g.servers.includes(contract)) g.servers.push(contract)
  node.grpc = g
  writeManifest(system, manifest)

  entry.server = service
  const now = new Date().toISOString()
  entry.methods = (entry.methods || []).map((m) => {
    if (typeof descriptions[m.name] !== 'string') return m
    const text = descriptions[m.name]
    const history = Array.isArray(m.history) ? m.history : []
    // A seeded description becomes the method's first changelog entry.
    return {
      ...m,
      description: text,
      history: text.trim()
        ? [...history, { at: now, change: text, conversationId: body.conversationId || null }]
        : history,
    }
  })
  if (typeof body.conversationId === 'string' && body.conversationId) entry.conversationId = body.conversationId
  writeRegistry(system, registry)
  return { ok: true, service, grpc: node.grpc }
}

function detachContract(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  const service = body.service
  const node = findServiceNode(manifest, service)
  const contract = body.contract
  if (typeof contract !== 'string' || !CONTRACT_RE.test(contract)) throw bad('invalid contract')

  // Mirror remove.js: refuse while other services still dial this server.
  const dialers = manifest.nodes
    .filter((n) => n.id !== service)
    .filter((n) => (n.grpc?.clients || []).some((c) => c.contract === contract && (c.targets || []).includes(service)))
    .map((n) => n.id)
  if (dialers.length) {
    throw new HttpError(409, `"${contract}" is still called by ${dialers.join(', ')} — remove those client wirings first`)
  }

  const g = node.grpc
  if (g) {
    g.servers = (g.servers || []).filter((c) => c !== contract)
    g.overrides = (g.overrides || []).filter((c) => c !== contract)
    if (!g.servers.length && !(g.clients || []).length && !g.overrides.length) delete node.grpc
  }
  writeManifest(system, manifest)

  const registry = readRegistry(system)
  if (registry.contracts[contract]?.server === service) {
    registry.contracts[contract].server = null
    writeRegistry(system, registry)
  }
  return { ok: true, service, contract }
}

function upsertDescription(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const contract = body.contract
  if (typeof contract !== 'string' || !CONTRACT_RE.test(contract)) throw bad('invalid contract')
  const registry = readRegistry(system)
  const entry = registry.contracts[contract]
  if (!entry) throw bad(`contract "${contract}" does not exist`)
  const method = (entry.methods || []).find((m) => m.name === body.method)
  if (!method) throw bad(`method "${body.method}" does not exist on ${contract}`)
  const description = body.description
  if (typeof description !== 'string' || description.length > 8000) throw bad('description is invalid or too long')
  const change = typeof body.change === 'string' ? body.change : ''
  if (change.length > 8000) throw bad('change is too long')

  // Append-only changelog: each entry is the raw delta the user typed in one
  // update (mirrors endpoints.js / consumers.js). Capture the prior description
  // BEFORE overwriting so a record that already had a description but no history
  // (e.g. a legacy attach) gets a sensible baseline "created" entry first.
  const prev = typeof method.description === 'string' ? method.description : ''
  if (!Array.isArray(method.history)) method.history = []
  if (method.history.length === 0 && prev.trim()) {
    method.history.push({ at: new Date().toISOString(), change: prev, conversationId: method.conversationId || null })
  }
  // Prefer the explicit delta; fall back to diffing (older clients that only
  // send the joined `description`).
  const derived =
    description !== prev ? (description.startsWith(prev) ? description.slice(prev.length) : description) : ''
  const effectiveChange = (change.trim() || derived).trim()
  if (effectiveChange) {
    method.history.push({ at: new Date().toISOString(), change: effectiveChange, conversationId: body.conversationId || null })
  }

  method.description = description
  if (typeof body.conversationId === 'string' && body.conversationId) {
    method.conversationId = body.conversationId
    entry.conversationId = body.conversationId
  }
  writeRegistry(system, registry)
  return { ok: true, contract, method: publicMethod(method) }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

export default function grpc() {
  return {
    name: 'grpc-contracts',
    configureServer(server) {
      const json = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }
      const route = (url, handler) => {
        server.middlewares.use(url, async (req, res, next) => {
          try {
            const out = await handler(req, next)
            if (out !== undefined) return json(res, 200, out)
          } catch (err) {
            return json(res, err.statusCode || 500, { ok: false, error: err.message })
          }
        })
      }

      route('/api/grpc-contracts', async (req, next) => {
        if (req.method === 'GET') {
          const system = new URL(req.url, 'http://localhost').searchParams.get('system')
          return listContracts(system)
        }
        if (req.method === 'POST') return createContract(await readJsonBody(req, 300_000))
        if (req.method === 'DELETE') return deleteContract(await readJsonBody(req))
        return void next()
      })

      route('/api/grpc-apply', async (req, next) => {
        if (req.method !== 'POST') return void next()
        return applyChanges(await readJsonBody(req, 1_000_000))
      })

      route('/api/grpc-service', async (req, next) => {
        if (req.method !== 'GET') return void next()
        const url = new URL(req.url, 'http://localhost')
        return getService(url.searchParams.get('system'), url.searchParams.get('id'))
      })

      route('/api/grpc-attach', async (req, next) => {
        if (req.method !== 'POST') return void next()
        return attachContract(await readJsonBody(req))
      })

      route('/api/grpc-detach', async (req, next) => {
        if (req.method !== 'POST') return void next()
        return detachContract(await readJsonBody(req))
      })

      route('/api/grpc-descriptions', async (req, next) => {
        if (req.method !== 'POST') return void next()
        return upsertDescription(await readJsonBody(req))
      })
    },
  }
}
