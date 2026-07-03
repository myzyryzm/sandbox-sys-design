// Vite dev-server plugin: a per-system "models bank" of reusable TypeScript model
// interfaces. Endpoints reference a model by name from their request/response (see
// requestModel/responseModel in endpoints.js) instead of an inline {key: type} schema.
//
//   GET    /api/models?system=<id>
//     -> { ok, models: [ { name, ts, description, createdAt, updatedAt } ] }
//   POST   /api/models  { system, name, ts, description }   -> upsert by name
//   DELETE /api/models  { system, name }                    -> remove
//
// Models are system-wide and stored in systems/<id>/models.json. A model's `name`
// is its immutable id (a TypeScript identifier, unique within the system); `ts` is
// raw TypeScript text and may reference other models by name (resolved at
// prompt-build time on the frontend). No docker rebuild — this is pure JSON, like
// the endpoint registry.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'

class HttpError extends Error {
  constructor(status, message) {
    super(message)
    this.statusCode = status
  }
}
const bad = (msg) => new HttpError(400, msg)

// A model name is a TypeScript identifier and doubles as its permanent id.
const MODEL_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
const MAX_NAME = 60
const MAX_TS = 20_000
const MAX_DESC = 2000

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > 50_000) reject(bad('request body too large'))
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

// Read systems/<id>/models.json as { models: [...] }, tolerating an absent/garbled
// file (a system with no bank simply has no models yet).
export function readModelsFile(system) {
  const file = path.join(systemDir(system), 'models.json')
  try {
    const raw = JSON.parse(fs.readFileSync(file, 'utf8'))
    return Array.isArray(raw?.models) ? { models: raw.models } : { models: [] }
  } catch {
    return { models: [] }
  }
}

function writeModelsFile(system, data) {
  const file = path.join(systemDir(system), 'models.json')
  fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n')
}

// Endpoints (across all services) that reference `model` via requestModel/responseModel
// — used to block deleting a model that's still in use. Tolerates an absent registry.
function endpointsReferencing(system, model) {
  const file = path.join(systemDir(system), 'endpoints.json')
  let registry = {}
  try {
    registry = JSON.parse(fs.readFileSync(file, 'utf8')) || {}
  } catch {
    return []
  }
  const refs = []
  for (const [service, list] of Object.entries(registry)) {
    if (!Array.isArray(list)) continue
    for (const e of list) {
      if (e && (e.requestModel === model || e.responseModel === model)) {
        refs.push(`${service} ${e.method} ${e.path}`)
      }
    }
  }
  return refs
}

// Event-stream topics (across all clusters) whose message schema references `model` via
// schemaModel — used to block deleting a model a topic still depends on. Each cluster's
// topics live in systems/<id>/<cluster>/streams.json; refs are labeled "<cluster>/<topic>".
// Tolerates an absent manifest/registry.
function streamsReferencing(system, model) {
  const refs = []
  let manifest
  try {
    manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  } catch {
    return refs
  }
  for (const n of manifest.nodes || []) {
    if (!n || n.origin !== 'create-event-stream') continue
    let raw
    try {
      raw = JSON.parse(fs.readFileSync(path.join(systemDir(system), n.id, 'streams.json'), 'utf8'))
    } catch {
      continue
    }
    for (const t of Array.isArray(raw?.topics) ? raw.topics : []) {
      if (t && t.schemaModel === model) refs.push(`${n.id}/${t.id}`)
    }
  }
  return refs
}

// Where every model in the bank is referenced across the system — the input to the
// "what will be affected if I change these models?" review. For each model name:
//   endpoints: [{ service, method, path, field:'request'|'response' }]  (endpoints.json)
//   databases: [{ id, engine }]                                          (manifest schemaModels)
//   streams:   [{ cluster, topic }]                                      (streams.json schemaModel)
// Every bank model gets an entry (empty arrays if unused). Generalizes
// endpointsReferencing() / streamsReferencing() to all models at once and adds the database
// scan. gRPC and client scenarios don't reference bank models, so they're not scanned.
function usageMap(system) {
  const usage = {}
  for (const m of readModelsFile(system).models) {
    if (m && typeof m.name === 'string') usage[m.name] = { endpoints: [], databases: [], streams: [] }
  }
  // A reference can name a model that was since deleted from the bank; still surface it.
  const ensure = (name) => {
    if (!usage[name]) usage[name] = { endpoints: [], databases: [], streams: [] }
    return usage[name]
  }

  // Endpoints: requestModel / responseModel on each registry record (path is the
  // stored service-local path, e.g. "/payment").
  try {
    const registry = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'endpoints.json'), 'utf8')) || {}
    for (const [service, list] of Object.entries(registry)) {
      if (!Array.isArray(list)) continue
      for (const e of list) {
        if (!e) continue
        if (e.requestModel) ensure(e.requestModel).endpoints.push({ service, method: e.method, path: e.path, field: 'request' })
        if (e.responseModel) ensure(e.responseModel).endpoints.push({ service, method: e.method, path: e.path, field: 'response' })
      }
    }
  } catch {
    /* no endpoint registry yet */
  }

  // Databases: a db node lists the bank models its schema was built from. Event-stream
  // topics: a topic's message schema references a bank model via schemaModel (in the
  // cluster's streams.json). Both are read from the manifest's node list.
  try {
    const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
    for (const n of manifest.nodes || []) {
      if (Array.isArray(n.schemaModels)) {
        for (const name of n.schemaModels) {
          if (typeof name === 'string') ensure(name).databases.push({ id: n.id, engine: n.type })
        }
      }
      if (n && n.origin === 'create-event-stream') {
        let raw
        try {
          raw = JSON.parse(fs.readFileSync(path.join(systemDir(system), n.id, 'streams.json'), 'utf8'))
        } catch {
          continue
        }
        for (const t of Array.isArray(raw?.topics) ? raw.topics : []) {
          if (t && typeof t.schemaModel === 'string' && t.schemaModel) {
            ensure(t.schemaModel).streams.push({ cluster: n.id, topic: t.id, enforce: t.enforceSchema === true })
          }
        }
      }
    }
  } catch {
    /* manifest unreadable — treat as no db/stream references */
  }

  return usage
}

function validateModelInput(body) {
  const name = body.name
  if (typeof name !== 'string' || !MODEL_NAME_RE.test(name) || name.length > MAX_NAME) {
    throw bad('model name must start with a letter or underscore and use only letters, digits and underscores')
  }
  const ts = body.ts
  if (typeof ts !== 'string' || !ts.trim()) throw bad('model definition (TypeScript) is required')
  if (ts.length > MAX_TS) throw bad('model definition is too long')
  const description = typeof body.description === 'string' ? body.description : ''
  if (description.length > MAX_DESC) throw bad('description is too long')
  return { name, ts, description }
}

// Create or replace a model by name. On update the name + createdAt are preserved;
// only ts/description change and updatedAt bumps (the name is an immutable id).
function upsertModel(system, input) {
  const data = readModelsFile(system)
  const now = new Date().toISOString()
  const i = data.models.findIndex((m) => m && m.name === input.name)
  if (i >= 0) {
    data.models[i] = { ...data.models[i], ts: input.ts, description: input.description, updatedAt: now }
  } else {
    data.models.push({ name: input.name, ts: input.ts, description: input.description, createdAt: now, updatedAt: now })
  }
  writeModelsFile(system, data)
  return i >= 0 ? data.models[i] : data.models[data.models.length - 1]
}

// Batch create/replace by name in ONE read+write (the "save multiple model edits"
// flow). Same per-model semantics as upsertModel (preserve name/createdAt, bump
// updatedAt). Returns the resulting records in input order.
function upsertModels(system, inputs) {
  const data = readModelsFile(system)
  const now = new Date().toISOString()
  const out = []
  for (const input of inputs) {
    const i = data.models.findIndex((m) => m && m.name === input.name)
    if (i >= 0) {
      data.models[i] = { ...data.models[i], ts: input.ts, description: input.description, updatedAt: now }
      out.push(data.models[i])
    } else {
      const rec = { name: input.name, ts: input.ts, description: input.description, createdAt: now, updatedAt: now }
      data.models.push(rec)
      out.push(rec)
    }
  }
  writeModelsFile(system, data)
  return out
}

function removeModel(system, name) {
  const refs = [...endpointsReferencing(system, name), ...streamsReferencing(system, name)]
  if (refs.length) {
    throw bad(`model "${name}" is referenced by ${refs.join(', ')} — change those first`)
  }
  const data = readModelsFile(system)
  const next = data.models.filter((m) => m && m.name !== name)
  const removed = next.length !== data.models.length
  writeModelsFile(system, { models: next })
  return removed
}

export default function models() {
  return {
    name: 'models',
    configureServer(server) {
      server.middlewares.use('/api/models', async (req, res, next) => {
        const json = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }

        if (req.method === 'GET') {
          try {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(400, { ok: false, error: 'unknown system' })
            return json(200, { ok: true, models: readModelsFile(system).models })
          } catch (err) {
            return json(500, { ok: false, error: err.message })
          }
        }

        if (req.method === 'POST') {
          try {
            const body = await readJsonBody(req)
            if (!isValidSystem(body.system)) throw bad(`unknown system "${body.system}"`)
            // Batch save: { models: [{name, ts, description}, ...] } — validate all,
            // then a single write. Used when several edited models are saved at once.
            if (Array.isArray(body.models)) {
              if (body.models.length === 0) throw bad('no models to save')
              if (body.models.length > 100) throw bad('too many models in one batch (max 100)')
              const inputs = body.models.map((m) => validateModelInput(m))
              const seen = new Set()
              for (const inp of inputs) {
                if (seen.has(inp.name)) throw bad(`duplicate model "${inp.name}" in batch`)
                seen.add(inp.name)
              }
              const models = upsertModels(body.system, inputs)
              return json(200, { ok: true, models })
            }
            const input = validateModelInput(body)
            const model = upsertModel(body.system, input)
            return json(200, { ok: true, model })
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
        }

        if (req.method === 'DELETE') {
          try {
            const body = await readJsonBody(req)
            if (!isValidSystem(body.system)) throw bad(`unknown system "${body.system}"`)
            if (typeof body.name !== 'string' || !body.name) throw bad('name is required')
            const removed = removeModel(body.system, body.name)
            return json(200, { ok: true, removed })
          } catch (err) {
            return json(err.statusCode || 500, { ok: false, error: err.message })
          }
        }

        return next()
      })

      // GET /api/model-usage?system=<id> -> { ok, usage } where usage maps each model
      // name to the endpoints/databases that reference it. Drives the "what will be
      // affected" review in the models modal.
      server.middlewares.use('/api/model-usage', (req, res, next) => {
        if (req.method !== 'GET') return next()
        const json = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }
        try {
          const system = new URL(req.url, 'http://localhost').searchParams.get('system')
          if (!isValidSystem(system)) return json(400, { ok: false, error: 'unknown system' })
          return json(200, { ok: true, usage: usageMap(system) })
        } catch (err) {
          return json(500, { ok: false, error: err.message })
        }
      })
    },
  }
}
