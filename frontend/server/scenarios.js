// Vite dev-server plugin: per-CLIENT "functions" — named, described, argument-taking call
// sequences a client (an external caller) runs against the system through the load balancer.
// Each function is OWNED by exactly one client; there is no shared bank and no attach-by-name.
// Identity is (client, name), so two clients may each have their own `checkout`.
//
// A client's functions are implemented as a REAL Python script, systems/<id>/clients/<module>.py
// (see clientScript.js). scenarios.json stays the registry of each function's metadata
// (client, name, args, description, conversationId, history); its `steps` are no longer
// hand-authored — they are STATICALLY INFERRED from the script's `lb.<method>("/path")` calls so
// the diagram trace and the delete guard keep reading the same { method, path } shape.
//
//   GET    /api/scenarios?system=<id>
//     -> { ok, functions: [{ client, name, args:[{name,type}], description,
//                            steps:[{method,path,label}], conversationId,
//                            createdAt, updatedAt, history }] }
//        Re-infers each function's steps from its client's script (cheap text scan) and persists
//        any change, so the live-polling diagram and remove.findDependents stay current.
//   POST   /api/scenarios  { system, client, name, args, description, conversationId }
//     -> upsert the function SHELL identified by (client, name), and ensure the client's script
//        exists (scaffold it) so the launched Claude session can author the function in code.
//   DELETE /api/scenarios  { system, client, name }
//     -> remove the (client, name) function. Returns { ok, removed }.
//   POST   /api/scenarios/run  { system, client, name, args }
//     -> THE RUNNER: validate the supplied `args` against the function's signature, then execute
//        `python3 systems/<id>/clients/<module>.py --<name> <args...>` on the host. The script
//        makes real calls through the published load balancer (http://localhost:8080) via the
//        stdlib `lb` helper, which prints the calls it made; the runner parses them and returns
//        per-step results (the same shape the diagram's Run panel always rendered).
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { systemDir, isValidSystem } from './systems.js'
import { bad, readJsonBody } from './scaffold.js'
import {
  clientScriptPath,
  clientScriptFile,
  readClientScript,
  scaffoldClientScript,
  scanFunctionSteps,
  hasFunctionDef,
} from './clientScript.js'

const pexec = promisify(execFile)

// One line lbclient.py prints at exit carrying the JSON array of calls the run made.
const RESULT_SENTINEL = '__LB_RESULTS__'
const RUN_TIMEOUT_MS = 30_000

// A function name (and each argument name) is a code-style identifier; together with its owner
// client, the name is the function's permanent id (unique per client).
const FUNCTION_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
const ARG_TYPES = new Set(['string', 'number', 'boolean'])
const MAX_NAME = 60
const MAX_ARGS = 20
const MAX_DESC = 4000

// --- registry (systems/<id>/scenarios.json) -------------------------------------

function scenariosFile(system) {
  return path.join(systemDir(system), 'scenarios.json')
}
// Tolerate an absent/garbled file (a system with no functions yet has an empty list).
function readScenarios(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(scenariosFile(system), 'utf8'))
    return Array.isArray(raw?.functions) ? { functions: raw.functions } : { functions: [] }
  } catch {
    return { functions: [] }
  }
}
function writeScenarios(system, data) {
  fs.writeFileSync(scenariosFile(system), JSON.stringify(data, null, 2) + '\n')
}
function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}

// --- cascade: rewrite a renamed endpoint's path in client scripts -----------------
//
// Called by the endpoint-rename flow (endpoints.js) so a client's authored code keeps hitting
// the same route after its path changes. Steps are inferred from code, so the source of truth is
// the script text — do a literal swap of the old LB path for the new one in every client script.
// A param/template rename (the local path carries a `{param}` slot) won't textually match an
// f-string literal like `f"/svc/{order_id}"`, so it's safely skipped for the user to fix by hand.
export function renameStepPaths(system, ownerService, _method, oldLocalPath, newLocalPath) {
  const oldLb = `/${ownerService}${oldLocalPath}`
  const newLb = `/${ownerService}${newLocalPath}`
  if (oldLb === newLb) return { changed: 0, warnings: [] }
  let manifest
  try {
    manifest = readManifest(system)
  } catch {
    return { changed: 0, warnings: [] }
  }
  const clientIds = manifest.nodes.filter((n) => n.type === 'client').map((n) => n.id)
  let changed = 0
  for (const id of clientIds) {
    const p = clientScriptPath(system, id)
    let src
    try {
      src = fs.readFileSync(p, 'utf8')
    } catch {
      continue
    }
    if (!src.includes(oldLb)) continue
    const next = src.split(oldLb).join(newLb)
    if (next !== src) {
      fs.writeFileSync(p, next)
      changed++
    }
  }
  return { changed, warnings: [] }
}

// --- validation -----------------------------------------------------------------

function validateFunctionInput(body) {
  const name = body.name
  if (typeof name !== 'string' || !FUNCTION_NAME_RE.test(name) || name.length > MAX_NAME) {
    throw bad('function name must start with a letter or underscore and use only letters, digits and underscores')
  }
  const rawArgs = body.args == null ? [] : body.args
  if (!Array.isArray(rawArgs)) throw bad('args must be an array')
  if (rawArgs.length > MAX_ARGS) throw bad(`a function can have at most ${MAX_ARGS} arguments`)
  const args = []
  const seen = new Set()
  for (const a of rawArgs) {
    const an = a?.name
    if (typeof an !== 'string' || !FUNCTION_NAME_RE.test(an) || an.length > MAX_NAME) {
      throw bad(`argument name "${an}" must be a valid identifier`)
    }
    if (seen.has(an)) throw bad(`duplicate argument "${an}"`)
    seen.add(an)
    const type = a?.type
    if (!ARG_TYPES.has(type)) throw bad(`argument "${an}" has unsupported type "${type}"`)
    args.push({ name: an, type })
  }
  const description = typeof body.description === 'string' ? body.description : ''
  if (description.length > MAX_DESC) throw bad('description is too long')
  const conversationId = typeof body.conversationId === 'string' ? body.conversationId : ''
  return { name, args, description, conversationId }
}

// Coerce this run's supplied argument values to the function's declared types (rejecting a
// missing one), then they go to the script as CLI strings. Coercion still validates: a non-numeric
// value for a number arg is caught here rather than confusing the script.
function coerceArgs(signature, provided) {
  const out = {}
  for (const a of signature) {
    if (!provided || !(a.name in provided)) throw bad(`missing argument "${a.name}"`)
    const v = provided[a.name]
    if (a.type === 'number') {
      const n = Number(v)
      if (Number.isNaN(n)) throw bad(`argument "${a.name}" must be a number`)
      out[a.name] = n
    } else if (a.type === 'boolean') {
      out[a.name] = v === true || v === 'true'
    } else {
      out[a.name] = v == null ? '' : String(v)
    }
  }
  return out
}

// A typed argument value as the single CLI token the script receives.
function stringifyArg(v) {
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  return String(v)
}

// --- operations -----------------------------------------------------------------

// Only a client owns + runs functions — the Functions "trigger bank" is client-only. (An external
// service still calls into the system, but through its own endpoints' `downstream` in endpoints.json,
// not through a function here — so it owns no functions.)
function findClientNode(manifest, id) {
  return manifest.nodes.find((n) => n.id === id && n.type === 'client')
}

// Return every client's functions, re-inferring each one's `steps` from its client's script and
// persisting any change. A function whose client has NO script (a pre-Python-model system) keeps
// its stored steps untouched; once a script exists, the code is the source of truth (a function
// not yet implemented there reads as having no steps — "pending").
function listFunctions(system) {
  const data = readScenarios(system)
  const srcCache = {}
  let changed = false
  for (const fn of data.functions) {
    if (!fn || !fn.client) continue
    if (!(fn.client in srcCache)) srcCache[fn.client] = readClientScript(system, fn.client)
    const src = srcCache[fn.client]
    if (src == null) continue // no script yet — leave the stored steps as-is
    const steps = scanFunctionSteps(src, fn.name)
    if (JSON.stringify(fn.steps || []) !== JSON.stringify(steps)) {
      fn.steps = steps
      changed = true
    }
  }
  if (changed) writeScenarios(system, data)
  return { ok: true, functions: data.functions }
}

// Create or replace a function shell identified by (client, name). The client + name + createdAt
// are preserved on update; args/description/conversationId change and updatedAt bumps. Also ensure
// the client's Python script exists so the launched session can author this function in code.
function upsertFunction(body) {
  if (!isValidSystem(body.system)) throw bad(`unknown system "${body.system}"`)
  const client = body.client
  const manifest = readManifest(body.system)
  if (!findClientNode(manifest, client)) throw bad(`"${client}" is not a client in this system`)
  const input = validateFunctionInput(body)
  scaffoldClientScript(body.system, client) // make sure clients/<module>.py + lbclient.py exist
  const data = readScenarios(body.system)
  const now = new Date().toISOString()
  const snapshot = { at: now, description: input.description, args: input.args }
  const i = data.functions.findIndex((f) => f && f.client === client && f.name === input.name)
  let fn
  if (i >= 0) {
    const prev = data.functions[i]
    const history = Array.isArray(prev.history) ? prev.history : []
    fn = {
      ...prev,
      client,
      args: input.args,
      description: input.description,
      conversationId: input.conversationId || prev.conversationId || '',
      steps: Array.isArray(prev.steps) ? prev.steps : [],
      updatedAt: now,
      history: [...history, snapshot],
    }
    data.functions[i] = fn
  } else {
    fn = {
      client,
      name: input.name,
      args: input.args,
      description: input.description,
      steps: [],
      conversationId: input.conversationId || '',
      createdAt: now,
      updatedAt: now,
      history: [snapshot],
    }
    data.functions.push(fn)
  }
  writeScenarios(body.system, data)
  return { ok: true, function: fn }
}

// Remove the (client, name) function from its owner client.
function deleteFunction(body) {
  const { system, client, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof client !== 'string' || !client) throw bad('client is required')
  if (typeof name !== 'string' || !name) throw bad('name is required')
  const data = readScenarios(system)
  const next = data.functions.filter((f) => !(f && f.client === client && f.name === name))
  const removed = next.length !== data.functions.length
  writeScenarios(system, { functions: next })
  return { ok: true, removed }
}

// The last non-blank line of some text (a Python traceback puts the exception there).
function lastLine(text) {
  const lines = String(text || '')
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
  return lines[lines.length - 1] || ''
}

// Pull the JSON array of calls out of the script's stdout (the last sentinel line lbclient emits).
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

async function runFunction(body) {
  const { system, client, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  // The owner client is part of the function's identity — validate it.
  const manifest = readManifest(system)
  if (!findClientNode(manifest, client)) throw bad(`"${client}" is not a client in this system`)
  const fn = readScenarios(system).functions.find((f) => f && f.client === client && f.name === name)
  if (!fn) throw bad(`unknown function "${name}"`)

  const src = readClientScript(system, client)
  if (src == null) {
    throw bad(`client "${client}" has no script yet — open this function and (re)author it under the Python-client model`)
  }
  if (!hasFunctionDef(src, name)) {
    throw bad(`"${name}" isn't implemented in clients/${clientScriptFile(client)} yet — Resume its session to author it`)
  }

  const typed = coerceArgs(fn.args || [], body.args || {})
  const argv = (fn.args || []).map((a) => stringifyArg(typed[a.name]))
  const scriptPath = clientScriptPath(system, client)

  let stdout = ''
  let scriptError = null
  try {
    const r = await pexec('python3', [scriptPath, '--' + name, ...argv], {
      cwd: systemDir(system),
      timeout: RUN_TIMEOUT_MS,
      maxBuffer: 8 * 1024 * 1024,
    })
    stdout = r.stdout || ''
  } catch (err) {
    if (err.code === 'ENOENT') {
      throw bad('python3 was not found on the host — install Python 3 to run client functions')
    }
    stdout = err.stdout || ''
    scriptError = err.killed ? 'the script timed out' : lastLine(err.stderr) || err.message
  }

  const calls = parseRunResults(stdout)
  if (calls == null) {
    throw bad(scriptError || 'the client script produced no results — check the script')
  }

  const out = calls.map((c, i) => {
    const row = {
      step: i + 1,
      method: c.method,
      path: c.path,
      label: '',
      sentBody: c.sentBody ?? null,
      status: c.status,
      ok: c.ok,
      response: c.response,
    }
    if (!c.ok && c.status === 0) row.error = scriptError || 'request failed'
    return row
  })
  // A crash that wasn't a recorded call (e.g. a KeyError after some calls) is otherwise invisible —
  // surface it as a trailing error row.
  if (scriptError && (calls.length === 0 || calls[calls.length - 1].ok)) {
    out.push({ step: out.length + 1, method: '', path: '(script error)', label: '', sentBody: null, status: 0, ok: false, error: scriptError })
  }
  return { ok: true, results: out }
}

export default function scenarios() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  const wrap = (fn) => async (req, res) => {
    try {
      json(res, 200, await fn(await readJsonBody(req)))
    } catch (err) {
      json(res, err.statusCode || 500, { ok: false, error: err.message })
    }
  }
  return {
    name: 'scenarios',
    configureServer(server) {
      server.middlewares.use('/api/scenarios/run', (req, res, next) =>
        req.method === 'POST' ? wrap(runFunction)(req, res) : next(),
      )
      server.middlewares.use('/api/scenarios', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(res, 400, { ok: false, error: 'unknown system' })
            return json(res, 200, listFunctions(system))
          }
          if (req.method === 'POST') return wrap(upsertFunction)(req, res)
          if (req.method === 'DELETE') return wrap(deleteFunction)(req, res)
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
