// Per-client Python scripts: systems/<id>/clients/<module>.py.
//
// A client is an EXTERNAL caller, so its "functions" are now a real Python script that runs
// on the host and calls the system through the load balancer (via the stdlib-only `lb` helper
// in clients/lbclient.py). This module owns the on-disk side of that:
//   - name mapping (client id -> python module/file name),
//   - scaffolding a new client's script + the shared helper (from templates/client/),
//   - cleaning it up on delete,
//   - and STATICALLY inferring each function's call "steps" from the code (no run needed),
//     which keeps the diagram trace (scenarioBank.deriveFunctionTrace) and the delete guard
//     (remove.findDependents) working off the same { method, path } step shape as before.
//
// Execution of a script (python3 <script> --<fn> ...) lives in scenarios.js (the runner); this
// module is pure filesystem + text analysis (no subprocess), so it is cheap to call on every
// GET /api/scenarios poll.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { systemDir } from './systems.js'

const TEMPLATE_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'templates', 'client')

// A client id (NAME_RE: ^[a-z][a-z0-9-]*$) maps to a python module/file name. Hyphens aren't
// legal in a module name so they become underscores; client ids can't contain underscores, so
// the mapping is lossless and unambiguous (mobile-app <-> mobile_app).
export function clientModule(id) {
  return String(id).replace(/-/g, '_')
}
export function clientScriptFile(id) {
  return clientModule(id) + '.py'
}
export function clientsDir(system) {
  return path.join(systemDir(system), 'clients')
}
export function clientScriptPath(system, id) {
  return path.join(clientsDir(system), clientScriptFile(id))
}
// A STATEFUL client's durable store lives beside its script as <module>.state.json (same
// hyphen->underscore module mapping). It's written by lbclient.py at exit only when the runner
// sets LB_CLIENT_STATE to this path (i.e. the client's manifest node has stateful:true); a
// stateless client never creates it. Shape: { values: {...}, history: [...] }.
export function clientStatePath(system, id) {
  return path.join(clientsDir(system), clientModule(id) + '.state.json')
}
// The script source, or null if this client has no script yet (a pre-Python-model client).
export function readClientScript(system, id) {
  try {
    return fs.readFileSync(clientScriptPath(system, id), 'utf8')
  } catch {
    return null
  }
}

// A client's accumulated durable state, or an empty store if it has none yet (stateless client,
// or a stateful one that hasn't run). Tolerates a missing/garbled file — the store is runtime data.
export function readClientState(system, id) {
  let data
  try {
    data = JSON.parse(fs.readFileSync(clientStatePath(system, id), 'utf8'))
  } catch {
    return { values: {}, history: [] }
  }
  return {
    values: data && typeof data.values === 'object' && data.values ? data.values : {},
    history: Array.isArray(data?.history) ? data.history : [],
  }
}

// Create systems/<id>/clients/ with the shared lbclient.py helper and a per-client <module>.py
// scaffolded from the template (filling its __CLIENT__/__MODULE__ placeholders). Never clobbers
// an existing script — re-authoring edits it in place.
export function scaffoldClientScript(system, id) {
  const dir = clientsDir(system)
  fs.mkdirSync(dir, { recursive: true })
  const lib = path.join(dir, 'lbclient.py')
  // Keep the shared helper in lockstep with the template: (re)write it when missing OR drifted
  // (e.g. an older system predating a new lb.* method like `stream`), so every system auto-upgrades
  // on the next client op. It's fixed infra — client sessions never edit it — so overwriting a
  // stale copy is safe.
  const libTemplate = fs.readFileSync(path.join(TEMPLATE_DIR, 'lbclient.py'))
  if (!fs.existsSync(lib) || !fs.readFileSync(lib).equals(libTemplate)) {
    fs.writeFileSync(lib, libTemplate)
  }
  const script = clientScriptPath(system, id)
  if (!fs.existsSync(script)) {
    const tmpl = fs.readFileSync(path.join(TEMPLATE_DIR, 'client.py.tmpl'), 'utf8')
    const filled = tmpl.split('__MODULE__').join(clientModule(id)).split('__CLIENT__').join(id)
    fs.writeFileSync(script, filled)
  }
  return script
}

// Remove a client's script and its durable state file (the shared lbclient.py stays for the
// other clients).
export function removeClientScript(system, id) {
  try {
    fs.rmSync(clientScriptPath(system, id), { force: true })
    fs.rmSync(clientStatePath(system, id), { force: true })
  } catch {
    /* nothing to clean up */
  }
}

// --- static step inference ------------------------------------------------------

function escapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}
// Matches a top-level (column-0) `def <name>(` — client functions are defined at module scope.
function defRegex(name) {
  return new RegExp('^def\\s+' + escapeRe(name) + '\\s*\\(', 'm')
}

// True if the script implements this function (so the runner can refuse to run a not-yet-authored one).
export function hasFunctionDef(src, name) {
  return defRegex(name).test(src || '')
}

// Slice a top-level function's body: from its `def name(...)` line down to (but not including)
// the next column-0 statement. Returns null if the function isn't defined.
function functionBody(src, name) {
  const m = defRegex(name).exec(src || '')
  if (!m) return null
  const lines = src.slice(m.index).split('\n')
  const out = [lines[0]]
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i]
    if (line.trim() && !/^\s/.test(line)) break // a non-blank, non-indented line ends the body
    out.push(line)
  }
  return out.join('\n')
}

// Statically infer a function's call steps by scanning its body for `lb.<method>("/path")`
// calls — across BOTH if/else branches, in source order. An f-string path like
// `f"/svc/{order_id}"` keeps its `{order_id}` segment, which matchEndpoint treats as a wildcard
// (just like a `{param}` route slot). `lb.stream(...)` is an SSE consume, recorded as a GET step
// so it matches the streaming (`protocol: sse`) route, whose method is GET. Returns
// [{ method, path, label }] (label '' — the diagram derives the row from the matched endpoint);
// [] when the function isn't implemented yet.
export function scanFunctionSteps(src, name) {
  const body = functionBody(src, name)
  if (body == null) return []
  const re = /\blb\.(get|post|put|patch|delete|stream)\s*\(\s*(?:rf|fr|f|r)?(["'])((?:[^"'\\]|\\.)*)\2/gi
  const steps = []
  const seen = new Set()
  let m
  while ((m = re.exec(body))) {
    // `stream` is an SSE GET consume — normalize it so the step matches the GET route.
    const verb = m[1].toLowerCase()
    const method = verb === 'stream' ? 'GET' : verb.toUpperCase()
    const p = m[3]
    if (!p.startsWith('/')) continue // not an LB path — skip
    const key = method + ' ' + p
    if (seen.has(key)) continue // collapse a call repeated across branches/loops
    seen.add(key)
    steps.push({ method, path: p, label: '' })
  }
  return steps
}
