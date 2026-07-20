// Shared helpers for per-client "functions" (systems/<id>/scenarios.json). A function is a
// named, argument-taking sequence of HTTP calls ("steps") that a client (external caller) makes
// through the load balancer; each function is owned by one client (identity is (client, name)).
// These helpers (1) build the Claude prompt that authors a function's steps, and (2) derive the
// diagram trace — which services/methods a function calls — from its steps.

import type {
  DiscoveredEndpoint,
  ScenarioArg,
  ScenarioFunction,
  ScenarioStep,
} from './types/registries'

// Stable identity for a discovered endpoint (matches SystemDiagram's `endpointKey`).
function endpointKey(e: { method: string; path: string }): string {
  return `${e.method} ${e.path}`
}

// The function's argument signature as a single line, e.g. "userId: string, qty: number".
export function signatureLine(args?: ScenarioArg[] | null): string {
  return (args || []).map((a) => `${a.name}: ${a.type}`).join(', ') || '(none)'
}

// A client id maps to a python module/file name (hyphens -> underscores); ids can't contain
// underscores, so the mapping is unambiguous (mirrors clientModule in server/clientScript.js).
function clientModuleFile(client: string): string {
  return `${String(client).replace(/-/g, '_')}.py`
}

// The lean prompt seeded into the Claude session that IMPLEMENTS a function in its client's Python
// script. systems/<id>/scenarios.json ALREADY holds the function entry (client/name/args/description);
// the session writes a `def <name>(...)` in systems/<id>/clients/<module>.py (its `steps` are then
// inferred from that code). The repeatable procedure lives in the sandbox-client-scenario skill, so
// this stays short (the terminal slices the positional prompt to 8000 chars): inline only the owner
// client, the script file, the signature, the description, and a capped list of callable endpoints.
export function buildScenarioFunctionPrompt({
  systemId,
  client,
  name,
  args,
  description,
  endpoints,
}: {
  systemId: string
  client: string
  name: string
  args?: ScenarioArg[] | null
  description?: string | null
  endpoints?: DiscoveredEndpoint[] | null
}): string {
  const moduleFile = clientModuleFile(client)
  const argNames = (args || []).map((a) => a.name)
  const cliHint = argNames.map((n) => `<${n}>`).join(' ')
  const MAX_EP = 40
  const all = endpoints || []
  const list = all.slice(0, MAX_EP).map((e) => {
    const alias = e.alias ? `  ${e.alias}` : ''
    const down = e.downstream?.length ? `  → [${e.downstream.join(', ')}]` : ''
    const desc = e.description ? `  — ${String(e.description).slice(0, 80)}` : ''
    return `- ${e.method} ${e.path}${alias}${down}${desc}`
  })
  const more =
    all.length > MAX_EP
      ? `\n- …and ${all.length - MAX_EP} more (read systems/${systemId}/endpoints.json for the full list)`
      : ''
  return [
    `Use the sandbox-client-scenario skill to implement the function "${name}" for client "${client}" in the "${systemId}" system.`,
    ``,
    `Implement it in: systems/${systemId}/clients/${moduleFile}`,
    `It will be invoked as: python3 ${moduleFile} --${name}${cliHint ? ' ' + cliHint : ''}`,
    `Arguments: ${signatureLine(args)}`,
    ``,
    `What it should do:`,
    (description || '').trim() || '(no description given — infer something reasonable)',
    ``,
    `Endpoints you may call (load-balancer paths):`,
    (list.length ? list.join('\n') : `- (none discovered — read systems/${systemId}/endpoints.json)`) + more,
    ``,
    `How to write it:`,
    `- Add a top-level \`def ${name}(${argNames.join(', ')})\` and register it in the FUNCTIONS map.`,
    `- Call the system through the load balancer with the \`lb\` helper — \`r = lb.post("/<service>/<path>", { ... })\``,
    `  (also lb.get/put/patch/delete); each returns the parsed JSON response body.`,
    `- Use REAL control flow: branch on a response (e.g. only call the next endpoint if the first came back`,
    `  valid), loop, and pass one call's result into the next via plain Python variables.`,
    `- Use real load-balancer paths from the list above as string literals (f-strings for path params) so the`,
    `  diagram can statically trace the calls; never invent an endpoint.`,
    `- CLI args arrive as strings; coerce number/boolean args in the body as needed.`,
    ``,
    `Edit ONLY systems/${systemId}/clients/${moduleFile} (never lbclient.py). Pure Python — no docker, no other files.`,
  ].join('\n')
}

function pathSegs(p?: string | null): string[] {
  return (p || '').split('/').filter(Boolean)
}

// Service-local path of a discovered endpoint (whose `.path` is LB-prefixed as `/<service><local>`).
// Mirrors localPathOf in endpointPolicy.js; inlined here to keep this module dependency-light.
function localPathOf(e: DiscoveredEndpoint): string {
  const prefix = `/${e.service}`
  const p = e.path && e.path.startsWith(prefix) ? e.path.slice(prefix.length) : e.path || '/'
  return p.replace(/\/+$/, '') || '/'
}

// Resolve one `downstreamMethods` entry to the discovered endpoint it names on `nodeId`. The
// entry is "METHOD ref" (or just "ref"), where ref is a service-local path, an LB-prefixed path,
// or the endpoint's alias — matched exactly like SystemDiagram lights up called method rows.
function resolveCall(
  nodeId: string,
  call: string,
  endpoints?: DiscoveredEndpoint[] | null,
): DiscoveredEndpoint | null {
  const parts = String(call).trim().split(/\s+/)
  const method = parts.length > 1 ? parts[0].toUpperCase() : ''
  const ref = parts.length > 1 ? parts.slice(1).join(' ') : parts[0]
  return (
    (endpoints || []).find(
      (e) =>
        e.service === nodeId &&
        (!method || e.method === method) &&
        (localPathOf(e) === ref || e.path === ref || e.alias === ref),
    ) || null
  )
}

// Find the discovered endpoint a step hits. Tries an exact "METHOD path" match first, then
// a template match: same method + same segment count, where an endpoint `{param}` slot or a
// step `${token}` segment matches anything (so concrete values / response tokens fill path
// params). Returns the endpoint record or null.
function matchEndpoint(
  step: ScenarioStep,
  endpoints?: DiscoveredEndpoint[] | null,
): DiscoveredEndpoint | null {
  const exact = (endpoints || []).find((e) => endpointKey(e) === `${step.method} ${step.path}`)
  if (exact) return exact
  const sSegs = pathSegs(step.path)
  for (const e of endpoints || []) {
    if (e.method !== step.method) continue
    const eSegs = pathSegs(e.path)
    if (eSegs.length !== sSegs.length) continue
    let ok = true
    for (let i = 0; i < eSegs.length; i++) {
      const es = eSegs[i]
      const ss = sSegs[i]
      const eParam = es.startsWith('{') && es.endsWith('}')
      const sToken = ss.includes('${')
      if (eParam || sToken) continue
      if (es !== ss) {
        ok = false
        break
      }
    }
    if (ok) return e
  }
  return null
}

// What a function call traces on the diagram. The client's own `steps` are the ENTRY points
// (`direct: true`) — client → LB → service, or client → external service directly. From each
// entry we then follow the `downstreamMethods` chain TRANSITIVELY (`direct: false`), so the whole
// call graph a function drives — including everything reached THROUGH an external service — gets
// its method rows lit and its edges drawn (the arrow already reaches the service; this lights the
// exact method it lands on and keeps going). Deduped by endpoint key, which also bounds the walk
// against cycles. Steps with no live match are skipped (they still run, but contribute no trace).
// Returns { client, name, methods:[{service, method, path, downstream, downstreamDescriptions, direct}] }
// where method/path are the DISCOVERED endpoint's (so the diagram's method-row highlight matches by
// key) and downstreamDescriptions (nodeId → text) lets each drawn edge carry its info-popup label.
export interface TraceMethod {
  service: string
  method: string
  path: string
  downstream: string[]
  downstreamDescriptions: Record<string, string>
  direct: boolean
}

export interface FunctionTrace {
  client: string
  name: string | undefined
  methods: TraceMethod[]
}

export function deriveFunctionTrace(
  fn: ScenarioFunction | null | undefined,
  endpoints: DiscoveredEndpoint[] | null | undefined,
  clientId: string,
): FunctionTrace {
  const methods: TraceMethod[] = []
  const seen = new Set<string>()
  const queue: DiscoveredEndpoint[] = []
  const add = (ep: DiscoveredEndpoint, direct: boolean) => {
    const key = endpointKey(ep)
    if (seen.has(key)) return
    seen.add(key)
    methods.push({ service: ep.service, method: ep.method, path: ep.path,
      downstream: ep.downstream || [], downstreamDescriptions: ep.downstreamDescriptions || {}, direct })
    queue.push(ep) // full record — its downstreamMethods drive the next hop
  }
  for (const step of fn?.steps || []) {
    const ep = matchEndpoint(step, endpoints)
    if (ep) add(ep, true)
  }
  while (queue.length) {
    const ep = queue.shift()!
    for (const [nodeId, calls] of Object.entries(ep.downstreamMethods || {})) {
      for (const c of calls || []) {
        const t = resolveCall(nodeId, c, endpoints)
        if (t) add(t, false)
      }
    }
  }
  return { client: clientId, name: fn?.name, methods }
}
