import { useCallback, useEffect, useState } from 'react'
import type { ChangeEvent, KeyboardEvent, ReactElement } from 'react'
import { endpointPolicy } from './endpointPolicy'
import { resolveModelTs } from './modelBank'
import type { ManifestNode } from './types/manifest'
import type { DiscoveredEndpoint, EndpointHistoryEntry, ModelRecord } from './types/registries'
import type { LaunchSession } from './types/customTypes'

/**
 * Per-service endpoint manager. Lists a service's endpoints and lets the user add
 * a new one: it captures an optional request/response schema (key -> type) and a
 * natural-language description, persists the endpoint to the registry via
 * POST /api/endpoints (with a freshly generated Claude session id), then asks the
 * parent to launch that Claude session in the terminal to implement it. Endpoints
 * that already carry a session id can be Resumed to edit later.
 */

const METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']

// `sse` is a streaming HTTP endpoint (a GET that serves `text/event-stream`); the seed prompt
// gets an SSE guidance block (see buildEndpointPrompt) so the launched session authors a
// StreamingResponse handler. Protocol is immutable on edit — switch by delete + re-add.
const PROTOCOLS = [
  { value: 'http', label: 'HTTP/S' },
  { value: 'sse', label: 'SSE (text/event-stream)' },
]

// The add/edit form's fields (all text inputs except the live-saved internal flag).
interface EndpointForm {
  method: string
  path: string
  protocol: string
  alias: string
  request: string
  response: string
  requestModel: string
  responseModel: string
  description: string
  internal: boolean
}

// The string-valued form fields the generic `set(k)` change handler may target.
type EndpointFormTextKey = Exclude<keyof EndpointForm, 'internal'>

function blankForm(): EndpointForm {
  return {
    method: 'GET',
    // The part AFTER the fixed `/<service>/` prefix shown in the form (no leading
    // slash). The full routed path is built on submit as `/<service>/<path>`.
    path: '',
    protocol: 'http',
    // Optional function-name alias for the endpoint (unique within the service).
    alias: '',
    // Inline {key: type} schema text. Ignored for a field when a model is referenced.
    request: '',
    response: '',
    // Optional reference to a model in the bank (a model name, or ''). When set the
    // field uses that model instead of the inline schema above.
    requestModel: '',
    responseModel: '',
    description: '',
    // When true, this route is kept off the load balancer's advertised surface (still
    // served for service-to-service calls). Toggled immediately on edit — see
    // toggleInternal — not part of the rebuild record.
    internal: false,
  }
}

// A history entry's ISO timestamp -> a short, local, human label (best-effort).
function fmtAt(at?: string): string {
  if (!at) return ''
  const d = new Date(at)
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleString()
}

// A flat {key: type} schema map (the inline request/response form value).
type SchemaMap = Record<string, string>

type SchemaParse = { value: SchemaMap; error?: undefined } | { value?: undefined; error: string }

// Parse a "key -> type" schema textarea. Empty -> {}. Must be a flat JSON object
// of string values. Returns { value } or { error }.
function parseSchema(text: string, label: string): SchemaParse {
  const t = text.trim()
  if (!t) return { value: {} }
  let obj: unknown
  try {
    obj = JSON.parse(t)
  } catch {
    return { error: `${label} must be valid JSON` }
  }
  if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
    return { error: `${label} must be a JSON object, e.g. {"name":"string"}` }
  }
  for (const v of Object.values(obj)) {
    if (typeof v !== 'string') return { error: `${label} values must be type names (strings)` }
  }
  return { value: obj as SchemaMap }
}

// Extra lines appended to the seed prompt when the endpoint's protocol is SSE, so the launched
// session authors (or keeps) a streaming `text/event-stream` handler. The full procedure lives in
// the sandbox-endpoint skill's SSE section — this just flags it and names the load-bearing rules.
// Returns [] for non-SSE endpoints (spread into the prompt array, so it disappears cleanly).
function sseGuidance(protocol: string): string[] {
  if (protocol !== 'sse') return []
  return [
    ``,
    'This is a Server-Sent Events (SSE) endpoint that streams `text/event-stream`. Author/keep it',
    "as a STREAMING route per the sandbox-endpoint skill's SSE section:",
    '- return StreamingResponse(<async generator>, media_type="text/event-stream") (from',
    '  fastapi.responses; no new dependency — do NOT add sse-starlette).',
    '- set response headers {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"} so the lb',
    "  (nginx) doesn't buffer the stream.",
    '- emit each event as `data: <json>\\n\\n` (the response schema/model above describes one',
    "  event's data payload); optionally include `event:`/`id:` lines.",
    '- make the generator BOUNDED (a finite event count or a capped asyncio.sleep loop) so it',
    '  terminates — never rely solely on client-disconnect detection.',
    '- keep the metrics middleware and the other routes untouched.',
  ]
}

// Compose the prompt seeded into the Claude session — the structured spec plus the
// user's description. The repeatable "how to build in this sandbox" procedure lives
// in the `sandbox-endpoint` skill (.claude/skills/), so we just point Claude at it.
// A request/response that references a model inlines that model's TypeScript (with
// its transitive deps) instead of the flat key->type line.
function buildEndpointPrompt({ systemId, service, method, path, protocol, alias, request, response, requestModel, responseModel, description, models }: {
  systemId: string
  service: string
  method: string
  path: string
  protocol: string
  alias: string
  request: SchemaMap
  response: SchemaMap
  requestModel: string
  responseModel: string
  description: string
  models: ModelRecord[]
}): string {
  const schema = (o: SchemaMap) => (Object.keys(o).length ? JSON.stringify(o) : 'none')
  const bodyLine = (label: string, model: string, inline: SchemaMap) =>
    model
      ? `${label} body type — model "${model}" (TypeScript):\n\`\`\`ts\n${resolveModelTs(model, models)}\n\`\`\``
      : `${label} body schema (key -> type): ${schema(inline)}`
  return [
    `Use the sandbox-endpoint skill to add/update an HTTP endpoint in the "${systemId}" system.`,
    ``,
    `Service: ${service}`,
    `Route inside the service: ${method} ${path}`,
    `Routed through the load balancer as: ${method} /${service}${path}  (protocol: ${protocol})`,
    alias ? `Function name (alias): ${alias}` : `Function name (alias): none`,
    bodyLine('Request', requestModel, request),
    bodyLine('Response', responseModel, response),
    ...sseGuidance(protocol),
    ``,
    `What it should do:`,
    description.trim() || '(no description given — infer something reasonable)',
    ``,
    `Also set this endpoint's \`downstream\` and a brief \`downstreamDescriptions\` map (one short`,
    `line per downstream node id) in systems/${systemId}/endpoints.json, per the skill. For every`,
    `downstream that is a service/external service, also set \`downstreamMethods\` (node id ->`,
    `["METHOD /path", …], service-local paths) listing the exact routes this endpoint calls there.`,
  ].join('\n')
}

// Prompt seeded into the Claude session when EDITING an existing endpoint. Unlike the add
// prompt it tells Claude the handler already exists and to read + modify it IN PLACE (not
// rebuild), and it separates the endpoint's existing behavior from the new change so an
// incremental edit stays incremental. The durable "edit in place" discipline also lives in
// the sandbox-endpoint skill, so it survives a Resume (which seeds no prompt).
function buildEndpointEditPrompt({ systemId, service, method, path, protocol, alias, request, response, requestModel, responseModel, priorDescription, newDescription, schemaChanged, models }: {
  systemId: string
  service: string
  method: string
  path: string
  protocol: string
  alias: string
  request: SchemaMap
  response: SchemaMap
  requestModel: string
  responseModel: string
  priorDescription: string
  newDescription: string
  schemaChanged: boolean
  models: ModelRecord[]
}): string {
  const schema = (o: SchemaMap) => (Object.keys(o).length ? JSON.stringify(o) : 'none')
  const bodyLine = (label: string, model: string, inline: SchemaMap) =>
    model
      ? `${label} body type — model "${model}" (TypeScript):\n\`\`\`ts\n${resolveModelTs(model, models)}\n\`\`\``
      : `${label} body schema (key -> type): ${schema(inline)}`
  const prior = (priorDescription || '').trim()
  const change = (newDescription || '').trim()
  return [
    `Use the sandbox-endpoint skill to UPDATE an existing HTTP endpoint in the "${systemId}" system.`,
    ``,
    `This endpoint ALREADY EXISTS and is implemented. FIRST read the current handler in`,
    `systems/${systemId}/${service}/app.py, then MODIFY it in place to apply the change below.`,
    `Do NOT rewrite the handler from scratch: preserve behavior the change doesn't mention, and`,
    `keep the metrics middleware and the other routes untouched.`,
    ``,
    `Service: ${service}`,
    `Route inside the service: ${method} ${path}`,
    `Routed through the load balancer as: ${method} /${service}${path}  (protocol: ${protocol})`,
    alias ? `Function name (alias): ${alias}` : `Function name (alias): none`,
    bodyLine('Request', requestModel, request),
    bodyLine('Response', responseModel, response),
    ...sseGuidance(protocol),
    ...(schemaChanged
      ? [`NOTE: the request/response contract above was just changed in this edit — reconcile the handler with it.`]
      : []),
    ``,
    `Current behavior (existing description):`,
    prior || '(none recorded)',
    ``,
    `Change to apply:`,
    change ||
      '(no new description — make only the changes the updated request/response contract above requires, leaving all other behavior intact)',
    ``,
    `Also keep this endpoint's \`downstream\`, \`downstreamDescriptions\` map, and`,
    `\`downstreamMethods\` map (node id -> ["METHOD /path", …], service-local, for each`,
    `service/external-service downstream) in systems/${systemId}/endpoints.json accurate for any`,
    `connection or downstream call the change adds or removes.`,
  ].join('\n')
}

// Prompt seeded into the Claude session when an endpoint's PATH is renamed. The mechanical
// cascade (registry record, downstreamMethods, client steps, internal nginx) has already run
// server-side; this session only does the CODE half: move the owner's route decorator and each
// caller's outbound call URL, optionally apply a concurrent contract/description change, then
// rebuild the owner + callers. It partitions owner vs caller edits so behavior never leaks into
// callers, and scopes the change to the exact route (not prefix-sibling routes). The alias is
// registry-only, so it's mentioned but is NOT a code change.
function buildEndpointRenamePrompt({ systemId, service, method, oldPath, newPath, alias, callers, request, response, requestModel, responseModel, priorDescription, newDescription, schemaChanged, models }: {
  systemId: string
  service: string
  method: string
  oldPath: string
  newPath: string
  alias: string
  callers: string[]
  request: SchemaMap
  response: SchemaMap
  requestModel: string
  responseModel: string
  priorDescription: string
  newDescription: string
  schemaChanged: boolean
  models: ModelRecord[]
}): string {
  const schema = (o: SchemaMap) => (Object.keys(o).length ? JSON.stringify(o) : 'none')
  const bodyLine = (label: string, model: string, inline: SchemaMap) =>
    model
      ? `${label} body type — model "${model}" (TypeScript):\n\`\`\`ts\n${resolveModelTs(model, models)}\n\`\`\``
      : `${label} body schema (key -> type): ${schema(inline)}`
  const prior = (priorDescription || '').trim()
  const change = (newDescription || '').trim()
  const callerList = (callers || []).filter((c) => c !== service)
  const oldLocal = oldPath.replace(/^\//, '')
  const newLocal = newPath.replace(/^\//, '')
  return [
    `Use the sandbox-endpoint skill to RENAME an existing endpoint's PATH in the "${systemId}" system.`,
    ``,
    `The endpoints.json registry, downstreamMethods, and client-function steps have ALREADY been`,
    `updated by the app — do NOT edit those. Your job is the code edits + the rebuild.`,
    ``,
    `Owner service: ${service}`,
    `Rename the route: ${method} ${oldPath}  ->  ${method} ${newPath}`,
    `Function name (alias): ${alias}  (registry-only — there is NO code change for the alias)`,
    ``,
    `In the owner (${service}): edit systems/${systemId}/${service}/app.py and change ONLY this`,
    `route's decorator path from "${oldPath}" to "${newPath}" (keep the handler logic and function`,
    `name); fix its own comments/docstrings that mention the old path. Do NOT touch sibling routes`,
    `that merely share the prefix (e.g. "${oldPath}/...").`,
    ...(callerList.length
      ? [
          ``,
          `These services CALL this route and must be updated too — in EACH, edit`,
          `systems/${systemId}/<caller>/app.py and change ONLY the outbound call URL for this route`,
          `from ".../${oldLocal}" to ".../${newLocal}" (the call targets http://${service}:8000${newPath}).`,
          `Do not change anything else about the callers' behavior:`,
          ...callerList.map((c) => `  - ${c}`),
        ]
      : [``, `(No other service currently calls this route.)`]),
    ...(schemaChanged || change
      ? [
          ``,
          `This save ALSO changed the contract/behavior — apply it to the OWNER handler only:`,
          bodyLine('Request', requestModel, request),
          bodyLine('Response', responseModel, response),
          ...(schemaChanged ? [`NOTE: reconcile the owner handler with the request/response above.`] : []),
          ``,
          `Current behavior (existing description):`,
          prior || '(none recorded)',
          ``,
          `Change to apply:`,
          change || '(no new description — just the contract change above)',
        ]
      : []),
    ``,
    `Then rebuild the owner and every caller, then verify:`,
    `  docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${[service, ...callerList].join(' ')}`,
    `Verify ${method} /${service}${newPath} responds through the lb and /${service}${oldPath} is gone (404).`,
  ].join('\n')
}

// Prompt for the "Update descriptions" button: (re)generate ONLY this endpoint's
// per-downstream connection metadata — the `downstreamDescriptions` text map AND the
// `downstreamMethods` call map — a pure endpoints.json edit, no code change and no rebuild.
// This is also the backfill path for endpoints created before downstreamMethods existed.
// The procedure lives in the sandbox-endpoint skill.
function buildDescriptionsPrompt({ systemId, service, method, path, alias, downstream }: {
  systemId: string
  service: string
  method: string
  path: string
  alias: string
  downstream: string[]
}): string {
  const list = (downstream || []).length ? downstream.join(', ') : '(none)'
  return [
    `Use the sandbox-endpoint skill to UPDATE the connection metadata for an HTTP endpoint`,
    `in the "${systemId}" system.`,
    ``,
    `Service: ${service}`,
    `Route inside the service: ${method} ${path}`,
    alias ? `Function name (alias): ${alias}` : `Function name (alias): none`,
    `Downstream nodes: ${list}`,
    ``,
    `Edit ONLY this endpoint's \`downstreamDescriptions\` and \`downstreamMethods\` maps in`,
    `systems/${systemId}/endpoints.json. For \`downstreamDescriptions\`, write one brief line per`,
    `downstream id describing what the handler uses that connection for. For \`downstreamMethods\`,`,
    `write, for every downstream that is a service/external service, the exact routes this endpoint`,
    `calls there (node id -> ["METHOD /path", …], service-local paths). Read the handler in`,
    `systems/${systemId}/${service}/app.py to be accurate. Do NOT modify app.py or nginx, and do`,
    `NOT rebuild — this is a pure JSON edit.`,
  ].join('\n')
}

// Prompt for deleting a live endpoint: its registry entry is already removed by the
// time this runs (see remove()); Claude only needs to strip the route from app.py and
// rebuild. The procedure lives in the sandbox-endpoint skill.
function buildDeletePrompt({ systemId, service, method, path }: {
  systemId: string
  service: string
  method: string
  path: string
}): string {
  return [
    `Use the sandbox-endpoint skill to DELETE an HTTP endpoint from the "${systemId}" system.`,
    ``,
    `Service: ${service}`,
    `Route to remove: ${method} ${path}`,
    `Routed through the load balancer as: ${method} /${service}${path}`,
    ``,
    `Its systems/${systemId}/endpoints.json entry has already been removed; remove the`,
    `route and its handler from the service code, rebuild the service, and verify the`,
    `route is gone.`,
  ].join('\n')
}

// The service-local path (what the registry/backend key on). `/api/endpoints`
// returns the LB-prefixed path `/<service><local>`, so strip the prefix back off.
function localPath(e: DiscoveredEndpoint): string {
  const prefix = `/${e.service}`
  return e.path.startsWith(prefix) ? e.path.slice(prefix.length) || '/' : e.path
}

function schemaToText(obj?: Record<string, unknown> | null): string {
  return obj && Object.keys(obj).length ? JSON.stringify(obj) : ''
}

// When editing, a new Describe entry is APPENDED to the endpoint's existing
// description rather than replacing it (an empty entry leaves it unchanged), so the
// description accumulates over successive edits.
function joinDescription(base?: string | null, addition?: string | null): string {
  const b = (base || '').trim()
  const a = (addition || '').trim()
  if (!b) return a
  if (!a) return b
  return `${b}\n\n${a}`
}

// One history snapshot's request/response in the exact form the changelog shows it: a
// model reference wins over the inline schema, and an empty schema renders as a dash.
// Diffing on this (rather than the raw request/requestModel pair) treats model↔inline
// and inline↔empty transitions as a single "changed" signal.
function bodyText(h: EndpointHistoryEntry, kind: 'request' | 'response'): string {
  return kind === 'request'
    ? h.requestModel || schemaToText(h.request) || '—'
    : h.responseModel || schemaToText(h.response) || '—'
}

// Reduce a history snapshot to just what changed versus the previous one, so the trail
// reads like a changelog instead of repeating the full spec each row. The first snapshot
// (prev == null) is the endpoint's creation. Descriptions accumulate by append
// (joinDescription), so a later description is the previous one plus "\n\n<chunk>" — we
// surface only that appended chunk. The slice math runs on the RAW stored strings (trim
// only the extracted chunk, never `prev` first — that would corrupt the offset).
interface InitialHistoryDiff {
  initial: true
  alias: string
  path: string
  request: string
  response: string
  description: string
}

interface ChangeHistoryDiff {
  initial?: false
  description?: string
  descriptionReplaced?: boolean
  path?: { from: string; to: string }
  alias?: { from: string; to: string }
  request?: { from: string; to: string }
  response?: { from: string; to: string }
  empty?: boolean
}

type HistoryDiff = InitialHistoryDiff | ChangeHistoryDiff

function diffHistoryEntry(curr: EndpointHistoryEntry, prev: EndpointHistoryEntry | null): HistoryDiff {
  if (!prev) {
    return {
      initial: true,
      alias: curr.alias || '',
      path: curr.path || '',
      request: bodyText(curr, 'request'),
      response: bodyText(curr, 'response'),
      description: (curr.description || '').trim(),
    }
  }
  const diff: ChangeHistoryDiff = {}
  const cd = curr.description || ''
  const pd = prev.description || ''
  if (cd !== pd) {
    diff.description = (cd.startsWith(pd) ? cd.slice(pd.length) : cd).trim()
    diff.descriptionReplaced = !cd.startsWith(pd)
  }
  // `path` was added to snapshots later; an older snapshot lacking it falls back to '—'.
  if ((curr.path || '') !== (prev.path || '')) {
    diff.path = { from: prev.path || '—', to: curr.path || '—' }
  }
  if ((curr.alias || '') !== (prev.alias || '')) {
    diff.alias = { from: prev.alias || '—', to: curr.alias || '—' }
  }
  const req = [bodyText(prev, 'request'), bodyText(curr, 'request')]
  if (req[0] !== req[1]) diff.request = { from: req[0], to: req[1] }
  const res = [bodyText(prev, 'response'), bodyText(curr, 'response')]
  if (res[0] !== res[1]) diff.response = { from: res[0], to: res[1] }
  diff.empty = !diff.description && !diff.path && !diff.alias && !diff.request && !diff.response
  return diff
}

// The endpoint identity captured when an edit starts (service-local path).
interface EditingOriginal {
  method: string
  path: string
  alias: string
}

// The code-affecting spec fields as the edit form first showed them.
interface EditingBaseline {
  request: string
  response: string
  requestModel: string
  responseModel: string
}

interface EndpointsModalProps {
  systemId: string
  service: string
  node: ManifestNode
  onClose: () => void
  onLaunch: LaunchSession
  embedded?: boolean
  onBusyChange?: (busy: boolean) => void
}

export default function EndpointsModal({ systemId, service, node, onClose, onLaunch, embedded = false, onBusyChange }: EndpointsModalProps) {
  const [endpoints, setEndpoints] = useState<DiscoveredEndpoint[] | null>(null) // null = loading
  const [models, setModels] = useState<ModelRecord[]>([]) // the system's models bank (for the ref dropdowns)
  const [adding, setAdding] = useState(false)
  const [editingOriginal, setEditingOriginal] = useState<EditingOriginal | null>(null) // { method, path, alias } when editing
  const [editingHistory, setEditingHistory] = useState<EndpointHistoryEntry[]>([]) // saved update snapshots (read-only)
  const [editingDescription, setEditingDescription] = useState('') // current accumulated description (read-only); new entries append to it
  const [editingDownstream, setEditingDownstream] = useState<string[]>([]) // downstream node ids of the endpoint being edited
  const [editingDownstreamDescriptions, setEditingDownstreamDescriptions] = useState<Record<string, string>>({}) // node id -> connection description (read-only)
  const [editingBaseline, setEditingBaseline] = useState<EditingBaseline | null>(null) // original request/response spec (text form) for change detection
  const [form, setForm] = useState<EndpointForm>(blankForm)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmKey, setConfirmKey] = useState<string | null>(null) // row pending delete confirm

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const loadEndpoints = useCallback(() => {
    return fetch(`/api/endpoints?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json() as Promise<{ endpoints?: DiscoveredEndpoint[] }>)
      .then((d) => setEndpoints((d.endpoints || []).filter((e) => e.service === service)))
      .catch(() => setEndpoints([]))
  }, [systemId, service])

  useEffect(() => {
    loadEndpoints()
  }, [loadEndpoints])

  // The models bank powers the request/response "reference a model" dropdowns.
  useEffect(() => {
    fetch(`/api/models?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json() as Promise<{ models?: ModelRecord[] }>)
      .then((d) => setModels(Array.isArray(d.models) ? d.models : []))
      .catch(() => setModels([]))
  }, [systemId])

  const set = (k: EndpointFormTextKey) => (e: ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) =>
    setForm((f) => ({ ...f, [k]: e.target.value }))

  const editing = editingOriginal !== null

  // During an edit, whether anything that requires regenerating the handler changed:
  // a new line typed into "Add to description", or an edited request/response schema or
  // model reference. Method/protocol stay immutable while editing; the `internal` flag is
  // saved live by toggleInternal (a pure nginx/registry edit, never a rebuild). When none of
  // these changed (and it's not a rename), Save needs no Claude session — it just closes.
  // (Adding always needs a session, so dirtyCode is true unless we're editing.)
  // Whether the request/response contract (inline schema or model ref) changed during this
  // edit — also handed to the launched session so it knows to reconcile the handler.
  const schemaChanged =
    !editingBaseline ||
    form.request.trim() !== editingBaseline.request.trim() ||
    form.response.trim() !== editingBaseline.response.trim() ||
    form.requestModel !== editingBaseline.requestModel ||
    form.responseModel !== editingBaseline.responseModel
  const dirtyCode = !editing || form.description.trim() !== '' || schemaChanged

  // Path and alias are editable on an existing endpoint; changing either is a RENAME. The
  // mechanical cascade (registry, downstreamMethods, client steps, internal nginx) runs in the
  // PUT /api/endpoints flow; a PATH change additionally needs a Claude session to move the
  // owner's route decorator and every caller's call URL, then rebuild. An ALIAS-only rename is
  // registry-only (alias never appears in code), so it can save instantly with no session.
  const newLocalPath = '/' + form.path.trim().replace(/^\/+/, '')
  const pathChanged = editing && newLocalPath !== editingOriginal!.path
  const aliasChanged = editing && form.alias.trim() !== (editingOriginal!.alias || '')
  const renamed = pathChanged || aliasChanged

  function startAdd() {
    setForm(blankForm())
    setEditingOriginal(null)
    setEditingHistory([])
    setEditingDescription('')
    setEditingDownstream([])
    setEditingDownstreamDescriptions({})
    setEditingBaseline(null)
    setError(null)
    setAdding(true)
  }

  function startEdit(e: DiscoveredEndpoint) {
    // Pre-fill the form with whatever this endpoint currently is (method, path,
    // alias, request/response schemas) so editing starts from the live spec rather
    // than a blank form. The Describe field is intentionally left EMPTY: anything
    // typed there is appended to the endpoint's existing description on save (kept
    // here, read-only, as `editingDescription`), not a replacement for it.
    setForm({
      method: e.method,
      path: localPath(e).replace(/^\/+/, ''),
      protocol: e.protocol || 'http',
      alias: e.alias || '',
      request: schemaToText(e.request),
      response: schemaToText(e.response),
      requestModel: e.requestModel || '',
      responseModel: e.responseModel || '',
      description: '',
      internal: e.internal === true,
    })
    // Snapshot the code-affecting fields as the form first shows them, so submit() can
    // tell whether the user actually changed any of them (vs. only flipping internal).
    setEditingBaseline({
      request: schemaToText(e.request),
      response: schemaToText(e.response),
      requestModel: e.requestModel || '',
      responseModel: e.responseModel || '',
    })
    setEditingOriginal({ method: e.method, path: localPath(e), alias: e.alias || '' })
    setEditingHistory(Array.isArray(e.history) ? e.history : [])
    setEditingDescription(e.description || '')
    setEditingDownstream(Array.isArray(e.downstream) ? e.downstream : [])
    setEditingDownstreamDescriptions(
      e.downstreamDescriptions && typeof e.downstreamDescriptions === 'object' ? e.downstreamDescriptions : {},
    )
    setError(null)
    setConfirmKey(null)
    setAdding(true)
  }

  function cancelForm() {
    setAdding(false)
    setEditingOriginal(null)
    setEditingHistory([])
    setEditingDescription('')
    setEditingDownstream([])
    setEditingDownstreamDescriptions({})
    setEditingBaseline(null)
    setError(null)
  }

  // Launch a focused Claude session that (re)writes ONLY this endpoint's
  // downstreamDescriptions in endpoints.json (pure JSON, no rebuild), then close.
  function updateDescriptions() {
    if (!editingOriginal) return
    const prompt = buildDescriptionsPrompt({
      systemId,
      service,
      method: editingOriginal.method,
      path: editingOriginal.path,
      alias: form.alias.trim(),
      downstream: editingDownstream,
    })
    onLaunch({ sessionId: crypto.randomUUID(), mode: 'new', prompt }, { kind: 'endpoint', target: service, title: 'descriptions' })
    onClose()
  }

  // Toggle the endpoint's "internal" flag. This is a pure metadata edit — it removes the
  // route from the load balancer's advertised surface (or restores it) with NO rebuild and
  // no Claude session — so it persists immediately (optimistic UI) and also updates the
  // in-memory list so the badge and the diagram's LB reflect it before the next poll.
  async function toggleInternal(next: boolean) {
    if (!editingOriginal) return
    setForm((f) => ({ ...f, internal: next }))
    setError(null)
    try {
      const res = await fetch('/api/endpoints', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          service,
          method: editingOriginal.method,
          path: editingOriginal.path,
          internal: next,
        }),
      })
      const data = (await res.json().catch(() => ({}))) as {
        ok?: boolean
        error?: string
        nginxReloaded?: boolean
        warning?: string
      }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setEndpoints((list) =>
        (list || []).map((e) =>
          e.method === editingOriginal.method && localPath(e) === editingOriginal.path
            ? { ...e, internal: next }
            : e,
        ),
      )
      // The flag is saved either way; if nginx couldn't reload (e.g. the system isn't
      // running) the block isn't live yet — surface that without undoing the toggle.
      if (data.nginxReloaded === false) {
        setError(`Saved, but nginx didn’t reload — the block isn’t live yet (is the system running?): ${data.warning || ''}`)
      }
    } catch (err) {
      setForm((f) => ({ ...f, internal: !next })) // revert the optimistic flip
      setError((err as Error).message)
    }
  }

  async function remove(e: DiscoveredEndpoint) {
    setBusy(true)
    setError(null)
    const path = localPath(e)
    try {
      // Always drop the registry entry first — instant, and it's all a pending
      // (registry-only) endpoint needs.
      const res = await fetch('/api/endpoints', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, service, method: e.method, path }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmKey(null)

      // A live endpoint is also served by the container, so it'd be re-discovered
      // unless we remove the route from the service code. Hand that to Claude.
      if (e.live) {
        const prompt = buildDeletePrompt({ systemId, service, method: e.method, path })
        onLaunch({ sessionId: crypto.randomUUID(), mode: 'new', prompt }, { kind: 'endpoint', target: service, title: `delete ${e.method} ${path}` })
        onClose()
        return
      }
      await loadEndpoints()
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  async function submit() {
    setError(null)
    // Editing with no code-affecting change AND no rename (e.g. the user only toggled `internal`,
    // which toggleInternal already persisted live): nothing for Claude to regenerate and no new
    // spec/identity to record, so don't launch a session or append a history entry — just close.
    if (editing && !dirtyCode && !renamed) {
      onClose()
      return
    }
    // The user edits only the part after `/<service>/`; build the service-local
    // path (what the registry stores; the LB prefixes `/<service>` at routing).
    const suffix = form.path.trim().replace(/^\/+/, '')
    const path = '/' + suffix
    if (!/^\/[A-Za-z0-9._~\-/{}]*$/.test(path)) {
      setError('Path may only use url-safe characters (braces allowed for params)')
      return
    }
    // A referenced model is the source of truth for that field; only parse the inline
    // schema when no model is chosen (then the inline map stays {}).
    const requestModel = form.requestModel || ''
    const responseModel = form.responseModel || ''
    let reqValue: SchemaMap = {}
    if (!requestModel) {
      const req = parseSchema(form.request, 'Request schema')
      if (req.error) return setError(req.error)
      reqValue = req.value! // no error → value present
    }
    let respValue: SchemaMap = {}
    if (!responseModel) {
      const resp = parseSchema(form.response, 'Response schema')
      if (resp.error) return setError(resp.error)
      respValue = resp.value! // no error → value present
    }

    const alias = form.alias.trim()
    if (!alias) {
      setError('Function name is required')
      return
    }
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) {
      setError('Function name must start with a letter or underscore and use only letters, digits and underscores')
      return
    }

    // When editing, the Describe field holds only the NEW text — append it to the
    // endpoint's existing description so the description accumulates. (Adding starts
    // from a blank base, so this is just the typed text.)
    const description = editing ? joinDescription(editingDescription, form.description) : form.description
    const conversationId = crypto.randomUUID()

    setBusy(true)
    try {
      // RENAME: the path and/or alias changed on an existing endpoint. PUT runs the mechanical
      // cascade (record-in-place + history, downstreamMethods, client-function steps, internal
      // nginx) and returns who calls this route. POST is not used on this path.
      if (renamed) {
        const res = await fetch('/api/endpoints', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            system: systemId,
            service,
            method: form.method,
            oldPath: editingOriginal!.path,
            newPath: path,
            newAlias: alias,
            protocol: form.protocol,
            request: reqValue,
            response: respValue,
            requestModel,
            responseModel,
            description,
            conversationId,
          }),
        })
        const data = (await res.json().catch(() => ({}))) as {
          ok?: boolean
          error?: string
          callers?: string[]
          scenarioWarnings?: string[]
        }
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)

        // A PATH change needs code edits (owner decorator + each caller's call URL) + rebuild —
        // one session. An ALIAS-only change is registry-only: launch a session only if the
        // spec/description ALSO changed; otherwise the save is instant (no session).
        if (pathChanged) {
          onLaunch({
            sessionId: conversationId,
            mode: 'new',
            prompt: buildEndpointRenamePrompt({
              systemId,
              service,
              method: form.method,
              oldPath: editingOriginal!.path,
              newPath: path,
              alias,
              callers: Array.isArray(data.callers) ? data.callers : [],
              request: reqValue,
              response: respValue,
              requestModel,
              responseModel,
              priorDescription: editingDescription,
              newDescription: form.description,
              schemaChanged,
              models,
            }),
          }, { kind: 'endpoint', target: service, title: `rename ${form.method} ${path}` })
        } else if (dirtyCode) {
          onLaunch({
            sessionId: conversationId,
            mode: 'new',
            prompt: buildEndpointEditPrompt({
              systemId,
              service,
              method: form.method,
              path,
              protocol: form.protocol,
              alias,
              request: reqValue,
              response: respValue,
              requestModel,
              responseModel,
              priorDescription: editingDescription,
              newDescription: form.description,
              schemaChanged,
              models,
            }),
          }, { kind: 'endpoint', target: service, title: `${form.method} ${path}` })
        }

        const warnings = Array.isArray(data.scenarioWarnings) ? data.scenarioWarnings : []
        if (warnings.length) {
          // Rename succeeded (and any session launched); keep the modal open to surface client
          // function steps that couldn't be rewritten automatically.
          setBusy(false)
          setError(`Renamed. Heads up — ${warnings.join('; ')}`)
          return
        }
        onClose()
        return
      }

      // Non-rename add/edit: upsert the record (same method+path), then launch the edit-in-place
      // or from-scratch session for the spec/description.
      const record = {
        system: systemId,
        service,
        method: form.method,
        path,
        protocol: form.protocol,
        alias,
        request: reqValue,
        response: respValue,
        requestModel,
        responseModel,
        description,
        conversationId,
      }
      const res = await fetch('/api/endpoints', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(record),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)

      // Editing: tell the session the handler already exists and to modify it in place
      // (separating the existing behavior from the newly typed change). Adding: the
      // from-scratch prompt. Both seed a fresh `new` session.
      const prompt = editing
        ? buildEndpointEditPrompt({
            systemId,
            service,
            method: form.method,
            path,
            protocol: form.protocol,
            alias,
            request: reqValue,
            response: respValue,
            requestModel,
            responseModel,
            priorDescription: editingDescription,
            newDescription: form.description,
            schemaChanged,
            models,
          })
        : buildEndpointPrompt({
            systemId,
            service,
            method: form.method,
            path,
            protocol: form.protocol,
            alias,
            request: reqValue,
            response: respValue,
            requestModel,
            responseModel,
            description,
            models,
          })
      onLaunch({ sessionId: conversationId, mode: 'new', prompt }, { kind: 'endpoint', target: service, title: `${form.method} ${path}` })
      onClose()
    } catch (err) {
      setBusy(false)
      setError((err as Error).message)
    }
  }

  // Enter in the description submits (Shift+Enter = newline).
  function onDescriptionKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      submit()
    }
  }

  // Render a request/response field: a "reference a model" dropdown plus, when no
  // model is chosen, the inline {key: type} JSON textarea (today's behavior). Picking
  // a model hides the textarea and shows the model's TypeScript as a read-only preview.
  function schemaField(kind: 'request' | 'response', label: string, placeholder: string): ReactElement {
    const modelKey = kind === 'request' ? 'requestModel' : 'responseModel'
    const modelName = form[modelKey]
    const model = models.find((m) => m.name === modelName)
    return (
      <div className="form-row form-row-stack">
        <span>{label}</span>
        <div className="schema-field">
          <select className="model-select" value={modelName} onChange={set(modelKey)} disabled={busy}>
            <option value="">— inline schema —</option>
            {models.map((m) => (
              <option key={m.name} value={m.name}>{m.name}</option>
            ))}
          </select>
          {modelName ? (
            <pre className="model-preview">
              {model
                ? (model.ts || '').trim()
                : `model "${modelName}" not found — pick another or define it in the models bank`}
            </pre>
          ) : (
            <textarea
              className="json-input"
              value={form[kind]}
              onChange={set(kind)}
              placeholder={placeholder}
              rows={2}
              disabled={busy}
            />
          )}
        </div>
      </div>
    )
  }

  // Hidden endpoints (e.g. a route owned entirely by a custom Edit tab) are never
  // listed; internal ones are listed but badged + locked.
  const visible = endpoints === null ? null : endpoints.filter((e) => endpointPolicy(e, node).visibility !== 'hidden')

  const body = (
    <>
      {/* Existing endpoints */}
      {visible === null ? (
          <p className="sim-desc">Loading…</p>
        ) : visible.length === 0 ? (
          <p className="sim-desc">No endpoints yet.</p>
        ) : (
          <ul className="endpoint-list">
            {visible.map((e) => {
              const key = `${e.method} ${e.path}`
              const confirming = confirmKey === key
              const pol = endpointPolicy(e, node)
              const locked = pol.locked
              const internal = pol.visibility === 'internal'
              return (
                <li key={key} className="endpoint-list-row">
                  <span className="endpoint-list-method">{e.method}</span>
                  <span className="endpoint-list-path">{e.path}</span>
                  {e.alias && (
                    <span className="endpoint-alias" title="Function name (alias)">{e.alias}()</span>
                  )}
                  <span className="endpoint-list-proto">{e.protocol || 'http'}</span>
                  {internal && (
                    <span className="endpoint-internal" title="Internal route — the load balancer returns 403 for external calls; service-to-service calls (which bypass the LB) still work">
                      internal
                    </span>
                  )}
                  {locked ? (
                    <span className="endpoint-list-actions">
                      <span className="endpoint-locked" title="Built-in / owned route — managed by the system, not editable here">
                        🔒 locked
                      </span>
                    </span>
                  ) : confirming ? (
                    <span className="endpoint-list-actions">
                      <span className="endpoint-confirm">
                        {e.live ? 'Delete & rebuild?' : 'Delete?'}
                      </span>
                      <button className="link" disabled={busy} onClick={() => remove(e)}>Yes</button>
                      <button className="link" disabled={busy} onClick={() => setConfirmKey(null)}>No</button>
                    </span>
                  ) : (
                    <span className="endpoint-list-actions">
                      {e.conversationId && (
                        <button
                          className="link"
                          disabled={busy}
                          title="Resume this endpoint’s Claude session"
                          onClick={() => {
                            onLaunch({ sessionId: e.conversationId!, mode: 'resume', prompt: '' })
                            onClose()
                          }}
                        >
                          Resume
                        </button>
                      )}
                      <button className="link" disabled={busy} onClick={() => startEdit(e)}>Edit</button>
                      <button className="link-danger" disabled={busy} onClick={() => setConfirmKey(key)}>Delete</button>
                    </span>
                  )}
                </li>
              )
            })}
          </ul>
        )}

        {/* Add / edit endpoint */}
        {!adding ? (
          <div className="form-section">
            <button className="link" onClick={startAdd}>
              ＋ Add endpoint
            </button>
          </div>
        ) : (
          <div className="form-section">
            <div className="form-section-head">
              <span>{editing ? 'Edit endpoint' : 'New endpoint'}</span>
            </div>

            {/* Read-only trail of every spec this endpoint was created/updated with.
                The editable form below composes the next update. */}
            {editing && editingHistory.length > 0 && (
              <div className="endpoint-history">
                <div className="endpoint-history-head">Changelog</div>
                <ol className="endpoint-history-list">
                  {editingHistory
                    .map((h, i) => ({ i, h, diff: diffHistoryEntry(h, i > 0 ? editingHistory[i - 1] : null) }))
                    .reverse()
                    .map(({ i, h, diff }) => (
                      <li key={i} className="endpoint-history-row">
                        <div className="endpoint-history-meta">
                          <span className="endpoint-history-num">#{i + 1}</span>
                          {diff.initial && <span className="endpoint-history-initial">created</span>}
                          {fmtAt(h.at) && <span className="endpoint-history-at">{fmtAt(h.at)}</span>}
                        </div>
                        {diff.initial ? (
                          <>
                            {diff.description && <div className="endpoint-history-desc">{diff.description}</div>}
                            <div className="endpoint-history-schemas">
                              {diff.alias && <code className="endpoint-history-alias">{diff.alias}()</code>}
                              {diff.path && <code>path: {diff.path}</code>}
                              <code>req: {diff.request}</code>
                              <code>res: {diff.response}</code>
                            </div>
                          </>
                        ) : diff.empty ? (
                          <div className="endpoint-history-empty">no spec changes</div>
                        ) : (
                          <>
                            {diff.description && (
                              <div className="endpoint-history-desc">
                                <span className="endpoint-history-field">
                                  {diff.descriptionReplaced ? 'description replaced:' : 'added:'}
                                </span>{' '}
                                {diff.description}
                              </div>
                            )}
                            {diff.path && (
                              <div className="endpoint-history-change">
                                <span className="endpoint-history-field">path:</span>
                                <code>{diff.path.from}</code>
                                <span className="endpoint-history-arrow">→</span>
                                <code>{diff.path.to}</code>
                              </div>
                            )}
                            {diff.alias && (
                              <div className="endpoint-history-change">
                                <span className="endpoint-history-field">alias:</span>
                                <code>{diff.alias.from}</code>
                                <span className="endpoint-history-arrow">→</span>
                                <code>{diff.alias.to}</code>
                              </div>
                            )}
                            {diff.request && (
                              <div className="endpoint-history-change">
                                <span className="endpoint-history-field">req:</span>
                                <code>{diff.request.from}</code>
                                <span className="endpoint-history-arrow">→</span>
                                <code>{diff.request.to}</code>
                              </div>
                            )}
                            {diff.response && (
                              <div className="endpoint-history-change">
                                <span className="endpoint-history-field">res:</span>
                                <code>{diff.response.from}</code>
                                <span className="endpoint-history-arrow">→</span>
                                <code>{diff.response.to}</code>
                              </div>
                            )}
                          </>
                        )}
                      </li>
                    ))}
                </ol>
              </div>
            )}

            <label className="form-row">
              <span>Method</span>
              <select value={form.method} onChange={set('method')} disabled={busy || editing}>
                {METHODS.map((m) => (
                  <option key={m} value={m}>{m}</option>
                ))}
              </select>
            </label>

            <label className="form-row">
              <span>Path</span>
              <div className="path-input">
                <span className="path-prefix">/{service}/</span>
                <input value={form.path} onChange={set('path')} placeholder="orders" disabled={busy} />
              </div>
            </label>

            <label className="form-row">
              <span>Protocol</span>
              <select value={form.protocol} onChange={set('protocol')} disabled={busy || editing}>
                {PROTOCOLS.map((p) => (
                  <option key={p.value} value={p.value}>{p.label}</option>
                ))}
              </select>
            </label>

            <label className="form-row">
              <span>Function</span>
              <input
                value={form.alias}
                onChange={set('alias')}
                placeholder="createOrder  (required — unique within this service)"
                disabled={busy}
              />
            </label>

            {/* Mark a route internal: drop it from the load balancer (it stays callable
                service-to-service). Pure metadata, so it takes effect immediately — no
                rebuild. Only editable endpoints of an in-system service can be flagged. */}
            {editing && !node?.external && (
              <label className="form-row form-row-check">
                <span>Internal</span>
                <span className="check-field">
                  <input
                    type="checkbox"
                    checked={form.internal}
                    onChange={(ev) => toggleInternal(ev.target.checked)}
                    disabled={busy}
                  />
                  <small className="form-hint">
                    Block external calls to this route at the load balancer (returns 403).
                    Service-to-service calls bypass the LB, so they keep working. Reloads
                    nginx — no rebuild.
                  </small>
                </span>
              </label>
            )}

            {schemaField('request', 'Request', '{"item": "string", "qty": "number"}  (optional)')}
            {schemaField('response', 'Response', '{"id": "number"}  (optional)')}

            <label className="form-row form-row-stack">
              <span>{editing ? 'Add to description' : 'Describe'}</span>
              <textarea
                className="desc-input"
                value={form.description}
                onChange={set('description')}
                onKeyDown={onDescriptionKeyDown}
                placeholder={editing
                  ? 'Add to this endpoint’s description — appended to the current one (Enter to submit, Shift+Enter for a newline)'
                  : 'What should this endpoint do? (Enter to submit, Shift+Enter for a newline)'}
                rows={3}
                disabled={busy}
                autoFocus
              />
            </label>
            {editing && (
              <small className="form-hint">
                Appended to this endpoint’s existing description — leave blank to keep it unchanged.
              </small>
            )}

            {editing && (
              <p className="sim-desc">
                {pathChanged
                  ? 'Renaming the path: callers and registries update automatically, then a Claude session moves the route in the owner + caller code and rebuilds them.'
                  : aliasChanged && !dirtyCode
                    ? 'Renaming the function name only — applied instantly across the diagram and registries (no rebuild, no Claude session).'
                    : dirtyCode
                      ? 'Saving rebuilds this endpoint in a fresh Claude session.'
                      : 'No code changes — Save just closes (no Claude session). The internal flag is applied the moment you toggle it.'}
              </p>
            )}

            {/* Per-downstream connection descriptions (Claude-authored, shown read-only).
                "Update descriptions" launches a session that rewrites just these — no rebuild. */}
            {editing && editingDownstream.length > 0 && (
              <div className="form-row form-row-stack">
                <span>Connections</span>
                <div className="endpoint-conn">
                  <ul className="endpoint-conn-list">
                    {editingDownstream.map((id) => (
                      <li key={id} className="endpoint-conn-row">
                        <code className="endpoint-conn-node">{id}</code>
                        <span className="endpoint-conn-text">
                          {editingDownstreamDescriptions[id] || '—'}
                        </span>
                      </li>
                    ))}
                  </ul>
                  <button type="button" className="link" onClick={updateDescriptions} disabled={busy}>
                    Update descriptions
                  </button>
                </div>
              </div>
            )}

            {error && <p className="modal-error">{error}</p>}

            <div className="modal-actions">
              <button type="button" onClick={cancelForm} disabled={busy}>Cancel</button>
              <button type="button" className="primary" onClick={submit} disabled={busy}>
                {busy
                  ? 'Working…'
                  : editing
                    ? dirtyCode || pathChanged
                      ? 'Save & open Claude'
                      : renamed
                        ? 'Save'
                        : 'Done'
                    : 'Create & open Claude'}
              </button>
            </div>
          </div>
        )}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>
            Endpoints · <code>{service}</code>
          </h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
