/**
 * Generic, manifest-driven diagram renderer.
 *
 * It knows nothing about a specific system: it lays out whatever nodes the
 * manifest lists at their `position`, draws every `edge` as a line between node
 * centers, and prints each node's metric values. Adding nodes/edges to a
 * manifest "just works" here with no code changes.
 *
 * One special case: a `load_balancer` node lists the system's live, routable
 * endpoints (passed in via `endpoints`) instead of metric rows. They're grouped
 * into one collapsible accordion per owning service (header = service label,
 * body = its method rows), collapsed by default to keep the LB compact. Clicking
 * a method row traces its lifecycle: LB -> owning service -> the service(s)/db(s)
 * that endpoint calls (`endpoint.downstream`), highlighting that path and dimming
 * everything else. Click the row again, or empty canvas, to clear.
 */

import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { customTypeOf, CUSTOM_TYPES } from './customTypes/index.js'
import { isExternalEndpoint, endpointPolicy, localPathOf } from './endpointPolicy.js'
import { deriveFunctionTrace } from './scenarioBank.js'

const NODE_W = 190
const HEADER_H = 30
const LINE_H = 20
const PAD = 10
const MARGIN = 40
// Pan/zoom (Excalidraw-style): the SVG renders into a bounded "world" rect ≈ PAN_FACTOR× the tight
// content bounds (centered), so there's empty space to scroll around without being infinite. `zoom`
// is the only screen↔user scale factor (pixel size = world × zoom, viewBox = world), clamped here.
const MIN_ZOOM = 0.2
const MAX_ZOOM = 4
const PAN_FACTOR = 3
// Bottom "Edit" button band on controllable nodes (services, dbs, event streams):
// a gap below the metrics, then the button itself.
const EDIT_GAP = 8
const EDIT_H = 24
// Gap between the metric rows and an optional custom service-type body (e.g. a bitmap grid).
const CUSTOM_GAP = 8
// Gap above the on-node API method rows (services / external services list their own
// callable methods below their metrics, like the LB lists its endpoints).
const METHOD_GAP = 8

const COLOR_HEX = {
  green: '#2e7d32',
  yellow: '#f9a825',
  red: '#c62828',
  gray: '#9e9e9e',
  // A user-initiated temporary outage (node shut down for N seconds). Distinct from
  // red (unhealthy/down for any reason) so a deliberate shutdown reads as intentional.
  orange: '#fb8c00',
}

const TRACE_COLOR = '#6ea8fe'
const GRPC_COLOR = '#b18cf2'
const REPLICA_COLOR = '#3fb6a8'

// Drag-mode resize handles for the system boundary box: 4 corners + 4 edge midpoints,
// positioned by a fraction of the box's width/height, each with its resize cursor.
const HANDLE_SIZE = 10
const BOUNDARY_HANDLES = [
  { handle: 'nw', fx: 0, fy: 0, cursor: 'nwse-resize' },
  { handle: 'n', fx: 0.5, fy: 0, cursor: 'ns-resize' },
  { handle: 'ne', fx: 1, fy: 0, cursor: 'nesw-resize' },
  { handle: 'e', fx: 1, fy: 0.5, cursor: 'ew-resize' },
  { handle: 'se', fx: 1, fy: 1, cursor: 'nwse-resize' },
  { handle: 's', fx: 0.5, fy: 1, cursor: 'ns-resize' },
  { handle: 'sw', fx: 0, fy: 1, cursor: 'nesw-resize' },
  { handle: 'w', fx: 0, fy: 0.5, cursor: 'ew-resize' },
]
const MIN_BOUNDARY = 40

// Resize a rect by dragging one named handle (n/s/e/w/ne/nw/se/sw) by (dx,dy) in user
// units, keeping the opposite edge anchored and never shrinking below MIN_BOUNDARY.
function resizeRect(rect, handle, dx, dy) {
  let { x, y, w, h } = rect
  if (handle.includes('e')) w = Math.max(MIN_BOUNDARY, rect.w + dx)
  if (handle.includes('s')) h = Math.max(MIN_BOUNDARY, rect.h + dy)
  if (handle.includes('w')) {
    const nw = Math.max(MIN_BOUNDARY, rect.w - dx)
    x = rect.x + (rect.w - nw)
    w = nw
  }
  if (handle.includes('n')) {
    const nh = Math.max(MIN_BOUNDARY, rect.h - dy)
    y = rect.y + (rect.h - nh)
    h = nh
  }
  return { x: Math.round(x), y: Math.round(y), w: Math.round(w), h: Math.round(h) }
}

function isLB(node) {
  return node.type === 'load_balancer'
}

// Services (incl. the base service-1), external services, clients, databases,
// event streams, and websocket-tier nodes can be torn down; the nginx LB (and any
// other infra) cannot. Deleting a websocket tier's own lb cascades the whole tier.
function isDeletable(node) {
  return (
    node.type === 'service' ||
    node.type === 'external_service' ||
    node.type === 'client' ||
    node.origin === 'create-database' ||
    node.origin === 'create-event-stream' ||
    node.origin === 'create-websockets'
  )
}

// Stable identity for an endpoint row (used for selection).
function endpointKey(e) {
  return `${e.method} ${e.path}`
}

// How many content rows a node draws. For the LB this is its precomputed accordion row
// count (one header per service group + the methods of each expanded group); `lbRows`
// is ignored for every other node, which counts its own metrics.
function rowCount(node, lbRows) {
  if (isLB(node)) return Math.max(lbRows || 0, 1)
  return (node.metrics || []).length
}

// Controllable nodes carry a bottom "Edit" button (which opens the tabbed edit modal);
// the LB and other infra do not.
function hasEditButton(node) {
  return isDeletable(node)
}

// y where the metric/endpoint rows end.
function metricsBottom(node, lbRows) {
  return HEADER_H + PAD + rowCount(node, lbRows) * LINE_H
}

// Height of the on-node API method rows band (the service's callable methods, listed
// below its metrics). 0 when the node lists none (LB, db, event stream, or a service
// with no visible methods).
function methodsBandHeight(methods) {
  return methods && methods.length ? METHOD_GAP + methods.length * LINE_H : 0
}

// y where the metrics + the method rows end.
function methodsBottom(node, lbRows, methods) {
  return metricsBottom(node, lbRows) + methodsBandHeight(methods)
}

// Height of a custom service-type body band (e.g. a Download Coordinator bitmap grid),
// reserved below the metrics. 0 when the node's type has no custom body for this state.
function customBandHeight(node, runtime) {
  const m = customTypeOf(node)
  return m?.diagramHeight ? m.diagramHeight(node, runtime, NODE_W) : 0
}

// y where all content (metrics + method rows + any custom band) ends — the Edit button
// sits a gap below.
function contentBottom(node, lbRows, runtime, methods) {
  const band = customBandHeight(node, runtime)
  return methodsBottom(node, lbRows, methods) + (band ? CUSTOM_GAP + band : 0)
}

function nodeHeight(node, lbRows, runtime, methods) {
  const bottom = contentBottom(node, lbRows, runtime, methods)
  // Reserve the edit band (gap + button + bottom pad) on controllable nodes; others
  // keep the original single trailing pad.
  return hasEditButton(node) ? bottom + EDIT_GAP + EDIT_H + PAD : bottom + PAD
}

function formatMetric(value, metric) {
  if (value == null) return '—'
  let v = value * (metric.scale || 1)
  let s
  if (Math.abs(v) >= 100) s = v.toFixed(0)
  else if (Math.abs(v) >= 10) s = v.toFixed(1)
  else s = v.toFixed(2)
  return `${s}${metric.unit || ''}`
}

// Breaker circle on a connection: filled = CLOSED (healthy), hollow = OPEN
// (blocking/fast-failing), half-filled = HALF-OPEN (testing recovery). Never invert
// this — "open" means broken. State comes from the fast in-memory read; absent live
// state we draw CLOSED (the at-rest, healthy look).
const BREAKER_COLOR = { closed: '#2e7d32', open: '#c62828', half_open: '#f9a825' }
function breakerStateOf(live) {
  const s = live?.circuit_breaker?.state
  return s === 'open' || s === 'half_open' ? s : 'closed'
}
function BreakerCircle({ cx, cy, live }) {
  const state = breakerStateOf(live)
  const color = BREAKER_COLOR[state]
  const r = 6
  return (
    <g className={`breaker breaker-${state}`} style={{ pointerEvents: 'none' }}>
      {/* Opaque ring so the connection line doesn't show through a hollow (OPEN) circle. */}
      <circle cx={cx} cy={cy} r={r} className="breaker-ring" style={{ stroke: color }} />
      {state === 'closed' && <circle cx={cx} cy={cy} r={r} stroke="none" style={{ fill: color }} />}
      {state === 'half_open' && (
        <path d={`M ${cx} ${cy - r} A ${r} ${r} 0 0 1 ${cx} ${cy + r} Z`} stroke="none" style={{ fill: color }} />
      )}
    </g>
  )
}

// Live overlay text near the connection, driven by the actual reported state. Returns
// null when there's nothing transient to show (healthy + idle).
function breakerLabel(live) {
  const cb = live?.circuit_breaker
  const rt = live?.retry
  if (cb?.state === 'open') return cb.open_behavior === 'fallback' ? 'breaker OPEN — serving fallback' : 'breaker OPEN — fast-failing'
  if (cb?.state === 'half_open') {
    const t = cb.trial
    return t ? `breaker HALF-OPEN — testing (${t.done}/${t.required})` : 'breaker HALF-OPEN — testing'
  }
  if (rt?.active) {
    const eta = typeof rt.next_backoff_seconds === 'number' ? ` in ${rt.next_backoff_seconds.toFixed(1)}s` : ''
    return `retrying — attempt ${rt.attempt}/${rt.max}${eta}`
  }
  if (rt?.exhausted) return 'retries exhausted'
  return null
}

export default function SystemDiagram({
  manifest,
  nodeData,
  endpoints = [],
  systemId,
  // Drag mode: when true, every node can be repositioned and the system boundary box can be
  // moved/resized; the normal click actions (trace/Edit/select) are suppressed. Drops are
  // persisted to the manifest via POST /api/layout.
  dragMode = false,
  onRequestEdit,
  onRequestConnectionResilience,
  resilienceState = {},
  outages = {},
  // Set of event-stream cluster ids whose consumers are paused — drives an amber
  // "consumers paused" badge under the cluster node (see /api/consumer-pause).
  pausedConsumers = new Set(),
  customState = {},
  // A service method picked from its Edit ▸ Calls tab OR by clicking the method row on
  // the service node itself (an endpoint record), traced service → its downstream nodes.
  // Takes precedence over the LB endpoint selection below; set via onSelectMethod,
  // cleared via onClearMethodTrace.
  methodTrace = null,
  onSelectMethod,
  onClearMethodTrace,
  // A client's attached functions, keyed by client node id (each is a resolved bank
  // function with its authored `steps`). Rendered as clickable rows on the client node.
  // A websocket client's entries are its pool script's builtin methods (wsBuiltin: true,
  // no steps) — same rows, but their trace is the tier path, derived here from wsTier/wsRole.
  clientFunctions = {},
  // The ws pool client's last-run delivery stats, keyed by client node id (the shape the
  // pool script writes to ws-clients/<id>.stats.json: { ts, args, results }). Rendered as
  // read-only metric-style rows under the client's ƒ rows.
  wsStats = {},
  // A selected client function, traced client → LB → each called service → its downstreams,
  // with every called method highlighted on its service. Top precedence over methodTrace /
  // the LB endpoint selection. Set via onSelectFunction, cleared via onClearFunctionTrace.
  functionTrace = null,
  onSelectFunction,
  onClearFunctionTrace,
  // A service's Kafka "consumer functions", keyed by service node id (each a registry record:
  // { name, cluster, topic, pollRate, implemented, … }). Rendered as clickable CONS rows on the
  // service node.
  consumerFunctions = {},
  // A selected consumer function, traced cluster → consuming service (the consume edge). Mutually
  // exclusive with the method / function / LB selections. Set via onSelectConsumer, cleared via
  // onClearConsumerTrace.
  consumerTrace = null,
  onSelectConsumer,
  onClearConsumerTrace,
}) {
  const [selectedKey, setSelectedKey] = useState(null)
  // Index of the trace hop whose description popup is open (or null). Reset whenever the active
  // trace changes so a stale index can't point at the wrong hop of a different trace.
  const [openInfo, setOpenInfo] = useState(null)
  // Which LB service-accordions are expanded (set of service node ids). The LB groups its
  // routable endpoints by owning service and draws each group as a collapsible accordion;
  // every group is collapsed by default, so this starts empty.
  const [openLbServices, setOpenLbServices] = useState(() => new Set())

  // Drag-mode layout overrides. `drag` maps nodeId -> {x,y} and `boundaryOverride` holds the
  // in-progress / just-dropped boundary rect. These win over the manifest so a freshly dropped
  // node/box doesn't flicker back to its saved spot while the POST /api/layout round-trips.
  const [drag, setDrag] = useState({})
  const [boundaryOverride, setBoundaryOverride] = useState(null)
  // Snapshot of the viewBox taken at drag start; while set, the canvas is pinned to it exactly
  // (no shrink, no grow) so the view stays put and doesn't scroll toward the edge mid-drag.
  const [frozenView, setFrozenView] = useState(null)
  const svgRef = useRef(null)
  const gesture = useRef(null) // active drag: { kind, nodeId?, handle?, startRect?, startPos?, ... }

  // Pan/zoom. Panning is just native scrolling of `scrollRef` (the overflow:auto wrapper). `zoom`
  // scales the SVG's pixel size only. `pendingScroll` carries a cursor-anchored scroll target that
  // must be applied AFTER the SVG has resized (see the useLayoutEffect keyed on `zoom`).
  const [zoom, setZoom] = useState(1)
  const scrollRef = useRef(null)
  const pendingScroll = useRef(null)
  // Set on drop: the frozen world origin + scroll offset at release, so the layout effect can
  // re-anchor the scroll to the same world point once the bounds re-fit (keeps the view still).
  const preDropScroll = useRef(null)

  const nodes = manifest.nodes
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n]))

  // Effective on-screen position of a node: a live drag override wins over the manifest.
  const posOf = (n) => drag[n.id] || n.position

  // Clients are callers that live outside the system; each connects to the load
  // balancer. The edges are derived (not stored on the manifest).
  const clientNodes = nodes.filter((n) => n.type === 'client')

  // The load balancer advertises only the EXTERNAL client surface: internal /
  // operational routes (health, resilience state, a custom type's control plane)
  // are classified by endpointPolicy against the node that serves them and dropped
  // here, so they neither show on the LB nor size its box.
  const visibleEndpoints = endpoints.filter((e) => isExternalEndpoint(e, byId[e.service]))

  // Group the LB's visible endpoints by their owning service so the LB can draw one
  // collapsible accordion per service (header = service label, body = its methods).
  // First-seen order keeps the layout stable; the count badges the header.
  const lbGroups = []
  {
    const byService = new Map()
    for (const e of visibleEndpoints) {
      let g = byService.get(e.service)
      if (!g) {
        g = { serviceId: e.service, label: byId[e.service]?.label || e.service, endpoints: [] }
        byService.set(e.service, g)
        lbGroups.push(g)
      }
      g.endpoints.push(e)
    }
  }
  // Rows the LB box draws: one per group header, plus the methods of each expanded group.
  // Feeds the layout helpers so collapsing a group actually shrinks the node.
  const lbRows = lbGroups.reduce(
    (n, g) => n + 1 + (openLbServices.has(g.serviceId) ? g.endpoints.length : 0),
    0,
  )

  // Each service / external service lists its OWN callable methods on its node (like the
  // LB lists endpoints), so the diagram can trace one straight from the service. Drops
  // hidden routes (e.g. an external service's /health) plus the generic /health liveness
  // probe — it's operational plumbing with no downstream to trace, so it's just noise on
  // the diagram (it stays listed, badged, in the Edit ▸ Endpoints/Calls tabs). Keyed by
  // node id; empty for non-endpoint-host nodes.
  const methodsByNode = new Map()
  for (const n of nodes) {
    if (n.type !== 'service' && n.type !== 'external_service') continue
    methodsByNode.set(
      n.id,
      endpoints.filter(
        (e) =>
          e.service === n.id &&
          endpointPolicy(e, n).visibility !== 'hidden' &&
          localPathOf(e) !== '/health',
      ),
    )
  }
  const methodsOf = (id) => methodsByNode.get(id) || []

  // Each controllable node lists clickable rows below its metrics. A service / external service
  // lists its callable METHODS; a client lists its own FUNCTIONS. `rowsOf` is the unified, ordered
  // list used for both sizing and rendering: method rows first, then function rows, each tagged
  // with its `kind`. (Today nodes carry one kind or the other, but the unified list keeps a mixed
  // node measured and drawn correctly should that ever change.)
  const functionsOf = (id) => clientFunctions[id] || []
  // A service's consumer functions, dropping any whose cluster node no longer exists (so a deleted
  // cluster can't leave a dangling CONS row that traces to nothing).
  const consumersOf = (id) => (consumerFunctions[id] || []).filter((c) => byId[c.cluster])
  // The ws pool client's last-run delivery stats, folded into the row list so they size
  // the node like every other row (read-only — the renderer draws them non-interactive).
  const wsStatRowsOf = (id) => {
    const s = wsStats[id]?.results
    if (!s) return []
    const lat = s.latencyMs || {}
    return [
      { label: 'delivered', value: `${s.delivered}/${s.sent}` },
      { label: 'dup · err', value: `${s.duplicates} · ${s.errors}` },
      { label: 'p50/p95', value: lat.p50 != null ? `${lat.p50}/${lat.p95} ms` : '—' },
      { label: 'last run', value: wsStats[id].ts ? new Date(wsStats[id].ts).toLocaleTimeString() : '—' },
    ].map((r) => ({ kind: 'wsstat', ...r }))
  }
  const rowsOf = (node) => {
    if (!node) return []
    return [
      ...methodsOf(node.id).map((e) => ({ kind: 'method', e })),
      ...functionsOf(node.id).map((fn) => ({ kind: 'fn', fn })),
      ...consumersOf(node.id).map((c) => ({ kind: 'cons', c })),
      ...wsStatRowsOf(node.id),
    ]
  }

  // Custom service types can contribute their own diagram edges (e.g. a Download
  // Coordinator's chain/source view: who is pulling chunks from whom). Each registered
  // module is asked once for the whole system; an edge is { from, to, label?, className? }.
  const customModules = [...new Set(Object.values(CUSTOM_TYPES))]
  const customEdges = customModules.flatMap((m) => m.diagramEdges?.({ manifest, customState }) || [])

  // Layout helpers that fold in a node's live custom-type runtime (so a custom body band
  // reserves the right vertical space and edges hit the recomputed center).
  const heightOf = (n) => nodeHeight(n, lbRows, customState[n.id], rowsOf(n))
  // Center using the EFFECTIVE position (so edges follow a node while it's being dragged).
  const centerOf = (id) => {
    const n = byId[id]
    const p = posOf(n)
    return { x: p.x + NODE_W / 2, y: p.y + heightOf(n) / 2 }
  }
  // End a trace line at the target node's border (+ a small gap) instead of its center, so the
  // arrowhead lands just OUTSIDE the box — visibly pointing into the node — instead of hidden
  // under it. Walks back from the target center toward the source until it crosses the box edge.
  const borderPointToward = (fromCenter, toNode) => {
    const c = centerOf(toNode.id)
    const hw = NODE_W / 2
    const hh = heightOf(toNode) / 2
    const dx = c.x - fromCenter.x
    const dy = c.y - fromCenter.y
    if (!dx && !dy) return c
    const k = Math.min(
      Math.abs(dx) ? hw / Math.abs(dx) : Infinity,
      Math.abs(dy) ? hh / Math.abs(dy) : Infinity,
    )
    const GAP = 6 // px of clearance outside the box for the arrow tip
    const t = k + GAP / Math.hypot(dx, dy)
    return { x: c.x - dx * t, y: c.y - dy * t }
  }
  // A trace hop's line runs border-to-border along the center↔center axis. Starting at the
  // source's BORDER (not its center) matters for short hops: with a center start the midpoint —
  // where the sequence badge + info button sit — lands inside the source box, and the nodes
  // (drawn after the edges) cover it. Border-to-border keeps the midpoint in the visible gap.
  const traceLine = (fromId, toId) => {
    const ac = centerOf(fromId)
    const bc = centerOf(toId)
    const a = byId[fromId] ? borderPointToward(bc, byId[fromId]) : ac
    const b = byId[toId] ? borderPointToward(ac, byId[toId]) : bc
    return { a, b, mid: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 } }
  }

  // gRPC edges are derived from each service's manifest `grpc.clients[].targets`
  // (client → the server it dials), drawn distinct from HTTP/trace edges. A target
  // that isn't a real node is skipped so we never draw a dangling edge.
  const grpcEdges = []
  for (const n of nodes) {
    for (const c of n.grpc?.clients || []) {
      for (const t of c.targets || []) {
        if (byId[t] && t !== n.id) grpcEdges.push({ from: n.id, to: t, contract: c.contract })
      }
    }
  }

  // Outbound dependency connections a resilience policy can attach to: explicit
  // manifest edges + endpoint downstreams + gRPC client targets, deduped by
  // `from->to`. These are drawn as persistent, clickable lines; gRPC keeps its dashed
  // style. A connection carrying a circuit-breaker policy gets a mid-line breaker
  // circle. (Replication edges are not request paths — excluded.)
  const connByKey = new Map()
  const addConn = (from, to, kind, contract) => {
    if (!byId[from] || !byId[to] || from === to) return
    const key = `${from}->${to}`
    const prev = connByKey.get(key)
    if (!prev) connByKey.set(key, { from, to, kind, contract })
    else if (kind === 'grpc') { prev.kind = 'grpc'; prev.contract = contract }
  }
  for (const e of manifest.edges || []) addConn(e.from, e.to, 'dep')
  for (const e of endpoints) {
    for (const d of e.downstream || []) addConn(e.service, d, 'dep')
  }
  // A consumer function's loop calls/reads/writes its `downstream` nodes (e.g. an API it POSTs
  // to, a db it touches) — draw the same persistent service->downstream line endpoints get, so
  // the diagram reflects what the loop actually does (the cluster->service consume edge is a
  // manifest edge handled above). Applies to every consumer, present and future.
  for (const fns of Object.values(consumerFunctions)) {
    for (const c of fns || []) {
      for (const d of c.downstream || []) addConn(c.service, d, 'dep')
    }
  }
  for (const e of grpcEdges) addConn(e.from, e.to, 'grpc', e.contract)
  const connections = [...connByKey.values()]

  // Per-connection resilience policy (from the manifest edge) for the breaker circle.
  const resByConn = new Map(
    (manifest.edges || []).filter((e) => e.resilience).map((e) => [`${e.from}->${e.to}`, e.resilience]),
  )

  // Replica clusters: a primary database and the read replicas that stream from
  // it (single source of truth: each secondary's `replicaOf`). This drives both
  // the always-on double-headed arrow and the dotted cluster box.
  const replicaEdges = []
  const clusterMembers = new Map() // primaryId -> Set(member node ids, incl. the primary)
  for (const n of nodes) {
    const primaryId = n.replicaOf
    if (!primaryId || !byId[primaryId]) continue
    replicaEdges.push({ from: primaryId, to: n.id })
    if (!clusterMembers.has(primaryId)) clusterMembers.set(primaryId, new Set([primaryId]))
    clusterMembers.get(primaryId).add(n.id)
  }
  const CLUSTER_PAD = 14
  const clusterBoxes = [...clusterMembers.values()].map((ids) => {
    const members = [...ids].map((id) => byId[id]).filter(Boolean)
    const minX = Math.min(...members.map((m) => posOf(m).x))
    const minY = Math.min(...members.map((m) => posOf(m).y))
    const maxX = Math.max(...members.map((m) => posOf(m).x + NODE_W))
    const maxY = Math.max(...members.map((m) => posOf(m).y + heightOf(m)))
    return {
      x: minX - CLUSTER_PAD,
      y: minY - CLUSTER_PAD,
      w: maxX - minX + 2 * CLUSTER_PAD,
      h: maxY - minY + 2 * CLUSTER_PAD,
    }
  })

  // The system boundary: the dotted box the user owns. It's a PERSISTED, freely
  // movable/resizable rectangle (manifest.boundary). Until the user customizes it, it
  // defaults to an auto-fit box around the internal (non-external) nodes so it starts
  // sensibly. Once persisted it's decoupled from node positions — a node may sit inside
  // or outside it, the user's call. A live override (mid-drag / just-dropped) wins.
  const BOUNDARY_PAD = 26
  const internalNodes = nodes.filter((n) => !n.external)
  let defaultBoundary = null
  if (internalNodes.length) {
    const minX = Math.min(...internalNodes.map((n) => posOf(n).x))
    const minY = Math.min(...internalNodes.map((n) => posOf(n).y))
    const maxX = Math.max(...internalNodes.map((n) => posOf(n).x + NODE_W))
    const maxY = Math.max(...internalNodes.map((n) => posOf(n).y + heightOf(n)))
    defaultBoundary = {
      x: minX - BOUNDARY_PAD,
      y: minY - BOUNDARY_PAD,
      w: maxX - minX + 2 * BOUNDARY_PAD,
      h: maxY - minY + 2 * BOUNDARY_PAD,
    }
  }
  const boundary = boundaryOverride || manifest.boundary || defaultBoundary

  // Resolve the selection against the live endpoint list every render, so a
  // selected endpoint that disappears (e.g. its service was deleted) clears
  // itself instead of leaving a dangling trace.
  const selected = visibleEndpoints.find((e) => endpointKey(e) === selectedKey) || null

  // Compute the trace: the set of nodes on the path and the directed edges
  // between them (LB -> service -> each downstream node).
  const lbNode = nodes.find(isLB)
  let traceNodes = null // null === nothing selected (no dimming)
  let traceEdges = []

  // A client-function trace wins over everything: client → LB → each called in-system service
  // (an external service is called directly, client → external, bypassing the LB) → its
  // downstreams, with every called method highlighted on its service (see fnSelected below).
  const ft = functionTrace && byId[functionTrace.client] ? functionTrace : null
  // A service-rooted method trace (picked from a service's Edit ▸ Calls tab) wins over
  // an LB endpoint selection: it highlights the service and ONLY the nodes that one
  // method calls (service → each downstream). The service node it points at must still
  // exist in this system.
  const methodService = methodTrace && byId[methodTrace.service] ? byId[methodTrace.service] : null
  // A consumer trace highlights one consume edge: cluster → consuming service. Both nodes must
  // still exist. Mutually exclusive with the other traces (App nulls the others when one is set).
  const ct = consumerTrace && byId[consumerTrace.cluster] && byId[consumerTrace.service] ? consumerTrace : null
  if (ft && ft.wsBuiltin) {
    // A ws pool client's builtin method has no authored steps — trace the TIER path
    // instead: client → its L4 lb → each relay server → the bus + presence redis.
    // Both methods (spawnAndSend / onReceive) share it: messages traverse the same
    // path in each direction. Derived from the manifest's wsTier/wsRole fields, so
    // there's nothing to go stale.
    const tierId = byId[ft.client]?.wsTier
    const ids = new Set([ft.client])
    if (byId[tierId]) {
      ids.add(tierId)
      traceEdges.push([ft.client, tierId])
      for (const srv of nodes) {
        if (srv.wsTier !== tierId || srv.wsRole !== 'server') continue
        ids.add(srv.id)
        traceEdges.push([tierId, srv.id])
        for (const r of nodes) {
          if (r.wsTier !== tierId || (r.wsRole !== 'bus' && r.wsRole !== 'presence')) continue
          ids.add(r.id)
          traceEdges.push([srv.id, r.id])
        }
      }
    }
    traceNodes = ids
  } else if (ft) {
    const ids = new Set([ft.client])
    const seenSvc = new Set()
    // Draw each directed edge once — a duplicate would stack a second sequence badge on the line.
    const seenEdge = new Set()
    // `label` (optional) becomes the edge's info-popup text — only the service → downstream hops
    // carry one (from the endpoint's downstreamDescriptions), matching the LB / method traces where
    // the client → LB / LB → service entry hops are label-less.
    const addEdge = (from, to, label) => {
      const k = `${from}->${to}`
      if (from === to || seenEdge.has(k)) return
      seenEdge.add(k)
      traceEdges.push([from, to, label || ''])
    }
    // A call to an in-system service goes through our load balancer (client → LB → service);
    // a call to a third-party external service is DIRECT (client → external) and never touches
    // our LB. So the client → LB hop is part of the trace only when some in-system service is hit.
    for (const m of ft.methods || []) {
      const svc = byId[m.service]
      if (!svc) continue
      ids.add(m.service)
      // Only the client's OWN steps get a client-entry hop. A method reached transitively
      // (through a downstream / external service) is linked instead by its caller's edge below,
      // so we never draw a phantom client → service line for something the client never calls.
      if (m.direct && !seenSvc.has(m.service)) {
        seenSvc.add(m.service)
        if (svc.type === 'external_service') {
          addEdge(ft.client, m.service) // direct client → external (outside our system)
        } else if (lbNode) {
          ids.add(lbNode.id)
          // Emit client → LB right before LB → service so the sequence badges read
          // client → LB (1) → service (2)…; addEdge dedupes, so later direct methods
          // reuse this same client → LB edge instead of stacking a second one at the end.
          addEdge(ft.client, lbNode.id)
          addEdge(lbNode.id, m.service)
        }
      }
      for (const d of m.downstream || []) {
        if (byId[d]) {
          ids.add(d)
          addEdge(m.service, d, (m.downstreamDescriptions || {})[d] || '')
        }
      }
    }
    traceNodes = new Set([...ids].filter((id) => byId[id]))
  } else if (methodService) {
    const ids = new Set([methodService.id])
    const dd = methodTrace.downstreamDescriptions || {}
    for (const d of methodTrace.downstream || []) {
      if (byId[d]) {
        ids.add(d)
        traceEdges.push([methodService.id, d, dd[d] || ''])
      }
    }
    traceNodes = ids
  } else if (ct) {
    // cluster → consuming service (the consume edge; the 3rd tuple element labels it with the
    // topic, drawn at the line midpoint like a connection description) → each node the loop then
    // calls/reads/writes (its downstream), so the trace shows the whole path the consumed message
    // drives, mirroring an endpoint trace's service → downstreams. Each downstream edge carries this
    // consumer's `downstreamDescriptions[d]` label, exactly as the endpoint trace does.
    const cds = (ct.downstream || []).filter((d) => byId[d])
    const cdd = ct.downstreamDescriptions || {}
    traceNodes = new Set([ct.cluster, ct.service, ...cds])
    traceEdges.push([ct.cluster, ct.service, ct.topic])
    for (const d of cds) traceEdges.push([ct.service, d, cdd[d] || ''])
  } else if (selected && lbNode) {
    const ids = new Set([lbNode.id, selected.service, ...(selected.downstream || [])])
    // Extend the trace back to the clients that actually call this endpoint:
    // client → LB → service → downstream. A client "calls" it when one of its functions
    // has a STEP that resolves to this endpoint (a direct entry, not a transitive downstream
    // hit); clients that never hit it (e.g. a trigger that only calls other routes) are left
    // out, so we don't draw a phantom client → LB line that visually cuts through unrelated nodes.
    const selKey = endpointKey(selected)
    for (const c of clientNodes) {
      const calls = functionsOf(c.id).some((fn) =>
        deriveFunctionTrace(fn, endpoints, c.id).methods.some((m) => m.direct && endpointKey(m) === selKey),
      )
      if (!calls) continue
      ids.add(c.id)
      traceEdges.push([c.id, lbNode.id])
    }
    // Only keep nodes that actually exist in this system.
    traceNodes = new Set([...ids].filter((id) => byId[id]))
    if (byId[selected.service]) {
      traceEdges.push([lbNode.id, selected.service])
      const dd = selected.downstreamDescriptions || {}
      for (const d of selected.downstream || []) {
        if (byId[d]) traceEdges.push([selected.service, d, dd[d] || ''])
      }
    }
  }

  // Size the canvas to the true bounding box of everything (nodes ⊕ the system
  // boundary), plus a margin. Clients sit LEFT of the system, so x can be negative —
  // the viewBox origin moves with it instead of being pinned at 0,0.
  const xStarts = nodes.map((n) => posOf(n).x)
  const xEnds = nodes.map((n) => posOf(n).x + NODE_W)
  const yStarts = nodes.map((n) => posOf(n).y)
  const yEnds = nodes.map((n) => posOf(n).y + heightOf(n))
  if (boundary) {
    xStarts.push(boundary.x)
    xEnds.push(boundary.x + boundary.w)
    yStarts.push(boundary.y)
    yEnds.push(boundary.y + boundary.h)
  }
  let originX = Math.min(...xStarts) - MARGIN
  let originY = Math.min(...yStarts) - MARGIN
  let width = Math.max(...xEnds) - originX + MARGIN
  let height = Math.max(...yEnds) - originY + MARGIN
  // During an active drag/resize, pin the world to the snapshot taken at drag start — no
  // shrink AND no grow. Growing (to keep an outward-dragged item in view) shifts worldX/worldW,
  // which scrolls the visible content toward the edge as you drag — exactly the "the window
  // follows the box" jank we want to avoid. There's PAN_FACTOR× empty margin around the content,
  // so a dragged item stays rendered into that margin; the bounds re-fit on drop.
  if (frozenView) {
    originX = frozenView.originX
    originY = frozenView.originY
    width = frozenView.width
    height = frozenView.height
  }

  // The bounded "world" the SVG renders into: the tight content bounds inflated symmetrically to
  // ~PAN_FACTOR× (so content stays centered and there's empty room to scroll around). viewBox = world,
  // pixel size = world × zoom, so on-screen scale is exactly `zoom`. Derived only from the tight
  // bounds + the constant factor, so it's stable across metric polls (centering won't fight scrolling).
  const worldX = originX - width
  const worldY = originY - height
  const worldW = width * PAN_FACTOR
  const worldH = height * PAN_FACTOR

  const dimmed = (id) => traceNodes && !traceNodes.has(id)

  // Every (service, method) a traced function calls — used to light up each called method
  // row across multiple services at once (as if the user clicked each one).
  const fnSelected = new Set((functionTrace?.methods || []).map((m) => `${m.service} ${m.method} ${m.path}`))

  // The method-trace analogue of fnSelected: when one method is selected (e.g. an external
  // service's), light up the SPECIFIC methods it calls on each downstream service — from the
  // endpoint's `downstreamMethods` map (node id -> ["METHOD /path", …], service-local). Each
  // call is resolved against that node's real rows, so a service-local path, an LB-prefixed
  // path, or the function alias all match — and only methods that actually exist light up.
  const methodSelected = new Set()
  if (methodService) {
    const dm = methodTrace.downstreamMethods || {}
    for (const [nodeId, calls] of Object.entries(dm)) {
      const nodeRows = methodsByNode.get(nodeId)
      if (!nodeRows) continue
      for (const c of calls || []) {
        const parts = String(c).trim().split(/\s+/)
        const m = parts.length > 1 ? parts[0].toUpperCase() : ''
        const ref = parts.length > 1 ? parts.slice(1).join(' ') : parts[0]
        const hit = nodeRows.find(
          (r) => (!m || r.method === m) && (localPathOf(r) === ref || r.path === ref || r.alias === ref),
        )
        if (hit) methodSelected.add(`${nodeId} ${endpointKey(hit)}`)
      }
    }
  }

  // --- Drag mode --------------------------------------------------------------
  // The SVG renders 1 user-unit = 1 px (width/height attrs match the viewBox dims, no CSS
  // scaling), so a screen-pixel pointer delta equals a user-unit delta — no CTM math needed.
  // Pointer capture on the <svg> routes move/up here even if the cursor leaves the element.
  const persistLayout = (payload) => {
    if (!systemId) return
    fetch('/api/layout', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ system: systemId, ...payload }),
    }).catch(() => {
      /* keep the optimistic local override; the next drop retries the save */
    })
  }

  const beginDrag = (e, g) => {
    if (!dragMode) return
    e.stopPropagation()
    try {
      svgRef.current?.setPointerCapture(e.pointerId)
    } catch {
      /* capture is best-effort */
    }
    setFrozenView({ originX, originY, width, height })
    gesture.current = { ...g, pointerId: e.pointerId, startClientX: e.clientX, startClientY: e.clientY, moved: false }
  }

  // The boundary rect for a given gesture + cumulative delta (shared by move + drop).
  const rectFor = (g, dx, dy) =>
    g.kind === 'boundary-move'
      ? { ...g.startRect, x: Math.round(g.startRect.x + dx), y: Math.round(g.startRect.y + dy) }
      : resizeRect(g.startRect, g.handle, dx, dy)

  const onPointerMove = (e) => {
    const g = gesture.current
    if (!g) return
    // Jitter gate uses raw SCREEN pixels; the layout deltas are divided by `zoom` because the SVG now
    // renders at `zoom` px per user-unit (it was 1:1 before pan/zoom).
    const sdx = e.clientX - g.startClientX
    const sdy = e.clientY - g.startClientY
    if (!g.moved && Math.abs(sdx) + Math.abs(sdy) <= 2) return // ignore sub-pixel jitter / a click
    g.moved = true
    const dx = sdx / zoom
    const dy = sdy / zoom
    if (g.kind === 'node') {
      setDrag((d) => ({ ...d, [g.nodeId]: { x: Math.round(g.startPos.x + dx), y: Math.round(g.startPos.y + dy) } }))
    } else {
      setBoundaryOverride(rectFor(g, dx, dy))
    }
  }

  const endDrag = (e) => {
    const g = gesture.current
    if (!g) return
    gesture.current = null
    // Before releasing the freeze, stash the frozen world origin (worldX/Y = originX/Y − w/h) and
    // the current scroll. Once frozenView clears, the bounds re-fit to the dropped position and the
    // world origin moves; the layout effect below shifts scroll by that delta so nothing jumps.
    const el = scrollRef.current
    if (el && frozenView && g.moved) {
      preDropScroll.current = {
        worldX: frozenView.originX - frozenView.width,
        worldY: frozenView.originY - frozenView.height,
        scrollLeft: el.scrollLeft,
        scrollTop: el.scrollTop,
      }
    }
    setFrozenView(null)
    try {
      svgRef.current?.releasePointerCapture(g.pointerId)
    } catch {
      /* already released */
    }
    if (!g.moved) return // a click with no movement: nothing to persist
    const dx = (e.clientX - g.startClientX) / zoom
    const dy = (e.clientY - g.startClientY) / zoom
    if (g.kind === 'node') {
      persistLayout({ positions: { [g.nodeId]: { x: Math.round(g.startPos.x + dx), y: Math.round(g.startPos.y + dy) } } })
    } else {
      persistLayout({ boundary: rectFor(g, dx, dy) })
    }
  }

  // --- Pan/zoom ----------------------------------------------------------------
  // Pan is native scrolling of `scrollRef` (the overflow:auto wrapper). Zoom only scales the SVG's
  // pixel size; the scroll offset is re-anchored so the point under the cursor stays fixed. The
  // scroll range only exists after the SVG resizes, so the re-anchor is stashed in `pendingScroll`
  // and applied in the useLayoutEffect keyed on `zoom` below.
  const clampZoom = (z) => Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, z))

  const applyZoom = (next, offsetX, offsetY) => {
    const el = scrollRef.current
    if (!el) return
    const z = clampZoom(next)
    if (z === zoom) return
    const ratio = z / zoom
    pendingScroll.current = {
      left: (el.scrollLeft + offsetX) * ratio - offsetX,
      top: (el.scrollTop + offsetY) * ratio - offsetY,
    }
    setZoom(z)
  }

  // Center the world (and the content centered within it) in the viewport.
  const centerView = () => {
    const el = scrollRef.current
    if (!el) return
    el.scrollLeft = (el.scrollWidth - el.clientWidth) / 2
    el.scrollTop = (el.scrollHeight - el.clientHeight) / 2
  }

  // Ctrl/Cmd+0 — back to 100% and recentered. Already at 100% → center now (no resize); otherwise
  // center after the resize via pendingScroll (target computed from the zoom-1 world pixel size).
  const resetView = () => {
    const el = scrollRef.current
    if (!el) return
    if (zoom === 1) {
      centerView()
      return
    }
    pendingScroll.current = {
      left: Math.max(0, (worldW - el.clientWidth) / 2),
      top: Math.max(0, (worldH - el.clientHeight) / 2),
    }
    setZoom(1)
  }

  // Apply the stashed cursor-anchored scroll AFTER the SVG has resized to the new zoom.
  useLayoutEffect(() => {
    const el = scrollRef.current
    const p = pendingScroll.current
    if (!el || !p) return
    pendingScroll.current = null
    el.scrollLeft = p.left
    el.scrollTop = p.top
  }, [zoom])

  // On drop, the freeze clears and the bounds re-fit to the moved item, shifting the world origin.
  // Re-anchor the scroll by that delta (× zoom) so the same world point stays under the same pixel
  // and the view holds perfectly still. worldX/worldY here are the fresh post-drop bounds.
  useLayoutEffect(() => {
    if (frozenView) return
    const p = preDropScroll.current
    if (!p) return
    preDropScroll.current = null
    const el = scrollRef.current
    if (!el) return
    el.scrollLeft = p.scrollLeft + (p.worldX - worldX) * zoom
    el.scrollTop = p.scrollTop + (p.worldY - worldY) * zoom
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [frozenView])

  // Center on first render and whenever the system changes.
  useLayoutEffect(() => {
    centerView()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [systemId])

  // Close any open trace-hop popup when the active trace changes (selecting another method / LB
  // endpoint / function / consumer), so a leftover popup index can't render against a new trace.
  useEffect(() => {
    setOpenInfo(null)
  }, [methodTrace, functionTrace, consumerTrace, selectedKey])

  // ctrl/cmd + wheel (and trackpad pinch, which browsers deliver as wheel+ctrlKey) zooms toward the
  // cursor; plain wheel falls through to native scroll (pan). React's onWheel is passive so its
  // preventDefault is unreliable — bind a native non-passive listener, rebound on `zoom` for scale.
  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    const onWheel = (e) => {
      // While a node/boundary drag is in progress, lock the view: swallow the wheel so neither
      // ctrl/cmd-zoom nor native scroll-pan fights the drag.
      if (gesture.current) { e.preventDefault(); return }
      if (!(e.ctrlKey || e.metaKey)) return
      e.preventDefault()
      const rect = el.getBoundingClientRect()
      applyZoom(zoom * Math.exp(-e.deltaY * 0.006), e.clientX - rect.left, e.clientY - rect.top)
    }
    el.addEventListener('wheel', onWheel, { passive: false })
    return () => el.removeEventListener('wheel', onWheel)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [zoom])

  // Ctrl/Cmd+0 resets the view; ignore it inside the terminal so typing isn't hijacked.
  useEffect(() => {
    const onKey = (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === '0' && !e.target?.closest?.('.terminal-panel')) {
        e.preventDefault()
        resetView()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [zoom])

  return (
    <div className="diagram-scroll" ref={scrollRef}>
    <svg
      ref={svgRef}
      className={dragMode ? 'diagram drag-mode' : 'diagram'}
      viewBox={`${worldX} ${worldY} ${worldW} ${worldH}`}
      width={worldW * zoom}
      height={worldH * zoom}
      onClick={
        dragMode
          ? undefined
          : () => {
              setSelectedKey(null)
              onClearMethodTrace?.()
              onClearFunctionTrace?.()
              onClearConsumerTrace?.()
            }
      }
      onPointerMove={dragMode ? onPointerMove : undefined}
      onPointerUp={dragMode ? endDrag : undefined}
      onPointerCancel={dragMode ? endDrag : undefined}
    >
      <defs>
        <marker
          id="trace-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill={TRACE_COLOR} />
        </marker>
        <marker
          id="grpc-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill={GRPC_COLOR} />
        </marker>
        <marker
          id="replica-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill={REPLICA_COLOR} />
        </marker>
      </defs>

      {/* System boundary: everything inside is our system; external services sit
          outside it. Drawn first so it sits behind every node and edge. */}
      {boundary && (
        <g style={{ pointerEvents: 'none' }}>
          <rect
            x={boundary.x}
            y={boundary.y}
            width={boundary.w}
            height={boundary.h}
            rx="14"
            className="system-boundary"
          />
          <text x={boundary.x + 12} y={boundary.y + 18} className="system-boundary-label">
            {manifest.name || manifest.system_id || 'system'}
          </text>
        </g>
      )}

      {/* Dotted box around each primary + its replicas; sits behind everything. */}
      {clusterBoxes.map((b, i) => (
        <rect
          key={`cluster-${i}`}
          x={b.x}
          y={b.y}
          width={b.w}
          height={b.h}
          rx="10"
          className="db-cluster-box"
        />
      ))}

      {/* Outbound dependency connections (deduped), behind the node boxes. Each is
          clickable to attach/edit a resilience policy; gRPC keeps its dashed style.
          A connection with a circuit breaker draws a mid-line breaker circle (+ live
          overlay). Dim while a lifecycle trace is active, matching the other edges. */}
      {connections.map(({ from, to, kind, contract }) => {
        const a = centerOf(from)
        const b = centerOf(to)
        const key = `${from}->${to}`
        const res = resByConn.get(key)
        const breakerOn = !!res?.circuit_breaker?.enabled
        const live = resilienceState[key]
        const dim = traceNodes ? ' dim' : ''
        const lineClass = kind === 'grpc' ? `grpc-edge${dim}` : `edge${dim}`
        const fromIsService = byId[from]?.type === 'service'
        const clickable = !!onRequestConnectionResilience && fromIsService && !dragMode
        const mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }
        const label = breakerOn ? breakerLabel(live) : null
        return (
          <g
            key={`conn-${key}`}
            className={clickable ? 'connection-hit' : undefined}
            onClick={
              clickable
                ? (e) => { e.stopPropagation(); onRequestConnectionResilience({ from, to }) }
                : undefined
            }
          >
            {clickable && <line x1={a.x} y1={a.y} x2={b.x} y2={b.y} className="connection-hitline" />}
            <line
              x1={a.x}
              y1={a.y}
              x2={b.x}
              y2={b.y}
              className={lineClass}
              markerEnd={kind === 'grpc' ? 'url(#grpc-arrow)' : undefined}
            >
              <title>{kind === 'grpc' ? `gRPC · ${contract}: ${from} → ${to}` : `${from} → ${to}`}</title>
            </line>
            {breakerOn && <BreakerCircle cx={mid.x} cy={mid.y} live={live} />}
            {label && (
              <text x={mid.x} y={mid.y - 11} className="breaker-label" style={{ pointerEvents: 'none' }}>
                {label}
              </text>
            )}
          </g>
        )
      })}

      {/* Replication: an always-on double-headed arrow primary↔secondary. Not
          dimmed during a trace — the cluster relationship is shown at all times. */}
      {replicaEdges.map((edge, i) => {
        const a = centerOf(edge.from)
        const b = centerOf(edge.to)
        return (
          <line
            key={`replica-${i}`}
            x1={a.x}
            y1={a.y}
            x2={b.x}
            y2={b.y}
            className="replica-edge"
            markerStart="url(#replica-arrow)"
            markerEnd="url(#replica-arrow)"
          >
            <title>{`replication · ${edge.from} ↔ ${edge.to}`}</title>
          </line>
        )
      })}

      {/* Custom service-type edges (e.g. the Download Coordinator's chunk-source view).
          Inert until a registered module returns edges. */}
      {customEdges.map((e, i) => {
        if (!byId[e.from] || !byId[e.to]) return null
        const a = centerOf(e.from)
        const b = centerOf(e.to)
        return (
          <line
            key={`custom-edge-${i}`}
            x1={a.x}
            y1={a.y}
            x2={b.x}
            y2={b.y}
            className={e.className || 'custom-edge'}
            markerEnd="url(#trace-arrow)"
          >
            <title>{e.label || `${e.from} → ${e.to}`}</title>
          </line>
        )
      })}

      {/* Highlighted trace edges (only while a trace is selected). Each line runs border-to-border
          (see traceLine) so the arrowhead is visible at the target and the midpoint stays in the
          gap between the boxes. The sequence badges + info buttons that sit at each midpoint are
          rendered AFTER the nodes (see the trace-badge block below) so a node box never covers
          them; the description popup stacks last of all (`openInfo` block). */}
      {traceEdges.map(([fromId, toId], i) => {
        const { a, b } = traceLine(fromId, toId)
        return (
          <line
            key={`trace-${i}`}
            x1={a.x}
            y1={a.y}
            x2={b.x}
            y2={b.y}
            className="trace-edge"
            markerEnd="url(#trace-arrow)"
          />
        )
      })}

      {nodes.map((node) => {
        const data = nodeData[node.id] || { metrics: {}, color: 'gray' }
        const rt = customState[node.id]
        // On-node clickable rows: a service's callable methods, or a client's attached
        // functions, listed below the metrics.
        const rows = rowsOf(node)
        const h = nodeHeight(node, lbRows, rt, rows)
        // A user-initiated temporary outage paints the node orange and wins over the
        // health-derived color (a deliberate shutdown looks down — but intentionally).
        const inOutage = outages[node.id]
        const color = inOutage ? COLOR_HEX.orange : COLOR_HEX[data.color] || COLOR_HEX.gray
        // Event-stream cluster with its consumers paused — badge it (amber, like an outage).
        const consumersPaused = pausedConsumers.has(node.id)
        // All per-node actions (schema/topics/endpoints/gRPC/shutdown/delete) now live
        // behind the bottom "Edit" button, so the node body itself is no longer clickable.
        const p = posOf(node)
        const gClass = [dimmed(node.id) ? 'dim' : '', dragMode ? 'draggable' : ''].filter(Boolean).join(' ') || undefined
        return (
          <g
            key={node.id}
            transform={`translate(${p.x}, ${p.y})`}
            className={gClass}
            onPointerDown={dragMode ? (e) => beginDrag(e, { kind: 'node', nodeId: node.id, startPos: p }) : undefined}
          >
            <rect
              width={NODE_W}
              height={h}
              rx="8"
              className={
                node.type === 'client'
                  ? 'node-box external client'
                  : node.external
                    ? 'node-box external'
                    : 'node-box'
              }
              style={{ stroke: color }}
            />
            {/* Header strip colored by health. */}
            <rect width={NODE_W} height={HEADER_H} rx="8" fill={color} />
            <rect width={NODE_W} height={HEADER_H / 2} fill={color} />
            <text x={PAD} y={HEADER_H / 2 + 5} className="node-label">
              {node.label}
            </text>
            <text x={NODE_W - PAD} y={HEADER_H / 2 + 5} className="node-type">
              {node.type === 'client' ? 'client' : node.external ? 'external' : node.type}
            </text>

            {isLB(node) ? (
              /* Service accordions: the EXTERNAL, routable endpoints (internal/operational
                 routes are filtered out) grouped under one collapsible header per owning
                 service. Headers toggle their group; each method row inside is clickable to
                 trace its lifecycle. Collapsed by default to keep the LB compact. */
              lbGroups.length === 0 ? (
                <text x={PAD} y={HEADER_H + PAD + 12} className="endpoint-empty">
                  no endpoints
                </text>
              ) : (
                (() => {
                  const rowsOut = []
                  let ri = 0
                  for (const g of lbGroups) {
                    const open = openLbServices.has(g.serviceId)
                    const hy = HEADER_H + PAD + ri * LINE_H
                    ri++
                    rowsOut.push(
                      <g
                        key={`grp-${g.serviceId}`}
                        className="endpoint-hit lb-group"
                        onClick={dragMode ? undefined : (ev) => {
                          ev.stopPropagation()
                          setOpenLbServices((prev) => {
                            const next = new Set(prev)
                            if (next.has(g.serviceId)) next.delete(g.serviceId)
                            else next.add(g.serviceId)
                            return next
                          })
                        }}
                      >
                        <rect x={4} y={hy - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                        <text x={PAD} y={hy + 12} className="lb-group-row">
                          <tspan className="lb-group-caret">{open ? '▾' : '▸'}</tspan> {g.label}
                          <tspan className="lb-group-count"> ({g.endpoints.length})</tspan>
                        </text>
                      </g>,
                    )
                    if (!open) continue
                    for (const e of g.endpoints) {
                      const y = HEADER_H + PAD + ri * LINE_H
                      ri++
                      const key = endpointKey(e)
                      const isSel = key === selectedKey
                      const name = e.alias || localPathOf(e)
                      rowsOut.push(
                        <g
                          key={key}
                          className={isSel ? 'endpoint-hit selected' : 'endpoint-hit'}
                          onClick={dragMode ? undefined : (ev) => {
                            ev.stopPropagation()
                            // An LB selection and a method/function/consumer trace never coexist.
                            onClearMethodTrace?.()
                            onClearFunctionTrace?.()
                            onClearConsumerTrace?.()
                            setSelectedKey(isSel ? null : key)
                          }}
                        >
                          <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                          <text x={PAD + 12} y={y + 12} className="endpoint-row">
                            <tspan className="endpoint-method">{e.method}</tspan> {name}
                          </text>
                        </g>,
                      )
                    }
                  }
                  return rowsOut
                })()
              )
            ) : (
              /* Metric rows. */
              (node.metrics || []).map((m, i) => {
                const y = HEADER_H + PAD + i * LINE_H + 12
                return (
                  <g key={m.label}>
                    <text x={PAD} y={y} className="metric-label">
                      {m.label}
                    </text>
                    <text x={NODE_W - PAD} y={y} className="metric-value">
                      {formatMetric(data.metrics[m.label], m)}
                    </text>
                  </g>
                )
              })
            )}

            {/* On-node clickable rows below the metrics, in one stacked band: a node's callable
                METHODS first (services + external services), then its own FUNCTIONS (clients).
                Clicking a method traces it individually (like the Edit ▸ Calls tab); clicking a
                function traces its whole call path (client → LB → services → downstreams) and
                highlights each called method on its service. */}
            {rows.map((row, i) => {
              const y = metricsBottom(node, lbRows) + METHOD_GAP + i * LINE_H
              if (row.kind === 'fn') {
                const fn = row.fn
                const active = !!functionTrace && functionTrace.client === node.id && functionTrace.name === fn.name
                return (
                  <g
                    key={`fn-${fn.name}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A function trace is exclusive with the LB / method / consumer selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearConsumerTrace?.()
                      if (active) onClearFunctionTrace?.()
                      else onSelectFunction?.(fn, node.id)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method">ƒ</tspan> {fn.name}
                    </text>
                  </g>
                )
              }
              if (row.kind === 'wsstat') {
                // A ws pool client's last-run delivery stat: read-only, metric-styled —
                // must come before the method fall-through below (which assumes row.e).
                return (
                  <g key={`wsstat-${row.label}`} style={{ pointerEvents: 'none' }}>
                    <text x={PAD} y={y + 12} className="metric-label">
                      {row.label}
                    </text>
                    <text x={NODE_W - PAD} y={y + 12} className="metric-value">
                      {row.value}
                    </text>
                  </g>
                )
              }
              if (row.kind === 'cons') {
                // A Kafka consumer function: clicking it traces the consume edge cluster → service.
                // "CONS" stands in for the GET/POST method badge an HTTP row would show.
                const c = row.c
                const active =
                  !!consumerTrace && consumerTrace.service === node.id && consumerTrace.name === c.name
                return (
                  <g
                    key={`cons-${c.name}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A consumer trace is exclusive with the LB / method / function selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearFunctionTrace?.()
                      if (active) onClearConsumerTrace?.()
                      else onSelectConsumer?.(c, node.id)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method">CONS</tspan> {c.name}
                    </text>
                  </g>
                )
              }
              const e = row.e
              const key = endpointKey(e)
              const isSel =
                (!!methodTrace && methodTrace.service === node.id && endpointKey(methodTrace) === key) ||
                fnSelected.has(`${node.id} ${key}`) ||
                methodSelected.has(`${node.id} ${key}`)
              const name = e.alias || localPathOf(e)
              return (
                <g
                  key={`method-${key}`}
                  className={isSel ? 'endpoint-hit selected' : 'endpoint-hit'}
                  onClick={dragMode ? undefined : (ev) => {
                    ev.stopPropagation()
                    // Method trace and LB / function / consumer selection never coexist.
                    setSelectedKey(null)
                    onClearFunctionTrace?.()
                    onClearConsumerTrace?.()
                    if (isSel) onClearMethodTrace?.()
                    else onSelectMethod?.(e)
                  }}
                >
                  <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                  <text x={PAD} y={y + 12} className="endpoint-row">
                    <tspan className="endpoint-method">{e.method}</tspan> {name}
                  </text>
                </g>
              )
            })}

            {/* Bottom "Edit" button on controllable nodes — opens the tabbed edit modal
                (endpoints / gRPC / schema / topics / shutdown / delete). Sits below the
                metrics: id on top, metrics in the middle, Edit at the bottom. */}
            {hasEditButton(node) && onRequestEdit && (
              <g
                className="node-edit"
                onClick={dragMode ? undefined : (e) => {
                  e.stopPropagation()
                  onRequestEdit(node)
                }}
              >
                <rect
                  x={PAD}
                  y={contentBottom(node, lbRows, rt, rows) + EDIT_GAP}
                  width={NODE_W - 2 * PAD}
                  height={EDIT_H}
                  rx="6"
                  className="node-edit-btn"
                />
                <text
                  x={NODE_W / 2}
                  y={contentBottom(node, lbRows, rt, rows) + EDIT_GAP + EDIT_H / 2}
                  className="node-edit-label"
                >
                  Edit
                </text>
              </g>
            )}

            {/* Custom service-type node body (e.g. the Download Coordinator's bitmap
                grid / aggregate progress). Rendered via the registry so the generic
                diagram stays type-agnostic. Inert until a module provides DiagramBody. */}
            {(() => {
              const mod = customTypeOf(node)
              if (!mod?.DiagramBody) return null
              const Body = mod.DiagramBody
              // Purely decorative — never interactive, so it can't swallow a click
              // meant for the Edit button (or anything beneath it).
              return (
                <g style={{ pointerEvents: 'none' }}>
                  <Body
                    node={node}
                    runtime={rt}
                    width={NODE_W}
                    top={methodsBottom(node, lbRows, rows) + CUSTOM_GAP}
                  />
                </g>
              )
            })()}

            {/* Outage countdown caption below the node, so it never collides with the
                metric/endpoint rows or the Edit button. */}
            {inOutage && (
              <text x={NODE_W / 2} y={h + 14} className="node-outage-label">
                ⏻ back in {inOutage.remaining_seconds}s
              </text>
            )}
            {/* Consumers-paused caption — offset below the outage line if both apply. */}
            {consumersPaused && (
              <text x={NODE_W / 2} y={h + (inOutage ? 28 : 14)} className="node-pause-label">
                ⏸ consumers paused
              </text>
            )}
          </g>
        )
      })}

      {/* Drag-mode handles for the system boundary box, drawn ON TOP of the nodes. The
          MOVE target is the box BORDER only (thick transparent stroke, fill:none) so the
          interior stays click-through to the nodes; the 8 squares resize it. */}
      {dragMode && boundary && (
        <g className="boundary-edit">
          <rect
            x={boundary.x}
            y={boundary.y}
            width={boundary.w}
            height={boundary.h}
            rx="14"
            className="boundary-move-target"
            onPointerDown={(e) => beginDrag(e, { kind: 'boundary-move', startRect: boundary })}
          >
            <title>Drag the border to move the system boundary</title>
          </rect>
          {BOUNDARY_HANDLES.map((hd) => (
            <rect
              key={hd.handle}
              x={boundary.x + hd.fx * boundary.w - HANDLE_SIZE / 2}
              y={boundary.y + hd.fy * boundary.h - HANDLE_SIZE / 2}
              width={HANDLE_SIZE}
              height={HANDLE_SIZE}
              className="boundary-handle"
              style={{ cursor: hd.cursor }}
              onPointerDown={(e) => beginDrag(e, { kind: 'boundary-resize', handle: hd.handle, startRect: boundary })}
            />
          ))}
        </g>
      )}

      {/* Trace hop badges: the sequence number (index + 1, edges are pushed in request order) and,
          for a hop that carries a description (the 3rd tuple element), an info button that opens
          the full text in a popup. Rendered after the nodes so a badge stays visible even when a
          short hop's midpoint sits right up against (or on) a node box. */}
      {traceEdges.map(([fromId, toId, label], i) => {
        const { mid } = traceLine(fromId, toId)
        const hasDesc = !!(label && label.trim())
        return (
          <foreignObject
            key={`trace-badge-${i}`}
            x={mid.x - 40}
            y={mid.y - 13}
            width={80}
            height={26}
            style={{ overflow: 'visible', pointerEvents: 'none' }}
          >
            <div className="trace-badge-row">
              <span className="trace-seq-badge">{i + 1}</span>
              {hasDesc && (
                <button
                  type="button"
                  className={`trace-info-btn${openInfo === i ? ' active' : ''}`}
                  aria-label={`Show step ${i + 1} description`}
                  onClick={(ev) => {
                    ev.stopPropagation()
                    setOpenInfo(openInfo === i ? null : i)
                  }}
                >
                  i
                </button>
              )}
            </div>
          </foreignObject>
        )
      })}

      {/* Open trace-hop description popup. Rendered last so it stacks above the nodes and the
          badges. Only one hop's popup is open at a time (`openInfo`). Closes via its × or by
          re-clicking its info button. */}
      {openInfo != null && traceEdges[openInfo] && (() => {
        const [fromId, toId, label] = traceEdges[openInfo]
        if (!label || !label.trim()) return null
        const { mid } = traceLine(fromId, toId)
        return (
          <foreignObject
            x={mid.x + 16}
            y={mid.y - 12}
            width={252}
            height={240}
            style={{ overflow: 'visible', pointerEvents: 'none' }}
          >
            <div className="trace-info-popup" onClick={(ev) => ev.stopPropagation()}>
              <div className="trace-info-head">
                <span className="trace-info-title">
                  Step {openInfo + 1} · {fromId} → {toId}
                </span>
                <button
                  type="button"
                  className="trace-info-close"
                  aria-label="Close description"
                  onClick={(ev) => {
                    ev.stopPropagation()
                    setOpenInfo(null)
                  }}
                >
                  ×
                </button>
              </div>
              <div className="trace-info-body">{label}</div>
            </div>
          </foreignObject>
        )
      })()}

    </svg>
    </div>
  )
}
