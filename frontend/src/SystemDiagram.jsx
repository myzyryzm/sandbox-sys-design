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
import { REDIS_BADGE, keyspaceLabel, keyspaceEdgeLabel } from './redisKeyspaceMeta.js'
import { CDC_BADGE, CDC_BADGE_CLASS, cdcOpsOf, cdcRuleKey, cdcEdgeLabel } from './cdcMeta.js'
import { DEFAULT_NODE_COLORS } from './nodeColors.js'

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
// Vertical gap between the stacked cards of a websocket server fleet (server → server →
// shared-methods panel) — small, so the tier reads as one contiguous combined body.
const STACK_GAP = 8
// Gap between the metric rows and an optional custom service-type body (e.g. a bitmap grid).
const CUSTOM_GAP = 8
// Gap above the on-node API method rows (services / external services list their own
// callable methods below their metrics, like the LB lists its endpoints).
const METHOD_GAP = 8

// A row's text spans NODE_W - 2*PAD px in 11px monospace (.endpoint-row, ~6.6px/char), so about
// this many characters fit before it spills past the card's right edge. Rows that can carry SEVERAL
// badges (a CDC rule badges every operation that fires it) budget against it and truncate the name.
const ROW_CHARS = Math.floor((NODE_W - 2 * PAD) / 6.6)
const truncate = (s, max) => (s.length > max ? s.slice(0, Math.max(1, max - 1)) + '…' : s)

const COLOR_HEX = {
  green: '#2e7d32',
  yellow: '#f9a825',
  red: '#c62828',
  gray: '#9e9e9e',
  // A user-initiated temporary outage (node shut down for N seconds). Distinct from
  // red (unhealthy/down for any reason) so a deliberate shutdown reads as intentional.
  orange: '#fb8c00',
}

// Defaults for the user-configurable relationship-edge colors (Settings → prefix colors).
// The LIVE values are derived from the `colors` prop inside the component; these fallbacks
// keep the diagram identical when no setting is present. Must match DEFAULT_PREFIX_COLORS in
// prefixColors.js and the badge fallbacks in styles.css so a row and its traced line stay in sync.
const DEFAULT_TRACE_COLOR = '#6ea8fe'
const DEFAULT_GRPC_COLOR = '#b18cf2'
const REPLICA_COLOR = '#3fb6a8'
// Kafka "consume" edge (a consumer service → the cluster it reads from). Solid amber so it
// reads as distinct from a solid-gray producer/dep edge that points the same way (into the cluster).
const DEFAULT_CONSUME_COLOR = '#e0a44f'
// Base dependency-edge color (matches `.edge` in styles.css) — reused for the arrowhead
// on the collapsed websocket-fleet in/out edges.
const EDGE_COLOR = '#5b6270'
// etcd discovery-wiring edge (registrant → etcd lease-put, etcd → listener watch). A muted
// cyan, distinct from the brighter trace blue / gRPC purple / replica teal; drawn faint +
// dashed (see `.etcd-edge`) so the always-on discovery topology stays subordinate to the
// bright click-trace that lights the same relationship.
const DEFAULT_ETCD_COLOR = '#5aa0c0'
// When A→B and B→A both exist, each line is nudged this many px perpendicular to its
// axis so the two opposing arrows render as separate parallel lines instead of overlapping.
const EDGE_PARALLEL_OFFSET = 7

// A synthetic trace endpoint standing for a websocket tier's whole server fleet (its dotted
// box), so a lifecycle trace collapses the relay fan-out into one hop into the box and one hop
// out per downstream — matching the collapsed base edges. Not a real manifest node id.
// An etcd node's KEY row labels its keyspace by the registrant service's name,
// camelCased (`llm-worker` → `llmWorker`) — friendlier than the /services/<name>/ prefix.
const camelName = (id) => id.replace(/-+([a-z0-9])/g, (_, c) => c.toUpperCase())
// A listening service's SUB row names its subscription after the etcd KEY it watches,
// `on`-prefixed like an event handler (`llm-worker` → `onLlmWorker`, `app-settings` → `onAppSettings`).
const onName = (id) => {
  const c = camelName(id)
  return 'on' + c.charAt(0).toUpperCase() + c.slice(1)
}

const WS_FLEET_PREFIX = 'ws-fleet:'
const wsFleetId = (tier) => `${WS_FLEET_PREFIX}${tier}`
const isFleetId = (id) => typeof id === 'string' && id.startsWith(WS_FLEET_PREFIX)
const fleetTierOf = (id) => id.slice(WS_FLEET_PREFIX.length)

// Auto-generated hop descriptions for a websocket pool client's builtin tier trace
// (client → L4 lb → relay fleet → presence + bus). The path has no authored steps — it's
// derived from the manifest's wsTier/wsRole fields — so, unlike an endpoint's
// downstreamDescriptions, these are synthesized from the tier's fixed routing model and
// shown in the same trace-hop info popup. The LB → server phrasing reflects the live lb
// algorithm; the presence lookup is numbered before the bus publish because that's the
// order the routing actually happens (find the target's server, then relay to it).
const WS_ALGO_PHRASE = {
  leastconn: 'least connections',
  roundrobin: 'round-robin',
  source: 'source-IP hashing',
}
const WS_HOP_DESC = {
  clientToLb:
    'User sends a chat message over its long-lived WebSocket connection to the L4 (TCP) load balancer. Because it balances at layer 4 — forwarding the raw TCP stream without terminating the WebSocket — the persistent connection stays pinned to one relay server for its whole lifetime, instead of being re-routed per message.',
  lbToServer: (algo) =>
    `Load balancer routes the message to a websocket server via ${WS_ALGO_PHRASE[algo] || 'least connections'}.`,
  serverToPresence:
    "Receiving server looks up the recipient in the presence cache to find which server that client is connected to — needed only when the recipient isn't already connected to the same server as the sender.",
  serverToBus:
    "Receiving server publishes the message to the redis pub/sub bus on the recipient's server channel — done only when the target client is connected to a different server than the one that received the message, so that server can deliver it to its locally-connected client.",
  serverToSink: (node) =>
    `When the recipient is offline — not connected to any relay server per the presence cache — the receiving server persists the undelivered message to ${node?.label || node?.id} (${node?.type || 'datastore'}) so it can be delivered or retrieved later.`,
}

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
// Prometheus is deletable too, but it's a VISUAL toggle only — its Delete tab removes
// the diagram node (+ self-scrape) via /api/prom-node; the container keeps running
// (all metric polling and every rebuild depend on it). Re-add it from the Add menu.
function isDeletable(node) {
  // A worker's token stream is a database node but never individually deletable —
  // it cascades with its worker (remove.js blocks a direct delete on streamOf).
  if (node.streamOf) return false
  return (
    node.type === 'service' ||
    node.type === 'service-lb' ||
    node.type === 'external_service' ||
    node.type === 'client' ||
    node.type === 'prometheus' ||
    node.origin === 'create-database' ||
    node.origin === 'create-event-stream' ||
    node.origin === 'create-etcd' ||
    node.origin === 'create-websockets'
  )
}

// Stable identity for an endpoint row (used for selection).
function endpointKey(e) {
  return `${e.method} ${e.path}`
}

// How many content rows a node draws, given its precomputed body-row count. For the LB
// this is the accordion count (one header per service group + the methods of each
// expanded group); for every other metric-bearing node it's the metrics-dropdown count
// (collapsed = 1 header row; expanded = header + metric/"no metrics" rows). The caller
// computes it per node via `bodyRowsOf`; the raw metric count is a defensive fallback.
function rowCount(node, bodyRows) {
  if (isLB(node)) return Math.max(bodyRows || 0, 1)
  return bodyRows != null ? bodyRows : (node.metrics || []).length
}

// Websocket relay servers are an interchangeable fleet: their whole editing surface
// (shared methods / per-server shutdown / tier delete) lives in the tier's
// shared-methods panel drawn below them, so the nodes themselves carry no Edit button.
function isWsServer(node) {
  return node.origin === 'create-websockets' && node.wsRole === 'server'
}

// An instance of a per-service load-balanced cluster: interchangeable, and managed only
// from the cluster entry's Load Balancing tab — so, like a ws server, it carries no Edit
// button of its own (and the diagram stacks it under its entry).
function isSvcInstance(node) {
  return node.type === 'service' && !!node.instanceOf
}

// The ordinal N of a load-balanced instance id `<entry>-N`, so the stack orders 1,2,3,….
function svcOrdinal(id) {
  const m = /-(\d+)$/.exec(id)
  return m ? Number(m[1]) : 0
}

// Controllable nodes carry a bottom "Edit" button (which opens the tabbed edit modal);
// the LB, other infra, websocket servers, and load-balanced instances do not. A scaler
// sidecar DOES carry one: it sits at the top of its group's stack and hosts the GROUP's
// Edit button (the click opens its base's modal — see the Edit onClick). EVERY redis
// node gets one regardless of origin — even a custom-type-owned stream (e.g. an LLM
// worker's) needs its Keyspaces tab, though its Shutdown/Delete stay with the owner.
function hasEditButton(node) {
  return (isDeletable(node) || node.type === 'redis') && !isWsServer(node) && !isSvcInstance(node)
}

// The BASE node of a WORKER GROUP — any custom type whose frontend module declares a
// `workerGroup` predicate (LLM workers, Kafka consumer groups). The group's name lives
// on the dotted box's enlarged top-left label instead of a header strip; the group's
// scaler card (scalerOf) renders at the base's position as the stack header, carrying
// the group's Edit button — a scaler-less group falls back to a bare Edit button. The
// base container's live data is shown on a render-only virtual `<name>-1` card stacked
// below, alongside the real `<name>-2..N` instances.
function isGroupBase(node) {
  return node.type === 'service' && !node.instanceOf && !!customTypeOf(node)?.workerGroup?.(node)
}

// Where a node's live data (nodeData / customState / outages) lives: a virtual group
// member card reads the base container's id via stateKey; every real node reads its own.
const dataKeyOf = (node) => node.stateKey || node.id

// Height of a worker group's entry (the bare Edit button).
const GROUP_ENTRY_H = EDIT_H
// Member cards stack in columns of at most this many; overflow wraps to a new column
// to the right, so a big group grows sideways instead of into the nodes below it.
const GROUP_COL_SIZE = 3
const GROUP_COL_GAP_X = 16

// y where the metric/endpoint rows end.
function metricsBottom(node, bodyRows) {
  return HEADER_H + PAD + rowCount(node, bodyRows) * LINE_H
}

// Height of the on-node API method rows band (the service's callable methods, listed
// below its metrics). 0 when the node lists none (LB, db, event stream, or a service
// with no visible methods).
function methodsBandHeight(methods) {
  return methods && methods.length ? METHOD_GAP + methods.length * LINE_H : 0
}

// y where the metrics + the method rows end.
function methodsBottom(node, bodyRows, methods) {
  return metricsBottom(node, bodyRows) + methodsBandHeight(methods)
}

// Height of a custom service-type body band (e.g. a Download Coordinator bitmap grid),
// reserved below the metrics. 0 when the node's type has no custom body for this state.
function customBandHeight(node, runtime) {
  const m = customTypeOf(node)
  return m?.diagramHeight ? m.diagramHeight(node, runtime, NODE_W) : 0
}

// y where all content (metrics + method rows + any custom band) ends — the Edit button
// sits a gap below.
function contentBottom(node, bodyRows, runtime, methods) {
  const band = customBandHeight(node, runtime)
  return methodsBottom(node, bodyRows, methods) + (band ? CUSTOM_GAP + band : 0)
}

function nodeHeight(node, bodyRows, runtime, methods) {
  const bottom = contentBottom(node, bodyRows, runtime, methods)
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

// Compact live pool badge from a service's /pool/state entry: "pool <active>/<max> · <idle> idle".
// Missing counts render as a dot so a partially-reporting service still shows something.
function poolLabel(live) {
  if (!live) return null
  const dot = (v) => (typeof v === 'number' ? v : '·')
  return `pool ${dot(live.active)}/${dot(live.max)} · ${dot(live.idle)} idle`
}

export default function SystemDiagram({
  manifest,
  nodeData,
  endpoints = [],
  systemId,
  // User-configurable relationship-edge colors (Settings → prefix colors): { function, grpc,
  // consumer, etcdEdge }. Fall back to the DEFAULT_* constants below so unset = original look.
  colors = {},
  // User-configurable colors for nodes no health rule paints (Settings → node colors):
  // { load_balancer }. See nodeColors.js; unset falls back to the gray they've always been.
  nodeColors = {},
  // Drag mode: when true, every node can be repositioned and the system boundary box can be
  // moved/resized; the normal click actions (trace/Edit/select) are suppressed. Drops are
  // persisted to the manifest via POST /api/layout.
  dragMode = false,
  onRequestEdit,
  onRequestConnectionResilience,
  resilienceState = {},
  poolState = {},
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
  // The websocket tier's SHARED server methods block (registry `methods`: onMessage /
  // onSend, each { base, entries, implemented, … }). Drawn in the per-tier panel below
  // the server fleet; its Edit button calls onRequestWsMethods(lbId) to open the shared
  // modal (methods / per-server shutdown / tier delete).
  wsMethods = null,
  onRequestWsMethods,
  // The websocket tier's lb balancing algorithm (registry `algorithm`: leastconn |
  // roundrobin | source). Only used to phrase the auto-generated LB → server hop
  // description on the builtin tier trace. One tier per system today.
  wsAlgorithm = 'leastconn',
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
  // A selected consumer function, traced consuming service → cluster (the consume edge). Mutually
  // exclusive with the method / function / LB selections. Set via onSelectConsumer, cleared via
  // onClearConsumerTrace.
  consumerTrace = null,
  onSelectConsumer,
  onClearConsumerTrace,
  // Each etcd cluster's keyspaces (systems/<id>/etcd.json), keyed by etcd node id. Rendered
  // as clickable KEY rows on the cluster node; clicking one traces registrant → etcd →
  // each listener.
  etcdKeyspaces = {},
  // A selected keyspace, traced registrant service → etcd (the lease-put keepalive) and
  // etcd → each listener (the watch push). Mutually exclusive with the other traces.
  keyspaceTrace = null,
  onSelectKeyspace,
  onClearKeyspaceTrace,
  // Trace ONE subscription (a service's SUB row): registrant → etcd → that one listener.
  // Reuses keyspaceTrace with a single-listener `listeners` + a `focus` marker.
  onSelectSubscription,
  // The gRPC contract registry (systems/<id>/grpc/_registry.json via /api/grpc-contracts) as
  // [{ name, methods:[{ name, … }] }]. A service that SERVES a contract (its manifest
  // `grpc.servers` lists the name) draws one clickable RPC row per method of that contract —
  // the method names live here, not on the node. Empty when nothing in the system serves gRPC.
  grpcContracts = [],
  // A selected served RPC method, traced each caller (a service dialing this contract with this
  // server in its `grpc.clients[].targets`) → this server. Mutually exclusive with the other
  // traces. Set via onSelectRpc, cleared via onClearRpcTrace.
  rpcTrace = null,
  onSelectRpc,
  onClearRpcTrace,
  // A selected redis keyspace (a `keyspaces` entry on a type:"redis" manifest node, rendered
  // as a typed KEY row), traced each declared writer → redis and redis → each declared reader.
  // Mutually exclusive with the other traces. Set via onSelectRedisKeyspace, cleared via
  // onClearRedisTrace.
  redisTrace = null,
  onSelectRedisKeyspace,
  onClearRedisTrace,
  // Each CDC worker's capture rules (systems/<id>/<db>/cdc.json), keyed by the WORKER's node id
  // — each { table, operations:[INSERT|UPDATE|DELETE], stream, topic }. Rendered as clickable
  // rule rows on the worker node, badged with the operations that fire them.
  cdcRules = {},
  // A selected CDC rule, traced worker → its stream ("publishes <entity> to <topic>") and onward
  // through every consumer function pulling that topic (its PULL row lights up and its own
  // downstream hops are drawn). Mutually exclusive with the other traces. Set via
  // onSelectCdcRule, cleared via onClearCdcTrace.
  cdcTrace = null,
  onSelectCdcRule,
  onClearCdcTrace,
}) {
  // Live relationship-edge colors from Settings, falling back to the module defaults so the
  // diagram is unchanged until the user overrides a color. These re-tint the same edges +
  // arrowheads whose badges the --badge-* CSS vars paint, keeping row and traced line in sync.
  const TRACE_COLOR = colors?.function ?? DEFAULT_TRACE_COLOR
  const GRPC_COLOR = colors?.grpc ?? DEFAULT_GRPC_COLOR
  const CONSUME_COLOR = colors?.consumer ?? DEFAULT_CONSUME_COLOR
  const ETCD_COLOR = colors?.etcdEdge ?? DEFAULT_ETCD_COLOR
  // The nginx LB carries no health rules, so it has always rendered in the "unknown" gray —
  // that static color is the user's to pick (see LB_COLOR's use in the node loop below).
  const LB_COLOR = nodeColors?.load_balancer ?? DEFAULT_NODE_COLORS.load_balancer
  const [selectedKey, setSelectedKey] = useState(null)
  // Index of the trace hop whose description popup is open (or null). Reset whenever the active
  // trace changes so a stale index can't point at the wrong hop of a different trace.
  const [openInfo, setOpenInfo] = useState(null)
  // Which LB service-accordions are expanded (set of service node ids). The LB groups its
  // routable endpoints by owning service and draws each group as a collapsible accordion;
  // every group is collapsed by default, so this starts empty.
  const [openLbServices, setOpenLbServices] = useState(() => new Set())
  // Which nodes have their metrics dropdown expanded (set of node ids). Every metric-bearing
  // node (service/db/kafka/cdc/prometheus) draws its metrics behind a collapsible "Metrics"
  // header like the LB's accordion; collapsed by default, so this starts empty.
  const [openNodeMetrics, setOpenNodeMetrics] = useState(() => new Set())

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

  // Render-only ordinal-1 cards for worker groups: the base node draws as the compact
  // group entry, and this virtual card shows the base CONTAINER's data (stateKey) as
  // `<name>-1` in the member column. `::` is outside NAME_RE, so the synthetic id can
  // never collide with a real node. instanceOf gives it the instance behaviors for free
  // (no Edit button, no method rows, group drag via the base). NOT added to byId — edges,
  // traces and drag-entry lookups must only ever see manifest nodes.
  const groupVirtuals = nodes.filter(isGroupBase).map((b) => ({
    ...b,
    id: `${b.id}::1`,
    label: `${b.id}-1`,
    instanceOf: b.id,
    stateKey: b.id,
    groupVirtual: true,
  }))
  const renderNodes = [...nodes, ...groupVirtuals]

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

  // Whether a Prometheus node is on the diagram. When it isn't, observability is "off":
  // App polls nothing, and every metrics dropdown reads "no metrics" instead of rows.
  const hasPrometheus = nodes.some((n) => n.type === 'prometheus')

  // Metric-bearing (non-LB) nodes draw their metrics behind a collapsible "Metrics" header.
  // Rows the body draws: 0 when the node has no metrics; 1 (just the header) when collapsed;
  // header + metric rows when expanded (or header + one "no metrics" row when Prometheus is
  // absent). Feeds the layout helpers so collapsing/expanding actually resizes the box.
  const metricBodyRows = (n) => {
    const count = (n.metrics || []).length
    if (!count) return 0
    if (!openNodeMetrics.has(n.id)) return 1
    return 1 + (hasPrometheus ? count : 1)
  }
  // The per-node body-row count the layout helpers want: the LB's accordion rows, or a
  // metric node's dropdown rows.
  const bodyRowsOf = (n) => (isLB(n) ? lbRows : metricBodyRows(n))

  // Each service / external service lists its OWN callable methods on its node (like the
  // LB lists endpoints), so the diagram can trace one straight from the service. Drops
  // hidden routes (e.g. an external service's /health) plus the generic /health liveness
  // probe — it's operational plumbing with no downstream to trace, so it's just noise on
  // the diagram (it stays listed, badged, in the Edit ▸ Endpoints/Calls tabs). Keyed by
  // node id; empty for non-endpoint-host nodes.
  const methodsByNode = new Map()
  for (const n of nodes) {
    // A load-balanced service's cluster entry (`service-lb`) still owns its endpoints under
    // `<name>`, so it lists them like a service. Its instances (`instanceOf`) serve the same
    // routes but are never addressed individually — they list nothing.
    if (n.type !== 'service' && n.type !== 'external_service' && n.type !== 'service-lb') continue
    if (n.instanceOf) continue
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
  // An etcd cluster's keyspaces, dropping any DISCOVERY keyspace whose registrant service no
  // longer exists (so a deleted service can't leave a dangling KEY row that traces to nothing).
  // Config keyspaces have no registrant and always render.
  const keyspacesOf = (id) => (etcdKeyspaces[id] || []).filter((k) => k.type === 'config' || byId[k.service])
  // A SERVICE's etcd subscriptions: every keyspace (across all etcd clusters) whose
  // `listeners` include this service. Each becomes a clickable SUB row on the service node
  // (the mirror image of the etcd node's KEY rows) that traces registrant → etcd → this
  // service. Returns { etcdId, ks } so the row's click knows which cluster owns the keyspace.
  const subscriptionsOf = (id) =>
    Object.entries(etcdKeyspaces).flatMap(([etcdId, kss]) =>
      (kss || [])
        .filter((ks) => (ks.listeners || []).some((l) => l.service === id))
        .map((ks) => ({ etcdId, ks })),
    )
  // A service's SERVED gRPC methods: for each contract its manifest `grpc.servers` lists, one
  // row per method of that contract (method names come from the registry, not the node). Only a
  // server-role attachment produces rows — a pure client (`grpc.clients` only) serves nothing,
  // and group instances (`instanceOf`) never carry the block, so both list nothing here.
  const grpcMethodsByContract = new Map((grpcContracts || []).map((c) => [c.name, c.methods || []]))
  const rpcsOf = (id) => {
    const servers = byId[id]?.grpc?.servers || []
    if (!servers.length) return []
    return servers.flatMap((contract) =>
      (grpcMethodsByContract.get(contract) || []).map((m) => ({ contract, method: m.name })),
    )
  }
  // A CDC worker's capture rules, dropping any whose target stream node no longer exists (so a
  // deleted cluster can't leave a dangling rule row that traces to nothing) — the same guard
  // consumersOf / keyspacesOf apply.
  const cdcRulesOf = (node) =>
    node.type === 'cdc' ? (cdcRules[node.id] || []).filter((r) => byId[r.stream]) : []
  const rowsOf = (node) => {
    if (!node) return []
    return [
      ...methodsOf(node.id).map((e) => ({ kind: 'method', e })),
      // Served RPC rows sit with the HTTP endpoints — both are callable surfaces this service
      // exposes. Like CONS/SUB, a worker group serves from its base container, so the virtual
      // `<name>-1` member card renders them (stateKey = the base id that carries the grpc block).
      ...rpcsOf(node.groupVirtual ? node.stateKey : node.id).map((r) => ({ kind: 'rpc', ...r })),
      ...functionsOf(node.id).map((fn) => ({ kind: 'fn', fn })),
      // A worker group's base renders as a bare Edit button, so its CONS rows live on
      // the virtual `<name>-1` member card instead (stateKey = the base id). Real
      // instances stay row-less — one row per group, not per member.
      ...consumersOf(node.groupVirtual ? node.stateKey : node.id).map((c) => ({ kind: 'cons', c })),
      ...keyspacesOf(node.id).map((ks) => ({ kind: 'ks', ks })),
      // A service's etcd subscriptions (SUB rows), anchored on the real node like CONS rows.
      ...subscriptionsOf(node.groupVirtual ? node.stateKey : node.id).map((s) => ({ kind: 'sub', ...s })),
      // A redis node's declared keyspaces (typed KEY rows), straight off the manifest node.
      ...(node.type === 'redis' ? node.keyspaces || [] : []).map((ks) => ({ kind: 'redisks', ks })),
      // A CDC worker's capture rules (one row per rule, badged with the ops that fire it).
      ...cdcRulesOf(node).map((r) => ({ kind: 'cdcrule', r })),
      ...wsStatRowsOf(node.id),
    ]
  }

  // Custom service types can contribute their own diagram edges (e.g. a Download
  // Coordinator's chain/source view: who is pulling chunks from whom). Each registered
  // module is asked once for the whole system; an edge is { from, to, label?, className? }.
  const customModules = [...new Set(Object.values(CUSTOM_TYPES))]
  const customEdges = customModules.flatMap((m) => m.diagramEdges?.({ manifest, customState }) || [])

  // etcd discovery wiring as ALWAYS-ON faint arrows, derived live from etcd.json (etcdKeyspaces)
  // — the persistent mirror of the click-trace: each discovery keyspace's registrant service →
  // its etcd cluster (the leased-key keepalive in), and etcd → each listener service (the watch
  // push out). Config keyspaces have no registrant, so they contribute only the listener edges.
  // Deduped per (from,to): a service listening on many keyspaces draws ONE arrow, and a worker
  // registering draws ONE arrow regardless of instance count. Nodes that no longer exist are
  // skipped (a deleted registrant/listener can't leave a dangling arrow). These are pure render
  // state — there are no matching manifest edges (see the keyspace-trace block below).
  const etcdEdgeMap = new Map() // "from->to" -> { from, to, kind }
  for (const [etcdId, kss] of Object.entries(etcdKeyspaces)) {
    if (!byId[etcdId]) continue
    for (const ks of kss || []) {
      if (ks.type !== 'config' && ks.service && byId[ks.service]) {
        etcdEdgeMap.set(`${ks.service}->${etcdId}`, { from: ks.service, to: etcdId, kind: 'register' })
      }
      for (const l of ks.listeners || []) {
        if (l.service && byId[l.service]) {
          etcdEdgeMap.set(`${etcdId}->${l.service}`, { from: etcdId, to: l.service, kind: 'watch' })
        }
      }
    }
  }
  const etcdEdges = [...etcdEdgeMap.values()]

  // Layout helpers that fold in a node's live custom-type runtime (so a custom body band
  // reserves the right vertical space and edges hit the recomputed center). A group base
  // renders as the fixed-height entry button; a virtual `<name>-1` card reads the base
  // container's runtime via dataKeyOf so its live-body band is reserved too.
  const heightOf = (n) =>
    isGroupBase(n) ? GROUP_ENTRY_H : nodeHeight(n, bodyRowsOf(n), customState[dataKeyOf(n)], rowsOf(n))

  // Websocket relay servers render as ONE combined body: a rigid vertical stack (one shared
  // column x, cascaded top-to-bottom by height + STACK_GAP) instead of each server floating at
  // its own manifest y. `effPos` returns that stacked position for a ws-server and the normal
  // (drag/manifest) position for everything else — every geometry consumer below reads it.
  const wsServersByTier = new Map() // tier id -> its server nodes, in creation order (ws-server-1,-2,…)
  for (const n of nodes) {
    if (!isWsServer(n)) continue
    if (!wsServersByTier.has(n.wsTier)) wsServersByTier.set(n.wsTier, [])
    wsServersByTier.get(n.wsTier).push(n)
  }
  const stackPosByServerId = new Map()
  for (const [, servers] of wsServersByTier) {
    const anchorX = Math.min(...servers.map((s) => posOf(s).x))
    let y = Math.min(...servers.map((s) => posOf(s).y))
    for (const s of servers) {
      stackPosByServerId.set(s.id, { x: anchorX, y })
      y += heightOf(s) + STACK_GAP
    }
  }

  // Per-service load-balancer clusters render like a ws fleet: the instances (instanceOf)
  // stack vertically to the RIGHT of their entry sidecar, and the entry sits at the
  // stack's left-middle. Positions are DERIVED from the entry's position (not each
  // instance's own y), so the group reads as one unit and drags together. The entry keeps
  // its own posOf; only the instances are placed here.
  const svcInstancesByEntry = new Map() // entry id -> instance nodes, ordinal order
  for (const n of nodes) {
    if (!isSvcInstance(n)) continue
    if (!svcInstancesByEntry.has(n.instanceOf)) svcInstancesByEntry.set(n.instanceOf, [])
    svcInstancesByEntry.get(n.instanceOf).push(n)
  }
  const svcStackPos = new Map()
  const SVC_STACK_GAP_X = 28 // horizontal gap between the entry sidecar and its instance column — kept tight so the instances read as sitting right next to their load balancer
  for (const [entryId, instances] of svcInstancesByEntry) {
    const entry = byId[entryId]
    if (!entry || isGroupBase(entry)) continue // worker groups stack BELOW their entry instead
    instances.sort((a, b) => svcOrdinal(a.id) - svcOrdinal(b.id))
    const ep = posOf(entry)
    const anchorX = ep.x + NODE_W + SVC_STACK_GAP_X
    const totalH = instances.reduce((s, inst) => s + heightOf(inst) + STACK_GAP, -STACK_GAP)
    // Center the instance column on the entry's vertical middle → the entry reads as the
    // left-middle sidecar of the group.
    let y = ep.y + heightOf(entry) / 2 - totalH / 2
    for (const inst of instances) {
      svcStackPos.set(inst.id, { x: anchorX, y })
      y += heightOf(inst) + STACK_GAP
    }
  }

  // Worker groups (LLM workers, consumer groups): the group's scaler card (scalerOf)
  // sits at the base's position — the TOP of the stack — as the group header carrying
  // the group's Edit button; the member cards hang below it in columns of at most
  // GROUP_COL_SIZE — the virtual `<name>-1` first (explicitly; its `::1` id doesn't
  // parse as an ordinal), then the real instances in ordinal order, wrapping to a new
  // column to the right when one fills. A scaler-less group (mid-migration system)
  // falls back to the bare Edit button at the base's position.
  const groupStackPos = new Map()
  const groupMembersByBase = new Map() // base id -> [virtual, ...instances] in render order
  const scalerByBase = new Map() // base id -> its scaler sidecar node (scalerOf)
  const scalerPos = new Map()
  for (const b of nodes.filter(isGroupBase)) {
    const virtual = groupVirtuals.find((v) => v.stateKey === b.id)
    const instances = (svcInstancesByEntry.get(b.id) || [])
      .slice()
      .sort((a, x) => svcOrdinal(a.id) - svcOrdinal(x.id))
    const members = [virtual, ...instances]
    groupMembersByBase.set(b.id, members)
    const bp = posOf(b)
    const scaler = nodes.find((s) => s.scalerOf === b.id)
    if (scaler) {
      scalerByBase.set(b.id, scaler)
      scalerPos.set(scaler.id, { x: bp.x, y: bp.y })
    }
    const topY = bp.y + (scaler ? heightOf(scaler) : GROUP_ENTRY_H) + STACK_GAP
    const colYs = [] // per-column running y (cards differ in height when live bodies show)
    members.forEach((m, i) => {
      const col = Math.floor(i / GROUP_COL_SIZE)
      if (colYs[col] == null) colYs[col] = topY
      groupStackPos.set(m.id, { x: bp.x + col * (NODE_W + GROUP_COL_GAP_X), y: colYs[col] })
      colYs[col] += heightOf(m) + STACK_GAP
    })
  }

  const effPos = (n) =>
    stackPosByServerId.get(n.id) ||
    groupStackPos.get(n.id) ||
    scalerPos.get(n.id) ||
    svcStackPos.get(n.id) ||
    posOf(n)
  // Drag payload for a whole tier: captures every server's CURRENT stacked position so a group
  // drag shifts them all by the same delta (the stack then re-derives identically, shifted).
  const fleetDragArg = (tier) => ({
    kind: 'ws-fleet',
    tier,
    startPositions: Object.fromEntries(
      (wsServersByTier.get(tier) || []).map((s) => [s.id, stackPosByServerId.get(s.id)]),
    ),
  })

  // Center using the EFFECTIVE position (so edges follow a node while it's being dragged).
  const centerOf = (id) => {
    const n = byId[id]
    const p = effPos(n)
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
  // A trace endpoint is a real node, a synthetic ws-fleet:<tier> box (used to collapse the
  // relay fan-out into one hop in / one hop out per downstream), OR a worker-group base
  // (LLM workers, consumer groups, persistence readers) whose whole scaled stack is one
  // dotted box — its edges anchor on that box border, not the entry card inside it. Resolve
  // any of these to its center and to the border point facing the other end.
  // (rectCenter/rectBorderToward/fleetBoxByTier/workerGroupBoxByBase are declared below but
  // only read at render time, so the forward reference is fine.)
  const traceCenter = (id) =>
    isFleetId(id)
      ? rectCenter(fleetBoxByTier.get(fleetTierOf(id)))
      : workerGroupBoxByBase.has(id)
        ? rectCenter(workerGroupBoxByBase.get(id))
        : centerOf(id)
  const traceBorder = (id, toward) =>
    isFleetId(id)
      ? rectBorderToward(fleetBoxByTier.get(fleetTierOf(id)), toward)
      : workerGroupBoxByBase.has(id)
        ? rectBorderToward(workerGroupBoxByBase.get(id), toward)
        : byId[id]
          ? borderPointToward(toward, byId[id])
          : centerOf(id)
  const traceLine = (fromId, toId) => {
    const ac = traceCenter(fromId)
    const bc = traceCenter(toId)
    const a = traceBorder(fromId, bc)
    const b = traceBorder(toId, ac)
    return { a, b, mid: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 } }
  }
  // Friendly name for a trace endpoint in the hop popup title: the synthetic ws-fleet box
  // reads as "server fleet", every real node uses its manifest label.
  const hopEndpointLabel = (id) => (isFleetId(id) ? 'server fleet' : byId[id]?.label || id)

  // The rect analogue of borderPointToward: the point on an arbitrary rectangle's border
  // along the line from its center toward `towardPt`, plus a small gap so an arrowhead there
  // lands just OUTSIDE the box. Used to anchor the collapsed websocket-fleet in/out edges to
  // the fleet box border instead of to each individual server.
  const rectCenter = (r) => ({ x: r.x + r.w / 2, y: r.y + r.h / 2 })
  const rectBorderToward = (rect, towardPt, gap = 6) => {
    const c = rectCenter(rect)
    const dx = towardPt.x - c.x
    const dy = towardPt.y - c.y
    if (!dx && !dy) return c
    const k = Math.min(
      Math.abs(dx) ? rect.w / 2 / Math.abs(dx) : Infinity,
      Math.abs(dy) ? rect.h / 2 / Math.abs(dy) : Infinity,
    )
    const t = k + gap / Math.hypot(dx, dy)
    return { x: c.x + dx * t, y: c.y + dy * t }
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
    // A service that both produces to and consumes from the same cluster collapses to one
    // line (same from→to) — style it as the consume edge so the Kafka relationship stays visible.
    else if (kind === 'consume' && prev.kind === 'dep') prev.kind = 'consume'
  }
  for (const e of manifest.edges || []) {
    // A per-service load balancer's entry→instance fan-out is implied by the dotted group
    // box (like a ws fleet), so it isn't drawn as N node-to-node lines.
    if (e.origin === 'service-lb') continue
    // A consumer function's consume edge (service → the cluster it reads) is drawn distinct from a
    // plain producer/dep edge that points the same way — see CONSUME_COLOR / `.consume-edge`.
    addConn(e.from, e.to, e.origin === 'consumer-fn' ? 'consume' : 'dep')
  }
  for (const e of endpoints) {
    for (const d of e.downstream || []) addConn(e.service, d, 'dep')
  }
  // A consumer function's loop calls/reads/writes its `downstream` nodes (e.g. an API it POSTs
  // to, a db it touches) — draw the same persistent service->downstream line endpoints get, so
  // the diagram reflects what the loop actually does (the service->cluster consume edge is a
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

  // Dotted box around each per-service load-balancer group: its entry sidecar + every
  // instance. Uses effPos (the derived stacked positions), and labels the box with the
  // service name — the group's "full service" outline. LLM groups are boxed separately
  // below (header + worker column, drawn even for a single-worker group).
  const groupBox = (entryId, members, labelBand = 16) => {
    const minX = Math.min(...members.map((m) => effPos(m).x))
    const minY = Math.min(...members.map((m) => effPos(m).y))
    const maxX = Math.max(...members.map((m) => effPos(m).x + NODE_W))
    const maxY = Math.max(...members.map((m) => effPos(m).y + heightOf(m)))
    return {
      entryId,
      label: entryId,
      x: minX - CLUSTER_PAD,
      y: minY - CLUSTER_PAD - labelBand, // extra top band for the label
      w: maxX - minX + 2 * CLUSTER_PAD,
      h: maxY - minY + 2 * CLUSTER_PAD + labelBand,
    }
  }
  const svcLbBoxes = [...svcInstancesByEntry.entries()]
    .map(([entryId, instances]) => {
      if (isGroupBase(byId[entryId] || {})) return null
      const members = [byId[entryId], ...instances].filter(Boolean)
      if (members.length < 2) return null
      return groupBox(entryId, members)
    })
    .filter(Boolean)
  // A worker-group box's label doubles as the group's TITLE (the entry is the scaler
  // header card, or a bare Edit button on a scaler-less group), so it gets a taller
  // band and the larger .llm-group-label style. The scaler sits INSIDE the box, at
  // the top of the stack.
  const workerGroupBoxes = [...groupMembersByBase.entries()].map(([baseId, members]) => ({
    ...groupBox(baseId, [byId[baseId], scalerByBase.get(baseId), ...members].filter(Boolean), 26),
    group: true,
  }))
  // Edges address a worker group by its base id; anchor them on the group's dotted box
  // (like a ws fleet) instead of the entry card inside it — see traceCenter/traceBorder.
  const workerGroupBoxByBase = new Map(workerGroupBoxes.map((b) => [b.entryId, b]))

  // The system boundary: the dotted box the user owns. It's a PERSISTED, freely
  // movable/resizable rectangle (manifest.boundary). Until the user customizes it, it
  // defaults to an auto-fit box around the internal (non-external) nodes so it starts
  // sensibly. Once persisted it's decoupled from node positions — a node may sit inside
  // or outside it, the user's call. A live override (mid-drag / just-dropped) wins.
  const BOUNDARY_PAD = 26
  const internalNodes = renderNodes.filter((n) => !n.external)
  let defaultBoundary = null
  if (internalNodes.length) {
    const minX = Math.min(...internalNodes.map((n) => effPos(n).x))
    const minY = Math.min(...internalNodes.map((n) => effPos(n).y))
    const maxX = Math.max(...internalNodes.map((n) => effPos(n).x + NODE_W))
    const maxY = Math.max(...internalNodes.map((n) => effPos(n).y + heightOf(n)))
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
  // A consumer trace highlights one consume edge: consuming service → cluster. Both nodes must
  // still exist. Mutually exclusive with the other traces (App nulls the others when one is set).
  const ct = consumerTrace && byId[consumerTrace.cluster] && byId[consumerTrace.service] ? consumerTrace : null
  // A keyspace trace highlights the discovery flow around the etcd cluster: registrant → etcd
  // (the lease-put keepalive in) and etcd → each listener (the watch push out).
  const kt = keyspaceTrace && byId[keyspaceTrace.etcd] &&
    (keyspaceTrace.type === 'config' || byId[keyspaceTrace.service]) ? keyspaceTrace : null
  // A served-RPC trace lights the server and every caller → server gRPC edge. The server node
  // it points at must still exist. Mutually exclusive with the other traces (App nulls the rest).
  const rpct = rpcTrace && byId[rpcTrace.service] ? rpcTrace : null
  // A redis-keyspace trace lights the redis node plus its declared writers/readers. The redis
  // node must still exist; deleted writers/readers are filtered at edge-build time below.
  const rkt = redisTrace && byId[redisTrace.redis] ? redisTrace : null
  // A CDC-rule trace lights the capture worker, the stream it publishes the entity to, and every
  // consumer function that pulls that topic. Both the worker and its stream must still exist.
  const cdct = cdcTrace && byId[cdcTrace.cdc] && byId[cdcTrace.stream] ? cdcTrace : null
  if (ft && ft.wsBuiltin) {
    // A ws pool client's builtin method has no authored steps — trace the TIER path
    // instead: client → its L4 lb → each relay server → the bus + presence redis.
    // Both methods (send / onReceive) share it: messages traverse the same
    // path in each direction. Derived from the manifest's wsTier/wsRole fields, so
    // there's nothing to go stale.
    const tierId = byId[ft.client]?.wsTier
    const ids = new Set([ft.client])
    if (byId[tierId]) {
      ids.add(tierId)
      // Each hop carries an auto-generated description (3rd tuple element) shown in the
      // trace-hop info popup, mirroring an endpoint trace's downstreamDescriptions.
      traceEdges.push([ft.client, tierId, WS_HOP_DESC.clientToLb])
      // The relay servers are an interchangeable fleet, so the trace collapses onto the fleet
      // box (see traceLine's ws-fleet handling): one hop lb → fleet, then one hop fleet → each
      // redis — instead of lb → every server and every server → every redis. Servers still join
      // `ids` so they stay highlighted (un-dimmed) inside the box.
      const servers = nodes.filter((n) => n.wsTier === tierId && n.wsRole === 'server')
      if (servers.length) {
        for (const srv of servers) ids.add(srv.id)
        const fleetId = wsFleetId(tierId)
        traceEdges.push([tierId, fleetId, WS_HOP_DESC.lbToServer(wsAlgorithm)])
        // Presence (find the target's server) is numbered before the bus publish (relay to
        // it) because that's the order routing happens — regardless of manifest node order.
        const presence = nodes.find((n) => n.wsTier === tierId && n.wsRole === 'presence')
        const bus = nodes.find((n) => n.wsTier === tierId && n.wsRole === 'bus')
        if (presence) {
          ids.add(presence.id)
          traceEdges.push([fleetId, presence.id, WS_HOP_DESC.serverToPresence])
        }
        if (bus) {
          ids.add(bus.id)
          traceEdges.push([fleetId, bus.id, WS_HOP_DESC.serverToBus])
        }
        // Beyond the fixed bus/presence redis, a relay server may connect to other sinks
        // (e.g. a notification-db it writes undelivered messages to) via plain manifest
        // edges. Collapse those onto the fleet box too — one hop fleet → each distinct
        // downstream — so the trace reflects the servers' real fan-out, not just the tier
        // primitives. Driven by the manifest edges (server.id → sink), so nothing goes stale.
        const serverIds = new Set(servers.map((s) => s.id))
        const tierNodeIds = new Set(nodes.filter((n) => n.wsTier === tierId).map((n) => n.id))
        const extraSinks = new Set()
        for (const e of manifest.edges || []) {
          if (serverIds.has(e.from) && !tierNodeIds.has(e.to) && byId[e.to]) extraSinks.add(e.to)
        }
        for (const sinkId of extraSinks) {
          ids.add(sinkId)
          traceEdges.push([fleetId, sinkId, WS_HOP_DESC.serverToSink(byId[sinkId])])
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
    // consuming service → cluster (the consume edge; the service subscribes to / reads from the
    // stream, so the arrow points service → cluster; the 3rd tuple element labels it with the topic,
    // drawn at the line midpoint like a connection description) plus service → each node the loop
    // then calls/reads/writes (its downstream), so the trace shows the full set of things the
    // consumer touches, mirroring an endpoint trace's service → downstreams. Each downstream edge
    // carries this consumer's `downstreamDescriptions[d]` label, exactly as the endpoint trace does.
    const cds = (ct.downstream || []).filter((d) => byId[d])
    const cdd = ct.downstreamDescriptions || {}
    traceNodes = new Set([ct.cluster, ct.service, ...cds])
    traceEdges.push([ct.service, ct.cluster, ct.topic])
    for (const d of cds) traceEdges.push([ct.service, d, cdd[d] || ''])
  } else if (kt) {
    // Registrant service → etcd (each worker keeps a leased key alive under the prefix), then
    // etcd → each listener (updates are PUSHED over the watch stream — no polling). The arrows
    // exist only while the keyspace row is selected; there are no permanent manifest edges.
    // A config keyspace has no registrant (the app writes its persistent values), so only the
    // etcd → listener watch edges are drawn.
    const listeners = (kt.listeners || []).filter((l) => byId[l])
    if (kt.type === 'config') {
      traceNodes = new Set([kt.etcd, ...listeners])
    } else {
      traceNodes = new Set([kt.service, kt.etcd, ...listeners])
      traceEdges.push([kt.service, kt.etcd, `lease-put ${kt.prefix}<worker> · TTL keepalive`])
    }
    for (const l of listeners) traceEdges.push([kt.etcd, l, `watch ${kt.prefix} (pushed)`])
  } else if (rkt) {
    // A redis keyspace: each declared writer → redis, then redis → each declared reader,
    // labeled with the type's canonical verbs (`XADD tokens:*` / `GET presence:*`). The
    // arrows exist only while the KEY row is selected — no permanent manifest edges. With
    // nothing declared yet, the redis node alone is lit (still shows where the keys live).
    const writers = (rkt.writers || []).filter((w) => byId[w])
    const readers = (rkt.readers || []).filter((r) => byId[r])
    traceNodes = new Set([rkt.redis, ...writers, ...readers])
    // A wait-mode writer's arrow also carries its WAIT contract (+WAIT(n,Tms)).
    for (const w of writers) traceEdges.push([w, rkt.redis, keyspaceEdgeLabel(rkt, 'write', w)])
    for (const r of readers) traceEdges.push([rkt.redis, r, keyspaceEdgeLabel(rkt, 'read')])
  } else if (cdct) {
    // A CDC rule: the worker publishes the entity's changes to its topic, and every consumer
    // function reading that same (cluster, topic) then pulls them — so the trace follows the
    // change all the way OUT of the topic, not just into it. Each such consumer's PULL row lights
    // up (see the `cons` row below) and its own downstream hops are drawn, exactly as clicking
    // that PULL row directly would, making the full propagation path visible in one click.
    // A consumer's arrow points service → cluster, matching the permanent consume edge.
    const ids = new Set([cdct.cdc, cdct.stream])
    // Two consumer functions on the same service and topic would otherwise stack a second
    // sequence badge on the one line they share.
    const seenEdge = new Set()
    const addEdge = (from, to, label) => {
      const k = `${from}->${to}`
      if (from === to || seenEdge.has(k)) return
      seenEdge.add(k)
      traceEdges.push([from, to, label || ''])
    }
    addEdge(cdct.cdc, cdct.stream, cdcEdgeLabel(cdct))
    for (const [svcId, fns] of Object.entries(consumerFunctions)) {
      if (!byId[svcId]) continue
      for (const c of fns) {
        if (c.cluster !== cdct.stream || c.topic !== cdct.topic) continue
        ids.add(svcId)
        addEdge(svcId, cdct.stream, c.topic)
        for (const d of (c.downstream || []).filter((x) => byId[x])) {
          ids.add(d)
          addEdge(svcId, d, (c.downstreamDescriptions || {})[d] || '')
        }
      }
    }
    traceNodes = ids
  } else if (rpct) {
    // A served gRPC method: light the server and draw each caller → server edge — every service
    // whose manifest `grpc.clients` dials this contract with this server in its `targets`. gRPC
    // is server-driven (the caller opens the channel), so the arrows point caller → server, the
    // same direction as the permanent gRPC edges. No callers yet → the server alone is lit,
    // still showing it's the RPC endpoint.
    const ids = new Set([rpct.service])
    for (const n of nodes) {
      if (n.id === rpct.service) continue
      const dials = (n.grpc?.clients || []).some(
        (c) => c.contract === rpct.contract && (c.targets || []).includes(rpct.service),
      )
      if (dials) {
        ids.add(n.id)
        traceEdges.push([n.id, rpct.service, `gRPC ${rpct.contract}.${rpct.method}`])
      }
    }
    traceNodes = ids
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

  // One shared-methods panel per websocket tier, drawn below the tier's server fleet:
  // the shared methods every relay runs (from ws-shared/hooks.js) plus the Edit button
  // that opens the tier's shared modal. Derived from the servers' EFFECTIVE positions,
  // so it follows them through drags.
  const wsPanels = []
  // Dotted "fleet" box per websocket tier, wrapping ALL of that tier's relay servers plus
  // its shared-methods panel as one grouped unit (padded on every side). The relays are an
  // interchangeable fleet: callers and the redis bus/presence connect to the box AS A UNIT
  // (a single arrow in, a single arrow out — see fleetConnections) rather than to each server.
  const wsFleetBoxes = []
  const fleetBoxByTier = new Map()
  const serverTier = new Map() // server node id -> its tier
  const FLEET_PAD = 20
  // Extra headroom added ABOVE the padding on the fleet box so the group id label has its own
  // title band and doesn't crowd the top server card.
  const FLEET_TITLE_GAP = 22
  {
    const byTier = new Map()
    for (const n of nodes) {
      if (!isWsServer(n)) continue
      serverTier.set(n.id, n.wsTier)
      if (!byTier.has(n.wsTier)) byTier.set(n.wsTier, [])
      byTier.get(n.wsTier).push(n)
    }
    for (const [tier, servers] of byTier) {
      const methodNames = wsMethods ? Object.keys(wsMethods) : ['onMessage', 'onSend']
      // Servers stack in a rigid column (effPos); the shared-methods panel is the bottom card
      // of that stack, STACK_GAP below the lowest server.
      const x = Math.min(...servers.map((s) => effPos(s).x))
      const y = Math.max(...servers.map((s) => effPos(s).y + heightOf(s))) + STACK_GAP
      const h = PAD + 14 + methodNames.length * LINE_H + EDIT_GAP + EDIT_H + PAD
      const panel = { tier, x, y, w: NODE_W, h, methodNames }
      wsPanels.push(panel)
      // Box bounds = the union of every server rect and the panel rect, inflated by FLEET_PAD.
      const minX = Math.min(...servers.map((s) => effPos(s).x), panel.x)
      const minY = Math.min(...servers.map((s) => effPos(s).y), panel.y)
      const maxX = Math.max(...servers.map((s) => effPos(s).x + NODE_W), panel.x + panel.w)
      const maxY = Math.max(...servers.map((s) => effPos(s).y + heightOf(s)), panel.y + panel.h)
      const box = {
        tier,
        // Group id label (upper-left): the server base name, e.g. ws-server-1 → "ws-server".
        groupLabel: servers[0].id.replace(/-\d+$/, '') || tier,
        x: minX - FLEET_PAD,
        // Extend upward by an extra title band so the label clears the top server card.
        y: minY - FLEET_PAD - FLEET_TITLE_GAP,
        w: maxX - minX + 2 * FLEET_PAD,
        h: maxY - minY + 2 * FLEET_PAD + FLEET_TITLE_GAP,
      }
      wsFleetBoxes.push(box)
      fleetBoxByTier.set(tier, box)
    }
  }

  // Split the persistent dependency edges into ones that touch a websocket-server fleet and
  // ones that don't. A fleet-touching edge is COLLAPSED onto the fleet box: every server →
  // <same target> becomes one box → target arrow, and <same source> → every server becomes
  // one source → box arrow. Edges between two servers of a fleet are internal and dropped.
  const normalConnections = []
  const fleetConnMap = new Map()
  for (const conn of connections) {
    const fromTier = serverTier.get(conn.from)
    const toTier = serverTier.get(conn.to)
    if (fromTier && toTier) continue // intra-/cross-fleet: not drawn as a node-to-node line
    if (toTier && fleetBoxByTier.has(toTier)) {
      const k = `in|${toTier}|${conn.from}`
      if (!fleetConnMap.has(k)) fleetConnMap.set(k, { dir: 'in', tier: toTier, ext: conn.from })
    } else if (fromTier && fleetBoxByTier.has(fromTier)) {
      const k = `out|${fromTier}|${conn.to}`
      if (!fleetConnMap.has(k)) fleetConnMap.set(k, { dir: 'out', tier: fromTier, ext: conn.to })
    } else {
      normalConnections.push(conn)
    }
  }
  const fleetConnections = [...fleetConnMap.values()]

  // Size the canvas to the true bounding box of everything (nodes ⊕ the system
  // boundary ⊕ the ws shared-methods panels), plus a margin. Clients sit LEFT of the
  // system, so x can be negative — the viewBox origin moves with it instead of being
  // pinned at 0,0.
  const xStarts = renderNodes.map((n) => effPos(n).x)
  const xEnds = renderNodes.map((n) => effPos(n).x + NODE_W)
  const yStarts = renderNodes.map((n) => effPos(n).y)
  const yEnds = renderNodes.map((n) => effPos(n).y + heightOf(n))
  if (boundary) {
    xStarts.push(boundary.x)
    xEnds.push(boundary.x + boundary.w)
    yStarts.push(boundary.y)
    yEnds.push(boundary.y + boundary.h)
  }
  for (const pl of wsPanels) {
    xStarts.push(pl.x)
    xEnds.push(pl.x + pl.w)
    yStarts.push(pl.y)
    yEnds.push(pl.y + pl.h)
  }
  for (const b of wsFleetBoxes) {
    xStarts.push(b.x)
    xEnds.push(b.x + b.w)
    yStarts.push(b.y)
    yEnds.push(b.y + b.h)
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

  // Node-aware dim test. A trace set only ever carries BASE ids (a service name, a cluster
  // entry) — never the member cards the group is actually drawn as: the `<base>-scaler`
  // sidecar (scalerOf), the render-only virtual `<base>::1` (instanceOf/stateKey), and any
  // real `<base>-N` instances (instanceOf). Keying dim purely on `node.id` therefore fades
  // every visible card of a worker group / LB cluster whenever a method that touches its
  // base is selected. Resolve a member card up to its base so selecting such a method lights
  // up the WHOLE group. See groupVirtuals, isSvcInstance, and the scaler's scalerOf.
  const dimmedNode = (node) => {
    if (!traceNodes) return false
    return ![node.id, node.instanceOf, node.scalerOf, node.stateKey]
      .filter(Boolean)
      .some((id) => traceNodes.has(id))
  }

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
    } else if (g.kind === 'ws-fleet') {
      // Shift every server of the tier by the SAME delta from its captured stacked start; the
      // stack then re-derives identically, translated — so the combined body moves as one.
      setDrag((d) => {
        const next = { ...d }
        for (const [id, p] of Object.entries(g.startPositions)) {
          next[id] = { x: Math.round(p.x + dx), y: Math.round(p.y + dy) }
        }
        return next
      })
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
    } else if (g.kind === 'ws-fleet') {
      // One POST carrying every server's new position; layout.js iterates the positions map.
      const positions = {}
      for (const [id, p] of Object.entries(g.startPositions)) {
        positions[id] = { x: Math.round(p.x + dx), y: Math.round(p.y + dy) }
      }
      persistLayout({ positions })
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
  }, [methodTrace, functionTrace, consumerTrace, keyspaceTrace, redisTrace, rpcTrace, cdcTrace, selectedKey])

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
              onClearKeyspaceTrace?.()
              onClearRpcTrace?.()
              onClearRedisTrace?.()
              onClearCdcTrace?.()
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
        <marker
          id="consume-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill={CONSUME_COLOR} />
        </marker>
        {/* Arrowhead for the always-on etcd discovery edges (registrant → etcd → listeners). */}
        <marker
          id="etcd-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill={ETCD_COLOR} />
        </marker>
        {/* Arrowhead for the collapsed websocket-fleet in/out edges (same color as `.edge`). */}
        <marker
          id="edge-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="7"
          markerHeight="7"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill={EDGE_COLOR} />
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

      {/* Dotted box around each per-service load-balancer group (entry sidecar + instances)
          and each worker group (entry button + member columns — LLM workers, consumer
          groups), labelled with the service name; sits behind the nodes it groups.
          Non-interactive. */}
      {[...svcLbBoxes, ...workerGroupBoxes].map((b) => (
        <g
          key={`svclb-${b.entryId}`}
          style={{ pointerEvents: 'none' }}
          // Dim the whole group (dotted box + its top-left title) when an active trace doesn't
          // touch this group's base — matching the member cards inside it (dimmedNode resolves
          // them to this same base id), so an unrelated group fades outline, label, and all.
          className={dimmed(b.entryId) ? 'dim' : undefined}
        >
          <rect x={b.x} y={b.y} width={b.w} height={b.h} rx="14" className="ws-fleet-box" />
          <text x={b.x + 12} y={b.y + (b.group ? 20 : 16)} className={b.group ? 'llm-group-label' : 'ws-fleet-label'}>
            {b.label}
          </text>
        </g>
      ))}

      {/* Dotted "fleet" box around each websocket tier's servers + its shared-methods panel;
          sits behind the nodes it groups, with the group id (server base name) in its upper-left.
          Non-interactive so it never intercepts a drag meant for the move-target/cards below. */}
      {wsFleetBoxes.map((b) => (
        <g
          key={`ws-fleet-${b.tier}`}
          style={{ pointerEvents: 'none' }}
          // Dim the fleet box + its title when a trace doesn't collapse onto this tier, matching
          // the servers inside it and the shared-methods panel below (both keyed on the tier id).
          className={dimmed(b.tier) ? 'dim' : undefined}
        >
          <rect x={b.x} y={b.y} width={b.w} height={b.h} rx="14" className="ws-fleet-box" />
          <text x={b.x + 12} y={b.y + 16} className="ws-fleet-label">
            {b.groupLabel}
          </text>
        </g>
      ))}

      {/* Drag-mode move target per fleet box: a transparent, fully-hittable rect spanning the box
          so dragging anywhere inside it (including the padding/gaps between cards) moves the whole
          tier stack. Drawn BEHIND the servers/panel, so their own handlers still win on their areas
          and this only catches the gaps. It spans just the box column, so it never steals events
          from ws-lb / ws-bus / ws-presence (which sit outside it and render on top). */}
      {dragMode &&
        wsFleetBoxes.map((b) => (
          <rect
            key={`ws-fleet-target-${b.tier}`}
            x={b.x}
            y={b.y}
            width={b.w}
            height={b.h}
            rx="14"
            className="ws-fleet-move-target"
            onPointerDown={(e) => beginDrag(e, fleetDragArg(b.tier))}
          >
            <title>Drag to move the whole websocket server fleet</title>
          </rect>
        ))}

      {/* Collapsed websocket-fleet edges: one arrow INTO the fleet box per distinct caller and
          one arrow OUT per distinct downstream (bus/presence), anchored on the box border rather
          than drawn to each server. Dim during a trace like the other edges. */}
      {fleetConnections.map(({ dir, tier, ext }) => {
        const box = fleetBoxByTier.get(tier)
        const extNode = byId[ext]
        if (!box || !extNode) return null
        const boxPt = rectBorderToward(box, centerOf(ext))
        const extPt = borderPointToward(rectCenter(box), extNode)
        const [a, b] = dir === 'in' ? [extPt, boxPt] : [boxPt, extPt]
        return (
          <line
            key={`fleet-${dir}-${tier}-${ext}`}
            x1={a.x}
            y1={a.y}
            x2={b.x}
            y2={b.y}
            className={`edge${traceNodes ? ' dim' : ''}`}
            markerEnd="url(#edge-arrow)"
          >
            <title>{dir === 'in' ? `${ext} → ${tier} servers` : `${tier} servers → ${ext}`}</title>
          </line>
        )
      })}

      {/* Outbound dependency connections (deduped), behind the node boxes. Each is
          clickable to attach/edit a resilience policy; gRPC keeps its dashed style.
          A connection with a circuit breaker draws a mid-line breaker circle (+ live
          overlay). Dim while a lifecycle trace is active, matching the other edges. */}
      {normalConnections.map(({ from, to, kind, contract }) => {
        const key = `${from}->${to}`
        // Border-to-border (like trace edges) so the arrowhead lands just outside the
        // target box instead of hidden under its center.
        let { a, b, mid } = traceLine(from, to)
        // Anti-parallel offset: when the reverse edge also exists (A→B AND B→A), nudge
        // each line perpendicular to its axis so the two opposing arrows don't overlap.
        // A→B and B→A have opposite direction vectors, so the same perpendicular auto-
        // separates them onto opposite sides; a lone edge stays centered (service→db).
        if (connByKey.has(`${to}->${from}`)) {
          const dx = b.x - a.x, dy = b.y - a.y
          const len = Math.hypot(dx, dy) || 1
          const ox = (-dy / len) * EDGE_PARALLEL_OFFSET
          const oy = (dx / len) * EDGE_PARALLEL_OFFSET
          a = { x: a.x + ox, y: a.y + oy }
          b = { x: b.x + ox, y: b.y + oy }
          mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }
        }
        const res = resByConn.get(key)
        const breakerOn = !!res?.circuit_breaker?.enabled
        const live = resilienceState[key]
        const dim = traceNodes ? ' dim' : ''
        const lineClass =
          kind === 'grpc' ? `grpc-edge${dim}` : kind === 'consume' ? `consume-edge${dim}` : `edge${dim}`
        const fromIsService = byId[from]?.type === 'service'
        // A Kafka consume edge (service → cluster) isn't an outbound request call, so it doesn't
        // take a resilience policy — exclude it from the clickable/breaker treatment.
        const clickable = !!onRequestConnectionResilience && fromIsService && kind !== 'consume' && !dragMode
        const label = breakerOn ? breakerLabel(live) : null
        const poolText = poolLabel(poolState[key])
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
              markerEnd={
                kind === 'grpc'
                  ? 'url(#grpc-arrow)'
                  : kind === 'consume'
                    ? 'url(#consume-arrow)'
                    : 'url(#edge-arrow)'
              }
            >
              <title>
                {kind === 'grpc'
                  ? `gRPC · ${contract}: ${from} → ${to}`
                  : kind === 'consume'
                    ? `Kafka consume · ${from} → ${to}`
                    : `${from} → ${to}`}
              </title>
            </line>
            {breakerOn && <BreakerCircle cx={mid.x} cy={mid.y} live={live} />}
            {label && (
              <text x={mid.x} y={mid.y - 11} className="breaker-label" style={{ pointerEvents: 'none' }}>
                {label}
              </text>
            )}
            {poolText && (
              <text x={mid.x} y={mid.y + (breakerOn ? 20 : 14)} className="pool-label" style={{ pointerEvents: 'none' }}>
                {poolText}
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
        // A SYNCHRONOUS standby is labelled on its edge: the primary's commits block until
        // this one acknowledges, which is the single most consequential thing about the
        // link and is invisible otherwise (async is the default, so it goes unlabelled).
        const sync = byId[edge.to]?.replication === 'sync'
        return (
          <g key={`replica-${i}`}>
            <line
              x1={a.x}
              y1={a.y}
              x2={b.x}
              y2={b.y}
              className="replica-edge"
              markerStart="url(#replica-arrow)"
              markerEnd="url(#replica-arrow)"
            >
              <title>{`replication · ${edge.from} ↔ ${edge.to}${sync ? ' · synchronous' : ''}`}</title>
            </line>
            {sync && (
              // 70% of the way toward the STANDBY, not the midpoint: the primary end of this
              // edge is where the member-dot caption sits, and a midpoint label lands on top
              // of it.
              <text
                x={a.x + (b.x - a.x) * 0.7}
                y={a.y + (b.y - a.y) * 0.7 - 4}
                className="node-pause-label"
                style={{ pointerEvents: 'none' }}
              >
                sync
              </text>
            )}
          </g>
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

      {/* etcd discovery wiring: always-on faint dashed arrows for the registrant → etcd (lease
          keepalive) and etcd → listener (watch) relationships, so the discovery topology reads
          off the static diagram instead of only appearing while a KEY row is selected. Border-to-
          border like the trace edges; dim during any active trace so the bright click-trace (which
          lights the same relationship) stands out over the faint underlay. */}
      {etcdEdges.map(({ from, to, kind }) => {
        if (!byId[from] || !byId[to]) return null
        const { a, b } = traceLine(from, to)
        return (
          <line
            key={`etcd-edge-${from}-${to}`}
            x1={a.x}
            y1={a.y}
            x2={b.x}
            y2={b.y}
            className={`etcd-edge${traceNodes ? ' dim' : ''}`}
            markerEnd="url(#etcd-arrow)"
          >
            <title>
              {kind === 'register'
                ? `etcd discovery · ${from} registers → ${to} (lease keepalive)`
                : `etcd discovery · ${from} → ${to} watches`}
            </title>
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

      {renderNodes.map((node) => {
        // Virtual llm `<name>-1` cards read the base container's live data via dataKeyOf.
        const data = nodeData[dataKeyOf(node)] || { metrics: {}, color: 'gray' }
        const rt = customState[dataKeyOf(node)]
        // On-node clickable rows: a service's callable methods, or a client's attached
        // functions, listed below the metrics.
        const rows = rowsOf(node)
        const h = nodeHeight(node, bodyRowsOf(node), rt, rows)
        // A user-initiated temporary outage paints the node orange and wins over the
        // health-derived color (a deliberate shutdown looks down — but intentionally).
        // A base outage paints both the llm header and its `<name>-1` card — same container.
        const inOutage = outages[dataKeyOf(node)]
        // Health paints most nodes. The load balancer has no health block (nothing scrapes it),
        // so it takes the color configured in Settings instead — but an outage still wins, and an
        // LB that someone DID give health rules stays health-colored.
        const color = inOutage
          ? COLOR_HEX.orange
          : node.type === 'load_balancer' && !node.health
            ? LB_COLOR
            : COLOR_HEX[data.color] || COLOR_HEX.gray
        // Event-stream cluster with its consumers paused — badge it (amber, like an outage).
        const consumersPaused = pausedConsumers.has(node.id)
        // All per-node actions (schema/topics/endpoints/gRPC/shutdown/delete) now live
        // behind the bottom "Edit" button, so the node body itself is no longer clickable.
        const p = effPos(node)
        const gClass = [dimmedNode(node) ? 'dim' : '', dragMode ? 'draggable' : ''].filter(Boolean).join(' ') || undefined

        // A worker group's base node renders NOTHING when the group has a scaler — the
        // scaler card occupies the base's position as the stack header and carries the
        // group's Edit button. The base stays the drag/edge anchor (posOf is untouched)
        // and its container's metrics/live body/health render on the virtual `<name>-1`
        // card stacked below (see groupVirtuals). A scaler-less group (mid-migration
        // system) falls back to the bare Edit button, sitting under the dotted box's
        // enlarged `<name>` label (which doubles as the title). No outage caption
        // either — the `<name>-1` card shows the countdown.
        if (isGroupBase(node)) {
          if (scalerByBase.has(node.id)) return null
          return (
            <g
              key={node.id}
              transform={`translate(${p.x}, ${p.y})`}
              className={gClass}
              onPointerDown={
                dragMode
                  ? (e) => beginDrag(e, { kind: 'node', nodeId: node.id, startPos: p })
                  : undefined
              }
            >
              {onRequestEdit && (
                <g
                  className="node-edit"
                  onClick={dragMode ? undefined : (e) => {
                    e.stopPropagation()
                    onRequestEdit(node)
                  }}
                >
                  <rect x={PAD} y={0} width={NODE_W - 2 * PAD} height={EDIT_H} rx="6" className="node-edit-btn" />
                  <text x={NODE_W / 2} y={EDIT_H / 2} className="node-edit-label">
                    Edit
                  </text>
                </g>
              )}
            </g>
          )
        }
        return (
          <g
            key={node.id}
            transform={`translate(${p.x}, ${p.y})`}
            className={gClass}
            onPointerDown={
              dragMode
                ? (e) =>
                    beginDrag(
                      e,
                      // A ws-server is part of a combined body: dragging it moves the whole
                      // tier stack (servers + panel) together, not just this one card. A
                      // load-balanced instance likewise moves its whole group — its position is
                      // derived from the entry, so we drag the entry. A scaler sidecar is the
                      // group's stack header (its position derives from the base), so dragging
                      // it drags the base — the whole group moves together.
                      isWsServer(node)
                        ? fleetDragArg(node.wsTier)
                        : isSvcInstance(node)
                          ? { kind: 'node', nodeId: node.instanceOf, startPos: posOf(byId[node.instanceOf]) }
                          : node.scalerOf && byId[node.scalerOf]
                            ? { kind: 'node', nodeId: node.scalerOf, startPos: posOf(byId[node.scalerOf]) }
                            : { kind: 'node', nodeId: node.id, startPos: p },
                    )
                : undefined
            }
          >
            {/* Websocket servers get a solid green outline (part of the combined fleet body) —
                health stays readable on the header strip, and an outage still paints
                the outline orange. */}
            <rect
              width={NODE_W}
              height={h}
              rx="8"
              className={
                node.type === 'client'
                  ? 'node-box external client'
                  : node.external
                    ? 'node-box external'
                    : isWsServer(node)
                      ? 'node-box ws-server'
                      : node.scalerOf
                        ? 'node-box scaler'
                        : 'node-box'
              }
              style={{ stroke: isWsServer(node) && !inOutage ? COLOR_HEX.green : color }}
            />
            {/* Header strip colored by health. */}
            <rect width={NODE_W} height={HEADER_H} rx="8" fill={color} />
            <rect width={NODE_W} height={HEADER_H / 2} fill={color} />
            <text x={PAD} y={HEADER_H / 2 + 5} className="node-label">
              {/* The load-balancer cluster ENTRY keeps its bare id (`<name>`) as the node id so it
                  still owns endpoints/gRPC, but reads as the service's load balancer — display it as
                  `<name>-lb` to distinguish it from its `<name>-N` instances. */}
              {node.type === 'service-lb' ? `${node.label}-lb` : node.label}
            </text>
            <text x={NODE_W - PAD} y={HEADER_H / 2 + 5} className="node-type">
              {node.type === 'client'
                ? (node.stateful ? 'client · stateful' : 'client')
                : node.external
                  ? 'external'
                  : node.scalerOf
                    ? 'scaler'
                    : node.type}
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
                            // An LB selection and any other trace never coexist. Unlike every other
                            // row, this one selects into DIAGRAM-LOCAL state (selectedKey), so App
                            // can't null the other traces for us — clearing them all here is what
                            // keeps a live rpc/redis/cdc trace from out-ranking the LB selection in
                            // the trace chain (and leaving the click with no visible effect).
                            onClearMethodTrace?.()
                            onClearFunctionTrace?.()
                            onClearConsumerTrace?.()
                            onClearKeyspaceTrace?.()
                            onClearRpcTrace?.()
                            onClearRedisTrace?.()
                            onClearCdcTrace?.()
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
            ) : (node.metrics || []).length === 0 ? null : (
              /* Metrics dropdown: a collapsible "Metrics" header (like the LB's service
                 accordion) over this node's metric rows. Collapsed by default. When no
                 Prometheus node is on the diagram there's nothing to query, so an expanded
                 dropdown reads "no metrics" instead of rows. */
              (() => {
                const open = openNodeMetrics.has(node.id)
                const hy = HEADER_H + PAD
                const out = [
                  <g
                    key="metrics-hdr"
                    className="endpoint-hit lb-group"
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      setOpenNodeMetrics((prev) => {
                        const next = new Set(prev)
                        if (next.has(node.id)) next.delete(node.id)
                        else next.add(node.id)
                        return next
                      })
                    }}
                  >
                    <rect x={4} y={hy - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={hy + 12} className="lb-group-row">
                      <tspan className="lb-group-caret">{open ? '▾' : '▸'}</tspan> Metrics
                    </text>
                  </g>,
                ]
                if (open && !hasPrometheus) {
                  out.push(
                    <text key="no-metrics" x={PAD} y={HEADER_H + PAD + LINE_H + 12} className="endpoint-empty">
                      no metrics
                    </text>,
                  )
                } else if (open) {
                  for (let i = 0; i < node.metrics.length; i++) {
                    const m = node.metrics[i]
                    const y = HEADER_H + PAD + (i + 1) * LINE_H + 12
                    out.push(
                      <g key={m.label}>
                        <text x={PAD} y={y} className="metric-label">
                          {m.label}
                        </text>
                        <text x={NODE_W - PAD} y={y} className="metric-value">
                          {formatMetric(data.metrics[m.label], m)}
                        </text>
                      </g>,
                    )
                  }
                }
                return out
              })()
            )}

            {/* On-node clickable rows below the metrics, in one stacked band: a node's callable
                METHODS first (services + external services), then its own FUNCTIONS (clients).
                Clicking a method traces it individually (like the Edit ▸ Calls tab); clicking a
                function traces its whole call path (client → LB → services → downstreams) and
                highlights each called method on its service. */}
            {rows.map((row, i) => {
              const y = metricsBottom(node, bodyRowsOf(node)) + METHOD_GAP + i * LINE_H
              if (row.kind === 'fn') {
                const fn = row.fn
                const active = !!functionTrace && functionTrace.client === node.id && functionTrace.name === fn.name
                return (
                  <g
                    key={`fn-${fn.name}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A function trace is exclusive with the LB / method / consumer / keyspace selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearConsumerTrace?.()
                      onClearKeyspaceTrace?.()
                      onClearCdcTrace?.()
                      if (active) onClearFunctionTrace?.()
                      else onSelectFunction?.(fn, node.id)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method endpoint-method-fn">ƒ</tspan> {fn.name}
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
                // A Kafka consumer function: clicking it traces the consume edge service → cluster.
                // "PULL" stands in for the GET/POST method badge an HTTP row would show, tinted to
                // match the consume edge (CONSUME_COLOR) it traces. On a
                // virtual group-member card the OWNER is the base node (stateKey) — the trace
                // must anchor on a real manifest node.
                const c = row.c
                const owner = node.stateKey || node.id
                const selfActive =
                  !!consumerTrace && consumerTrace.service === owner && consumerTrace.name === c.name
                // A CDC-rule trace lights this row too, when the loop pulls the very topic that
                // rule publishes to — that's how a capture shows up as arriving HERE. The two flags
                // stay separate: `active` only paints the row, while the click keeps toggling on
                // THIS row's own trace. Folding them together would make a cdc-lit click take the
                // `active` branch and clear a consumer trace that was never set, dropping the
                // cdc trace (the click's clear-list above) and leaving the diagram blank.
                const cdcLit = !!cdct && c.cluster === cdct.stream && c.topic === cdct.topic
                const active = selfActive || cdcLit
                return (
                  <g
                    key={`cons-${c.name}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A consumer trace is exclusive with the LB / method / function / keyspace / cdc selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearFunctionTrace?.()
                      onClearKeyspaceTrace?.()
                      onClearCdcTrace?.()
                      if (selfActive) onClearConsumerTrace?.()
                      else onSelectConsumer?.(c, owner)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method endpoint-method-consume">PULL</tspan> {c.name}
                    </text>
                  </g>
                )
              }
              if (row.kind === 'ks') {
                // An etcd keyspace: clicking it traces registrant → etcd (lease-put) and
                // etcd → each listener (watch push); a config keyspace has no registrant, so
                // its trace is etcd → listeners only. "KEY" stands in for the method badge.
                const ks = row.ks
                const ident = ks.type === 'config' ? ks.name : ks.service
                // Active for BOTH a full KEY-row trace and a focused subscription (SUB row)
                // trace of the same keyspace — clicking a service's SUB row also lights the
                // matching KEY on etcd.
                const active =
                  !!keyspaceTrace && keyspaceTrace.etcd === node.id &&
                  (ks.type === 'config' ? keyspaceTrace.name === ks.name : keyspaceTrace.service === ks.service)
                return (
                  <g
                    key={`ks-${ident}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A keyspace trace is exclusive with the LB / method / function / consumer selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearFunctionTrace?.()
                      onClearConsumerTrace?.()
                      onClearCdcTrace?.()
                      if (active) onClearKeyspaceTrace?.()
                      else onSelectKeyspace?.(ks, node.id)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method endpoint-method-etcd">KEY</tspan> {camelName(ident)}
                    </text>
                  </g>
                )
              }
              if (row.kind === 'sub') {
                // A service's etcd subscription: clicking it traces just this one watch —
                // registrant → etcd → this service (a config keyspace has no registrant, so
                // etcd → this service only). "SUB" stands in for the method badge; the name is
                // the KEY it watches, `on`-prefixed. On a virtual group-member card the OWNER is
                // the base node (stateKey), matching the CONS rows.
                const ks = row.ks
                const etcdId = row.etcdId
                const owner = node.stateKey || node.id
                const ident = ks.type === 'config' ? ks.name : ks.service
                const active =
                  !!keyspaceTrace && keyspaceTrace.etcd === etcdId && keyspaceTrace.focus === owner &&
                  (ks.type === 'config' ? keyspaceTrace.name === ks.name : keyspaceTrace.service === ks.service)
                return (
                  <g
                    key={`sub-${etcdId}-${ident}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A subscription trace is exclusive with the LB / method / function / consumer selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearFunctionTrace?.()
                      onClearConsumerTrace?.()
                      onClearCdcTrace?.()
                      if (active) onClearKeyspaceTrace?.()
                      else onSelectSubscription?.(ks, etcdId, owner)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method endpoint-method-etcd">SUB</tspan> {onName(ident)}
                    </text>
                  </g>
                )
              }
              if (row.kind === 'redisks') {
                // A redis keyspace (the node's manifest `keyspaces` entry): the declared TYPE
                // stands in for the method badge (STREAM → STRM, STRING → STR), the label is the
                // shorthand when set, else the raw key name. Clicking traces each declared
                // writer → redis and redis → each declared reader. A scan-discovered entry the
                // user hasn't confirmed yet carries an amber "verify" marker (the Verify button
                // lives in the node's Keyspaces tab).
                const ks = row.ks
                const active =
                  !!redisTrace && redisTrace.redis === node.id && redisTrace.name === ks.name
                return (
                  <g
                    key={`redisks-${ks.name}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A redis-keyspace trace is exclusive with the LB / method / function /
                      // consumer / etcd-keyspace selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearFunctionTrace?.()
                      onClearConsumerTrace?.()
                      onClearKeyspaceTrace?.()
                      onClearCdcTrace?.()
                      if (active) onClearRedisTrace?.()
                      else onSelectRedisKeyspace?.(ks, node.id)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method endpoint-method-redis">{REDIS_BADGE[ks.type] || 'KEY'}</tspan> {keyspaceLabel(ks)}
                    </text>
                    {ks.verified === false && (
                      <text x={NODE_W - PAD} y={y + 12} className="keyspace-unverified">
                        <title>unverified — confirm it in the node's Keyspaces tab</title>
                        verify
                      </text>
                    )}
                  </g>
                )
              }
              if (row.kind === 'cdcrule') {
                // A CDC capture rule (systems/<id>/<db>/cdc.json). Instead of the ONE badge every
                // other row carries, it badges each OPERATION that fires it — INS / UPD / DEL, a
                // color each — followed by the entity being captured. Clicking it traces the publish
                // edge worker → stream ("publishes <entity> to <topic>") and lights every consumer
                // function that pulls that topic, so the change's whole path shows in one click.
                const r = row.r
                const ops = cdcOpsOf(r)
                // Each badge spends 4 characters of the row ("INS "); the entity name gets the rest.
                const label = truncate(r.table, ROW_CHARS - ops.length * 4)
                const active = !!cdct && cdct.cdc === node.id && cdcRuleKey(cdct) === cdcRuleKey(r)
                return (
                  <g
                    key={`cdcrule-${cdcRuleKey(r)}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // A cdc trace is exclusive with the LB / method / function / consumer /
                      // etcd-keyspace / redis / rpc selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearFunctionTrace?.()
                      onClearConsumerTrace?.()
                      onClearKeyspaceTrace?.()
                      onClearRedisTrace?.()
                      onClearRpcTrace?.()
                      if (active) onClearCdcTrace?.()
                      else onSelectCdcRule?.(r, node.id)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      {ops.map((op) => (
                        <tspan key={op} className={`endpoint-method ${CDC_BADGE_CLASS[op]}`}>
                          {`${CDC_BADGE[op]} `}
                        </tspan>
                      ))}
                      {label}
                      {label !== r.table && <title>{r.table}</title>}
                    </text>
                  </g>
                )
              }
              if (row.kind === 'rpc') {
                // A SERVED gRPC method (this service is in the contract's server set). Clicking it
                // traces each caller → this server. "RPC" stands in for the HTTP method badge,
                // tinted purple to match the gRPC edges it lights; the name is the RPC method. On a
                // virtual group-member card the OWNER is the base node (stateKey), which carries the
                // grpc block and is the real manifest node the trace must anchor on — like CONS/SUB.
                const owner = node.stateKey || node.id
                const active =
                  !!rpcTrace && rpcTrace.service === owner &&
                  rpcTrace.contract === row.contract && rpcTrace.method === row.method
                return (
                  <g
                    key={`rpc-${row.contract}-${row.method}`}
                    className={active ? 'endpoint-hit selected' : 'endpoint-hit'}
                    onClick={dragMode ? undefined : (ev) => {
                      ev.stopPropagation()
                      // An RPC trace is exclusive with the LB / method / function / consumer / keyspace selections.
                      setSelectedKey(null)
                      onClearMethodTrace?.()
                      onClearFunctionTrace?.()
                      onClearConsumerTrace?.()
                      onClearKeyspaceTrace?.()
                      onClearCdcTrace?.()
                      if (active) onClearRpcTrace?.()
                      else onSelectRpc?.(row, owner)
                    }}
                  >
                    <rect x={4} y={y - 2} width={NODE_W - 8} height={LINE_H} rx="3" className="endpoint-bg" />
                    <text x={PAD} y={y + 12} className="endpoint-row">
                      <tspan className="endpoint-method endpoint-method-rpc">RPC</tspan> {row.method}
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
                    // Method trace and LB / function / consumer / keyspace selection never coexist.
                    setSelectedKey(null)
                    onClearFunctionTrace?.()
                    onClearConsumerTrace?.()
                    onClearKeyspaceTrace?.()
                    onClearCdcTrace?.()
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
                metrics: id on top, metrics in the middle, Edit at the bottom. A scaler
                card hosts its GROUP's Edit button — the click opens the BASE's modal
                (where the Scaling tab lives; Delete there cascades the scaler). */}
            {hasEditButton(node) && onRequestEdit && (
              <g
                className="node-edit"
                onClick={dragMode ? undefined : (e) => {
                  e.stopPropagation()
                  onRequestEdit(node.scalerOf ? (byId[node.scalerOf] || node) : node)
                }}
              >
                <rect
                  x={PAD}
                  y={contentBottom(node, bodyRowsOf(node), rt, rows) + EDIT_GAP}
                  width={NODE_W - 2 * PAD}
                  height={EDIT_H}
                  rx="6"
                  className="node-edit-btn"
                />
                <text
                  x={NODE_W / 2}
                  y={contentBottom(node, bodyRowsOf(node), rt, rows) + EDIT_GAP + EDIT_H / 2}
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
                    top={methodsBottom(node, bodyRowsOf(node), rows) + CUSTOM_GAP}
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
            {/* etcd cluster caption: per-member health dots (leader ringed) + the quorum
                math derived from N, below the node like the outage/pause captions. Member
                up/leader state comes from the per-member Prometheus series (nodeData). */}
            {node.type === 'etcd' && node.etcd && (() => {
              const members = node.etcd.members || []
              const live = data?.members || {}
              const size = node.etcd.size || members.length
              const quorum = node.etcd.quorum || Math.floor(size / 2) + 1
              const yDots = h + (inOutage ? 26 : 12)
              const dotGap = 16
              const x0 = NODE_W / 2 - ((members.length - 1) * dotGap) / 2
              return (
                <g style={{ pointerEvents: 'none' }}>
                  {members.map((m, mi) => {
                    const st = live[m]
                    const fill = st ? (st.up ? COLOR_HEX.green : COLOR_HEX.red) : COLOR_HEX.gray
                    return (
                      <g key={m}>
                        <circle cx={x0 + mi * dotGap} cy={yDots} r={4.5} fill={fill} />
                        {st?.leader && (
                          <circle cx={x0 + mi * dotGap} cy={yDots} r={7} fill="none"
                            stroke={fill} strokeWidth="1.5" />
                        )}
                      </g>
                    )
                  })}
                  <text x={NODE_W / 2} y={yDots + 18} className="node-pause-label">
                    {size} nodes · quorum {quorum} · tolerates {size - quorum}
                  </text>
                </g>
              )
            })()}
            {/* Redis topology caption (Topology tab): per-member health dots + the shape
                math, same pattern as the etcd strip. Cluster mode dots are the member
                containers with shard MASTERS ringed (redis_instance_info role="master");
                sentinel mode dots are the 3 sentinels watching the primary. */}
            {node.type === 'redis' && (node.sentinel || node.redisCluster) && (() => {
              const members = node.redisCluster?.members || node.sentinel.members || []
              const live = data?.members || {}
              const yDots = h + (inOutage ? 26 : 12)
              const dotGap = Math.min(16, members.length > 1 ? (NODE_W - 24) / (members.length - 1) : 16)
              const x0 = NODE_W / 2 - ((members.length - 1) * dotGap) / 2
              const caption = node.redisCluster
                ? `${node.redisCluster.shards} shards · ${node.redisCluster.replicasPerShard}/shard · 16384 slots`
                : `${node.sentinel.size} sentinels · quorum ${node.sentinel.quorum} · ${
                    nodes.filter((n) => n.replicaOf === node.id).length} replica(s)`
              return (
                <g style={{ pointerEvents: 'none' }}>
                  {members.map((m, mi) => {
                    const st = live[m]
                    const fill = st ? (st.up ? COLOR_HEX.green : COLOR_HEX.red) : COLOR_HEX.gray
                    return (
                      <g key={m}>
                        <circle cx={x0 + mi * dotGap} cy={yDots} r={4.5} fill={fill} />
                        {st?.leader && (
                          <circle cx={x0 + mi * dotGap} cy={yDots} r={7} fill="none"
                            stroke={fill} strokeWidth="1.5" />
                        )}
                      </g>
                    )
                  })}
                  <text x={NODE_W / 2} y={yDots + 18} className="node-pause-label">
                    {caption}
                  </text>
                </g>
              )
            })()}
            {/* Postgres topology caption (Topology tab): per-member dots with the LIVE PRIMARY
                ringed — the etcd leader-ring convention. The roles come from the failover
                watcher's pg_ha_* series (see App.jsx), not the manifest, because after a
                failover the primary is a standby container and only the watcher knows which.
                A stale FENCED ex-primary is drawn amber: it is up, but read-only, so writers
                skip it. That is the whole failover story in one strip of dots. */}
            {node.type === 'postgres' && node.postgresHa && (() => {
              const members = node.postgresHa.members || []
              const live = data?.members || {}
              const yDots = h + (inOutage ? 26 : 12)
              const dotGap = Math.min(16, members.length > 1 ? (NODE_W - 24) / (members.length - 1) : 16)
              const x0 = NODE_W / 2 - ((members.length - 1) * dotGap) / 2
              const syncCount = (node.postgresHa.sync?.standbys || []).length
              const caption = syncCount
                ? `${members.length} members · sync ANY ${node.postgresHa.sync?.quorum ?? 1}/${syncCount} · failover ${node.postgresHa.enabled === false ? 'off' : 'on'}`
                : `${members.length} members · async · failover ${node.postgresHa.enabled === false ? 'off' : 'on'}`
              return (
                <g style={{ pointerEvents: 'none' }}>
                  {members.map((m, mi) => {
                    const st = live[m]
                    const fill = !st
                      ? COLOR_HEX.gray
                      : !st.up
                        ? COLOR_HEX.red
                        : st.fenced
                          ? COLOR_HEX.yellow
                          : COLOR_HEX.green
                    return (
                      <g key={m}>
                        <circle cx={x0 + mi * dotGap} cy={yDots} r={4.5} fill={fill} />
                        {st?.leader && (
                          <circle cx={x0 + mi * dotGap} cy={yDots} r={7} fill="none"
                            stroke={fill} strokeWidth="1.5" />
                        )}
                      </g>
                    )
                  })}
                  <text x={NODE_W / 2} y={yDots + 18} className="node-pause-label">
                    {caption}
                  </text>
                </g>
              )
            })()}
            {/* Redis persistence caption (Persistence tab): only when the node carries an
                explicit RDB/AOF block (absent = image defaults, nothing to say). Sits below
                the topology strip when one is drawn, else in the first caption slot.
                NOTE: persistence_reader custom nodes reuse the `persistence` key with a
                different shape — the type gate here is load-bearing. */}
            {node.type === 'redis' && node.persistence && (() => {
              const p = node.persistence
              const yTopo = (node.sentinel || node.redisCluster) ? h + (inOutage ? 26 : 12) + 32 : null
              const y = yTopo ?? (h + (inOutage ? 28 : 14))
              const rdb = p.rdb.enabled
                ? `RDB ${p.rdb.rules.length} rule${p.rdb.rules.length === 1 ? '' : 's'}`
                : 'RDB off'
              const aof = p.aof.enabled ? `AOF ${p.aof.fsync}` : 'AOF off'
              return (
                <text x={NODE_W / 2} y={y} className="node-pause-label" style={{ pointerEvents: 'none' }}>
                  💾 {rdb} · {aof}
                </text>
              )
            })()}
          </g>
        )
      })}

      {/* Shared-methods panel, one per websocket tier, below its server fleet: the shared
          methods every relay runs (ws-shared/hooks.js) with a pending badge while an added
          description entry awaits its authored hook, and the tier's single Edit button —
          the per-server Edit buttons are gone (see hasEditButton). */}
      {wsPanels.map((pl) => (
        <g
          key={`ws-panel-${pl.tier}`}
          transform={`translate(${pl.x}, ${pl.y})`}
          className={dimmed(pl.tier) ? 'dim' : undefined}
          // The panel is the bottom card of the combined body: in drag mode it's a drag surface
          // for the whole tier stack (the Edit button's onClick is disabled in drag mode below).
          onPointerDown={dragMode ? (e) => beginDrag(e, fleetDragArg(pl.tier)) : undefined}
        >
          <rect width={pl.w} height={pl.h} rx="8" className="ws-shared-panel" />
          <text x={PAD} y={PAD + 8} className="ws-shared-title">
            shared methods
          </text>
          {pl.methodNames.map((name, i) => {
            const y = PAD + 14 + i * LINE_H
            const pending = wsMethods?.[name]?.implemented === false
            return (
              <g key={name}>
                <text x={PAD} y={y + 12} className="endpoint-row">
                  <tspan className="endpoint-method endpoint-method-fn">ƒ</tspan> {name}
                </text>
                {pending && (
                  <text x={pl.w - PAD} y={y + 12} className="ws-shared-pending">
                    pending
                  </text>
                )}
              </g>
            )
          })}
          {onRequestWsMethods && (
            <g
              className="node-edit"
              onClick={dragMode ? undefined : (e) => {
                e.stopPropagation()
                onRequestWsMethods(pl.tier)
              }}
            >
              <rect
                x={PAD}
                y={pl.h - PAD - EDIT_H}
                width={pl.w - 2 * PAD}
                height={EDIT_H}
                rx="6"
                className="node-edit-btn"
              />
              <text x={pl.w / 2} y={pl.h - PAD - EDIT_H / 2} className="node-edit-label">
                Edit
              </text>
            </g>
          )}
        </g>
      ))}

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
                  Step {openInfo + 1} · {hopEndpointLabel(fromId)} → {hopEndpointLabel(toId)}
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
