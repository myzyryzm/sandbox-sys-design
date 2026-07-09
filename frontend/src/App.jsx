import { useEffect, useRef, useState } from 'react'
import SystemDiagram from './SystemDiagram.jsx'
import Terminal from './Terminal.jsx'
import EditQueuePanel from './EditQueuePanel.jsx'
import AddMenu from './AddMenu.jsx'
import CreateDatabase from './CreateDatabase.jsx'
import CreateService from './CreateService.jsx'
import CreateExternalService from './CreateExternalService.jsx'
import CreateClient from './CreateClient.jsx'
import CreateEventStream from './CreateEventStream.jsx'
import CreateEtcd from './CreateEtcd.jsx'
import CreateWebsockets from './CreateWebsockets.jsx'
import GrpcContractsModal from './GrpcContractsModal.jsx'
import ModelsModal from './ModelsModal.jsx'
import ConnectionResilienceModal from './ConnectionResilienceModal.jsx'
import NodeEditModal from './NodeEditModal.jsx'
import WsSharedMethodsModal from './WsSharedMethodsModal.jsx'
import { CUSTOM_RUNTIMES } from './customTypes/index.js'
import EndToEndModal from './EndToEndModal.jsx'
import SkillsModal from './SkillsModal.jsx'
import { queryInstant, queryVector } from './prometheus.js'
import { pickColor } from './health.js'
import { deriveFunctionTrace } from './scenarioBank.js'

const SYSTEM_ID = import.meta.env.VITE_SYSTEM_ID || 'hello-lb'

// The 4-direction "move" glyph for the drag-mode toggle (four arrows out from center).
// Inline SVG (the app uses no icon library); inherits the button's text color.
function MoveIcon() {
  return (
    <svg
      className="move-icon"
      viewBox="0 0 24 24"
      width="14"
      height="14"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <polyline points="5 9 2 12 5 15" />
      <polyline points="9 5 12 2 15 5" />
      <polyline points="15 19 12 22 9 19" />
      <polyline points="19 9 22 12 19 15" />
      <line x1="2" y1="12" x2="22" y2="12" />
      <line x1="12" y1="2" x2="12" y2="22" />
    </svg>
  )
}

/**
 * Poll every node's metric + health queries once and return a map:
 *   { [nodeId]: { metrics: { [label]: value|null }, color } }
 */
async function pollSystem(manifest) {
  // No Prometheus node on the diagram ⇒ observability is "off": skip every query so all
  // nodes go gray and their metrics dropdowns read "no metrics" (matches the diagram gate).
  if (!manifest.nodes.some((n) => n.type === 'prometheus')) return {}
  const base = manifest.prometheus_base
  const state = {}

  await Promise.all(
    manifest.nodes.map(async (node) => {
      const metrics = {}

      // Each displayed metric is an independent instant query.
      await Promise.all(
        (node.metrics || []).map(async (m) => {
          try {
            metrics[m.label] = await queryInstant(base, m.query)
          } catch (err) {
            console.warn(`metric "${m.label}" on ${node.id} failed:`, err.message)
            metrics[m.label] = null
          }
        }),
      )

      // Health query drives node color. Nodes without a health block stay
      // neutral (gray).
      let color = 'gray'
      if (node.health?.query) {
        try {
          const value = await queryInstant(base, node.health.query)
          color = pickColor(node.health.rules, value)
        } catch (err) {
          console.warn(`health on ${node.id} failed:`, err.message)
        }
      }

      // An etcd cluster node additionally reads PER-MEMBER series (all N `up` and
      // `is_leader` series of its job) to drive the member-dot strip under the node.
      let members = null
      if (node.type === 'etcd') {
        try {
          const [ups, leaders] = await Promise.all([
            queryVector(base, `up{job="${node.id}"}`),
            queryVector(base, `etcd_server_is_leader{job="${node.id}"}`),
          ])
          members = {}
          for (const s of ups) {
            const m = (s.labels.instance || '').split(':')[0]
            if (m) members[m] = { up: s.value === 1, leader: false }
          }
          for (const s of leaders) {
            const m = (s.labels.instance || '').split(':')[0]
            if (members[m]) members[m].leader = s.value === 1
          }
        } catch (err) {
          console.warn(`etcd members on ${node.id} failed:`, err.message)
        }
      }

      state[node.id] = members ? { metrics, color, members } : { metrics, color }
    }),
  )

  return state
}

export default function App() {
  const [manifest, setManifest] = useState(null)
  const [nodeData, setNodeData] = useState({})
  const [error, setError] = useState(null)
  const [showTerminal, setShowTerminal] = useState(false)
  // Drag mode: when on, nodes can be repositioned and the system boundary box moved/resized
  // on the diagram (and the normal click actions are suppressed). The toggle turns green.
  const [dragMode, setDragMode] = useState(false)
  const [showCreateDb, setShowCreateDb] = useState(false)
  const [showCreateSvc, setShowCreateSvc] = useState(false)
  const [showCreateExternal, setShowCreateExternal] = useState(false)
  const [showCreateClient, setShowCreateClient] = useState(false)
  const [showCreateEventStream, setShowCreateEventStream] = useState(false)
  const [showCreateEtcd, setShowCreateEtcd] = useState(false)
  const [showCreateWebsockets, setShowCreateWebsockets] = useState(false)
  const [showGrpcContracts, setShowGrpcContracts] = useState(false)
  const [showModels, setShowModels] = useState(false)
  const [connectionTarget, setConnectionTarget] = useState(null)
  const [resilienceState, setResilienceState] = useState({})
  const [poolState, setPoolState] = useState({})
  const [outages, setOutages] = useState({})
  // Set of event-stream cluster ids whose consumers are currently paused (drives a
  // diagram badge). Polled from /api/consumer-pause, mirroring the outage poll.
  const [pausedConsumers, setPausedConsumers] = useState(() => new Set())
  const [showEndToEnd, setShowEndToEnd] = useState(false)
  const [showSkills, setShowSkills] = useState(false)
  const [endpoints, setEndpoints] = useState([])
  // The node whose tabbed "Edit" modal is open (endpoints / gRPC / calls / schema /
  // topics / shutdown / delete — whichever tabs apply to that node's kind).
  const [editTarget, setEditTarget] = useState(null)
  // A service method selected from its Edit ▸ Calls tab, traced on the main diagram
  // (service → the nodes it calls, with request/response schema arrows). Mutually
  // exclusive with the load balancer's own endpoint selection. Cleared on canvas click.
  const [methodTrace, setMethodTrace] = useState(null)
  // The per-system "client function" bank (systems/<id>/scenarios.json), polled below.
  // Resolved per-client into clickable rows on each client node.
  const [scenarios, setScenarios] = useState([])
  // A client function selected on the diagram, traced client → LB → its called services →
  // their downstreams (with each called method highlighted). Mutually exclusive with the
  // method/LB selections. Cleared on canvas click.
  const [functionTrace, setFunctionTrace] = useState(null)
  // The per-system consumer-function registry (systems/<id>/consumers.json), polled below.
  // Grouped per service into clickable CONS rows on each service node.
  const [consumers, setConsumers] = useState([])
  // The websocket tier info (GET /api/websockets: registry + the pool client's builtin
  // method descriptors + its last run's delivery stats), polled below only while the
  // manifest actually contains a tier. Drives the ws client node's ƒ rows + stat rows.
  const [wsInfo, setWsInfo] = useState(null)
  // The websocket tier (lb id) whose SHARED editing modal is open — the shared methods
  // (onMessage/onSend) + per-server shutdown + tier delete. Opened from the Edit button
  // on the shared-methods panel the diagram draws below the server fleet.
  const [wsMethodsTier, setWsMethodsTier] = useState(null)
  // A consumer function selected on the diagram, traced cluster → consuming service. Mutually
  // exclusive with the method/function/LB selections. Cleared on canvas click.
  const [consumerTrace, setConsumerTrace] = useState(null)
  // The per-etcd keyspace registry (systems/<id>/etcd.json via GET /api/etcd?live=0), keyed by
  // etcd node id. Rendered as clickable KEY rows on the etcd cluster node.
  const [etcdKeyspaces, setEtcdKeyspaces] = useState({})
  // A keyspace selected on the etcd node, traced registrant → etcd (the lease-put) and
  // etcd → each listener (the watch push). Mutually exclusive with the other traces.
  const [keyspaceTrace, setKeyspaceTrace] = useState(null)
  // The gRPC contract registry (systems/<id>/grpc/_registry.json via GET /api/grpc-contracts),
  // as [{ name, methods, … }]. A service that SERVES a contract lists its methods as RPC rows.
  const [grpcContracts, setGrpcContracts] = useState([])
  // A served RPC method selected on a server service, traced each caller → this server.
  // Mutually exclusive with the other traces.
  const [rpcTrace, setRpcTrace] = useState(null)
  // Live runtime state for custom service types (e.g. Download Coordinator worker
  // bitmaps / distribution progress), keyed by node id. Filled by the poll below.
  const [customState, setCustomState] = useState({})
  const [terminalSession, setTerminalSession] = useState(null)
  // Edit queue: Claude sessions to run one at a time instead of clobbering each other.
  // Items: { id, sessionId, prompt, meta:{kind,target,title}, status }, status is
  // 'pending' | 'running' | 'done' ('done' = finished, in the ~10s pre-advance hold).
  const [queue, setQueue] = useState([])
  const [showQueue, setShowQueue] = useState(false)
  const [doneCountdown, setDoneCountdown] = useState(null)
  const advanceTimerRef = useRef(null)
  const countdownTimerRef = useRef(null)
  const timerRef = useRef(null)
  const manifestJsonRef = useRef(null)

  // Load the manifest, then keep re-fetching it so components added from the
  // terminal (which edits manifest.json) appear in the diagram without a reload.
  // Only swap state when the JSON actually changed.
  useEffect(() => {
    let cancelled = false

    const loadManifest = async () => {
      try {
        const res = await fetch(`/systems/${SYSTEM_ID}/manifest.json`)
        if (!res.ok) throw new Error(`manifest HTTP ${res.status}`)
        const text = await res.text()
        if (cancelled || text === manifestJsonRef.current) return
        manifestJsonRef.current = text
        setManifest(JSON.parse(text))
        setError(null)
      } catch (err) {
        if (!manifestJsonRef.current) setError(`Failed to load manifest: ${err.message}`)
      }
    }

    loadManifest()
    const id = setInterval(loadManifest, 3000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Poll metrics on the manifest's cadence once the manifest is loaded.
  useEffect(() => {
    if (!manifest) return

    let cancelled = false
    const tick = async () => {
      const state = await pollSystem(manifest)
      if (!cancelled) setNodeData(state)
    }

    tick() // immediate first poll
    const interval = manifest.poll_interval_ms || 4000
    timerRef.current = setInterval(tick, interval)

    return () => {
      cancelled = true
      clearInterval(timerRef.current)
    }
  }, [manifest])

  // Poll the system's live, routable endpoints (shown on the LB node). They
  // change as services are added or as a service gains endpoints.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/endpoints?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setEndpoints(data.endpoints)
      } catch {
        /* leave the last good list in place */
      }
    }
    tick()
    const id = setInterval(tick, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Poll the per-client functions registry so each client's function rows (and their traces)
  // update as functions are authored/edited from the Functions tab.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/scenarios?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setScenarios(data.functions || [])
      } catch {
        /* keep the last good list */
      }
    }
    tick()
    const id = setInterval(tick, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Poll the websocket tier (registry + builtin client methods + last pool-run stats).
  // Gated on the manifest containing a tier, so tier-less systems never hit the route;
  // the gate flipping (tier created / deleted) starts and stops the poll automatically.
  const hasWsTier = !!manifest?.nodes?.some((n) => n.origin === 'create-websockets')
  useEffect(() => {
    if (!hasWsTier) {
      setWsInfo(null)
      return
    }
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/websockets?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setWsInfo(data)
      } catch {
        /* keep the last good response */
      }
    }
    tick()
    const id = setInterval(tick, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [hasWsTier])

  // Poll the per-service consumer-function registry so each service's CONS rows (and their
  // cluster→service traces) update as consumer functions are authored/edited/deleted.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/consumers?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setConsumers(data.consumers || [])
      } catch {
        /* keep the last good list */
      }
    }
    tick()
    const id = setInterval(tick, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Poll each etcd cluster's keyspace registry (registry-only, ?live=0 — no docker
  // probing) so the cluster node's KEY rows and their traces update as keyspaces /
  // listeners are added. Gated on the manifest containing an etcd node.
  const etcdIds = (manifest?.nodes || []).filter((n) => n.type === 'etcd').map((n) => n.id).join(',')
  useEffect(() => {
    if (!etcdIds) {
      setEtcdKeyspaces({})
      return
    }
    let cancelled = false
    const tick = async () => {
      const entries = await Promise.all(
        etcdIds.split(',').map(async (id) => {
          try {
            const res = await fetch(`/api/etcd?system=${SYSTEM_ID}&id=${encodeURIComponent(id)}&live=0`)
            const data = await res.json()
            return data.ok ? [id, data.keyspaces || []] : null
          } catch {
            return null
          }
        }),
      )
      if (!cancelled) setEtcdKeyspaces(Object.fromEntries(entries.filter(Boolean)))
    }
    tick()
    const id = setInterval(tick, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [etcdIds])

  // Poll the gRPC contract registry so a server service's RPC rows (contract name in its manifest
  // `grpc.servers`) stay in sync as methods are added/removed. Gated on the manifest having any
  // node that serves a contract, so systems with no gRPC servers never hit the route.
  const hasGrpcServers = (manifest?.nodes || []).some((n) => (n.grpc?.servers || []).length > 0)
  useEffect(() => {
    if (!hasGrpcServers) {
      setGrpcContracts([])
      return
    }
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/grpc-contracts?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setGrpcContracts(data.contracts || [])
      } catch {
        /* keep the last good list */
      }
    }
    tick()
    const id = setInterval(tick, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [hasGrpcServers])

  // Fast-poll the in-memory resilience state (breaker/retry) so the diagram can show
  // a breaker trip live, faster than Prometheus' scrape. The aggregator returns {}
  // until services are wired, so this is harmless before any policy exists.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/resilience-state?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setResilienceState(data.connections || {})
      } catch {
        /* keep the last good state */
      }
    }
    tick()
    const id = setInterval(tick, 750)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Fast-poll live connection-pool state (active/idle counts) so the diagram can show a
  // pool badge on the line. Empty until a service exposes /pool/state, so it's harmless
  // before any pool is wired.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/connection-pool-state?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setPoolState(data.connections || {})
      } catch {
        /* keep the last good state */
      }
    }
    tick()
    const id = setInterval(tick, 750)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Poll active outages (nodes temporarily shut down) once a second so the diagram
  // can paint them orange and tick down the remaining time. Reduced to a node-keyed
  // map for O(1) lookup in the diagram.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/outage?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) {
          setOutages(Object.fromEntries((data.outages || []).map((o) => [o.node, o])))
        }
      } catch {
        /* keep the last good state */
      }
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Poll which event-stream clusters have their consumers paused (registry-only, so
  // it's cheap) every few seconds, so the diagram can badge them. The flag is a pure
  // streams.json write toggled from the Topics tab — no rebuild involved.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/consumer-pause?system=${SYSTEM_ID}`)
        const data = await res.json()
        if (!cancelled && data.ok) setPausedConsumers(new Set(data.paused || []))
      } catch {
        /* keep the last good state */
      }
    }
    tick()
    const id = setInterval(tick, 3000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // Poll every registered custom service type's runtime endpoint (~1s) and merge the
  // results into one node-keyed map the diagram + edit tabs read. No registered runtime
  // → this is a no-op. Each endpoint returns { ok, nodes: { [id]: state } }.
  useEffect(() => {
    if (CUSTOM_RUNTIMES.length === 0) return
    let cancelled = false
    const tick = async () => {
      const maps = await Promise.all(
        CUSTOM_RUNTIMES.map(async (rt) => {
          try {
            const res = await fetch(rt.url(SYSTEM_ID))
            const data = await res.json()
            return data.ok ? data.nodes || {} : {}
          } catch {
            return {}
          }
        }),
      )
      if (!cancelled) setCustomState(Object.assign({}, ...maps))
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  // ─── Edit queue ─────────────────────────────────────────────────────────────
  // Submissions used to launch a Claude session immediately, so a second edit
  // clobbered the first. Now each is ENQUEUED and run one at a time; the queue
  // auto-advances ~10s after a session prints its completion sentinel (wired via
  // terminal.js → Terminal.jsx's onSessionDone).
  const queueRef = useRef(queue)
  useEffect(() => {
    queueRef.current = queue
  }, [queue])

  const clearAdvanceTimers = () => {
    if (advanceTimerRef.current) {
      clearTimeout(advanceTimerRef.current)
      advanceTimerRef.current = null
    }
    if (countdownTimerRef.current) {
      clearInterval(countdownTimerRef.current)
      countdownTimerRef.current = null
    }
    setDoneCountdown(null)
  }
  useEffect(() => () => clearAdvanceTimers(), [])

  // The seam every modal calls (passed as onLaunch). `meta` ({ kind, target, title })
  // only renders the queue row; missing meta degrades to a generic label.
  const enqueueSession = (cfg, meta = {}) => {
    if (!cfg || !cfg.sessionId) return
    // "Resume" launches (re-opening an existing endpoint/function session) bypass the
    // queue — they're a direct "show me this session" action, not a new edit to run in turn.
    if (cfg.mode === 'resume') {
      setTerminalSession(cfg)
      setShowTerminal(true)
      return
    }
    setQueue((q) => [
      ...q,
      { id: cfg.sessionId, sessionId: cfg.sessionId, prompt: cfg.prompt || '', meta, status: 'pending' },
    ])
    setShowQueue(true)
  }

  // Runner: when nothing is running (and nothing is in its post-completion hold),
  // promote the first pending item and open its session in the terminal.
  useEffect(() => {
    if (queue.some((it) => it.status === 'running' || it.status === 'done')) return
    const next = queue.find((it) => it.status === 'pending')
    if (!next) return
    setQueue((q) => q.map((it) => (it.id === next.id ? { ...it, status: 'running' } : it)))
    setTerminalSession({ sessionId: next.sessionId, mode: 'new', prompt: next.prompt })
    setShowTerminal(true)
  }, [queue])

  // Drop the finished (done) item; the runner then starts the next pending one.
  const advanceQueue = () => {
    clearAdvanceTimers()
    setQueue((q) => q.filter((it) => it.status !== 'done'))
  }

  // Called by Terminal when the running session prints the completion sentinel.
  const handleSessionDone = (sessionId) => {
    const running = queueRef.current.find((it) => it.status === 'running')
    if (!running || running.sessionId !== sessionId) return // ignore ad-hoc / stale signals
    setQueue((q) => q.map((it) => (it.status === 'running' ? { ...it, status: 'done' } : it)))
    // Hold ~10s (with a visible countdown) before starting the next edit.
    clearAdvanceTimers()
    let n = 10
    setDoneCountdown(n)
    countdownTimerRef.current = setInterval(() => {
      n -= 1
      setDoneCountdown(Math.max(0, n))
    }, 1000)
    advanceTimerRef.current = setTimeout(advanceQueue, 10000)
  }

  // Remove a queued item. For the running/done item this also stops it: removing it
  // lets the runner start the next pending session (whose terminal reconnect kills the
  // old PTY), or — if none remain — hides the terminal so the session is torn down.
  const cancelItem = (id) => {
    const item = queueRef.current.find((it) => it.id === id)
    if (!item) return
    if (item.status === 'running' || item.status === 'done') {
      clearAdvanceTimers()
      const hasNext = queueRef.current.some((it) => it.id !== id && it.status === 'pending')
      if (!hasNext) {
        setTerminalSession(null)
        setShowTerminal(false)
      }
    }
    setQueue((q) => q.filter((it) => it.id !== id))
  }

  // Once a 'new' session is live its id exists, so demote it to 'resume' and drop
  // the prompt — otherwise re-mounting the terminal would re-run --session-id on
  // an id that now exists (an error).
  const onSessionLaunched = () => {
    setTerminalSession((s) => (s && s.mode === 'new' ? { ...s, mode: 'resume', prompt: '' } : s))
  }

  // Add the Prometheus node to the diagram (only offered when none exists). No modal —
  // there's nothing to configure — just a POST; the 3s manifest poll shows the new node.
  const addPrometheus = async () => {
    try {
      const res = await fetch('/api/prom-node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: SYSTEM_ID }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
    } catch (err) {
      setError(`Failed to add Prometheus: ${err.message}`)
    }
  }

  if (error) {
    return (
      <div className="app">
        <h1>Distributed Systems Sandbox</h1>
        <p className="error">{error}</p>
      </div>
    )
  }

  if (!manifest) {
    return (
      <div className="app">
        <h1>Distributed Systems Sandbox</h1>
        <p>Loading <code>{SYSTEM_ID}</code>…</p>
      </div>
    )
  }

  // Group the live functions registry by owner client into the function objects (with steps)
  // the diagram renders as rows + traces. Only clients own functions (external services don't).
  // A websocket client's rows are its pool script's BUILTIN ws methods (from /api/websockets,
  // synthesized here and traced along the tier path) PLUS — like any client — its own
  // authorable HTTP functions (from `scenarios`, traced client → LB → services). The builtin
  // methods are NOT injected into `scenarios`, so the end-to-end modal's client_list method
  // dropdown (built from `scenarios`) lists only a client's real HTTP functions.
  const clientFunctions = {}
  for (const n of manifest.nodes || []) {
    if (n.type !== 'client') continue
    const own = scenarios.filter((f) => f.client === n.id)
    if (n.origin === 'create-websockets') {
      const builtin = (wsInfo?.clientMethods || []).map((m) => ({
        client: n.id,
        name: m.name,
        args: m.args || [],
        wsBuiltin: true,
      }))
      clientFunctions[n.id] = [...builtin, ...own]
      continue
    }
    clientFunctions[n.id] = own
  }

  // The ws pool client's last-run delivery stats, keyed by its node id for the diagram.
  const wsClientNode = (manifest.nodes || []).find(
    (n) => n.origin === 'create-websockets' && n.wsRole === 'client',
  )
  const wsStats = wsClientNode && wsInfo?.stats ? { [wsClientNode.id]: wsInfo.stats } : {}

  // Group the live consumer-function registry by owner service into the CONS rows the diagram
  // renders on each service node. Only internal services own consumer functions.
  const consumerFunctions = {}
  for (const n of manifest.nodes || []) {
    if (n.type !== 'service') continue
    consumerFunctions[n.id] = consumers.filter((c) => c.service === n.id)
  }

  return (
    <div className="app">
      <header>
        <h1>{manifest.name}</h1>
        <span className="system-id">{manifest.system_id}</span>
        <button
          className={`header-btn drag-toggle ${dragMode ? 'active' : ''}`}
          onClick={() => setDragMode((v) => !v)}
          title="Drag mode — move nodes and the system boundary"
        >
          <MoveIcon /> Edit
        </button>
        <button className="header-btn no-auto" onClick={() => setShowEndToEnd(true)}>
          🔁 End-to-End
        </button>
        <button className="header-btn no-auto" onClick={() => setShowSkills(true)}>
          📖 Skills
        </button>
        <AddMenu
          groups={[
            {
              label: 'Nodes',
              items: [
                { label: 'Service', onClick: () => setShowCreateSvc(true) },
                { label: 'External service', onClick: () => setShowCreateExternal(true) },
                { label: 'Client', onClick: () => setShowCreateClient(true) },
                { label: 'Database', onClick: () => setShowCreateDb(true) },
                { label: 'Event stream', onClick: () => setShowCreateEventStream(true) },
                // Only one etcd cluster may exist — hide the option while one is on the
                // diagram (the backend also 409s a second add), like Prometheus below.
                ...(manifest.nodes.some((n) => n.type === 'etcd')
                  ? []
                  : [{ label: 'etcd', onClick: () => setShowCreateEtcd(true) }]),
                { label: 'WebSockets', onClick: () => setShowCreateWebsockets(true) },
                // Only one Prometheus node may exist — hide the option while one is on the
                // diagram (the backend also 409s a second add).
                ...(manifest.nodes.some((n) => n.type === 'prometheus')
                  ? []
                  : [{ label: 'Prometheus', onClick: addPrometheus }]),
              ],
            },
            {
              label: 'Contracts & schemas',
              items: [
                { label: 'gRPC contract', onClick: () => setShowGrpcContracts(true) },
                { label: 'Models', onClick: () => setShowModels(true) },
              ],
            },
          ]}
        />
        <button
          className={`header-btn no-auto ${queue.length ? 'has-queue' : ''}`}
          onClick={() => setShowQueue((v) => !v)}
          title="Edit queue — pending Claude sessions run one at a time"
        >
          🗒 Queue{queue.length ? ` (${queue.length})` : ''}
        </button>
        <button className="term-toggle" onClick={() => setShowTerminal((v) => !v)}>
          {showTerminal ? 'Hide terminal' : 'Edit with Claude ▸'}
        </button>
      </header>
      <div className="canvas">
      {showQueue && queue.length > 0 && (
        <EditQueuePanel
          items={queue}
          countdown={doneCountdown}
          onRemove={cancelItem}
          onNext={advanceQueue}
          onClose={() => setShowQueue(false)}
        />
      )}
      <SystemDiagram
        manifest={manifest}
        nodeData={nodeData}
        endpoints={endpoints}
        systemId={SYSTEM_ID}
        dragMode={dragMode}
        onRequestEdit={setEditTarget}
        onRequestConnectionResilience={setConnectionTarget}
        resilienceState={resilienceState}
        poolState={poolState}
        outages={outages}
        pausedConsumers={pausedConsumers}
        customState={customState}
        methodTrace={methodTrace}
        onSelectMethod={(ep) => { setFunctionTrace(null); setConsumerTrace(null); setKeyspaceTrace(null); setRpcTrace(null); setMethodTrace(ep) }}
        onClearMethodTrace={() => setMethodTrace(null)}
        clientFunctions={clientFunctions}
        wsStats={wsStats}
        wsMethods={wsInfo?.tier?.methods || null}
        wsAlgorithm={wsInfo?.tier?.algorithm || 'leastconn'}
        onRequestWsMethods={setWsMethodsTier}
        functionTrace={functionTrace}
        onSelectFunction={(fn, clientId) => {
          setMethodTrace(null)
          setConsumerTrace(null)
          setKeyspaceTrace(null)
          setRpcTrace(null)
          // A ws builtin has no authored steps — the diagram traces the tier path itself.
          setFunctionTrace(fn.wsBuiltin
            ? { client: clientId, name: fn.name, wsBuiltin: true, methods: [] }
            : deriveFunctionTrace(fn, endpoints, clientId))
        }}
        onClearFunctionTrace={() => setFunctionTrace(null)}
        consumerFunctions={consumerFunctions}
        consumerTrace={consumerTrace}
        onSelectConsumer={(c, serviceId) => { setMethodTrace(null); setFunctionTrace(null); setKeyspaceTrace(null); setRpcTrace(null); setConsumerTrace({ cluster: c.cluster, service: serviceId, topic: c.topic, name: c.name, downstream: c.downstream || [], downstreamDescriptions: c.downstreamDescriptions || {} }) }}
        onClearConsumerTrace={() => setConsumerTrace(null)}
        etcdKeyspaces={etcdKeyspaces}
        keyspaceTrace={keyspaceTrace}
        onSelectKeyspace={(ks, etcdId) => {
          setMethodTrace(null)
          setFunctionTrace(null)
          setConsumerTrace(null)
          setRpcTrace(null)
          setKeyspaceTrace({
            etcd: etcdId,
            type: ks.type || 'discovery',
            service: ks.service,
            name: ks.name,
            prefix: ks.prefix,
            listeners: (ks.listeners || []).map((l) => l.service),
          })
        }}
        onClearKeyspaceTrace={() => setKeyspaceTrace(null)}
        onSelectSubscription={(ks, etcdId, listenerId) => {
          setMethodTrace(null)
          setFunctionTrace(null)
          setConsumerTrace(null)
          setRpcTrace(null)
          // Same keyspaceTrace shape as onSelectKeyspace, but focused on ONE listener: the
          // `kt` branch draws registrant → etcd → this listener only (config: etcd → listener).
          // `focus` lets the diagram mark the service's SUB row active without lighting the
          // etcd node's whole KEY row.
          setKeyspaceTrace({
            etcd: etcdId,
            type: ks.type || 'discovery',
            service: ks.service,
            name: ks.name,
            prefix: ks.prefix,
            listeners: [listenerId],
            focus: listenerId,
          })
        }}
        grpcContracts={grpcContracts}
        rpcTrace={rpcTrace}
        onSelectRpc={(r, serviceId) => {
          setMethodTrace(null)
          setFunctionTrace(null)
          setConsumerTrace(null)
          setKeyspaceTrace(null)
          setRpcTrace({ service: serviceId, contract: r.contract, method: r.method })
        }}
        onClearRpcTrace={() => setRpcTrace(null)}
      />
      </div>
      {showTerminal && (
        <div className="terminal-panel">
          <Terminal
            systemId={SYSTEM_ID}
            session={terminalSession}
            onLaunched={onSessionLaunched}
            onSessionDone={handleSessionDone}
          />
        </div>
      )}
      {showCreateDb && (
        <CreateDatabase
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateDb(false)}
          onLaunch={enqueueSession}
        />
      )}
      {showCreateSvc && (
        <CreateService systemId={SYSTEM_ID} onClose={() => setShowCreateSvc(false)} />
      )}
      {showCreateExternal && (
        <CreateExternalService systemId={SYSTEM_ID} onClose={() => setShowCreateExternal(false)} />
      )}
      {showCreateClient && (
        <CreateClient systemId={SYSTEM_ID} onClose={() => setShowCreateClient(false)} />
      )}
      {showCreateEventStream && (
        <CreateEventStream systemId={SYSTEM_ID} onClose={() => setShowCreateEventStream(false)} />
      )}
      {showCreateEtcd && (
        <CreateEtcd systemId={SYSTEM_ID} onClose={() => setShowCreateEtcd(false)} />
      )}
      {showCreateWebsockets && (
        <CreateWebsockets systemId={SYSTEM_ID} onClose={() => setShowCreateWebsockets(false)} />
      )}
      {showEndToEnd && (
        <EndToEndModal
          systemId={SYSTEM_ID}
          manifest={manifest}
          scenarios={scenarios}
          onLaunch={enqueueSession}
          onClose={() => setShowEndToEnd(false)}
        />
      )}
      {showSkills && <SkillsModal onClose={() => setShowSkills(false)} />}
      {showGrpcContracts && (
        <GrpcContractsModal
          systemId={SYSTEM_ID}
          onClose={() => setShowGrpcContracts(false)}
          onLaunch={enqueueSession}
        />
      )}
      {showModels && (
        <ModelsModal
          systemId={SYSTEM_ID}
          manifest={manifest}
          onLaunch={enqueueSession}
          onClose={() => setShowModels(false)}
        />
      )}
      {connectionTarget && (
        <ConnectionResilienceModal
          key={`${connectionTarget.from}->${connectionTarget.to}`}
          systemId={SYSTEM_ID}
          from={connectionTarget.from}
          to={connectionTarget.to}
          initial={
            (manifest.edges || []).find(
              (e) => e.from === connectionTarget.from && e.to === connectionTarget.to,
            )?.resilience || null
          }
          initialPool={
            (manifest.edges || []).find(
              (e) => e.from === connectionTarget.from && e.to === connectionTarget.to,
            )?.connection_pool || null
          }
          poolEligible={
            !(manifest.nodes || []).find((n) => n.id === connectionTarget.to)?.external
          }
          onClose={() => setConnectionTarget(null)}
          onLaunch={enqueueSession}
        />
      )}
      {wsMethodsTier && wsInfo?.tier && (
        <WsSharedMethodsModal
          systemId={SYSTEM_ID}
          tier={wsInfo.tier}
          manifest={manifest}
          outages={outages}
          onClose={() => setWsMethodsTier(null)}
          onLaunch={enqueueSession}
        />
      )}
      {editTarget && (
        <NodeEditModal
          systemId={SYSTEM_ID}
          node={editTarget}
          manifest={manifest}
          current={outages[editTarget.id] || null}
          onClose={() => setEditTarget(null)}
          onLaunch={enqueueSession}
          onTraceMethod={(ep) => {
            // Trace the picked method on the main diagram and close the modal so it's
            // visible behind the (now-dismissed) overlay.
            setFunctionTrace(null)
            setConsumerTrace(null)
            setKeyspaceTrace(null)
            setMethodTrace(ep)
            setEditTarget(null)
          }}
        />
      )}
    </div>
  )
}
