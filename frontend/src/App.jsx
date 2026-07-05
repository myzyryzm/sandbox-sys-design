import { useEffect, useRef, useState } from 'react'
import SystemDiagram from './SystemDiagram.jsx'
import Terminal from './Terminal.jsx'
import EditQueuePanel from './EditQueuePanel.jsx'
import CreateDatabase from './CreateDatabase.jsx'
import CreateService from './CreateService.jsx'
import CreateExternalService from './CreateExternalService.jsx'
import CreateClient from './CreateClient.jsx'
import CreateEventStream from './CreateEventStream.jsx'
import CreateWebsockets from './CreateWebsockets.jsx'
import GrpcContractsModal from './GrpcContractsModal.jsx'
import ModelsModal from './ModelsModal.jsx'
import ConnectionResilienceModal from './ConnectionResilienceModal.jsx'
import NodeEditModal from './NodeEditModal.jsx'
import { CUSTOM_RUNTIMES } from './customTypes/index.js'
import TestPanel from './TestPanel.jsx'
import EndToEndModal from './EndToEndModal.jsx'
import SkillsModal from './SkillsModal.jsx'
import { queryInstant } from './prometheus.js'
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

      state[node.id] = { metrics, color }
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
  const [showCreateWebsockets, setShowCreateWebsockets] = useState(false)
  const [showGrpcContracts, setShowGrpcContracts] = useState(false)
  const [showModels, setShowModels] = useState(false)
  const [connectionTarget, setConnectionTarget] = useState(null)
  const [resilienceState, setResilienceState] = useState({})
  const [outages, setOutages] = useState({})
  // Set of event-stream cluster ids whose consumers are currently paused (drives a
  // diagram badge). Polled from /api/consumer-pause, mirroring the outage poll.
  const [pausedConsumers, setPausedConsumers] = useState(() => new Set())
  const [showTest, setShowTest] = useState(false)
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
  // A consumer function selected on the diagram, traced cluster → consuming service. Mutually
  // exclusive with the method/function/LB selections. Cleared on canvas click.
  const [consumerTrace, setConsumerTrace] = useState(null)
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
  const clientFunctions = {}
  for (const n of manifest.nodes || []) {
    if (n.type !== 'client') continue
    clientFunctions[n.id] = scenarios.filter((f) => f.client === n.id)
  }

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
          className={`header-btn drag-toggle no-auto ${dragMode ? 'active' : ''}`}
          onClick={() => setDragMode((v) => !v)}
          title="Drag mode — move nodes and the system boundary"
        >
          <MoveIcon /> Drag
        </button>
        <button className="header-btn" onClick={() => setShowTest(true)}>
          🧪 Test
        </button>
        <button className="header-btn no-auto" onClick={() => setShowEndToEnd(true)}>
          🔁 End-to-End
        </button>
        <button className="header-btn no-auto" onClick={() => setShowSkills(true)}>
          📖 Skills
        </button>
        <button className="header-btn no-auto" onClick={() => setShowCreateSvc(true)}>
          ＋ Add service
        </button>
        <button className="header-btn no-auto" onClick={() => setShowCreateExternal(true)}>
          ＋ Add external service
        </button>
        <button className="header-btn no-auto" onClick={() => setShowCreateClient(true)}>
          ＋ Add client
        </button>
        <button className="header-btn no-auto" onClick={() => setShowCreateDb(true)}>
          ＋ Add database
        </button>
        <button className="header-btn no-auto" onClick={() => setShowCreateEventStream(true)}>
          ＋ Add event stream
        </button>
        <button className="header-btn no-auto" onClick={() => setShowCreateWebsockets(true)}>
          ＋ Add WebSockets
        </button>
        <button className="header-btn no-auto" onClick={() => setShowGrpcContracts(true)}>
          ＋ gRPC contract
        </button>
        <button className="header-btn no-auto" onClick={() => setShowModels(true)}>
          ＋ Models
        </button>
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
        outages={outages}
        pausedConsumers={pausedConsumers}
        customState={customState}
        methodTrace={methodTrace}
        onSelectMethod={(ep) => { setFunctionTrace(null); setConsumerTrace(null); setMethodTrace(ep) }}
        onClearMethodTrace={() => setMethodTrace(null)}
        clientFunctions={clientFunctions}
        functionTrace={functionTrace}
        onSelectFunction={(fn, clientId) => { setMethodTrace(null); setConsumerTrace(null); setFunctionTrace(deriveFunctionTrace(fn, endpoints, clientId)) }}
        onClearFunctionTrace={() => setFunctionTrace(null)}
        consumerFunctions={consumerFunctions}
        consumerTrace={consumerTrace}
        onSelectConsumer={(c, serviceId) => { setMethodTrace(null); setFunctionTrace(null); setConsumerTrace({ cluster: c.cluster, service: serviceId, topic: c.topic, name: c.name, downstream: c.downstream || [], downstreamDescriptions: c.downstreamDescriptions || {} }) }}
        onClearConsumerTrace={() => setConsumerTrace(null)}
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
      {showCreateWebsockets && (
        <CreateWebsockets systemId={SYSTEM_ID} onClose={() => setShowCreateWebsockets(false)} />
      )}
      {showTest && (
        <TestPanel systemId={SYSTEM_ID} onClose={() => setShowTest(false)} />
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
          systemId={SYSTEM_ID}
          from={connectionTarget.from}
          to={connectionTarget.to}
          initial={
            (manifest.edges || []).find(
              (e) => e.from === connectionTarget.from && e.to === connectionTarget.to,
            )?.resilience || null
          }
          onClose={() => setConnectionTarget(null)}
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
            setMethodTrace(ep)
            setEditTarget(null)
          }}
        />
      )}
    </div>
  )
}
