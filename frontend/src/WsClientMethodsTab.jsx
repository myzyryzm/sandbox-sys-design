import { useEffect, useState } from 'react'

/**
 * Read-only "Functions" tab for a WEBSOCKET pool client (origin create-websockets).
 *
 * Unlike a regular client's ClientScenarioTab (authorable python functions), a ws
 * client's behavior is its generated host pool script (ws-clients/<id>.mjs), whose
 * two methods are BUILT-IN: not editable, not deletable, and not runnable from here —
 * only end-to-end processes invoke them (their WebSocket client pool rows). This tab
 * renders the method descriptors served by GET /api/websockets (names, args with
 * defaults + bounds, summaries) plus the last pool run's delivery stats, which the
 * script writes next to itself as ws-clients/<id>.stats.json on every run.
 */

// Shown until /api/websockets answers (and if an older backend omits clientMethods).
const FALLBACK_METHODS = [
  {
    name: 'spawnAndSend',
    builtin: true,
    summary:
      'spawn N pool clients that connect through the L4 load balancer; each sends messages to random peers at the given rate for the duration, then reports delivery stats',
    args: [
      { name: 'count', type: 'number', default: 5, min: 1, max: 200 },
      { name: 'durationSeconds', type: 'number', default: 10, min: 1, max: 120 },
      { name: 'rate', type: 'number', default: 1, min: 1, max: 20 },
    ],
  },
  {
    name: 'onReceive',
    builtin: true,
    summary:
      "the 'message' handler: dedupes by msgId (a repeat only bumps the duplicates counter), records the delivery, and measures latency from the sender's sentAt timestamp",
    args: [{ name: 'message', type: 'json' }],
  },
]

const sig = (m) =>
  `${m.name}(${(m.args || [])
    .map((a) => `${a.name}: ${a.type}${a.default !== undefined ? ` = ${a.default}` : ''}`)
    .join(', ')})`

const boundsNote = (m) => {
  const capped = (m.args || []).filter((a) => a.max !== undefined)
  if (!capped.length) return null
  return `bounds: ${capped.map((a) => `${a.name} ${a.min ?? 1}–${a.max}`).join(' · ')}`
}

export default function WsClientMethodsTab({ systemId, node, onClose, embedded = false }) {
  const [info, setInfo] = useState(null) // last good GET /api/websockets response

  // Poll while open (same 4s cadence as the app's registry polls) so the stats
  // section refreshes live when an end-to-end run finishes.
  useEffect(() => {
    let live = true
    const load = () =>
      fetch(`/api/websockets?system=${encodeURIComponent(systemId)}`)
        .then((r) => r.json())
        .then((d) => { if (live && d?.ok) setInfo(d) })
        .catch(() => {}) // keep the last good response
    load()
    const timer = setInterval(load, 4000)
    return () => { live = false; clearInterval(timer) }
  }, [systemId])

  const methods = info?.clientMethods?.length ? info.clientMethods : FALLBACK_METHODS
  const stats = info?.stats || null
  const r = stats?.results

  const body = (
    <>
      <p className="sim-desc">
        Built-in methods of <strong>{node.label}</strong>'s host pool script{' '}
        <code>ws-clients/{node.id}.mjs</code>. They come with the websocket tier — not
        editable, not deletable, and not runnable from here: only an{' '}
        <strong>end-to-end process</strong> invokes them (add this client under{' '}
        <em>WebSocket clients</em> when defining a process).
      </p>

      {methods.map((m) => (
        <div className="form-section" key={m.name}>
          <div>
            <code className="scenario-fn-sig">{sig(m)}</code>{' '}
            <span className="scenario-stepcount" title="Part of the generated pool script — no edit, delete or manual run">
              built-in
            </span>
          </div>
          <p className="sim-desc">
            {m.summary}
            {boundsNote(m) && <> — {boundsNote(m)}</>}
          </p>
        </div>
      ))}

      <div className="scenario-results">
        <div className="scenario-results-head">Last pool run</div>
        {!r ? (
          <p className="sim-desc">
            No pool run recorded yet — run one from an end-to-end process.
          </p>
        ) : (
          <>
            <p className="sim-desc">
              {stats.ts ? new Date(stats.ts).toLocaleString() : 'time unknown'}
              {stats.args && (
                <>
                  {' '}· count {stats.args.count} · {stats.args.durationSeconds}s · rate{' '}
                  {stats.args.rate}/s
                </>
              )}
            </p>
            <pre className="scenario-result-body">
              spawned {r.spawned} · connected {r.connected} · sent {r.sent} · delivered {r.delivered}
              {'\n'}duplicates {r.duplicates} · errors {r.errors}
              {'\n'}latency p50 {r.latencyMs?.p50 ?? '—'} ms · p95 {r.latencyMs?.p95 ?? '—'} ms · max {r.latencyMs?.max ?? '—'} ms
            </pre>
          </>
        )}
      </div>

      {!embedded && (
        <div className="modal-actions">
          <button type="button" onClick={onClose}>Close</button>
        </div>
      )}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Functions · <code>{node.id}</code></h2>
          <button className="modal-close" onClick={onClose}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
