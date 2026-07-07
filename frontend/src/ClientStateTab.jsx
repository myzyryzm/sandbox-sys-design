import { useEffect, useState } from 'react'

/**
 * "State" tab for a client node. A client is either STATELESS (fire-and-forget — each function
 * run is an independent subprocess that remembers nothing, today's default) or STATEFUL (its
 * function-call outcomes persist across runs in a durable per-client store,
 * clients/<module>.state.json — like a websocket session that accumulates state).
 *
 * This tab is the mode switch + store viewer:
 *   - flip Stateless/Stateful (PATCH /api/clients) — the diagram badge follows on the next
 *     manifest poll;
 *   - show the accumulated store (the state.get/set `values` map + the auto-recorded call
 *     `history`), polled from GET /api/clients/state every 4s so it refreshes after a run;
 *   - Clear it (DELETE /api/clients/state).
 *
 * Toggling OFF leaves the file inert on disk (the runner just stops pointing at it); use Clear to
 * discard it. `state.set(...)` in a stateless run works in-memory for that run only.
 */
export default function ClientStateTab({ systemId, node, onClose, onBusyChange, embedded = false }) {
  const [info, setInfo] = useState(null) // { ok, stateful, state:{ values, history } }
  const [error, setError] = useState(null)

  const setBusy = (v) => onBusyChange?.(v)

  // Poll the client's store + current mode (the backend reads `stateful` off the manifest, so this
  // also reflects a toggle done elsewhere).
  useEffect(() => {
    let live = true
    const load = () =>
      fetch(`/api/clients/state?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(node.id)}`)
        .then((r) => r.json())
        .then((d) => { if (live && d?.ok) setInfo(d) })
        .catch(() => {}) // keep the last good response
    load()
    const t = setInterval(load, 4000)
    return () => { live = false; clearInterval(t) }
  }, [systemId, node.id])

  const stateful = info ? info.stateful : !!node.stateful
  const values = info?.state?.values || {}
  const history = info?.state?.history || []
  const valueKeys = Object.keys(values)
  const hasStore = valueKeys.length > 0 || history.length > 0

  async function setMode(next) {
    if (next === stateful) return
    setBusy(true); setError(null)
    try {
      const res = await fetch('/api/clients', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: node.id, stateful: next }),
      })
      const d = await res.json().catch(() => ({}))
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      setInfo((prev) => (prev ? { ...prev, stateful: next } : { ok: true, stateful: next, state: { values: {}, history: [] } }))
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function clearState() {
    setBusy(true); setError(null)
    try {
      const res = await fetch('/api/clients/state', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: node.id }),
      })
      const d = await res.json().catch(() => ({}))
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      setInfo((prev) => (prev ? { ...prev, state: { values: {}, history: [] } } : prev))
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  const fmtTime = (at) => {
    if (typeof at !== 'number') return ''
    try { return new Date(at * 1000).toLocaleTimeString() } catch { return '' }
  }
  const fmtVal = (v) => (typeof v === 'string' ? v : JSON.stringify(v))

  const body = (
    <>
      <p className="sim-desc">
        <strong>{node.label}</strong> is{' '}
        <strong>{stateful ? 'stateful' : 'stateless'}</strong>.{' '}
        {stateful
          ? <>Its function-call outcomes persist across runs in <code>clients/{node.id}.state.json</code> — a <code>state.get/set</code> store plus an auto-recorded call history.</>
          : <>Fire-and-forget: each run is independent and remembers nothing. <code>state.set(...)</code> in a function works in-memory for that run only.</>}
      </p>

      <div className="form-section">
        <div>
          <label className="dc-radio">
            <input type="radio" name="client-state-mode" checked={!stateful} onChange={() => setMode(false)} /> Stateless
          </label>
          <label className="dc-radio">
            <input type="radio" name="client-state-mode" checked={stateful} onChange={() => setMode(true)} /> Stateful
          </label>
        </div>
      </div>

      <div className="scenario-results">
        <div className="scenario-results-head">Stored values</div>
        {valueKeys.length === 0 ? (
          <p className="sim-desc">
            {stateful ? 'Nothing stored yet — run a function that calls state.set(...).' : 'No stored state.'}
            {!stateful && hasStore && ' (inactive — this client is stateless; the file is kept until you Clear it)'}
          </p>
        ) : (
          <pre className="scenario-result-body">
            {valueKeys.map((k) => `${k} = ${fmtVal(values[k])}`).join('\n')}
          </pre>
        )}
      </div>

      <div className="scenario-results">
        <div className="scenario-results-head">Call history ({history.length})</div>
        {history.length === 0 ? (
          <p className="sim-desc">No calls recorded yet.</p>
        ) : (
          <pre className="scenario-result-body">
            {history
              .slice(-20)
              .reverse()
              .map((h) => `${h.ok ? '✓' : '✕'} ${h.method} ${h.path} · ${h.status}${h.at ? ` · ${fmtTime(h.at)}` : ''}`)
              .join('\n')}
          </pre>
        )}
      </div>

      {error && <p className="modal-error">{error}</p>}

      <div className="modal-actions">
        <button type="button" onClick={clearState} disabled={!hasStore}>Clear state</button>
        {!embedded && <button type="button" onClick={onClose}>Close</button>}
      </div>
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>State · <code>{node.id}</code></h2>
          <button className="modal-close" onClick={onClose}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
