import { useEffect, useState } from 'react'

/**
 * "Test" modal — simulate things against the running system. Today it has one
 * simulation, "Generate load", which hammers a chosen endpoint (method + path)
 * through the LB via /api/test/load (see frontend/server/simulate.js). The
 * endpoint list comes from /api/endpoints. Built as a list so more simulations
 * can slot in later.
 */

const FALLBACK = { service: 'service-1', method: 'GET', path: '/service-1/health' }

export default function TestPanel({ systemId, onClose }) {
  const [rps, setRps] = useState(20)
  const [endpoints, setEndpoints] = useState([])
  const [target, setTarget] = useState(0) // index into options
  const [load, setLoad] = useState({ running: false })
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  const options = endpoints.length ? endpoints : [FALLBACK]

  // Load the endpoint catalog and any load already running when the modal opens.
  useEffect(() => {
    let cancelled = false
    Promise.all([
      fetch(`/api/endpoints?system=${encodeURIComponent(systemId)}`).then((r) => r.json()).catch(() => ({})),
      fetch(`/api/test/load?system=${encodeURIComponent(systemId)}`).then((r) => r.json()).catch(() => ({})),
    ]).then(([eps, st]) => {
      if (cancelled) return
      const list = eps.ok && eps.endpoints.length ? eps.endpoints : [FALLBACK]
      setEndpoints(eps.ok ? eps.endpoints : [])
      if (st.ok) {
        setLoad(st)
        if (st.running) {
          const i = list.findIndex((e) => e.method === st.method && e.path === st.path)
          if (i >= 0) setTarget(i)
        }
      }
    })
    return () => { cancelled = true }
  }, [systemId])

  async function send(action) {
    setBusy(true)
    setError(null)
    const sel = options[target] || FALLBACK
    try {
      const res = await fetch('/api/test/load', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          action,
          rps: Number(rps),
          method: sel.method,
          path: sel.path,
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setLoad(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Test the system</h2>
          <button className="modal-close" onClick={onClose}>✕</button>
        </header>

        <div className="sim">
          <div className="sim-head">
            <span className="sim-title">Generate load</span>
            <span className={`sim-status ${load.running ? 'on' : 'off'}`}>
              {load.running ? `running · ${load.method} ${load.path} · ~${load.rps}/s` : 'stopped'}
            </span>
          </div>
          <p className="sim-desc">
            Hammers a specific endpoint through the load balancer so its metrics
            move on the diagram.
          </p>
          <div className="sim-controls">
            <label className="sim-target">
              <span>endpoint</span>
              <select value={target} onChange={(e) => setTarget(Number(e.target.value))} disabled={busy}>
                {options.map((e, i) => (
                  <option key={`${e.method} ${e.path}`} value={i}>{e.method} {e.path}</option>
                ))}
              </select>
            </label>
            <label>
              <span>rate</span>
              <input
                type="number"
                min="1"
                max="500"
                value={rps}
                onChange={(e) => setRps(e.target.value)}
                disabled={busy}
              />
              <span>req/s</span>
            </label>
            {load.running ? (
              <button className="danger" onClick={() => send('stop')} disabled={busy}>Stop</button>
            ) : (
              <button className="primary" onClick={() => send('start')} disabled={busy}>Start load</button>
            )}
          </div>
        </div>

        {error && <p className="modal-error">{error}</p>}
      </div>
    </div>
  )
}
