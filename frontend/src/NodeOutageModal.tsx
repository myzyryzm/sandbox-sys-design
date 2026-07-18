import { useEffect, useState } from 'react'

/**
 * "Shut down for N seconds" modal. Temporarily stops a node's container so it stops
 * accepting connections (callers get connection-refused / the LB returns 502), then
 * the dev server auto-restarts it when the window elapses — see
 * frontend/server/outage.js. While a node is already down this instead shows the time
 * remaining and a "Bring back now" button.
 *
 * Cancel or a click outside the card dismisses. Renders standalone by default; pass
 * `embedded` to drop the overlay/header and return just the body, so it can live
 * inside the NodeEditModal "Shutdown" tab (`onBusyChange` reports in-flight state up).
 */
export default function NodeOutageModal({ systemId, node, current, onClose, embedded = false, onBusyChange }) {
  const [seconds, setSeconds] = useState(30)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const isDown = !!current

  async function shutDown() {
    const n = Number(seconds)
    if (!Number.isInteger(n) || n < 1 || n > 300) {
      setError('Choose a whole number of seconds between 1 and 300.')
      return
    }
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/outage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: node.id, duration_seconds: n }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose() // the outage poll turns the node orange on the diagram
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
  }

  async function bringBack() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/outage', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: node.id }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
  }

  const body = (
    <>
      {isDown ? (
          <>
            <p className="sim-desc">
              <strong>{node.label}</strong> is stopped — it's refusing all inbound
              connections. It will come back automatically in about{' '}
              <strong>{current.remaining_seconds}s</strong>, or you can restore it now.
            </p>
            {error && <p className="modal-error">{error}</p>}
            <div className="modal-actions">
              <button type="button" onClick={onClose} disabled={busy}>Close</button>
              <button type="button" className="primary" onClick={bringBack} disabled={busy}>
                {busy ? 'Restarting…' : 'Bring back now'}
              </button>
            </div>
          </>
        ) : (
          <>
            <p className="sim-desc">
              Temporarily stops <strong>{node.label}</strong>'s container so it rejects
              all inbound connections (callers see connection-refused / 502 through the
              load balancer). It restarts automatically when the timer runs out.
            </p>
            <div className="form-row">
              <label htmlFor="outage-seconds">Duration (seconds)</label>
              <input
                id="outage-seconds"
                type="number"
                min={1}
                max={300}
                value={seconds}
                disabled={busy}
                onChange={(e) => setSeconds(e.target.value)}
              />
            </div>
            <input
              type="range"
              min={1}
              max={300}
              value={Number(seconds) || 1}
              disabled={busy}
              onChange={(e) => setSeconds(e.target.value)}
              aria-label="Duration in seconds"
            />
            {error && <p className="modal-error">{error}</p>}
            <div className="modal-actions">
              <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
              <button type="button" className="danger" onClick={shutDown} disabled={busy}>
                {busy ? 'Shutting down…' : `Shut down ${seconds || 0}s`}
              </button>
            </div>
          </>
        )}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>{isDown ? `${node.label} is down` : `Shut down ${node.label}?`}</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
