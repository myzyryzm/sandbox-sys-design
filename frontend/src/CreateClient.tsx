import { useState } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName'

/**
 * Modal for "Add client". A client is a caller that lives OUTSIDE the system (drawn
 * to the left of the boundary) and runs multi-step API call chains against it — call
 * an internal service, take a field from the response, use it to call another service
 * or external service. It has no container (the calls are issued by the dev-server
 * through the real load balancer), so creating one is instant: POST /api/clients just
 * appends a `client` node to the manifest.
 */
export default function CreateClient({ systemId, onClose }) {
  const [name, setName] = useState('mobile-app')
  const [stateful, setStateful] = useState(false) // false = stateless (fire-and-forget, default)
  const [status, setStatus] = useState('idle') // idle | submitting | error
  const [error, setError] = useState(null)

  const busy = status === 'submitting'
  const nameErr = nodeNameError(name)

  async function submit(e) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      const res = await fetch('/api/clients', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, name: name.trim(), stateful }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setStatus('error')
      setError(err.message)
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Add a client</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <form onSubmit={submit}>
          <p className="sim-desc">
            A caller that lives <strong>outside</strong> your system (drawn to the left of
            the boundary, connected to the load balancer). Give it a <strong>scenario</strong>{' '}
            — an ordered list of API calls — and <strong>Run</strong> it: the calls fire for
            real through the load balancer, and each response can be fed into the next step
            (e.g. call an internal service, then call an external service with the id it
            returned). No container is created.
          </p>

          <label className="form-row">
            <span>Name</span>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="mobile-app"
              disabled={busy}
            />
          </label>
          {name.trim() && nameErr
            ? <small className="field-error">{nameErr}</small>
            : <small className="form-hint">{NODE_NAME_HINT}</small>}

          <label className="form-row">
            <span>Mode</span>
            <span>
              <label className="dc-radio">
                <input
                  type="radio"
                  name="client-mode"
                  checked={!stateful}
                  onChange={() => setStateful(false)}
                  disabled={busy}
                /> Stateless
              </label>
              <label className="dc-radio">
                <input
                  type="radio"
                  name="client-mode"
                  checked={stateful}
                  onChange={() => setStateful(true)}
                  disabled={busy}
                /> Stateful
              </label>
            </span>
          </label>
          <small className="form-hint">
            {stateful
              ? 'Stateful: outcomes of API calls persist across runs in clients/<module>.state.json (a state.get/set store + auto call history) — like a websocket session. Set it later on the client’s State tab.'
              : 'Stateless: fire-and-forget — each run is independent and remembers nothing (today’s behavior). You can switch to Stateful later on the client’s State tab.'}
          </small>

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
            <button type="submit" className="primary" disabled={busy || !!nameErr}>
              {busy ? 'Adding…' : 'Create client'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
