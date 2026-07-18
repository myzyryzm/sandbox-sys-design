import { useEffect, useState } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName'

/**
 * Modal for "Add service". Creates either a generic FastAPI service (a clone of the
 * system's backend, with hand-written /health + /metrics) via POST /api/services, or
 * a registered custom service type (e.g. a Download Coordinator) via
 * POST /api/custom-services. Custom types are discovered at runtime from
 * GET /api/custom-types, so adding a new type needs no change here.
 */
export default function CreateService({ systemId, onClose }) {
  const [name, setName] = useState('worker')
  const [type, setType] = useState('generic') // 'generic' | <serviceType>
  const [customTypes, setCustomTypes] = useState([]) // [{ serviceType, displayName, description }]
  const [status, setStatus] = useState('idle') // idle | submitting | error
  const [error, setError] = useState(null)

  const busy = status === 'submitting'
  const nameErr = nodeNameError(name)

  // Discover registered custom service types for the picker.
  useEffect(() => {
    let cancelled = false
    fetch('/api/custom-types')
      .then((r) => r.json())
      .then((d) => { if (!cancelled && d.ok) setCustomTypes(d.types || []) })
      .catch(() => { /* no custom types available — generic only */ })
    return () => { cancelled = true }
  }, [])

  const selected = customTypes.find((t) => t.serviceType === type) || null

  async function submit(e) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      const [url, body] =
        type === 'generic'
          ? ['/api/services', { system: systemId, name: name.trim() }]
          : ['/api/custom-services', { system: systemId, serviceType: type, name: name.trim() }]
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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
          <h2>Add a service</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <form onSubmit={submit}>
          {customTypes.length > 0 && (
            <label className="form-row">
              <span>Type</span>
              <select value={type} onChange={(e) => setType(e.target.value)} disabled={busy}>
                <option value="generic">Generic service</option>
                {customTypes.map((t) => (
                  <option key={t.serviceType} value={t.serviceType}>{t.displayName}</option>
                ))}
              </select>
            </label>
          )}

          <p className="sim-desc">
            {selected ? (
              selected.description
            ) : (
              <>
                Adds a generic FastAPI service — a clone of this system's backend with the
                same hand-instrumented <code>/health</code> and <code>/metrics</code>{' '}
                endpoints. It's scraped and shown on the diagram, but not wired to anything
                yet.
              </>
            )}
          </p>

          <label className="form-row">
            <span>Name</span>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="worker"
              disabled={busy}
            />
          </label>
          {name.trim() && nameErr
            ? <small className="field-error">{nameErr}</small>
            : <small className="form-hint">{NODE_NAME_HINT}</small>}

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
            <button type="submit" className="primary" disabled={busy || !!nameErr}>
              {busy ? 'Building… (can take a minute)' : selected ? `Create ${selected.displayName}` : 'Create service'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
