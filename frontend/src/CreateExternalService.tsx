import { useState, type FormEvent } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName'

interface CreateExternalServiceProps {
  systemId: string
  onClose: () => void
}

/**
 * Modal for "Add external service". Creates a real FastAPI container (the same
 * hand-instrumented template a generic service uses) via POST /api/external-services,
 * but the node is marked external: it's drawn OUTSIDE the system boundary, isn't
 * scraped by Prometheus and has no health check, and can't serve gRPC. It simulates
 * a third-party API that in-system services call out to — you can add HTTP endpoints
 * to it and wrap calls to it in a circuit breaker.
 */
export default function CreateExternalService({ systemId, onClose }: CreateExternalServiceProps) {
  const [name, setName] = useState('payments-api')
  const [status, setStatus] = useState<'idle' | 'submitting' | 'error'>('idle')
  const [error, setError] = useState<string | null>(null)

  const busy = status === 'submitting'
  const nameErr = nodeNameError(name)

  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      const res = await fetch('/api/external-services', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, name: name.trim() }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setStatus('error')
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Add an external service</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <form onSubmit={submit}>
          <p className="sim-desc">
            Adds a third-party dependency that lives <strong>outside</strong> your system —
            it's drawn beyond the boundary, isn't scraped by Prometheus and has no health
            check. Give it HTTP endpoints (its API), have an in-system service call it, and
            wrap that call in a circuit breaker. It can't serve gRPC contracts.
          </p>

          <label className="form-row">
            <span>Name</span>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="payments-api"
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
              {busy ? 'Building… (can take a minute)' : 'Create external service'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
