import { useEffect, useState } from 'react'

/**
 * A service's "Load Balancing" tab (embedded in NodeEditModal). It puts a per-service
 * load balancer in front of the service: the service runs as N real instances behind an
 * haproxy sidecar that keeps the service's own network name, so every existing caller
 * balances with no code changes (see frontend/server/serviceLb.js).
 *
 * Pick the number of instances (1 = plain single service, no load balancer) and the
 * balancing algorithm. Submitting is a mechanical POST /api/service-lb — no launched
 * Claude session — that enables, scales, re-balances, or disables in one docker rebuild.
 *
 * The tab reads the current state off the node: a load-balanced service is the cluster
 * ENTRY (`type:'service-lb'`, `svcLb:{algorithm, instances}`); a plain service has
 * neither. It still owns its endpoints/gRPC under its `<name>` id either way.
 */

const ALGORITHMS = [
  { value: 'roundrobin', label: 'Round robin (default)' },
  { value: 'leastconn', label: 'Least connections' },
  { value: 'source', label: 'Source hash (sticky by client IP)' },
]

const MAX_INSTANCES = 8

export default function ServiceLbTab({ systemId, node, onClose, embedded = false, onBusyChange }) {
  const svcLb = node.type === 'service-lb' ? node.svcLb : null
  const currentCount = svcLb?.instances?.length || 1
  const currentAlgorithm = svcLb?.algorithm || 'roundrobin'

  const [instances, setInstances] = useState(currentCount)
  const [algorithm, setAlgorithm] = useState(currentAlgorithm)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const n = Number(instances)
  const instancesErr =
    !Number.isInteger(n) || n < 1 || n > MAX_INSTANCES ? `Between 1 and ${MAX_INSTANCES} instances` : null
  const unchanged = n === currentCount && algorithm === currentAlgorithm
  const willDisable = svcLb && n <= 1
  const willEnable = !svcLb && n >= 2

  async function submit() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/service-lb', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, service: node.id, instances: n, algorithm }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  const base = node.id
  const idsHint =
    n >= 2 ? `${base} (haproxy sidecar) · ${base}-1…${n}` : `${base} (single service, no load balancer)`

  const body = (
    <div className="modal-body">
      <p className="sim-desc">
        {svcLb ? (
          <>
            <code>{base}</code> is load balanced across <strong>{currentCount}</strong>{' '}
            instance{currentCount === 1 ? '' : 's'} (<code>{currentAlgorithm}</code>). Callers reach it
            transparently through its haproxy sidecar — no code changes.
          </>
        ) : (
          <>
            Run <code>{base}</code> as multiple instances behind its own load-balancer sidecar. The sidecar
            keeps the <code>{base}</code> network name, so every existing caller balances automatically. The
            service keeps its endpoints and gRPC under <code>{base}</code>.
          </>
        )}
      </p>

      <label className="form-row">
        <span>Instances</span>
        <input
          type="number"
          min={1}
          max={MAX_INSTANCES}
          value={instances}
          onChange={(e) => setInstances(e.target.value)}
          disabled={busy}
        />
      </label>
      {instancesErr ? (
        <small className="field-error">{instancesErr}</small>
      ) : (
        <small className="form-hint">1 = a single service (no load balancer). Creates: {idsHint}</small>
      )}

      <label className="form-row">
        <span>LB algorithm</span>
        <select value={algorithm} onChange={(e) => setAlgorithm(e.target.value)} disabled={busy || n <= 1}>
          {ALGORITHMS.map((a) => (
            <option key={a.value} value={a.value}>{a.label}</option>
          ))}
        </select>
      </label>

      {willEnable && (
        <p className="form-hint">
          Enabling swaps <code>{base}</code> for an haproxy sidecar and starts {n} instances. Circuit
          breakers / resilience attached to <code>{base}</code> stay a single cluster-level policy.
        </p>
      )}
      {willDisable && (
        <p className="form-hint">
          Disabling removes the sidecar and extra instances — <code>{base}</code> goes back to a single
          container. Endpoints are untouched.
        </p>
      )}

      {error && <p className="modal-error">{error}</p>}

      <div className="modal-actions">
        <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
        <button
          type="button"
          className="primary"
          onClick={submit}
          disabled={busy || !!instancesErr || unchanged}
        >
          {busy
            ? 'Applying… (building instance images can take a minute)'
            : willDisable
              ? 'Disable load balancing'
              : willEnable
                ? 'Enable load balancing'
                : 'Apply'}
        </button>
      </div>
    </div>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Load Balancing · <code>{base}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
