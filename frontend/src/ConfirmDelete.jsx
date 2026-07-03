import { useEffect, useState } from 'react'

/**
 * "Are you sure?" modal for deleting a service or database node. Confirm tears
 * the component down via POST /api/delete (see frontend/server/remove.js);
 * Cancel or a click outside the card dismisses without deleting.
 *
 * Before offering the button it asks GET /api/dependents who still calls/uses this
 * node (HTTP downstream, gRPC target, Kafka produce/consume, client function step).
 * If anything depends on it the delete is BLOCKED — we list those api calls and
 * disable Delete (the backend enforces the same guard). The user must remove those
 * calls first. Read replicas / CDC workers cascade and don't count as dependents.
 *
 * Renders standalone by default; pass `embedded` to drop the overlay/header and
 * return just the body, so it can live inside the NodeEditModal "Delete" tab.
 * `onBusyChange` lets that parent disable tab-switching during the delete.
 */
export default function ConfirmDelete({ systemId, node, onClose, embedded = false, onBusyChange }) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  const [dependents, setDependents] = useState([])
  const [checking, setChecking] = useState(false)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // A client has no container/route/scrape — it's a manifest node + a scenario, torn
  // down through its own endpoint. Everything else goes through the docker-aware
  // /api/delete (remove.js). Nothing depends on a client, so it skips the probe.
  const isClient = node.type === 'client'

  // Probe dependents up front (read-only) so we can warn before the user clicks.
  useEffect(() => {
    if (isClient) return
    let live = true
    setChecking(true)
    fetch(`/api/dependents?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(node.id)}`)
      .then((r) => r.json())
      .then((d) => { if (live) setDependents(Array.isArray(d.dependents) ? d.dependents : []) })
      .catch(() => { if (live) setDependents([]) })
      .finally(() => { if (live) setChecking(false) })
    return () => { live = false }
  }, [systemId, node.id, isClient])

  const blocked = dependents.length > 0

  async function confirm() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch(isClient ? '/api/clients' : '/api/delete', {
        method: isClient ? 'DELETE' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: node.id }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) {
        // A blocked-delete 400 carries the dependent list — surface it like the probe.
        if (Array.isArray(data.dependents) && data.dependents.length) setDependents(data.dependents)
        throw new Error(data.error || `HTTP ${res.status}`)
      }
      onClose() // the manifest/endpoints polls drop the node from the diagram
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
  }

  // Group dependents by the node that depends on us, so each caller is one row.
  const groups = []
  for (const d of dependents) {
    let g = groups.find((x) => x.node === d.node)
    if (!g) { g = { node: d.node, label: d.label || d.node, refs: [] }; groups.push(g) }
    g.refs.push(d)
  }
  const viaLabel = { http: 'HTTP', grpc: 'gRPC', kafka: 'Kafka', scenario: 'function', consumer: 'consumer' }

  const body = (
    <>
      <p className="sim-desc">
        {isClient ? (
          <>This removes <strong>{node.label}</strong> and its scenario from the diagram. This can't be undone.</>
        ) : blocked ? (
          <><strong>{node.label}</strong> can't be deleted — {dependents.length} call
          {dependents.length === 1 ? '' : 's'} still depend on it. Remove the call
          {dependents.length === 1 ? '' : 's'} below first, then delete it.</>
        ) : (
          <>This removes <strong>{node.label}</strong>'s container(s), its load-balancer
          route and Prometheus scrape, and its node on the diagram. This can't be undone.</>
        )}
      </p>

      {checking && <p className="sim-desc">Checking dependencies…</p>}

      {blocked && (
        <ul className="dep-list">
          {groups.map((g) => (
            <li key={g.node}>
              <code>{g.label}</code>
              {g.refs.map((r, i) => (
                <span className="dep-ref" key={i}>
                  <span className="dep-via">{viaLabel[r.via] || r.via}</span>
                  {' '}{r.detail}
                  {r.calls?.length ? <> → <code>{r.calls.join(', ')}</code></> : null}
                </span>
              ))}
            </li>
          ))}
        </ul>
      )}

      {error && <p className="modal-error">{error}</p>}

      <div className="modal-actions">
        <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
        <button type="button" className="danger" onClick={confirm} disabled={busy || blocked || checking}>
          {busy ? 'Deleting…' : 'Delete'}
        </button>
      </div>
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Delete {node.id}?</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
