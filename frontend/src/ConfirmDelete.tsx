import './ConfirmDelete.css'
import { useEffect, useState } from 'react'
import type { Manifest, ManifestNode } from './types/manifest'

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
 * A websocket tier is one unit: a non-lb member (server, bus, presence, pool client)
 * gets no Delete button at all — just a pointer at its L4 load balancer, whose own
 * delete cascades the whole tier (the backend rejects member deletes the same way).
 *
 * Renders standalone by default; pass `embedded` to drop the overlay/header and
 * return just the body, so it can live inside the NodeEditModal "Delete" tab.
 * `onBusyChange` lets that parent disable tab-switching during the delete.
 */

// One row of GET /api/dependents (remove.js findDependents): who still uses this node.
interface DependentRef {
  node: string
  label?: string
  via: string
  detail?: string
  calls?: string[]
}

interface ConfirmDeleteProps {
  systemId: string
  node: ManifestNode
  manifest: Manifest
  onClose: () => void
  embedded?: boolean
  onBusyChange?: (busy: boolean) => void
}

export default function ConfirmDelete({ systemId, node, manifest, onClose, embedded = false, onBusyChange }: ConfirmDeleteProps) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [dependents, setDependents] = useState<DependentRef[]>([])
  const [checking, setChecking] = useState(false)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // A client has no container/route/scrape — it's a manifest node + a scenario, torn
  // down through its own endpoint. Everything else goes through the docker-aware
  // /api/delete (remove.js). Nothing depends on a client, so it skips the probe.
  const isClient = node.type === 'client'
  // Prometheus is a VISUAL toggle: its Delete removes only the diagram node + self-scrape
  // via /api/prom-node; the shared container stays up. Nothing depends on it, so like a
  // client it skips the dependents probe.
  const isPrometheus = node.type === 'prometheus'

  // Websocket tier members (servers, bus, presence, the pool client) are never
  // individually deletable — the whole tier goes away via its lb's cascade.
  const isWsTierMember = node.origin === 'create-websockets' && node.wsRole !== 'lb'
  const isWsLb = node.origin === 'create-websockets' && node.wsRole === 'lb'
  const wsTierChildren = isWsLb
    ? (manifest?.nodes || []).filter((n) => n.wsTier === node.id).map((n) => n.id)
    : []

  // Probe dependents up front (read-only) so we can warn before the user clicks.
  useEffect(() => {
    if (isClient || isPrometheus || isWsTierMember) return
    let live = true
    setChecking(true)
    fetch(`/api/dependents?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(node.id)}`)
      .then((r) => r.json() as Promise<{ dependents?: DependentRef[] }>)
      .then((d) => { if (live) setDependents(Array.isArray(d.dependents) ? d.dependents : []) })
      .catch(() => { if (live) setDependents([]) })
      .finally(() => { if (live) setChecking(false) })
    return () => { live = false }
  }, [systemId, node.id, isClient, isPrometheus, isWsTierMember])

  const blocked = dependents.length > 0

  async function confirm() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch(
        isClient ? '/api/clients' : isPrometheus ? '/api/prom-node' : '/api/delete',
        {
          method: isClient || isPrometheus ? 'DELETE' : 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system: systemId, id: node.id }),
        },
      )
      const data = (await res.json().catch(() => ({}))) as {
        ok?: boolean
        error?: string
        dependents?: DependentRef[]
      }
      if (!res.ok || !data.ok) {
        // A blocked-delete 400 carries the dependent list — surface it like the probe.
        if (Array.isArray(data.dependents) && data.dependents.length) setDependents(data.dependents)
        throw new Error(data.error || `HTTP ${res.status}`)
      }
      onClose() // the manifest/endpoints polls drop the node from the diagram
    } catch (err) {
      setBusy(false)
      setError((err as Error).message)
    }
  }

  // Group dependents by the node that depends on us, so each caller is one row.
  const groups: { node: string; label: string; refs: DependentRef[] }[] = []
  for (const d of dependents) {
    let g = groups.find((x) => x.node === d.node)
    if (!g) { g = { node: d.node, label: d.label || d.node, refs: [] }; groups.push(g) }
    g.refs.push(d)
  }
  const viaLabel: Record<string, string> = { http: 'HTTP', grpc: 'gRPC', kafka: 'Kafka', scenario: 'function', consumer: 'consumer' }

  // A websocket tier member offers no delete at all — just the pointer at its lb
  // (the backend rejects the delete the same way, so this isn't merely cosmetic).
  const body = isWsTierMember ? (
    <>
      <p className="sim-desc">
        <strong>{node.label}</strong> is part of the <strong>{node.wsTier}</strong> websocket
        tier and can't be deleted on its own. The whole websocket process — load balancer,
        servers, bus, presence and client — is deleted in one shot from its L4 load
        balancer: open <code>{node.wsTier}</code> → Delete.
      </p>
      <div className="modal-actions">
        <button type="button" onClick={onClose}>Close</button>
      </div>
    </>
  ) : (
    <>
      <p className="sim-desc">
        {isPrometheus ? (
          <>This removes the <strong>Prometheus</strong> node from the diagram (and its self-scrape).
          The container keeps running — every node's metrics just read <em>“no metrics”</em> until you
          add Prometheus back from <strong>＋ Add</strong>.</>
        ) : isClient ? (
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

      {isWsLb && wsTierChildren.length > 0 && (
        <p className="sim-desc">
          Deleting this load balancer removes the <strong>whole websocket tier</strong>:{' '}
          {wsTierChildren.map((cid, i) => (
            <span key={cid}>{i > 0 && ', '}<code>{cid}</code></span>
          ))}.
        </p>
      )}

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
