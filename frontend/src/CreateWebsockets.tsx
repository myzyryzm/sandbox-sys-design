import { useEffect, useState, type FormEvent } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName'

/**
 * Modal for "Add WebSockets". Provisions a complete websocket tier in one mechanical
 * POST /api/websockets (see frontend/server/websockets.js): an haproxy L4 (tcp) load
 * balancer, N node.js `ws` relay servers, a redis pub/sub bus + a redis presence
 * cache (each with an exporter), and a container-less websocket client whose pool
 * script runs on the host. One tier per system today — the form disables itself if
 * the system already has one.
 *
 * The pub/sub and presence selectors are dropdowns with only "redis" today, built to
 * grow as more engines are added (same pattern as the event-stream Type control).
 */

const ALGORITHMS = [
  { value: 'leastconn', label: 'Least connections (default)' },
  { value: 'roundrobin', label: 'Round robin' },
  { value: 'source', label: 'Source hash' },
]

interface CreateWebsocketsProps {
  systemId: string
  onClose: () => void
}

export default function CreateWebsockets({ systemId, onClose }: CreateWebsocketsProps) {
  const [name, setName] = useState('ws')
  // Holds the raw input text while editing (coerced with Number() on use).
  const [servers, setServers] = useState<number | string>(2)
  const [algorithm, setAlgorithm] = useState('leastconn')
  const [bus, setBus] = useState('redis')
  const [presence, setPresence] = useState('redis')
  // Tier registry when one already exists.
  const [existing, setExisting] = useState<{ lb: string } | null>(null)
  const [status, setStatus] = useState<'idle' | 'submitting' | 'error'>('idle')
  const [error, setError] = useState<string | null>(null)

  const busy = status === 'submitting'
  const nameErr =
    nodeNameError(name) ||
    (name.trim().length > 20 ? 'Name is too long (20 characters max — it prefixes every node id)' : null)
  const serversErr =
    !Number.isInteger(Number(servers)) || Number(servers) < 1 || Number(servers) > 8
      ? 'Between 1 and 8 servers'
      : null

  useEffect(() => {
    fetch(`/api/websockets?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json() as Promise<{ ok?: boolean; tier?: { lb: string } | null }>)
      .then((d) => {
        if (d.ok && d.tier) setExisting(d.tier)
      })
      .catch(() => {})
  }, [systemId])

  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      const res = await fetch('/api/websockets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          name: name.trim(),
          servers: Number(servers),
          algorithm,
          bus,
          presence,
        }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setStatus('error')
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const base = name.trim() || 'ws'
  const idsHint = `${base}-lb · ${base}-server-1…${Number(servers) || 2} · ${base}-bus · ${base}-presence · ${base}-client`

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Add WebSockets</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        {existing ? (
          <div>
            <p className="sim-desc">
              This system already has a websocket tier (<code>{existing.lb}</code>).
              Delete its load balancer node to remove the whole tier before adding a new one.
            </p>
            <div className="modal-actions">
              <button type="button" onClick={onClose}>Close</button>
            </div>
          </div>
        ) : (
          <form onSubmit={submit}>
            <p className="sim-desc">
              Provisions an L4 (tcp) haproxy load balancer, the websocket servers behind it,
              a redis pub/sub bus for cross-server message routing, and a redis presence
              cache mapping connected clients to their server — plus a websocket client
              whose pool script runs on the host (drive it from end-to-end tests with a
              configurable client count).
            </p>

            <label className="form-row">
              <span>Name</span>
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="ws"
                disabled={busy}
              />
            </label>
            {name.trim() && nameErr
              ? <small className="field-error">{nameErr}</small>
              : <small className="form-hint">{NODE_NAME_HINT} Creates: {idsHint}</small>}

            <label className="form-row">
              <span>Servers</span>
              <input
                type="number"
                min={1}
                max={8}
                value={servers}
                onChange={(e) => setServers(e.target.value)}
                disabled={busy}
              />
            </label>
            {serversErr && <small className="field-error">{serversErr}</small>}

            <label className="form-row">
              <span>LB algorithm</span>
              <select value={algorithm} onChange={(e) => setAlgorithm(e.target.value)} disabled={busy}>
                {ALGORITHMS.map((a) => (
                  <option key={a.value} value={a.value}>{a.label}</option>
                ))}
              </select>
            </label>

            <label className="form-row">
              <span>Pub/Sub</span>
              <select value={bus} onChange={(e) => setBus(e.target.value)} disabled={busy}>
                <option value="redis">Redis (only choice today)</option>
              </select>
            </label>

            <label className="form-row">
              <span>Presence store</span>
              <select value={presence} onChange={(e) => setPresence(e.target.value)} disabled={busy}>
                <option value="redis">Redis (only choice today)</option>
              </select>
            </label>

            {error && <p className="modal-error">{error}</p>}

            <div className="modal-actions">
              <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
              <button type="submit" className="primary" disabled={busy || !!nameErr || !!serversErr}>
                {busy ? 'Provisioning… (npm install in the server builds can take a minute)' : 'Create websocket tier'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  )
}
