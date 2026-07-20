import { useState } from 'react'
import type { FormEvent } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName'

/**
 * Modal for "Add etcd". Provisions a real N-member etcd Raft cluster (N odd, one
 * container per member, no host ports) via POST /api/etcd (frontend/server/etcd.js).
 * Only one etcd setup may exist per system — the Add menu hides the option while a
 * cluster is on the diagram, and the backend 409s a second create.
 *
 * The size selector derives the quorum math live (quorum = ⌊N/2⌋+1) so the
 * fault-tolerance / write-cost tradeoff of bumping N is visible before creating.
 */

const SIZES = [3, 5, 7]
const ELECTION_FACTOR = 5 // keep in sync with the backend's validation

interface CreateEtcdProps {
  systemId: string
  onClose: () => void
}

export default function CreateEtcd({ systemId, onClose }: CreateEtcdProps) {
  const [name, setName] = useState('etcd')
  const [size, setSize] = useState(3)
  const [heartbeatMs, setHeartbeatMs] = useState<number | string>(100)
  const [electionMs, setElectionMs] = useState<number | string>(1000)
  const [leaseTtlSeconds, setLeaseTtlSeconds] = useState<number | string>(15)
  const [status, setStatus] = useState<'idle' | 'submitting' | 'error'>('idle')
  const [error, setError] = useState<string | null>(null)

  const busy = status === 'submitting'
  const nameErr = nodeNameError(name)
  const quorum = Math.floor(size / 2) + 1
  const hb = Number(heartbeatMs)
  const el = Number(electionMs)
  const timingWarn = hb > 0 && el > 0 && el < ELECTION_FACTOR * hb

  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      const res = await fetch('/api/etcd', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          name: name.trim(),
          size,
          heartbeatMs: hb,
          electionMs: el,
          leaseTtlSeconds: Number(leaseTtlSeconds),
        }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setStatus('error')
      setError((err as Error).message)
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Add etcd</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <form onSubmit={submit}>
          <p className="sim-desc">
            Provisions a real {size}-member etcd cluster (one container per member,
            scraped natively by Prometheus). Services then register workers under{' '}
            <code>/services/&lt;service&gt;/</code> as leased keys and other services
            watch those keyspaces (with the <code>sandbox-etcd</code> skill).
          </p>

          <label className="form-row">
            <span>Name</span>
            <input value={name} onChange={(e) => setName(e.target.value)} placeholder="etcd" disabled={busy} />
          </label>
          {name.trim() && nameErr
            ? <small className="field-error">{nameErr}</small>
            : <small className="form-hint">{NODE_NAME_HINT}</small>}

          <label className="form-row">
            <span>Cluster size</span>
            <select value={size} onChange={(e) => setSize(Number(e.target.value))} disabled={busy}>
              {SIZES.map((n) => <option key={n} value={n}>{n} members</option>)}
            </select>
          </label>
          <small className="form-hint">
            {size} nodes → quorum {quorum} · tolerates {size - quorum} failure{size - quorum === 1 ? '' : 's'} —
            every write needs {quorum} acks.
          </small>

          <label className="form-row">
            <span>Heartbeat (ms)</span>
            <input type="number" min="10" max="10000" value={heartbeatMs}
              onChange={(e) => setHeartbeatMs(e.target.value)} disabled={busy} />
          </label>
          <label className="form-row">
            <span>Election timeout (ms)</span>
            <input type="number" min="50" max="50000" value={electionMs}
              onChange={(e) => setElectionMs(e.target.value)} disabled={busy} />
          </label>
          {timingWarn
            ? <small className="field-error">
                Election timeout should be at least {ELECTION_FACTOR}× the heartbeat ({ELECTION_FACTOR * hb} ms)
                or followers start spurious elections.
              </small>
            : <small className="form-hint">
                Heartbeat = how often the leader pings followers; election timeout = how long a
                follower waits before starting an election. Lower = faster failover, twitchier.
              </small>}

          <label className="form-row">
            <span>Lease TTL (s)</span>
            <input type="number" min="2" max="3600" value={leaseTtlSeconds}
              onChange={(e) => setLeaseTtlSeconds(e.target.value)} disabled={busy} />
          </label>
          <small className="form-hint">
            How long a worker registration outlives its last keepalive before etcd expires the
            key (and watchers see it vanish). Editable live later — no rebuild.
          </small>

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
            <button type="submit" className="primary" disabled={busy || !!nameErr || timingWarn}>
              {busy ? 'Provisioning… (pulling images can take a minute)' : 'Create etcd cluster'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
