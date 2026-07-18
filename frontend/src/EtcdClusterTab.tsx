import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * The etcd node's "Cluster" tab (embedded in NodeEditModal). Two sections:
 *
 *   1. Cluster config — size (3/5/7) + the Raft timing knobs (heartbeat interval,
 *      election timeout) + the lease TTL, with the quorum math derived LIVE from the
 *      form's size so the tradeoff of bumping N is visible before applying. A
 *      TTL-only save is a pure etcd.json write (registration loops re-read it by
 *      mtime — instant, no rebuild); changing size or a Raft knob RECREATES the
 *      cluster (fresh bootstrap; leased registrations re-establish automatically).
 *
 *   2. Members — one row per member with live health (via `etcdctl endpoint status`
 *      inside each container) and a leader star, plus Stop/Start buttons. Stopping
 *      ⌈N/2⌉ members loses quorum: writes fail, the node goes red — the Raft demo.
 */

const SIZES = [3, 5, 7]
const ELECTION_FACTOR = 5 // keep in sync with the backend's validation

export default function EtcdClusterTab({ systemId, node, onClose, embedded = false, onBusyChange }) {
  const id = node.id
  const [info, setInfo] = useState(null) // GET /api/etcd response (cluster + memberStatus)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [form, setForm] = useState(null) // { size, heartbeatMs, electionMs, leaseTtlSeconds }
  const seededForm = useRef(false)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // Fast registry-only paint, then the live member probe fills memberStatus in;
  // keep re-probing so a stopped member's dot flips without closing the modal.
  const load = useCallback(async (live) => {
    try {
      const res = await fetch(
        `/api/etcd?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(id)}&live=${live ? 1 : 0}`,
      )
      const data = await res.json()
      if (!data.ok) throw new Error(data.error || 'failed to load')
      setInfo((prev) => (live ? data : { ...data, memberStatus: prev?.memberStatus || null }))
      if (!seededForm.current) {
        seededForm.current = true
        const c = data.cluster
        setForm({
          size: c.size,
          heartbeatMs: c.heartbeatMs ?? 100,
          electionMs: c.electionMs ?? 1000,
          leaseTtlSeconds: c.leaseTtlSeconds ?? 15,
        })
      }
      return data
    } catch (err) {
      setError(err.message)
      return null
    }
  }, [systemId, id])

  useEffect(() => {
    let cancelled = false
    load(false).then(() => { if (!cancelled) load(true) })
    const t = setInterval(() => { if (!document.hidden) load(true) }, 5000)
    return () => {
      cancelled = true
      clearInterval(t)
    }
  }, [load])

  if (!info || !form) {
    return <p className="sim-desc">{error ? `Error: ${error}` : 'Loading…'}</p>
  }

  const cluster = info.cluster
  const size = Number(form.size)
  const quorum = Math.floor(size / 2) + 1
  const hb = Number(form.heartbeatMs)
  const el = Number(form.electionMs)
  const timingWarn = hb > 0 && el > 0 && el < ELECTION_FACTOR * hb
  const raftChanged =
    size !== cluster.size || hb !== cluster.heartbeatMs || el !== cluster.electionMs
  const ttlChanged = Number(form.leaseTtlSeconds) !== cluster.leaseTtlSeconds
  const dirty = raftChanged || ttlChanged

  const setField = (k) => (e) => setForm((f) => ({ ...f, [k]: e.target.value }))

  async function apply() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          id,
          size,
          heartbeatMs: hb,
          electionMs: el,
          leaseTtlSeconds: Number(form.leaseTtlSeconds),
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      seededForm.current = false
      await load(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function memberAction(member, action) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/member', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id, member, action }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  const statusOf = Object.fromEntries((info.memberStatus || []).map((m) => [m.id, m]))
  const upCount = (info.memberStatus || []).filter((m) => m.healthy).length

  const body = (
    <>
      <p className="sim-desc">
        A real {cluster.size}-member etcd (Raft) cluster. Quorum = ⌊N/2⌋+1: every write needs{' '}
        {cluster.quorum} acks, and the cluster survives {cluster.tolerates} member failure
        {cluster.tolerates === 1 ? '' : 's'}.
      </p>

      {/* ---- Cluster config ---- */}
      <div className="form-section">
        <div className="form-section-head"><span>Cluster config</span></div>

        <label className="form-row">
          <span>Cluster size</span>
          <select value={size} onChange={setField('size')} disabled={busy}>
            {SIZES.map((n) => <option key={n} value={n}>{n} members</option>)}
          </select>
        </label>
        <small className="form-hint">
          {size} nodes → quorum {quorum} · tolerates {size - quorum} failure{size - quorum === 1 ? '' : 's'} —
          bigger survives more, but every write waits on more acks.
        </small>

        <label className="form-row">
          <span>Heartbeat (ms)</span>
          <input type="number" min="10" max="10000" value={form.heartbeatMs}
            onChange={setField('heartbeatMs')} disabled={busy} />
        </label>
        <label className="form-row">
          <span>Election timeout (ms)</span>
          <input type="number" min="50" max="50000" value={form.electionMs}
            onChange={setField('electionMs')} disabled={busy} />
        </label>
        {timingWarn && (
          <small className="field-error">
            Election timeout should be at least {ELECTION_FACTOR}× the heartbeat ({ELECTION_FACTOR * hb} ms)
            or followers start spurious elections.
          </small>
        )}

        <label className="form-row">
          <span>Lease TTL (s)</span>
          <input type="number" min="2" max="3600" value={form.leaseTtlSeconds}
            onChange={setField('leaseTtlSeconds')} disabled={busy} />
        </label>
        <small className="form-hint">
          How long a worker registration outlives its last keepalive. Applied live — registration
          loops re-read etcd.json and re-grant their lease, no rebuild.
        </small>

        {raftChanged && (
          <p className="sim-desc">
            ⚠ Changing size / Raft timing <strong>recreates the cluster</strong> (fresh bootstrap,
            new leader election). Keys are wiped, but leased registrations re-put themselves on
            reconnect — watchers see a brief empty-then-repopulate.
          </p>
        )}

        <div className="modal-actions">
          <button
            type="button"
            className="primary"
            onClick={apply}
            disabled={busy || !dirty || timingWarn}
          >
            {busy ? 'Working…' : raftChanged ? 'Apply (recreates cluster)' : 'Apply'}
          </button>
        </div>
      </div>

      {/* ---- Members ---- */}
      <div className="form-section">
        <div className="form-section-head">
          <span>Members {info.memberStatus ? `(${upCount}/${cluster.members.length} up)` : ''}</span>
        </div>
        <ul className="endpoint-list">
          {cluster.members.map((m) => {
            const st = statusOf[m]
            const state = !info.memberStatus ? '…' : st?.healthy ? (st.isLeader ? '● leader' : '● up') : '○ down'
            return (
              <li key={m} className="endpoint-list-row">
                <code className="endpoint-alias">{m}</code>
                <span className="endpoint-list-path">{state}</span>
                <span className="endpoint-list-actions">
                  {st?.healthy !== false ? (
                    <button className="link-danger" disabled={busy || !info.memberStatus}
                      onClick={() => memberAction(m, 'stop')}>Stop</button>
                  ) : (
                    <button className="link" disabled={busy}
                      onClick={() => memberAction(m, 'start')}>Start</button>
                  )}
                </span>
              </li>
            )
          })}
        </ul>
        <small className="form-hint">
          Stop {cluster.tolerates + 1} of {cluster.members.length} members to lose quorum — writes
          (and registrations) fail and the node turns red until you start one back up. Stopping the
          leader triggers a live re-election (watch the ⭘ ring move on the diagram).
        </small>
      </div>

      {error && <p className="modal-error">{error}</p>}

      {!embedded && (
        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={busy}>Close</button>
        </div>
      )}
    </>
  )

  if (embedded) return body
  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Cluster · <code>{id}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
