import './PgTopologyTab.css'
import { useCallback, useEffect, useRef, useState } from 'react'
import { queryVector } from './prometheus'
import { buildPgTopologyRetrofitPrompt } from './pgTopologyPrompts'
import type { ManifestNode, PostgresHaBlock } from './types/manifest'
import type { LaunchSession } from './types/customTypes'

interface PgReplica {
  id: string
  ordinal: number
  replication?: 'sync' | 'async'
}

// GET/POST /api/postgres/topology response.
interface PgTopoState {
  ok?: boolean
  error?: string
  mode: 'standalone' | 'replicated'
  replicas: PgReplica[]
  ha?: PostgresHaBlock | null
  limits?: { replicasMin: number; replicasMax: number; commitLevels?: string[] }
  services?: string[]
  node?: ManifestNode | null
  dsn?: { readWrite?: string; readOnly?: string }
  warnings?: string[]
}

// Per-member live role read from the watcher's pg_ha_* series.
interface PgLiveMember {
  up?: boolean
  primary?: boolean
  fenced?: boolean
  lag?: number
}

/**
 * A postgres node's "Topology" tab (create-database primaries only). Reconciles the node
 * between two REAL container shapes via POST /api/postgres/topology:
 *
 *   - Standalone — the single container "Add database" created.
 *   - Replicated — N streaming standbys, each async or SYNCHRONOUS (a real
 *     `synchronous_standby_names = ANY k (…)` quorum), plus a `<db>-failover` watcher: the
 *     postgres analog of redis Sentinel. It promotes the most caught-up standby when the
 *     primary dies, repoints the survivors, and fences a returning stale primary.
 *
 * The backend does the mechanical work (containers/scrape/manifest/watcher). Applying then
 * enqueues a Claude session (sandbox-postgres-topology skill) that retrofits the attached
 * services to a multi-host DSN — the one code change a postgres failover actually needs.
 *
 * ROLES ARE LIVE, MEMBERSHIP IS MANIFEST: after a failover the primary is a `<db>-<n>`
 * container while `<db>` is still the manifest's cluster entry. So the "Live cluster" panel
 * below reads the watcher's pg_ha_* series from Prometheus rather than trusting the
 * manifest — that is also what powers the Promote / Rejoin actions.
 */
interface PgTopologyTabProps {
  systemId: string
  node: ManifestNode
  onClose: () => void
  onLaunch?: LaunchSession
  embedded?: boolean
  onBusyChange?: (busy: boolean) => void
}

export default function PgTopologyTab({ systemId, node, onClose, onLaunch, embedded = false, onBusyChange }: PgTopologyTabProps) {
  const dbId = node.id
  const [topo, setTopo] = useState<PgTopoState | null>(null)
  const [live, setLive] = useState<Record<string, PgLiveMember> | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null) // 'apply' | `promote:<id>` | `rejoin:<id>` | null
  const [result, setResult] = useState<{ mode: string; warnings: string[] } | null>(null)

  // Form state, seeded from the live topology on first load only (edits survive polls).
  const seeded = useRef(false)
  const [mode, setMode] = useState<'standalone' | 'replicated'>('standalone')
  const [replicas, setReplicas] = useState(2)
  const [syncOrdinals, setSyncOrdinals] = useState<number[]>([])
  const [quorum, setQuorum] = useState(1)
  const [commitLevel, setCommitLevel] = useState('on')
  const [foEnabled, setFoEnabled] = useState(true)
  const [autoDegrade, setAutoDegrade] = useState(true)
  const [downAfterMs, setDownAfterMs] = useState(5000)

  useEffect(() => onBusyChange?.(!!busy), [busy, onBusyChange])

  const load = useCallback(async () => {
    try {
      const res = await fetch(
        `/api/postgres/topology?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(dbId)}`,
      )
      const data = (await res.json()) as PgTopoState
      if (!data.ok) throw new Error(data.error || 'failed to load')
      setTopo(data)
      if (!seeded.current) {
        seeded.current = true
        setMode(data.mode)
        if (data.replicas?.length) setReplicas(data.replicas.length)
        if (data.ha) {
          setSyncOrdinals(data.replicas.filter((r) => r.replication === 'sync').map((r) => r.ordinal))
          setQuorum(data.ha.sync?.quorum ?? 1)
          setCommitLevel(data.ha.sync?.commitLevel || 'on')
          setFoEnabled(data.ha.enabled !== false)
          setAutoDegrade(data.ha.autoDegrade !== false)
          setDownAfterMs(data.ha.downAfterMs ?? 5000)
        }
      }
      // Live roles come from the watcher's own series — the exporter cannot tell you who
      // is primary, and after a failover the manifest cannot either.
      if (data.ha) {
        const job = data.ha.watcher || `${dbId}-failover`
        const [ups, prim, fenced, lags] = await Promise.all([
          queryVector('/api/prometheus', `pg_ha_member_up{job="${job}"}`),
          queryVector('/api/prometheus', `pg_ha_is_primary{job="${job}"}`),
          queryVector('/api/prometheus', `pg_ha_is_fenced{job="${job}"}`),
          queryVector('/api/prometheus', `pg_ha_replay_lag_seconds{job="${job}"}`),
        ])
        const m: Record<string, PgLiveMember> = {}
        for (const s of ups) if (s.labels.member) m[s.labels.member] = { up: s.value === 1 }
        for (const s of prim) if (m[s.labels.member]) m[s.labels.member].primary = s.value === 1
        for (const s of fenced) if (m[s.labels.member]) m[s.labels.member].fenced = s.value === 1
        for (const s of lags) if (m[s.labels.member]) m[s.labels.member].lag = s.value
        setLive(m)
      } else {
        setLive(null)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }, [systemId, dbId])

  useEffect(() => {
    load()
    const t = setInterval(() => { if (!document.hidden) load() }, 5000)
    return () => clearInterval(t)
  }, [load])

  if (!topo) {
    return <p className="sim-desc">{error ? `Error: ${error}` : 'Loading…'}</p>
  }

  const limits = topo.limits || { replicasMin: 1, replicasMax: 4 }
  const attached = (topo.services || []).length
  const noChange =
    (mode === 'standalone' && topo.mode === 'standalone') ||
    (mode === 'replicated' && topo.mode === 'replicated' &&
      topo.replicas.length === replicas &&
      sameSet(topo.replicas.filter((r) => r.replication === 'sync').map((r) => r.ordinal), syncOrdinals) &&
      (topo.ha?.sync?.quorum ?? 1) === quorum &&
      (topo.ha?.sync?.commitLevel || 'on') === commitLevel &&
      (topo.ha?.enabled !== false) === foEnabled &&
      (topo.ha?.autoDegrade !== false) === autoDegrade &&
      (topo.ha?.downAfterMs ?? 5000) === downAfterMs)

  const livePrimary = live ? Object.keys(live).find((m) => live[m].primary) : null
  const failedOver = livePrimary && livePrimary !== dbId
  const stale = live ? Object.keys(live).filter((m) => live[m].fenced) : []

  const ordinals = Array.from({ length: replicas }, (_, i) => i + 1)
  const toggleSync = (o: number) =>
    setSyncOrdinals((s) => (s.includes(o) ? s.filter((x) => x !== o) : [...s, o].sort((a, b) => a - b)))
  // A quorum bigger than the sync set would block every commit — the backend rejects it,
  // so keep the picker honest instead of letting them submit it.
  const maxQuorum = Math.max(1, syncOrdinals.length)
  const effQuorum = Math.min(quorum, maxQuorum)

  async function call(url: string, body: Record<string, unknown>, busyKey: string) {
    setBusy(busyKey)
    setError(null)
    setResult(null)
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = (await res.json().catch(() => ({}))) as PgTopoState
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load()
      return data
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      return null
    } finally {
      setBusy(null)
    }
  }

  const promote = (member: string) =>
    call('/api/postgres/failover', { system: systemId, id: dbId, target: member }, `promote:${member}`)
  const rejoin = (member: string) =>
    call('/api/postgres/rejoin', { system: systemId, id: dbId, member }, `rejoin:${member}`)

  async function apply() {
    const data = await call('/api/postgres/topology', {
      system: systemId,
      id: dbId,
      mode,
      ...(mode === 'replicated'
        ? {
          replicas,
          sync: { standbys: syncOrdinals, quorum: effQuorum, method: 'ANY', commitLevel },
          failover: { enabled: foEnabled, autoDegrade, downAfterMs },
        }
        : {}),
    }, 'apply')
    if (!data) return
    // The mechanical reconcile is done — switching the services to a multi-host DSN (the
    // one code change a failover needs) is a launched session's judgment work.
    if ((data.services || []).length && onLaunch) {
      onLaunch({
        sessionId: crypto.randomUUID(),
        mode: 'new',
        prompt: buildPgTopologyRetrofitPrompt({
          systemId,
          dbId,
          mode: data.mode,
          ha: data.node?.postgresHa || null,
          replicas: data.mode === 'replicated' ? replicas : 0,
          services: data.services,
          dsn: data.dsn,
        }),
      }, { kind: 'database', target: dbId, title: 'postgres topology retrofit' })
      onClose()
      return
    }
    setResult({ mode: data.mode, warnings: data.warnings || [] })
  }

  const currentLabel =
    topo.mode === 'replicated'
      ? `Replicated — ${(topo.ha?.members || []).length} members (${(topo.ha?.members || []).join(', ')})`
      : 'Standalone — a single container'

  const body = (
    <>
      <p className="sim-desc">
        <strong>Topology</strong> of <code>{dbId}</code>: real containers, reconciled by the backend
        on Apply. Current: {currentLabel}.
      </p>

      {/* ---- live cluster (roles are runtime, not manifest) ---- */}
      {topo.mode === 'replicated' && live && (
        <div className="form-section">
          <div className="form-section-head"><span>Live cluster</span></div>
          {failedOver && (
            <small className="form-hint" style={{ color: '#d8a657' }}>
              ⚠ A failover has happened: <code>{livePrimary}</code> is serving writes, not{' '}
              <code>{dbId}</code>. Writers using the multi-host DSN followed it automatically.
            </small>
          )}
          {(topo.ha?.members || []).map((m) => {
            const st = live[m] || {}
            const isPrimary = !!st.primary
            const role = !st.up ? 'down' : isPrimary ? 'primary' : st.fenced ? 'stale · fenced' : 'standby'
            const color = !st.up ? '#e06c6c' : isPrimary ? '#89b482' : st.fenced ? '#d8a657' : '#7daea3'
            return (
              <div className="pg-ha-row" key={m}>
                <span className="pg-ha-label">
                  <code>{m}</code>
                  <span className="pg-ha-role" style={{ color }}>{role}</span>
                  {st.up && !isPrimary && !st.fenced && st.lag != null && (
                    <span className="pg-ha-lag">lag {st.lag.toFixed(1)}s</span>
                  )}
                </span>
                <span className="pg-ha-actions">
                  {st.up && !isPrimary && !st.fenced && (
                    <button
                      type="button"
                      disabled={!!busy}
                      onClick={() => promote(m)}
                      title="Planned switchover: fence the current primary, then promote this standby"
                    >
                      {busy === `promote:${m}` ? 'Promoting…' : 'Promote'}
                    </button>
                  )}
                  {st.fenced && (
                    <button
                      type="button"
                      disabled={!!busy}
                      onClick={() => rejoin(m)}
                      title="Discard this node's stale data and re-clone it from the live primary"
                    >
                      {busy === `rejoin:${m}` ? 'Rejoining…' : 'Rejoin as standby'}
                    </button>
                  )}
                </span>
              </div>
            )
          })}
          {stale.length > 0 && (
            <small className="form-hint">
              A <strong>fenced</strong> node is an ex-primary that came back after a failover. It is set
              read-only, so libpq's <code>target_session_attrs=read-write</code> skips it and writers keep
              reaching the real primary — no split brain. <strong>Rejoin</strong> throws away its stale data
              and re-clones it from the live primary, making it a useful standby again.
            </small>
          )}
        </div>
      )}

      {/* ---- mode ---- */}
      <div className="form-section">
        <div className="form-section-head"><span>Mode</span></div>
        <label className="pg-ha-row">
          <span className="pg-ha-label">
            <input type="radio" name="pg-topo-mode" checked={mode === 'standalone'}
              onChange={() => setMode('standalone')} disabled={!!busy} />
            Standalone
          </span>
        </label>
        <label className="pg-ha-row">
          <span className="pg-ha-label">
            <input type="radio" name="pg-topo-mode" checked={mode === 'replicated'}
              onChange={() => setMode('replicated')} disabled={!!busy} />
            Replicated (streaming standbys + failover watcher)
          </span>
        </label>
      </div>

      {mode === 'replicated' && (
        <>
          {/* ---- replication ---- */}
          <div className="form-section">
            <div className="form-section-head"><span>Replication</span></div>
            <label className="form-row form-row-wide">
              <span>Standbys</span>
              <select value={replicas} disabled={!!busy}
                onChange={(e) => {
                  const n = Number(e.target.value)
                  setReplicas(n)
                  setSyncOrdinals((s) => s.filter((o) => o <= n))
                }}>
                {Array.from({ length: limits.replicasMax - limits.replicasMin + 1 },
                  (_, i) => limits.replicasMin + i).map((n) => <option key={n} value={n}>{n}</option>)}
              </select>
            </label>
            {ordinals.map((o) => (
              <label className="pg-ha-row" key={o}>
                <span className="pg-ha-label">
                  <input type="checkbox" checked={syncOrdinals.includes(o)}
                    onChange={() => toggleSync(o)} disabled={!!busy} />
                  <code>{`${dbId}-${o}`}</code> is <strong>synchronous</strong>
                </span>
                <span className="replica-mode">{syncOrdinals.includes(o) ? 'sync' : 'async'}</span>
              </label>
            ))}
            <small className="form-hint">
              <strong>Async (default)</strong> — the primary commits as soon as it has written its own WAL.
              Fast, but a crash can lose the last transactions: they were never on a standby.{' '}
              <strong>Synchronous</strong> — the commit does not return until the standby has acknowledged
              the WAL, so a promoted standby has every committed row. You pay a network round-trip on
              every write, which is the real cost of "no committed transaction is ever lost".
            </small>
            {syncOrdinals.length > 0 && (
              <>
                <label className="form-row form-row-wide">
                  <span>Quorum — commits wait for</span>
                  <select value={effQuorum} onChange={(e) => setQuorum(Number(e.target.value))} disabled={!!busy}>
                    {Array.from({ length: maxQuorum }, (_, i) => i + 1)
                      .map((n) => <option key={n} value={n}>{n} of {syncOrdinals.length}</option>)}
                  </select>
                </label>
                <label className="form-row form-row-wide">
                  <span>synchronous_commit</span>
                  <select value={commitLevel} onChange={(e) => setCommitLevel(e.target.value)} disabled={!!busy}>
                    {(limits.commitLevels || ['on']).map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </label>
                <small className="form-hint">
                  Written as <code>synchronous_standby_names = ANY {effQuorum} (
                  {syncOrdinals.map((o) => `"${dbId}-${o}"`).join(', ')})</code>. <code>on</code> waits for the
                  standby to <em>flush</em> WAL to disk; <code>remote_apply</code> also waits for it to be
                  <em> replayed</em>, so a read on that standby is guaranteed to see the write you just made
                  (read-your-writes) — at the cost of more latency.
                </small>
              </>
            )}
          </div>

          {/* ---- failover ---- */}
          <div className="form-section">
            <div className="form-section-head"><span>Failover</span></div>
            <label className="pg-ha-row">
              <span className="pg-ha-label">
                <input type="checkbox" checked={foEnabled} onChange={(e) => setFoEnabled(e.target.checked)}
                  disabled={!!busy} />
                Promote a standby automatically when the primary dies
              </span>
            </label>
            <label className="form-row form-row-wide">
              <span>Declare the primary dead after</span>
              <select value={downAfterMs} onChange={(e) => setDownAfterMs(Number(e.target.value))} disabled={!!busy}>
                {[2000, 5000, 10000, 30000].map((n) => <option key={n} value={n}>{n / 1000}s</option>)}
              </select>
            </label>
            <label className="pg-ha-row">
              <span className="pg-ha-label">
                <input type="checkbox" checked={autoDegrade} onChange={(e) => setAutoDegrade(e.target.checked)}
                  disabled={!!busy} />
                Auto-degrade a dead <strong>synchronous</strong> standby
              </span>
            </label>
            <small className="form-hint">
              <strong>Why replicas alone are not enough:</strong> standbys give you read scaling and a copy
              of the data, but nothing notices when the primary dies — the standbys keep serving stale reads
              and every write fails until a human intervenes. The <code>{dbId}-failover</code> watcher adds
              the missing pieces: it detects the death, promotes the standby that has replayed the most WAL,
              repoints the other standbys at it, and fences the old primary if it ever comes back.
            </small>
            {!autoDegrade && syncOrdinals.length > 0 && (
              <small className="form-hint" style={{ color: '#d8a657' }}>
                ⚠ With auto-degrade OFF, a synchronous standby going down blocks <strong>every write</strong> on
                the primary — it waits forever for an acknowledgement that can never come. That stall is a real
                property of synchronous replication (and a good thing to demonstrate deliberately); leave this
                ON if you just want the cluster to stay writable.
              </small>
            )}
          </div>
        </>
      )}

      {mode === 'standalone' && topo.mode === 'replicated' && (
        <div className="form-section">
          <small className="form-hint">
            Converting back deletes the standbys and the failover watcher, and clears
            <code> synchronous_standby_names</code> from the primary (leaving it set would block every commit
            once the standbys are gone). <code>{dbId}</code> keeps its data.
          </small>
        </div>
      )}

      {/* ---- apply ---- */}
      <div className="form-section">
        {attached > 0 && !noChange && (
          <small className="form-hint">
            Applying also queues a Claude session to retrofit the {attached} attached service(s) —{' '}
            {mode === 'replicated'
              ? 'a multi-host DSN with target_session_attrs=read-write, so writers follow a failover with no code change'
              : 'stripping the multi-host DSN back to a single host'}.
          </small>
        )}
        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={!!busy}>Close</button>
          <button type="button" className="primary" onClick={apply} disabled={!!busy || noChange}>
            {busy === 'apply' ? 'Applying… (can take a minute)' : noChange ? 'No change' : 'Apply topology'}
          </button>
        </div>
      </div>

      {result && (
        <div className="form-section">
          <small className="form-hint">Applied: now {result.mode}.</small>
          {result.warnings.map((w, i) => (
            <small className="form-hint" style={{ color: '#d8a657' }} key={i}>⚠ {w}</small>
          ))}
        </div>
      )}
      {error && <p className="modal-error">{error}</p>}
    </>
  )

  if (embedded) return body
  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Topology · <code>{dbId}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={!!busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}

function sameSet(a: number[], b: number[]) {
  return a.length === b.length && a.every((x) => b.includes(x))
}
