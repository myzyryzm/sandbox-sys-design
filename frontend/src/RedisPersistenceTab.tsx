import { useCallback, useEffect, useRef, useState } from 'react'
import type { ManifestNode } from './types/manifest'

// The persistence block as /api/redis/persistence serves it (numbers, not form text).
interface RedisPersistenceCfg {
  rdb: { enabled: boolean; rules: Array<{ seconds: number; changes: number }> }
  aof: { enabled: boolean; fsync: string; rewritePercent: number; rewriteMinMb: number }
}

interface RedisPersistenceLimits {
  secondsMin: number
  secondsMax: number
  changesMin: number
  changesMax: number
  maxRules: number
  rewritePercentMin: number
  rewritePercentMax: number
  rewriteMinMbMin: number
  rewriteMinMbMax: number
  fsync: string[]
}

interface RedisPersistenceStatus {
  target?: string
  rdb_last_save_time: number
  rdb_changes_since_last_save?: number
  rdb_bgsave_in_progress?: boolean
  rdb_last_bgsave_status?: string
  aof_enabled?: boolean
  aof_rewrite_in_progress?: boolean
  aof_last_bgrewrite_status?: string
  aof_last_write_status?: string
  live: { save?: string | null; appendonly?: string; appendfsync?: string }
}

interface RedisPersistenceResponse {
  ok?: boolean
  error?: string
  persistence?: RedisPersistenceCfg | null
  defaults: RedisPersistenceCfg
  limits: RedisPersistenceLimits
  targets: string[]
  status?: RedisPersistenceStatus | null
  statusError?: string
}

interface PersistenceActionResult {
  target: string
  ok?: boolean
  message?: string
}

interface RedisPersistenceTabProps {
  systemId: string
  node: ManifestNode
  onClose: () => void
  embedded?: boolean
  onBusyChange?: (busy: boolean) => void
}

/**
 * A redis node's "Persistence" tab (create-database redis primaries only). Manages
 * the two real redis persistence mechanisms via /api/redis/persistence:
 *
 *   - RDB — point-in-time snapshots, driven by save rules ("snapshot after N
 *     seconds if ≥ C changes"). Cheap, compact, but a crash loses everything
 *     since the last snapshot.
 *   - AOF — an append-only log of every write, replayed on startup. Durability is
 *     set by appendfsync (always / everysec / no); auto-rewrite compacts the log.
 *
 * Applying is live (CONFIG SET on every running data container — primary +
 * replicas, or every cluster member) AND durable (the flags are baked into each
 * container's compose command, and a Topology reshape re-derives them from the
 * manifest block). No Claude session is involved — persistence is server-side only.
 */
export default function RedisPersistenceTab({ systemId, node, onClose, embedded = false, onBusyChange }: RedisPersistenceTabProps) {
  const redisId = node.id
  const [cfg, setCfg] = useState<RedisPersistenceResponse | null>(null) // last GET payload
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  // Warnings of the last apply.
  const [result, setResult] = useState<{ label: string; warnings: string[] } | null>(null)
  const [actionResults, setActionResults] = useState<PersistenceActionResult[] | null>(null)

  // Form state, seeded from the live block (or the image defaults) on first load
  // only — edits survive the poll.
  const seeded = useRef(false)
  const [rdbOn, setRdbOn] = useState(true)
  // [{ seconds, changes }] as strings while editing.
  const [rules, setRules] = useState<Array<{ seconds: string; changes: string }>>([])
  const [aofOn, setAofOn] = useState(false)
  const [fsync, setFsync] = useState('everysec')
  const [rewritePercent, setRewritePercent] = useState('100')
  const [rewriteMinMb, setRewriteMinMb] = useState('64')

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const load = useCallback(async () => {
    try {
      const res = await fetch(
        `/api/redis/persistence?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(redisId)}`,
      )
      const data = (await res.json()) as RedisPersistenceResponse
      if (!data.ok) throw new Error(data.error || 'failed to load')
      setCfg(data)
      if (!seeded.current) {
        seeded.current = true
        const p = data.persistence || data.defaults
        setRdbOn(p.rdb.enabled)
        setRules(p.rdb.rules.map((r) => ({ seconds: String(r.seconds), changes: String(r.changes) })))
        setAofOn(p.aof.enabled)
        setFsync(p.aof.fsync)
        setRewritePercent(String(p.aof.rewritePercent))
        setRewriteMinMb(String(p.aof.rewriteMinMb))
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }, [systemId, redisId])

  useEffect(() => {
    load()
    const t = setInterval(() => { if (!document.hidden) load() }, 5000)
    return () => clearInterval(t)
  }, [load])

  if (!cfg) {
    return <p className="sim-desc">{error ? `Error: ${error}` : 'Loading…'}</p>
  }

  const limits = cfg.limits

  function formError() {
    if (rdbOn && rules.length === 0) return 'RDB needs at least one save rule — or disable it'
    const seen = new Set()
    for (const r of rules) {
      const s = Number(r.seconds)
      const c = Number(r.changes)
      if (!Number.isInteger(s) || s < limits.secondsMin || s > limits.secondsMax) {
        return `rule seconds must be an integer ${limits.secondsMin}-${limits.secondsMax}`
      }
      if (!Number.isInteger(c) || c < limits.changesMin || c > limits.changesMax) {
        return `rule changes must be an integer ${limits.changesMin}-${limits.changesMax}`
      }
      if (seen.has(s)) return `duplicate ${s}s rule — one change threshold per time window`
      seen.add(s)
    }
    if (aofOn) {
      const p = Number(rewritePercent)
      const m = Number(rewriteMinMb)
      if (!Number.isInteger(p) || p < limits.rewritePercentMin || p > limits.rewritePercentMax) {
        return `rewrite growth must be an integer ${limits.rewritePercentMin}-${limits.rewritePercentMax} %`
      }
      if (!Number.isInteger(m) || m < limits.rewriteMinMbMin || m > limits.rewriteMinMbMax) {
        return `rewrite min size must be an integer ${limits.rewriteMinMbMin}-${limits.rewriteMinMbMax} MB`
      }
    }
    return null
  }
  const invalid = formError()

  async function call(body: Record<string, unknown>) {
    setBusy(true)
    setError(null)
    setResult(null)
    setActionResults(null)
    try {
      const res = await fetch('/api/redis/persistence', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: redisId, ...body }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string; warnings?: string[] }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      return data
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      return null
    } finally {
      setBusy(false)
    }
  }

  async function apply() {
    const data = await call({
      persistence: {
        rdb: { enabled: rdbOn, rules: rules.map((r) => ({ seconds: Number(r.seconds), changes: Number(r.changes) })) },
        aof: { enabled: aofOn, fsync, rewritePercent: Number(rewritePercent), rewriteMinMb: Number(rewriteMinMb) },
      },
    })
    if (!data) return
    setResult({ label: 'Applied.', warnings: data.warnings || [] })
    await load()
  }

  async function reset() {
    const data = await call({ reset: true })
    if (!data) return
    setResult({ label: 'Reset to image defaults.', warnings: data.warnings || [] })
    seeded.current = false // reseed the form from the defaults
    await load()
  }

  async function runAction(action: string) {
    setBusy(true)
    setError(null)
    setActionResults(null)
    try {
      const res = await fetch('/api/redis/persistence/action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: redisId, action }),
      })
      const data = (await res.json().catch(() => ({}))) as {
        ok?: boolean
        error?: string
        results?: PersistenceActionResult[]
      }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setActionResults(data.results || null)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  function setRule(i: number, key: 'seconds' | 'changes', value: string) {
    setRules((rs) => rs.map((r, j) => (j === i ? { ...r, [key]: value } : r)))
  }

  const ago = (unixSeconds: number) => {
    const s = Math.max(0, Math.round(Date.now() / 1000 - unixSeconds))
    if (s < 60) return `${s}s ago`
    if (s < 3600) return `${Math.floor(s / 60)}m ago`
    return `${Math.floor(s / 3600)}h ago`
  }

  const status = cfg.status

  const body = (
    <>
      <p className="sim-desc">
        <strong>Persistence</strong> of <code>{redisId}</code>: how (and whether) this redis
        survives a process restart. Settings apply live to the running container
        {cfg.targets.length > 1 ? `s (all ${cfg.targets.length}: ${cfg.targets.join(', ')})` : ''} and
        are baked into the compose file, so replicas and cluster members created later inherit them.
        {cfg.persistence === null && ' Currently running the redis image defaults.'}
      </p>

      {/* ---- RDB ---- */}
      <div className="form-section">
        <div className="form-section-head"><span>RDB snapshots</span></div>
        <div className="form-row form-row-check">
          <input
            id="redis-persist-rdb" type="checkbox" checked={rdbOn}
            onChange={(e) => setRdbOn(e.target.checked)} disabled={busy}
          />
          <div className="check-field">
            <label htmlFor="redis-persist-rdb">Enable RDB</label>
            <p className="form-hint">
              Point-in-time snapshots of the whole dataset (<code>dump.rdb</code>), written when a
              save rule fires: "snapshot after N seconds if at least C keys changed". Compact and
              fast to reload, but a crash loses everything since the last snapshot.
            </p>
          </div>
        </div>
        {rdbOn && rules.map((r, i) => (
          <label className="form-row" key={i}>
            <span>rule {i + 1}</span>
            <input
              type="number" min={limits.secondsMin} max={limits.secondsMax} value={r.seconds}
              onChange={(e) => setRule(i, 'seconds', e.target.value)} disabled={busy}
              style={{ width: 90, flex: 'none' }}
            />
            <small>s, if ≥</small>
            <input
              type="number" min={limits.changesMin} max={limits.changesMax} value={r.changes}
              onChange={(e) => setRule(i, 'changes', e.target.value)} disabled={busy}
              style={{ width: 110, flex: 'none' }}
            />
            <small>changes</small>
            <button
              type="button" onClick={() => setRules((rs) => rs.filter((_, j) => j !== i))}
              disabled={busy} title="remove rule"
            >
              ✕
            </button>
          </label>
        ))}
        {rdbOn && (
          <div className="form-row">
            <span />
            <button
              type="button" disabled={busy || rules.length >= limits.maxRules}
              onClick={() => setRules((rs) => [...rs, { seconds: '60', changes: '10' }])}
            >
              + Add rule
            </button>
          </div>
        )}
      </div>

      {/* ---- AOF ---- */}
      <div className="form-section">
        <div className="form-section-head"><span>AOF (append-only file)</span></div>
        <div className="form-row form-row-check">
          <input
            id="redis-persist-aof" type="checkbox" checked={aofOn}
            onChange={(e) => setAofOn(e.target.checked)} disabled={busy}
          />
          <div className="check-field">
            <label htmlFor="redis-persist-aof">Enable AOF</label>
            <p className="form-hint">
              Every write is appended to a log and replayed on startup — durability down to one
              write (or one second), at the cost of larger files and slower restarts. Commonly
              combined with RDB: the snapshot bounds replay time, the log covers the gap.
            </p>
          </div>
        </div>
        {aofOn && (
          <>
            <label className="form-row">
              <span>fsync</span>
              <select value={fsync} onChange={(e) => setFsync(e.target.value)} disabled={busy}>
                {limits.fsync.map((f) => <option key={f} value={f}>{f}</option>)}
              </select>
            </label>
            <small className="form-hint">
              <code>always</code> fsyncs every write (durable, slow) · <code>everysec</code> loses
              at most ~1s on a crash (the default trade-off) · <code>no</code> leaves flushing to
              the OS (fastest, weakest).
            </small>
            <label className="form-row">
              <span>rewrite</span>
              <small>at</small>
              <input
                type="number" min={limits.rewritePercentMin} max={limits.rewritePercentMax}
                value={rewritePercent} onChange={(e) => setRewritePercent(e.target.value)}
                disabled={busy} style={{ width: 80, flex: 'none' }}
              />
              <small>% growth, min</small>
              <input
                type="number" min={limits.rewriteMinMbMin} max={limits.rewriteMinMbMax}
                value={rewriteMinMb} onChange={(e) => setRewriteMinMb(e.target.value)}
                disabled={busy} style={{ width: 80, flex: 'none' }}
              />
              <small>MB</small>
            </label>
            <small className="form-hint">
              Auto-rewrite compacts the log once it has grown this % past its last compacted size
              (0% disables it); the min size stops tiny files from rewriting constantly.
            </small>
          </>
        )}
      </div>

      {/* ---- apply ---- */}
      <div className="form-section">
        {!rdbOn && !aofOn && (
          <small className="form-hint" style={{ color: '#d8a657' }}>
            ⚠ RDB and AOF both off — a restart of this container loses every key.
          </small>
        )}
        {aofOn && fsync === 'always' && (
          <small className="form-hint" style={{ color: '#d8a657' }}>
            ⚠ <code>always</code> fsyncs on every write — maximum durability, significant write latency.
          </small>
        )}
        <small className="form-hint">
          This sandbox keeps redis containers ephemeral (no data volume): data survives a container
          restart, but the next re-creation — e.g. a Topology change — still starts empty and
          replays the keyspace seeds.
        </small>
        {invalid && <small className="field-error">{invalid}</small>}
        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={busy}>Close</button>
          <button type="button" onClick={reset} disabled={busy || cfg.persistence === null}>
            Reset to image defaults
          </button>
          <button type="button" className="primary" onClick={apply} disabled={busy || !!invalid}>
            {busy ? 'Applying…' : 'Apply persistence'}
          </button>
        </div>
      </div>

      {result && (
        <div className="form-section">
          <small className="form-hint">{result.label}</small>
          {result.warnings.map((w, i) => (
            <small className="form-hint" style={{ color: '#d8a657' }} key={i}>⚠ {w}</small>
          ))}
        </div>
      )}

      {/* ---- manual actions + live status ---- */}
      <div className="form-section">
        <div className="form-section-head"><span>Live status</span></div>
        {status ? (
          <>
            <small className="form-hint">
              Last RDB save: <strong>{ago(status.rdb_last_save_time)}</strong>
              {' '}· {status.rdb_changes_since_last_save} change(s) since
              {status.rdb_bgsave_in_progress ? ' · snapshot in progress…' : ` · last snapshot ${status.rdb_last_bgsave_status}`}
            </small>
            <small className="form-hint">
              AOF: <strong>{status.aof_enabled ? 'on' : 'off'}</strong>
              {status.aof_enabled ? (
                status.aof_rewrite_in_progress
                  ? ' · rewrite in progress…'
                  : ` · last rewrite ${status.aof_last_bgrewrite_status} · last write ${status.aof_last_write_status}`
              ) : ''}
            </small>
            <small className="form-hint">
              Server config ({status.target}): save "{status.live.save ?? ''}" ·
              appendonly {status.live.appendonly} · appendfsync {status.live.appendfsync}
            </small>
          </>
        ) : (
          <small className="form-hint" style={{ color: '#888' }}>
            No reachable container — {cfg.statusError}
          </small>
        )}
        <div className="modal-actions">
          <button type="button" onClick={() => runAction('bgsave')} disabled={busy}>
            Snapshot now (BGSAVE)
          </button>
          <button type="button" onClick={() => runAction('bgrewriteaof')} disabled={busy}>
            Rewrite AOF now
          </button>
        </div>
        {actionResults && actionResults.map((r) => (
          <small className="form-hint" key={r.target} style={r.ok ? undefined : { color: '#d8a657' }}>
            {r.ok ? '✓' : '⚠'} {r.target}: {r.message}
          </small>
        ))}
      </div>

      {error && <p className="modal-error">{error}</p>}
    </>
  )

  if (embedded) return body
  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Persistence · <code>{redisId}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
