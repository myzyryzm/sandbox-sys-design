import { useCallback, useEffect, useRef, useState } from 'react'
import { affectedServices, buildRedisTopologyRetrofitPrompt } from './redisTopologyPrompts'

/**
 * A redis node's "Topology" tab (create-database redis primaries only). Reconciles
 * the node between three REAL container shapes via POST /api/redis/topology:
 *
 *   - Standalone — the single container "Add database" created.
 *   - Replicated — pick a replica COUNT (fixed count for now; autoscaling later).
 *     Enabling it also provisions a real 3-node Redis Sentinel (quorum 2), because
 *     replicas alone are only read scaling: nothing detects a dead primary or
 *     promotes a replica. The explainer below spells that out before Apply.
 *   - Sharded — pick shards (3-5) + replicas per shard (0-2). This provisions a
 *     real Redis Cluster (16384 hash slots): keys are partitioned, so it needs
 *     cluster-aware clients, and its built-in failover REPLACES sentinel.
 *
 * The backend does the mechanical work (containers/scrape/manifest). Applying then
 * enqueues a Claude session (sandbox-redis-topology skill) that retrofits the
 * declared writer/reader services' code — Sentinel discovery for writes,
 * RedisCluster clients, per-keyspace WAIT calls (declared in the Keyspaces tab).
 */
export default function RedisTopologyTab({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }) {
  const redisId = node.id
  const [topo, setTopo] = useState(null)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState(null) // { mode, warnings, enqueued } of the last apply

  // Form state, seeded from the live topology on first load only (edits survive polls).
  const seeded = useRef(false)
  const [mode, setMode] = useState('standalone')
  const [replicas, setReplicas] = useState(1)
  const [shards, setShards] = useState(3)
  const [rps, setRps] = useState(1)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const load = useCallback(async () => {
    try {
      const res = await fetch(
        `/api/redis/topology?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(redisId)}`,
      )
      const data = await res.json()
      if (!data.ok) throw new Error(data.error || 'failed to load')
      setTopo(data)
      if (!seeded.current) {
        seeded.current = true
        setMode(data.mode)
        if (data.replicas?.length) setReplicas(data.replicas.length)
        if (data.cluster) {
          setShards(data.cluster.shards)
          setRps(data.cluster.replicasPerShard)
        }
      }
    } catch (err) {
      setError(err.message)
    }
  }, [systemId, redisId])

  useEffect(() => {
    load()
    const t = setInterval(() => { if (!document.hidden) load() }, 5000)
    return () => clearInterval(t)
  }, [load])

  if (!topo) {
    return <p className="sim-desc">{error ? `Error: ${error}` : 'Loading…'}</p>
  }

  const limits = topo.limits || { replicasMin: 1, replicasMax: 4, shardsMin: 3, shardsMax: 5, replicasPerShardMax: 2 }
  const noChange =
    (mode === 'standalone' && topo.mode === 'standalone') ||
    (mode === 'replicated' && topo.mode === 'replicated' && topo.replicas.length === replicas)
  // Same-params cluster apply is offered as a deliberate RE-FORM (recreate the
  // members + re-run the init) — the repair action for a failed/degraded cluster.
  const reform = mode === 'cluster' && topo.mode === 'cluster' &&
    topo.cluster?.shards === shards && topo.cluster?.replicasPerShard === rps
  const attached = affectedServices(node.keyspaces).size
  const dataLoss = mode === 'cluster' || topo.mode === 'cluster'

  async function apply() {
    setBusy(true)
    setError(null)
    setResult(null)
    try {
      const res = await fetch('/api/redis/topology', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          id: redisId,
          mode,
          ...(mode === 'replicated' ? { replicas } : {}),
          ...(mode === 'cluster' ? { shards, replicasPerShard: rps } : {}),
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load()
      // The mechanical reconcile is done — the code retrofit (Sentinel discovery /
      // RedisCluster clients / WAIT calls) is a launched session's judgment work.
      const enqueued = attached > 0 && !!onLaunch
      if (enqueued) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: buildRedisTopologyRetrofitPrompt({
            systemId,
            redisId,
            mode: data.mode,
            sentinel: data.node?.sentinel || null,
            cluster: data.node?.redisCluster || null,
            replicas: (data.node && topoReplicasOf(data)) || [],
            keyspaces: data.node?.keyspaces || node.keyspaces || [],
          }),
        }, { kind: 'database', target: redisId, title: 'redis topology retrofit' })
        onClose()
        return
      }
      setResult({ mode: data.mode, warnings: data.warnings || [], enqueued })
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  // The replica list after an apply: re-derive from the POST's manifest node echo
  // (GET may lag a poll behind).
  function topoReplicasOf(data) {
    if (data.mode !== 'replicated') return []
    const count = replicas
    return Array.from({ length: count }, (_, i) => ({ id: `${redisId}-${i + 1}` }))
  }

  const currentLabel =
    topo.mode === 'cluster'
      ? `Redis Cluster — ${topo.cluster.shards} shards × ${1 + topo.cluster.replicasPerShard} (members: ${topo.cluster.members.join(', ')})`
      : topo.mode === 'replicated'
        ? `Replicated — primary + ${topo.replicas.length} replica(s) (${topo.replicas.map((r) => r.id).join(', ')}), sentinel ${topo.sentinel?.members?.join(', ')}`
        : 'Standalone — a single container'

  const body = (
    <>
      <p className="sim-desc">
        <strong>Topology</strong> of <code>{redisId}</code>: real containers, reconciled by the
        backend on Apply. Current: {currentLabel}.
        {topo.mode === 'standalone' && topo.replicas.length > 0 &&
          ` (${topo.replicas.length} existing replica(s) will be adopted into the count.)`}
      </p>

      {/* ---- mode picker ---- */}
      <div className="form-section">
        <div className="form-section-head"><span>Mode</span></div>
        <label className="form-row">
          <span>
            <input
              type="radio" name="redis-topo-mode" checked={mode === 'standalone'}
              onChange={() => setMode('standalone')} disabled={busy}
            />{' '}
            Standalone
          </span>
        </label>
        <label className="form-row">
          <span>
            <input
              type="radio" name="redis-topo-mode" checked={mode === 'replicated'}
              onChange={() => setMode('replicated')} disabled={busy}
            />{' '}
            Replicated (adds Redis Sentinel)
          </span>
        </label>
        <label className="form-row">
          <span>
            <input
              type="radio" name="redis-topo-mode" checked={mode === 'cluster'}
              onChange={() => setMode('cluster')} disabled={busy}
            />{' '}
            Sharded (adds Redis Cluster)
          </span>
        </label>
      </div>

      {/* ---- per-mode params + explainer (shown on selection, BEFORE apply) ---- */}
      {mode === 'replicated' && (
        <div className="form-section">
          <div className="form-section-head"><span>Replication</span></div>
          <label className="form-row">
            <span>Replicas</span>
            <select value={replicas} onChange={(e) => setReplicas(Number(e.target.value))} disabled={busy}>
              {Array.from({ length: limits.replicasMax - limits.replicasMin + 1 }, (_, i) => limits.replicasMin + i)
                .map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
          </label>
          <small className="form-hint">
            <strong>Why this adds Sentinel:</strong> replicas alone are read scaling — every write
            still lands on one primary, and if it dies nothing notices: the replicas keep serving
            stale reads and writes fail until a human intervenes. Redis Sentinel (3 nodes here,
            quorum 2) adds the missing pieces: failure detection (quorum agreement that the primary
            is down, no single flaky observer can trigger it), automatic promotion of the most
            caught-up replica, and master discovery — writers ask sentinel "who is the master
            named <code>{redisId}</code>?" instead of hardcoding a hostname.
          </small>
          <small className="form-hint">
            Replication is asynchronous by default. Writers that need stronger guarantees can
            declare a per-keyspace <strong>WAIT</strong> write mode in the Keyspaces tab
            (pseudo-sync: each write blocks until N replicas acknowledge, with a timeout).
          </small>
        </div>
      )}
      {mode === 'cluster' && (
        <div className="form-section">
          <div className="form-section-head"><span>Sharding</span></div>
          <label className="form-row">
            <span>Shards</span>
            <select value={shards} onChange={(e) => setShards(Number(e.target.value))} disabled={busy}>
              {Array.from({ length: limits.shardsMax - limits.shardsMin + 1 }, (_, i) => limits.shardsMin + i)
                .map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
          </label>
          <label className="form-row">
            <span>Replicas / shard</span>
            <select value={rps} onChange={(e) => setRps(Number(e.target.value))} disabled={busy}>
              {Array.from({ length: (limits.replicasPerShardMax ?? 2) + 1 }, (_, i) => i)
                .map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
          </label>
          <small className="form-hint">
            <strong>Why this adds Redis Cluster:</strong> sharding splits the keyspace across
            {' '}{shards} masters, and something must decide which keys live where and route every
            command there. Redis Cluster does both natively: the 16384 hash slots are divided among
            the masters ({shards * (1 + rps)} containers total), each key hashes to a slot, and a
            cluster-aware client follows MOVED redirects. Its cluster bus also does failure
            detection and per-shard failover built in — so Sentinel is not used with it.
          </small>
          <small className="form-hint">
            Trade-offs to know: multi-key operations only work inside one hash slot (co-locate
            related keys with <code>{'{hash}'}</code> tags); a <em>prefix</em> keyspace's keys
            spread across all shards by design{rps === 0 ? (
              <>; and with <strong>0 replicas per shard, losing any single shard loses that shard's
              data</strong> and takes the cluster unhealthy</>
            ) : (
              <>; each shard's {rps} replica(s) take over on a shard master failure</>
            )}.
          </small>
        </div>
      )}
      {mode === 'standalone' && topo.mode !== 'standalone' && (
        <div className="form-section">
          <small className="form-hint">
            Converting back tears down {topo.mode === 'cluster' ? 'the cluster members' : 'the replicas and sentinels'} and
            restores the single <code>{redisId}</code> container. Any WAIT write modes declared in
            the Keyspaces tab stop making sense (no replicas to acknowledge) — they are kept but
            flagged.
          </small>
        </div>
      )}

      {/* ---- apply ---- */}
      <div className="form-section">
        {dataLoss && !noChange && (
          <small className="form-hint" style={{ color: '#d8a657' }}>
            ⚠ Converting {mode === 'cluster' ? 'into' : 'out of'} cluster mode recreates the
            data-bearing containers: existing data is cleared and the keyspace seeds are replayed.
          </small>
        )}
        {attached > 0 && !noChange && (
          <small className="form-hint">
            Applying also queues a Claude session to retrofit the {attached} attached
            writer/reader service(s) — {mode === 'cluster'
              ? 'cluster-aware clients (MOVED redirects)'
              : mode === 'replicated'
                ? 'sentinel-based master discovery for writes'
                : 'plain single-host clients'} and any declared WAIT calls.
          </small>
        )}
        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={busy}>Close</button>
          <button type="button" className="primary" onClick={apply} disabled={busy || noChange}>
            {busy ? 'Applying… (can take a minute)'
              : noChange ? 'No change'
                : reform ? 'Re-form cluster' : 'Apply topology'}
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
          <h2>Topology · <code>{redisId}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
