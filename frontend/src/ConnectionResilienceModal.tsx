import { useState } from 'react'

/**
 * Per-connection resilience policy editor. Opened by clicking a connection (a
 * source service -> target node outbound call) on the diagram. Configures a circuit
 * breaker + retry policy for that connection, persists it to manifest.edges via
 * POST /api/connection-resilience (config only — no rebuild), then launches a Claude
 * session (sandbox-resilience skill) to wire the shared wrapper into the `from`
 * service on first attach, or just confirm the (no-rebuild) config edit otherwise.
 *
 * Circuit-breaker states (do NOT invert): CLOSED = healthy/flowing, OPEN =
 * tripped/blocking (fast-fail or fallback), HALF-OPEN = testing recovery.
 */

const RETRY_STRATEGIES = [
  { value: 'exponential_backoff', label: 'exponential backoff' },
  { value: 'exponential_backoff_jitter', label: 'exponential backoff + jitter' },
]

// Defaults match the brief's example policy; an existing policy (re-open) wins. The
// connection pool defaults OFF (unlike breaker/retry) so opening a resilience-only edge
// doesn't imply a pool; an existing pool block seeds the fields.
function initialState(initial, initialPool) {
  const cb = initial?.circuit_breaker || {}
  const rt = initial?.retry || {}
  const pool = initialPool || {}
  return {
    cbEnabled: cb.enabled ?? true,
    failure_threshold: String(cb.failure_threshold ?? 5),
    pause_duration_seconds: String(cb.pause_duration_seconds ?? 10),
    half_open_trial_calls: String(cb.half_open_trial_calls ?? 1),
    open_behavior: cb.open_behavior || 'fail_fast',
    fallback_response: cb.fallback_response == null ? '' : String(cb.fallback_response),
    rtEnabled: rt.enabled ?? true,
    max_attempts: String(rt.max_attempts ?? 3),
    strategy: rt.strategy || 'exponential_backoff_jitter',
    base_delay_seconds: String(rt.base_delay_seconds ?? 0.5),
    max_delay_seconds: String(rt.max_delay_seconds ?? 8),
    poolEnabled: pool.enabled ?? false,
    max_connections: String(pool.max_connections ?? 10),
    min_idle: String(pool.min_idle ?? 2),
    idle_timeout_seconds: String(pool.idle_timeout_seconds ?? 30),
    max_lifetime_seconds: String(pool.max_lifetime_seconds ?? 1800),
    instruction: initial?.instruction || initialPool?.instruction || '',
  }
}

function buildResiliencePrompt({ systemId, from, to, firstAttach, circuit_breaker, retry, instruction }) {
  const cb = circuit_breaker.enabled
    ? `circuit breaker: trip after ${circuit_breaker.failure_threshold} consecutive failures, pause ${circuit_breaker.pause_duration_seconds}s, then ${circuit_breaker.half_open_trial_calls} half-open trial call(s); while OPEN → ${circuit_breaker.open_behavior}${circuit_breaker.open_behavior === 'fallback' ? ` (serve: ${JSON.stringify(circuit_breaker.fallback_response)})` : ''}`
    : 'circuit breaker: disabled'
  const rt = retry.enabled
    ? `retry: up to ${retry.max_attempts} attempts, ${retry.strategy}, base ${retry.base_delay_seconds}s capped at ${retry.max_delay_seconds}s`
    : 'retry: disabled'
  return [
    `Use the sandbox-resilience skill to apply a resilience policy on the "${systemId}" system.`,
    ``,
    `Connection: ${from} -> ${to}  (label "${from}->${to}")`,
    `The policy is ALREADY written to systems/${systemId}/manifest.json under`,
    `edges[] where from="${from}" and to="${to}". Do NOT rewrite the policy — read it from there.`,
    ``,
    `Policy summary:`,
    `  ${cb}`,
    `  ${rt}`,
    ``,
    firstAttach
      ? [
          `This is the FIRST resilience policy on "${from}", so wire it up:`,
          `1. Create/extend the shared wrapper package systems/${systemId}/resilience/ (one shared`,
          `   copy: breaker state machine CLOSED/OPEN/HALF-OPEN + retry; breaker is the outer gate,`,
          `   retry the inner loop; reads the per-connection policy from the mounted manifest at`,
          `   runtime so config edits need no rebuild).`,
          `2. Route ${from}'s outbound call to ${to} through the wrapper (wrap the real call site —`,
          `   the psycopg/db call if ${to} is a database, the gRPC/HTTP call if it's a service).`,
          `3. Add the six Prometheus metrics (labeled connection="${from}->${to}") to ${from}'s`,
          `   existing /metrics, and a GET /resilience/state returning live in-memory state.`,
          `4. Mount manifest.json (read-only) + set SERVICE_ID in docker-compose, then rebuild ONLY`,
          `   ${from}:  docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${from}`,
        ].join('\n')
      : [
          `"${from}" is already wired for resilience, and editing thresholds is config-only — the`,
          `wrapper re-reads the manifest at runtime. Confirm ${from} already routes its call to ${to}`,
          `through the shared wrapper (wire it if that specific call site isn't yet), and that the`,
          `connection="${from}->${to}" metrics + /resilience/state are reported. Rebuild ${from} ONLY`,
          `if you changed its code.`,
        ].join('\n'),
    ``,
    `Notes / intent:`,
    instruction.trim() || '(none given)',
  ].join('\n')
}

function buildConnectionPoolPrompt({ systemId, from, to, firstAttach, pool, instruction }) {
  const summary = `max ${pool.max_connections} connections, min ${pool.min_idle} idle, reap an idle connection after ${pool.idle_timeout_seconds}s, recycle a connection after ${pool.max_lifetime_seconds}s`
  return [
    `Use the sandbox-connection-pool skill to apply a connection pool on the "${systemId}" system.`,
    ``,
    `Connection: ${from} -> ${to}  (label "${from}->${to}")`,
    `The pool config is ALREADY written to systems/${systemId}/manifest.json under`,
    `edges[] where from="${from}" and to="${to}" (the "connection_pool" block). Read it from there —`,
    `do NOT rewrite it.`,
    ``,
    `Pool summary:`,
    `  ${summary}`,
    ``,
    firstAttach
      ? [
          `This is the FIRST pooled connection on "${from}", so wire it up:`,
          `1. Replace ${from}'s per-request connection to ${to} with a module-level shared pool sized`,
          `   from the manifest (read the connection_pool block at STARTUP, keyed by SERVICE_ID + to="${to}"):`,
          `   - postgres: psycopg_pool.ConnectionPool(min_size=min_idle, max_size=max_connections,`,
          `     max_idle=idle_timeout_seconds, max_lifetime=max_lifetime_seconds); use "with pool.connection()".`,
          `   - mongodb: pass maxPoolSize/minPoolSize/maxIdleTimeMS to MongoClient (no max_lifetime equiv —`,
          `     note it).`,
          `   - service->service HTTP: one shared httpx.Client(limits=httpx.Limits(max_connections=...,`,
          `     max_keepalive_connections=min_idle, keepalive_expiry=idle_timeout_seconds)); max_lifetime is`,
          `     not supported by httpx — document that, don't fake it.`,
          `2. Export pool gauges (connection_pool_max/active/idle, labeled connection="${from}->${to}") on`,
          `   ${from}'s existing /metrics, and add GET /pool/state returning live {to,max,active,idle}.`,
          `3. Mount manifest.json (read-only) + set SERVICE_ID in docker-compose (idempotent — may already`,
          `   be set for resilience/gRPC), then rebuild ONLY ${from}:`,
          `   docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${from}`,
        ].join('\n')
      : [
          `"${from}" already has a connection pool wired. Pool sizes are set when the pool is CREATED, so`,
          `this is NOT a no-op like a breaker-threshold edit: confirm ${from} routes its call to ${to}`,
          `through the shared pool (wire that call site if it isn't yet), keep the connection="${from}->${to}"`,
          `gauges + /pool/state, then REBUILD/RESTART ${from} so the new sizes take effect:`,
          `   docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${from}`,
        ].join('\n'),
    ``,
    `Notes / intent:`,
    (instruction || '').trim() || '(none given)',
  ].join('\n')
}

export default function ConnectionResilienceModal({ systemId, from, to, initial, initialPool, poolEligible = true, onClose, onLaunch }) {
  const [f, setF] = useState(() => initialState(initial, initialPool))
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  const set = (k) => (e) => setF((s) => ({ ...s, [k]: e.target.value }))
  const toggle = (k) => (e) => setF((s) => ({ ...s, [k]: e.target.checked }))

  async function submit() {
    setError(null)
    const resilienceRequested = f.cbEnabled || f.rtEnabled
    const poolRequested = poolEligible && f.poolEnabled
    if (!resilienceRequested && !poolRequested) {
      return setError('Enable circuit breaking, retry, and/or a connection pool (or close to leave it unset).')
    }
    if (f.cbEnabled && f.open_behavior === 'fallback' && !f.fallback_response.trim()) {
      return setError('Provide a fallback response (served while the breaker is OPEN).')
    }
    if (poolRequested) {
      const maxC = Number(f.max_connections)
      const minI = Number(f.min_idle)
      if (!(maxC >= 1)) return setError('Max connections must be at least 1.')
      if (!(minI >= 0)) return setError('Min idle must be 0 or more.')
      if (minI > maxC) return setError('Min idle must be ≤ max connections.')
    }

    const circuit_breaker = f.cbEnabled
      ? {
          enabled: true,
          failure_threshold: Number(f.failure_threshold),
          pause_duration_seconds: Number(f.pause_duration_seconds),
          half_open_trial_calls: Number(f.half_open_trial_calls),
          open_behavior: f.open_behavior,
          ...(f.open_behavior === 'fallback' ? { fallback_response: f.fallback_response } : {}),
        }
      : { enabled: false }
    const retry = f.rtEnabled
      ? {
          enabled: true,
          max_attempts: Number(f.max_attempts),
          strategy: f.strategy,
          base_delay_seconds: Number(f.base_delay_seconds),
          max_delay_seconds: Number(f.max_delay_seconds),
        }
      : { enabled: false }

    setBusy(true)
    try {
      // Resilience (circuit breaker + retry) — unchanged flow.
      if (resilienceRequested) {
        const conversationId = crypto.randomUUID()
        const res = await fetch('/api/connection-resilience', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system: systemId, from, to, circuit_breaker, retry, instruction: f.instruction, conversationId }),
        })
        const data = await res.json().catch(() => ({}))
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
        onLaunch({
          sessionId: conversationId,
          mode: 'new',
          prompt: buildResiliencePrompt({
            systemId, from, to, firstAttach: data.firstAttach,
            circuit_breaker, retry, instruction: f.instruction,
          }),
        }, { kind: 'resilience', target: `${from}→${to}`, title: 'policy' })
      }

      // Connection pool — its own config write + session (queue serializes them).
      if (poolRequested) {
        const connection_pool = {
          enabled: true,
          max_connections: Number(f.max_connections),
          min_idle: Number(f.min_idle),
          idle_timeout_seconds: Number(f.idle_timeout_seconds),
          max_lifetime_seconds: Number(f.max_lifetime_seconds),
        }
        const conversationId = crypto.randomUUID()
        const res = await fetch('/api/connection-pool', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system: systemId, from, to, connection_pool, instruction: f.instruction, conversationId }),
        })
        const data = await res.json().catch(() => ({}))
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
        onLaunch({
          sessionId: conversationId,
          mode: 'new',
          prompt: buildConnectionPoolPrompt({
            systemId, from, to, firstAttach: data.firstAttach,
            pool: connection_pool, instruction: f.instruction,
          }),
        }, { kind: 'connection-pool', target: `${from}→${to}`, title: 'pool' })
      }
      onClose()
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Connection · <code>{from}</code> → <code>{to}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <p className="sim-desc">
          Configure this connection's outbound call: a circuit breaker and/or retry (read by a shared
          wrapper at runtime — editing thresholds needs no rebuild){poolEligible ? ', and a connection pool sized from the manifest' : ''}.
        </p>

        {/* Circuit breaker */}
        <div className="form-section">
          <label className="res-section-head">
            <input type="checkbox" checked={f.cbEnabled} onChange={toggle('cbEnabled')} disabled={busy} />
            <span>Circuit breaker</span>
          </label>
          {f.cbEnabled && (
            <>
              <label className="form-row">
                <span>Failure threshold</span>
                <input type="number" min="1" step="1" value={f.failure_threshold} onChange={set('failure_threshold')} disabled={busy} />
              </label>
              <label className="form-row">
                <span>Pause (s)</span>
                <input type="number" min="0" step="0.5" value={f.pause_duration_seconds} onChange={set('pause_duration_seconds')} disabled={busy} />
              </label>
              <label className="form-row">
                <span>Half-open trials</span>
                <input type="number" min="1" step="1" value={f.half_open_trial_calls} onChange={set('half_open_trial_calls')} disabled={busy} />
              </label>
              <label className="form-row">
                <span>While OPEN</span>
                <select value={f.open_behavior} onChange={set('open_behavior')} disabled={busy}>
                  <option value="fail_fast">fail fast (return an error)</option>
                  <option value="fallback">fallback (return a default)</option>
                </select>
              </label>
              {f.open_behavior === 'fallback' && (
                <label className="form-row">
                  <span>Fallback</span>
                  <input value={f.fallback_response} onChange={set('fallback_response')} placeholder='e.g. {"items": []}' disabled={busy} />
                </label>
              )}
            </>
          )}
        </div>

        {/* Retry */}
        <div className="form-section">
          <label className="res-section-head">
            <input type="checkbox" checked={f.rtEnabled} onChange={toggle('rtEnabled')} disabled={busy} />
            <span>Retry</span>
          </label>
          {f.rtEnabled && (
            <>
              <label className="form-row">
                <span>Max attempts</span>
                <input type="number" min="1" step="1" value={f.max_attempts} onChange={set('max_attempts')} disabled={busy} />
              </label>
              <label className="form-row">
                <span>Strategy</span>
                <select value={f.strategy} onChange={set('strategy')} disabled={busy}>
                  {RETRY_STRATEGIES.map((s) => <option key={s.value} value={s.value}>{s.label}</option>)}
                </select>
              </label>
              <label className="form-row">
                <span>Base delay (s)</span>
                <input type="number" min="0" step="0.1" value={f.base_delay_seconds} onChange={set('base_delay_seconds')} disabled={busy} />
              </label>
              <label className="form-row">
                <span>Max delay (s)</span>
                <input type="number" min="0" step="0.5" value={f.max_delay_seconds} onChange={set('max_delay_seconds')} disabled={busy} />
              </label>
            </>
          )}
        </div>

        {/* Connection pool — internal targets only (external services sit outside the boundary). */}
        {poolEligible && (
          <div className="form-section">
            <label className="res-section-head">
              <input type="checkbox" checked={f.poolEnabled} onChange={toggle('poolEnabled')} disabled={busy} />
              <span>Connection pool</span>
            </label>
            {f.poolEnabled && (
              <>
                <label className="form-row">
                  <span>Max connections</span>
                  <input type="number" min="1" step="1" value={f.max_connections} onChange={set('max_connections')} disabled={busy} />
                </label>
                <label className="form-row">
                  <span>Min idle</span>
                  <input type="number" min="0" step="1" value={f.min_idle} onChange={set('min_idle')} disabled={busy} />
                </label>
                <label className="form-row">
                  <span>Idle timeout (s)</span>
                  <input type="number" min="1" step="1" value={f.idle_timeout_seconds} onChange={set('idle_timeout_seconds')} disabled={busy} />
                </label>
                <label className="form-row">
                  <span>Max lifetime (s)</span>
                  <input type="number" min="1" step="1" value={f.max_lifetime_seconds} onChange={set('max_lifetime_seconds')} disabled={busy} />
                </label>
                <p className="sim-desc" style={{ margin: '6px 0 0' }}>
                  Pool sizes are set when the connection is created, so changing them rebuilds/restarts{' '}
                  <code>{from}</code> (unlike breaker/retry thresholds).
                </p>
              </>
            )}
          </div>
        )}

        <label className="form-row">
          <span>Notes</span>
          <textarea
            className="desc-input"
            value={f.instruction}
            onChange={set('instruction')}
            placeholder="Anything the wrapper implementation should know (optional)"
            rows={2}
            disabled={busy}
          />
        </label>

        {error && <p className="modal-error">{error}</p>}

        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
          <button type="button" className="primary" onClick={submit} disabled={busy}>
            {busy ? 'Saving…' : 'Apply & open Claude'}
          </button>
        </div>
      </div>
    </div>
  )
}
