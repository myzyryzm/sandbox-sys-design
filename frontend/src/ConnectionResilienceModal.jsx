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

// Defaults match the brief's example policy; an existing policy (re-open) wins.
function initialState(initial) {
  const cb = initial?.circuit_breaker || {}
  const rt = initial?.retry || {}
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
    instruction: initial?.instruction || '',
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

export default function ConnectionResilienceModal({ systemId, from, to, initial, onClose, onLaunch }) {
  const [f, setF] = useState(() => initialState(initial))
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  const set = (k) => (e) => setF((s) => ({ ...s, [k]: e.target.value }))
  const toggle = (k) => (e) => setF((s) => ({ ...s, [k]: e.target.checked }))

  async function submit() {
    setError(null)
    if (!f.cbEnabled && !f.rtEnabled) {
      return setError('Enable circuit breaking and/or retry (or close to leave it unset).')
    }
    if (f.cbEnabled && f.open_behavior === 'fallback' && !f.fallback_response.trim()) {
      return setError('Provide a fallback response (served while the breaker is OPEN).')
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

    const conversationId = crypto.randomUUID()
    const body = { system: systemId, from, to, circuit_breaker, retry, instruction: f.instruction, conversationId }
    setBusy(true)
    try {
      const res = await fetch('/api/connection-resilience', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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
          <h2>Resilience · <code>{from}</code> → <code>{to}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <p className="sim-desc">
          Wrap this connection's outbound call with a circuit breaker and/or retry. The policy is
          stored on the connection and read by a shared wrapper at runtime — editing thresholds
          later needs no rebuild.
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
