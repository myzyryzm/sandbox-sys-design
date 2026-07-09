// LLM Worker — Scaling tab (embedded body; NodeEditModal owns the chrome).
//
// Three sections, mirroring the consumer group's Scaling tab:
//   1. Live scaler state — batch utilization (active sequences / total max_active),
//      per-worker occupancy, the scaler's desired count and its latest decision
//      (from the scaler's /state, polled through the aggregate route).
//   2. Scaling policy — min/max, utilization thresholds, stability windows, cooldown,
//      enabled — written to the group's mounted scaler.json via the policy route: the
//      scaler container mtime-polls it, so edits apply LIVE with no rebuild.
//   3. Workers — the manual "set worker count" input (same idempotent reconciler the
//      autoscaler applies through). With autoscaling enabled a manual count is soon
//      reconciled back to the policy's desired — the tab says so.
import { useEffect, useState } from 'react'

const STATE_URL = (sys) => `/api/custom/llm-worker/state?system=${encodeURIComponent(sys)}`
const POLICY_FIELDS = [
  ['min', 'Min workers', 'never fewer than this'],
  ['max', 'Max workers', 'never more than this'],
  ['scale_up_util', 'Scale-up util (0-1)', 'add a worker when utilization stays above this'],
  ['scale_down_util', 'Scale-down util (0-1)', 'drop a worker when utilization stays below this'],
  ['up_stable_seconds', 'Up after (s)', 'utilization must stay high this long'],
  ['down_stable_seconds', 'Down after (s)', 'utilization must stay low this long'],
  ['cooldown_seconds', 'Cooldown (s)', 'minimum gap between scaling steps'],
]

const pct = (u) => (u == null ? '—' : `${Math.round(u * 100)}%`)

export default function ScalingTab({ systemId, node, onClose, onBusyChange }) {
  const [state, setState] = useState(null) // this base's { scaler, policy } entry
  const [form, setForm] = useState(null) // policy form; seeded once from the registry
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  const [savedAt, setSavedAt] = useState(0)
  const [workerCount, setWorkerCount] = useState(1 + (node.replicas?.instances?.length || 0))

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // Poll the aggregate state; seed the policy form from the on-disk file once.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(STATE_URL(systemId))
        const data = await res.json()
        if (cancelled || !data.ok) return
        const s = data.nodes[node.id] || null
        setState(s)
        if (s?.policy) {
          setForm((f) => f || {
            enabled: s.policy.enabled !== false,
            min: s.policy.min ?? 1,
            max: s.policy.max ?? 8,
            scale_up_util: s.policy.scale_up_util ?? 0.8,
            scale_down_util: s.policy.scale_down_util ?? 0.3,
            up_stable_seconds: s.policy.up_stable_seconds ?? 15,
            down_stable_seconds: s.policy.down_stable_seconds ?? 60,
            cooldown_seconds: s.policy.cooldown_seconds ?? 90,
          })
        }
      } catch {
        /* keep last good */
      }
    }
    tick()
    const t = setInterval(tick, 1500)
    return () => { cancelled = true; clearInterval(t) }
  }, [systemId, node.id])

  const live = state?.scaler
  const enabled = form ? form.enabled : state?.policy?.enabled !== false
  const currentTotal = 1 + (node.replicas?.instances?.length || 0)
  const wc = Number(workerCount)
  const workerCountErr = !Number.isInteger(wc) || wc < 1 || wc > 8

  async function savePolicy() {
    if (!form) return
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/custom/llm-worker/policy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          node: node.id,
          enabled: !!form.enabled,
          min: Number(form.min),
          max: Number(form.max),
          scale_up_util: Number(form.scale_up_util),
          scale_down_util: Number(form.scale_down_util),
          up_stable_seconds: Number(form.up_stable_seconds),
          down_stable_seconds: Number(form.down_stable_seconds),
          cooldown_seconds: Number(form.cooldown_seconds),
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setSavedAt(Date.now())
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function scaleWorkers() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/custom/llm-worker/scale', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: node.id, instances: wc }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  return (
    <div>
      <p className="sim-desc">
        <code>{node.id}</code> runs as N worker containers under one service id.{' '}
        <code>{node.id}-scaler</code> watches the group's batch utilization (active sequences /
        total <code>max_active</code>) and drives the count automatically within the policy below.
      </p>

      {/* Live scaler state */}
      <div className="form-section">
        <div className="form-section-head"><span>Live scaler state</span></div>
        {!live ? (
          <p className="sim-desc">scaler not reachable yet… {state?.policy ? '(container may still be building)' : ''}</p>
        ) : (
          <>
            <div className="form-row">
              <span>Utilization</span>
              <code>{pct(live.utilization)} · {live.active}/{live.capacity} active</code>
            </div>
            <div className="form-row">
              <span>Workers</span>
              <code>{live.current} live · desired {live.desired ?? '—'}</code>
            </div>
            {(live.members || []).map((m) => (
              <div className="form-row" key={m.id}>
                <span>{m.id}</span>
                <code>{m.reachable ? `${m.active}/${m.max_active} active` : 'unreachable'}</code>
              </div>
            ))}
            {live.lastDecision && (
              <div className="form-row">
                <span>Last decision</span>
                <code>{live.lastDecision.from}→{live.lastDecision.to} · {live.lastDecision.reason}</code>
              </div>
            )}
            {live.error && <small className="field-error">{live.error}</small>}
          </>
        )}
      </div>

      {/* Scaling policy (live, no rebuild) */}
      <div className="form-section">
        <div className="form-section-head"><span>Scaling policy</span></div>
        {!form ? (
          <p className="sim-desc">loading policy…</p>
        ) : (
          <>
            <label className="form-check">
              <input
                type="checkbox"
                checked={!!form.enabled}
                onChange={(e) => setForm({ ...form, enabled: e.target.checked })}
                disabled={busy}
              />
              <span>Autoscale — apply the scaler's desired count automatically</span>
            </label>
            {POLICY_FIELDS.map(([key, label, hint]) => (
              <label className="form-row" key={key} title={hint}>
                <span>{label}</span>
                <input
                  type="number"
                  min={0}
                  step={key.endsWith('_util') ? 0.05 : 1}
                  value={form[key]}
                  onChange={(e) => setForm({ ...form, [key]: e.target.value })}
                  disabled={busy}
                />
              </label>
            ))}
            <div className="modal-actions">
              <button type="button" className="primary" onClick={savePolicy} disabled={busy}>
                Save policy
              </button>
              {savedAt > 0 && Date.now() - savedAt < 4000 && <span className="sim-desc">applied live — no rebuild</span>}
            </div>
            <p className="sim-desc">
              Applies live (the scaler mtime-polls its mounted policy). Utilization is bursty — a
              batch drains in seconds — so the stability windows and cooldown are what keep the
              count from whipsawing; tune them rather than the thresholds first.
            </p>
          </>
        )}
      </div>

      {/* Manual worker count */}
      <div className="form-section">
        <div className="form-section-head"><span>Workers</span></div>
        <p className="sim-desc">
          Instances (<code>{node.id}-2…N</code>) share this worker's build, tunables, hook and token
          stream — <strong>no load balancer</strong>; callers reach them over gRPC
          (<code>{node.id}-i:50051</code>) and do their own forwarding across the group.
        </p>
        <label className="form-row">
          <span>Total workers</span>
          <input
            type="number"
            min={1}
            max={8}
            value={workerCount}
            onChange={(e) => setWorkerCount(e.target.value)}
            disabled={busy}
          />
        </label>
        {workerCountErr ? (
          <small className="field-error">Between 1 and 8 workers</small>
        ) : (
          <small className="form-hint">
            {wc >= 2 ? `Creates: ${node.id} + ${node.id}-2…${wc}` : '1 = a single worker (no replicas)'}
            {enabled ? ' — autoscaling is ON, so the scaler will reconcile this back toward its own desired count; disable it above to hold a manual size.' : ''}
          </small>
        )}
        <div className="modal-actions">
          <button
            type="button"
            className="primary"
            onClick={scaleWorkers}
            disabled={busy || workerCountErr || wc === currentTotal}
          >
            {busy ? 'Applying… (building instances can take a minute)' : 'Apply'}
          </button>
        </div>
      </div>

      {error && <p className="modal-error">{error}</p>}
    </div>
  )
}
