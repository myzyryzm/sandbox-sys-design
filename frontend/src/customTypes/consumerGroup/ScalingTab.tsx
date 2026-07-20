// Kafka Consumer Group — custom Edit tab (embedded body; NodeEditModal owns the chrome).
//
// Three sections:
//   1. Live group state — lag, partitions, live members + their assigned partitions,
//      the scaler's desired count and its latest decision (from the scaler's /state,
//      polled through the aggregate route).
//   2. Scaling policy — min/max, lag thresholds, stability windows, cooldown, enabled —
//      written to the group's mounted scaler.json via the policy route: the scaler
//      container mtime-polls it, so edits apply LIVE with no rebuild.
//   3. Members — the manual "set member count" input (same idempotent reconciler the
//      autoscaler applies through). With autoscaling enabled a manual count is soon
//      reconciled back to the policy's desired — the tab says so.
import { useEffect, useState } from 'react'
import type { EditTabProps } from '../../types/customTypes'
import type { ConsumerGroupState } from './DiagramBody'

const STATE_URL = (sys: string) => `/api/custom/consumer-group/state?system=${encodeURIComponent(sys)}`

// The policy form's numeric fields (enabled is the checkbox); inputs hold the raw
// string while the user types, Number()-ed on save.
type PolicyNumField =
  | 'min'
  | 'max'
  | 'scale_up_lag'
  | 'scale_down_lag'
  | 'up_stable_seconds'
  | 'down_stable_seconds'
  | 'cooldown_seconds'

interface PolicyForm extends Record<PolicyNumField, number | string> {
  enabled: boolean
}

interface StateResponse {
  ok: boolean
  nodes: Record<string, ConsumerGroupState>
}

const POLICY_FIELDS: Array<[PolicyNumField, string, string]> = [
  ['min', 'Min members', 'never fewer than this'],
  ['max', 'Max members', 'never more (also capped by the topic’s partition count)'],
  ['scale_up_lag', 'Scale-up lag', 'add a member when total lag stays above this'],
  ['scale_down_lag', 'Scale-down lag', 'drop a member when total lag stays below this'],
  ['up_stable_seconds', 'Up after (s)', 'lag must stay high this long'],
  ['down_stable_seconds', 'Down after (s)', 'lag must stay low this long'],
  ['cooldown_seconds', 'Cooldown (s)', 'minimum gap between scaling steps'],
]

export default function ScalingTab({ systemId, node, onClose, onBusyChange }: EditTabProps) {
  const [state, setState] = useState<ConsumerGroupState | null>(null) // this base's { live, policy } entry
  const [form, setForm] = useState<PolicyForm | null>(null) // policy form; seeded once from the registry
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [savedAt, setSavedAt] = useState(0)
  const [memberCount, setMemberCount] = useState<number | string>(1 + (node.replicas?.instances?.length || 0))

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // Poll the aggregate state; seed the policy form from the on-disk file once.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(STATE_URL(systemId))
        const data = (await res.json()) as StateResponse
        if (cancelled || !data.ok) return
        const s = data.nodes[node.id] || null
        setState(s)
        if (s?.policy) {
          setForm((f) => f || {
            enabled: s.policy!.enabled !== false,
            min: s.policy!.min ?? 1,
            max: s.policy!.max ?? 8,
            scale_up_lag: s.policy!.scale_up_lag ?? 1000,
            scale_down_lag: s.policy!.scale_down_lag ?? 100,
            up_stable_seconds: s.policy!.up_stable_seconds ?? 15,
            down_stable_seconds: s.policy!.down_stable_seconds ?? 60,
            cooldown_seconds: s.policy!.cooldown_seconds ?? 90,
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

  const live = state?.live
  const enabled = form ? form.enabled : state?.policy?.enabled !== false
  const currentTotal = 1 + (node.replicas?.instances?.length || 0)
  const mc = Number(memberCount)
  const memberCountErr = !Number.isInteger(mc) || mc < 1 || mc > 8

  async function savePolicy() {
    if (!form) return
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/custom/consumer-group/policy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          node: node.id,
          enabled: !!form.enabled,
          min: Number(form.min),
          max: Number(form.max),
          scale_up_lag: Number(form.scale_up_lag),
          scale_down_lag: Number(form.scale_down_lag),
          up_stable_seconds: Number(form.up_stable_seconds),
          down_stable_seconds: Number(form.down_stable_seconds),
          cooldown_seconds: Number(form.cooldown_seconds),
        }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setSavedAt(Date.now())
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  async function scaleMembers() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/custom/consumer-group/scale', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: node.id, instances: mc }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setError((err as Error).message)
      setBusy(false)
    }
  }

  return (
    <div>
      <p className="sim-desc">
        <code>{node.id}</code> runs as N member containers under one service id, all consuming with
        Kafka group <code>{node.consumerGroup?.groupId}</code> — the broker itself rebalances the
        topic's partitions across members. <code>{node.id}-scaler</code> watches the group's lag and
        drives the count automatically within the policy below.
      </p>

      {/* Live group state */}
      <div className="form-section">
        <div className="form-section-head">
          <span>Live group state</span>
          {live?.paused && <span className="scenario-pending">consumers paused</span>}
        </div>
        {!live ? (
          <p className="sim-desc">scaler not reachable yet… {state?.live === null && state?.policy ? '(container may still be building)' : ''}</p>
        ) : (
          <>
            <div className="form-row">
              <span>Topic</span>
              <code>{live.topic ?? '—'} · {live.partitions} partition{live.partitions === 1 ? '' : 's'}</code>
            </div>
            <div className="form-row">
              <span>Lag</span>
              <code>{live.lag ?? '—'}</code>
            </div>
            <div className="form-row">
              <span>Members</span>
              <code>{live.current} live · desired {live.desired ?? '—'}</code>
            </div>
            {(live.members || []).map((m) => (
              <div className="form-row" key={m.clientId || m.host}>
                <span>{m.clientId || '(unknown)'}</span>
                <code>
                  {Array.isArray(m.partitions) && m.partitions.length
                    ? `partitions ${m.partitions.join(', ')}`
                    : 'no partitions (idle or rebalancing)'}
                </code>
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
              Applies live (the scaler mtime-polls its mounted policy). Scale-up is suppressed while
              the cluster's consumers are paused — lag grows by design then.
            </p>
          </>
        )}
      </div>

      {/* Manual member count */}
      <div className="form-section">
        <div className="form-section-head"><span>Members</span></div>
        <label className="form-row">
          <span>Total members</span>
          <input
            type="number"
            min={1}
            max={8}
            value={memberCount}
            onChange={(e) => setMemberCount(e.target.value)}
            disabled={busy}
          />
        </label>
        {memberCountErr ? (
          <small className="field-error">Between 1 and 8 members</small>
        ) : (
          <small className="form-hint">
            {mc >= 2 ? `Creates: ${node.id} + ${node.id}-2…${mc}` : '1 = a single member (no replicas)'}
            {enabled ? ' — autoscaling is ON, so the scaler will reconcile this back toward its own desired count; disable it above to hold a manual size.' : ''}
          </small>
        )}
        <div className="modal-actions">
          <button
            type="button"
            className="primary"
            onClick={scaleMembers}
            disabled={busy || memberCountErr || mc === currentTotal}
          >
            {busy ? 'Applying… (building instances can take a minute)' : 'Apply'}
          </button>
        </div>
      </div>

      {error && <p className="modal-error">{error}</p>}
    </div>
  )
}
