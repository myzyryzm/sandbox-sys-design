// LLM Worker — custom diagram body: one progress bar per active sequence (generated /
// target_len fills live as tokens stream out) plus a one-line batch/cache summary.
// The scaler card shows the group's utilization + desired count and its latest scaling
// decision. Rendered inside the node <g> via the registry seam, so SystemDiagram stays
// type-agnostic. Runtime here is the state route's per-node entry: { live, config,
// hook } for a worker, { live } (the scaler's /state) for the scaler.
import type { ManifestNode } from '../../types/manifest'
import type { CustomNodeState, DiagramBodyProps } from '../../types/customTypes'

// ─── The state route's per-node runtime entries this module reads ───────────

// One in-flight sequence of the worker's continuous batch.
export interface LlmSeq {
  seq_id: number | string
  user_message_id?: number | string
  chat?: number | string | null
  prefilled?: boolean
  generated?: number
  target_len?: number
}

// The worker's own live state (its /llm/state).
export interface LlmWorkerLive {
  active_count?: number
  cached_count?: number
  config?: { max_active?: number } | null
  active?: LlmSeq[]
  cached?: Array<{ chat?: number | string; age_s?: number }>
}

// The scaler's /state (the `scaler` entry on the base node AND the scaler node's `live`).
export interface LlmScalerLive {
  utilization?: number | null
  active?: number
  capacity?: number
  current?: number
  desired?: number | null
  members?: Array<{ id?: string; reachable?: boolean; active?: number; max_active?: number }>
  lastDecision?: { from: number; to: number; reason?: string } | null
  error?: string
}

// The group's mounted scaler.json policy.
export interface LlmScalerPolicy {
  enabled?: boolean
  min?: number
  max?: number
  scale_up_util?: number
  scale_down_util?: number
  up_stable_seconds?: number
  down_stable_seconds?: number
  cooldown_seconds?: number
}

// The worker's hook.json entry (on_cache_evict).
export interface LlmHookEntry {
  description?: string
  implemented?: boolean
  conversationId?: string
}

// A worker node's state-route entry.
export interface LlmWorkerState {
  live?: LlmWorkerLive | null
  config?: { ttl_seconds?: number; max_active?: number; chat_db?: string | null } | null
  hook?: LlmHookEntry | null
  scaler?: LlmScalerLive | null
  policy?: LlmScalerPolicy | null
}

// The scaler node's state-route entry.
export interface LlmScalerState {
  live?: LlmScalerLive | null
}

const PADX = 10
const BAR_H = 5
const GAP = 3
const LABEL_H = 16
const TOP_PAD = 4

// Height (px) this node's custom body needs. MUST match what DiagramBody draws so
// SystemDiagram reserves exactly the right space.
export function bodyHeight(node: ManifestNode, runtime: CustomNodeState | undefined): number {
  if (node.service_type === 'llm_scaler') {
    const st = runtime as LlmScalerState | undefined
    if (!st?.live) return 0
    return st.live.lastDecision ? 2 * LABEL_H : LABEL_H
  }
  const st = runtime as LlmWorkerState | undefined
  if (!st?.live) return 0
  const n = st.live.active_count || 0
  return TOP_PAD + n * (BAR_H + GAP) + LABEL_H
}

export function DiagramBody({ node, runtime, width, top }: DiagramBodyProps) {
  if (node.service_type === 'llm_scaler') {
    const live = (runtime as LlmScalerState | undefined)?.live
    if (!live) return null
    const util = live.utilization == null ? '—' : `${Math.round(live.utilization * 100)}%`
    return (
      <g>
        <text x={width / 2} y={top + 12} className="dc-node-label">
          {`util ${util} · desired ${live.desired ?? '—'}`}
        </text>
        {live.lastDecision && (
          <text x={width / 2} y={top + LABEL_H + 12} className="dc-node-label">
            {`scaled ${live.lastDecision.from}→${live.lastDecision.to}`}
          </text>
        )}
      </g>
    )
  }
  const live = (runtime as LlmWorkerState | undefined)?.live
  if (!live) return null
  const active = live.active || []
  const w = width - 2 * PADX
  const bars = active.map((s, i) => {
    const frac = s.target_len ? Math.min(1, (s.generated || 0) / s.target_len) : 0
    const y = top + TOP_PAD + i * (BAR_H + GAP)
    return (
      <g key={s.seq_id}>
        <rect x={PADX} y={y} width={w} height={BAR_H} rx={2} className="llm-node-bar" />
        <rect
          x={PADX}
          y={y}
          width={Math.max(2, Math.round(w * frac))}
          height={BAR_H}
          rx={2}
          className={s.prefilled ? 'llm-node-bar fill' : 'llm-node-bar prefill'}
        />
      </g>
    )
  })
  const label = `batch ${live.active_count}/${live.config?.max_active ?? '?'} · cached ${live.cached_count}`
  return (
    <g>
      {bars}
      <text
        x={width / 2}
        y={top + TOP_PAD + active.length * (BAR_H + GAP) + 12}
        className="dc-node-label"
      >
        {label}
      </text>
    </g>
  )
}
