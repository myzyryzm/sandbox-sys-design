// Kafka Consumer Group — custom diagram body.
//
// Member cards (base + instances) show the partitions Kafka currently assigns them —
// THE live rebalancing view: scale the group and watch the numbers redistribute.
// The scaler card shows its latest scaling decision. Runtime here is the state route's
// per-node entry: { live?, policy?, partitions? }.
import type { ManifestNode } from '../../types/manifest'
import type { CustomNodeState, DiagramBodyProps } from '../../types/customTypes'

// ─── The state route's per-node runtime entry this module reads ─────────────

export interface ConsumerGroupMember {
  clientId?: string
  host?: string
  partitions?: number[]
}

export interface ConsumerGroupDecision {
  from: number
  to: number
  reason?: string
}

// The scaler's /state (also the scaler node's own `live` entry).
export interface ConsumerGroupLive {
  paused?: boolean
  topic?: string | null
  partitions?: number
  lag?: number | null
  current?: number
  desired?: number | null
  members?: ConsumerGroupMember[]
  lastDecision?: ConsumerGroupDecision | null
  error?: string
}

// The group's mounted scaler.json policy.
export interface ConsumerGroupPolicy {
  enabled?: boolean
  min?: number
  max?: number
  scale_up_lag?: number
  scale_down_lag?: number
  up_stable_seconds?: number
  down_stable_seconds?: number
  cooldown_seconds?: number
}

// { live?, policy? } on the base/scaler entries; { partitions? } on each member.
export interface ConsumerGroupState {
  live?: ConsumerGroupLive | null
  policy?: ConsumerGroupPolicy | null
  partitions?: number[]
}

const LINE_H = 16

// Height (px) this node's custom body needs. MUST match what DiagramBody draws so
// SystemDiagram reserves exactly the right space.
export function bodyHeight(node: ManifestNode, runtime: CustomNodeState | undefined): number {
  const st = runtime as ConsumerGroupState | undefined
  if (node.service_type === 'consumer_scaler') return st?.live?.lastDecision ? LINE_H : 0
  return Array.isArray(st?.partitions) ? LINE_H : 0
}

export function DiagramBody({ node, runtime, width, top }: DiagramBodyProps) {
  const st = runtime as ConsumerGroupState | undefined
  if (node.service_type === 'consumer_scaler') {
    const d = st?.live?.lastDecision
    if (!d) return null
    return (
      <text x={width / 2} y={top + 12} className="dc-node-label">
        {`scaled ${d.from}→${d.to}`}
      </text>
    )
  }
  const parts = st?.partitions
  if (!Array.isArray(parts)) return null
  const label = parts.length ? `partitions: ${parts.join(', ')}` : 'partitions: — (rebalancing)'
  return (
    <text x={width / 2} y={top + 12} className="dc-node-label">
      {label}
    </text>
  )
}
