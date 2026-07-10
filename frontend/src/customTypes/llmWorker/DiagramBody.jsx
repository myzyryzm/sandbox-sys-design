// LLM Worker — custom diagram body: one progress bar per active sequence (generated /
// target_len fills live as tokens stream out) plus a one-line batch/cache summary.
// The scaler card shows the group's utilization + desired count and its latest scaling
// decision. Rendered inside the node <g> via the registry seam, so SystemDiagram stays
// type-agnostic. Runtime here is the state route's per-node entry: { live, config,
// hook } for a worker, { live } (the scaler's /state) for the scaler.

const PADX = 10
const BAR_H = 5
const GAP = 3
const LABEL_H = 16
const TOP_PAD = 4

// Height (px) this node's custom body needs. MUST match what DiagramBody draws so
// SystemDiagram reserves exactly the right space.
export function bodyHeight(node, runtime) {
  if (node.service_type === 'llm_scaler') {
    if (!runtime?.live) return 0
    return runtime.live.lastDecision ? 2 * LABEL_H : LABEL_H
  }
  if (!runtime?.live) return 0
  const n = runtime.live.active_count || 0
  return TOP_PAD + n * (BAR_H + GAP) + LABEL_H
}

export function DiagramBody({ node, runtime, width, top }) {
  if (node.service_type === 'llm_scaler') {
    const live = runtime?.live
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
  const live = runtime?.live
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
