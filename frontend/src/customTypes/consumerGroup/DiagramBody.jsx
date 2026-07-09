// Kafka Consumer Group — custom diagram body.
//
// Member cards (base + instances) show the partitions Kafka currently assigns them —
// THE live rebalancing view: scale the group and watch the numbers redistribute.
// The scaler card shows its latest scaling decision. Runtime here is the state route's
// per-node entry: { live?, policy?, partitions? }.

const LINE_H = 16

// Height (px) this node's custom body needs. MUST match what DiagramBody draws so
// SystemDiagram reserves exactly the right space.
export function bodyHeight(node, runtime) {
  if (node.service_type === 'consumer_scaler') return runtime?.live?.lastDecision ? LINE_H : 0
  return Array.isArray(runtime?.partitions) ? LINE_H : 0
}

export function DiagramBody({ node, runtime, width, top }) {
  if (node.service_type === 'consumer_scaler') {
    const d = runtime?.live?.lastDecision
    if (!d) return null
    return (
      <text x={width / 2} y={top + 12} className="dc-node-label">
        {`scaled ${d.from}→${d.to}`}
      </text>
    )
  }
  const parts = runtime?.partitions
  if (!Array.isArray(parts)) return null
  const label = parts.length ? `partitions: ${parts.join(', ')}` : 'partitions: — (rebalancing)'
  return (
    <text x={width / 2} y={top + 12} className="dc-node-label">
      {label}
    </text>
  )
}
