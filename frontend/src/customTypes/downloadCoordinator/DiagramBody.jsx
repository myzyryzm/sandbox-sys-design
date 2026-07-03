// Download Coordinator — custom diagram body: a per-node bitmap grid (held chunks
// fill in, out-of-order arrival visible) plus a one-line label (aggregate % for the
// coordinator, held/total for a worker). Rendered inside the node <g> via the registry
// seam, so SystemDiagram stays type-agnostic.

const PADX = 10
const GAP = 2
const MAXCOLS = 12
const LABEL_H = 16
const TOP_PAD = 4

function chunkCount(runtime) {
  if (!runtime) return 0
  return runtime.chunk_count || (runtime.bitmap ? runtime.bitmap.length : 0)
}

function grid(count, width) {
  const cols = Math.min(count, MAXCOLS)
  const cell = Math.max(4, Math.floor((width - 2 * PADX - (cols - 1) * GAP) / cols))
  const rows = Math.ceil(count / cols)
  return { cols, cell, rows, gridH: rows * cell + (rows - 1) * GAP }
}

// Height (px) this node's custom body needs for the given runtime. MUST match what
// DiagramBody draws so SystemDiagram reserves exactly the right space.
export function bodyHeight(node, runtime, width) {
  const count = chunkCount(runtime)
  if (!count) {
    // Idle coordinator shows a one-line hint; an idle worker shows nothing.
    return runtime?.role === 'coordinator' ? LABEL_H : 0
  }
  const { gridH } = grid(count, width)
  return TOP_PAD + gridH + LABEL_H
}

export function DiagramBody({ runtime, width, top }) {
  const count = chunkCount(runtime)
  if (!count) {
    if (runtime?.role === 'coordinator') {
      return <text x={width / 2} y={top + 11} className="dc-node-hint">no distribution yet</text>
    }
    return null
  }
  const { cols, cell, gridH } = grid(count, width)
  const bm = runtime.bitmap || []
  const cells = []
  for (let i = 0; i < count; i++) {
    const r = Math.floor(i / cols)
    const c = i % cols
    cells.push(
      <rect
        key={i}
        x={PADX + c * (cell + GAP)}
        y={top + TOP_PAD + r * (cell + GAP)}
        width={cell}
        height={cell}
        rx={1}
        className={bm[i] === 1 ? 'dc-node-cell held' : 'dc-node-cell'}
      />,
    )
  }
  const pct =
    runtime.progress != null
      ? Math.round(runtime.progress * 100)
      : Math.round(((runtime.held || 0) / count) * 100)
  const label =
    runtime.role === 'coordinator'
      ? `${pct}% distributed`
      : `${runtime.held || 0}/${count}${runtime.complete ? ' ✓' : runtime.alive === false ? ' · down' : ''}`
  return (
    <g>
      {cells}
      <text x={width / 2} y={top + TOP_PAD + gridH + 12} className="dc-node-label">{label}</text>
    </g>
  )
}
