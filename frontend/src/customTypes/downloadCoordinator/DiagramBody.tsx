// Download Coordinator — custom diagram body: a per-node bitmap grid (held chunks
// fill in, out-of-order arrival visible) plus a one-line label (aggregate % for the
// coordinator, held/total for a worker). Rendered inside the node <g> via the registry
// seam, so SystemDiagram stays type-agnostic.
import type { ReactElement } from 'react'
import type { ManifestNode } from '../../types/manifest'
import type { CustomNodeState, DiagramBodyProps } from '../../types/customTypes'

// ─── The state route's per-node runtime entry this module reads ─────────────

// One recorded chunk transfer (source node → pulling node).
export interface DcTransfer {
  from: string
  to: string
}

export interface DcNodeState {
  role?: 'coordinator' | 'worker' | (string & {})
  status?: string
  phase?: string
  error?: string | null
  ready?: boolean
  alive?: boolean
  complete?: boolean
  held?: number
  chunk_count?: number
  file_size?: number
  progress?: number | null
  bitmap?: number[] | null
  // Coordinator only: the most recent transfers (drives the star → mesh edges).
  recent?: DcTransfer[]
}

const PADX = 10
const GAP = 2
const MAXCOLS = 12
const LABEL_H = 16
const TOP_PAD = 4

function chunkCount(runtime: DcNodeState | undefined): number {
  if (!runtime) return 0
  return runtime.chunk_count || (runtime.bitmap ? runtime.bitmap.length : 0)
}

function grid(count: number, width: number) {
  const cols = Math.min(count, MAXCOLS)
  const cell = Math.max(4, Math.floor((width - 2 * PADX - (cols - 1) * GAP) / cols))
  const rows = Math.ceil(count / cols)
  return { cols, cell, rows, gridH: rows * cell + (rows - 1) * GAP }
}

// Height (px) this node's custom body needs for the given runtime. MUST match what
// DiagramBody draws so SystemDiagram reserves exactly the right space.
export function bodyHeight(node: ManifestNode, runtime: CustomNodeState | undefined, width: number): number {
  const st = runtime as DcNodeState | undefined
  const count = chunkCount(st)
  if (!count) {
    // Idle coordinator shows a one-line hint; an idle worker shows nothing.
    return st?.role === 'coordinator' ? LABEL_H : 0
  }
  const { gridH } = grid(count, width)
  return TOP_PAD + gridH + LABEL_H
}

export function DiagramBody({ runtime, width, top }: DiagramBodyProps) {
  const st = runtime as DcNodeState | undefined
  const count = chunkCount(st)
  if (!count) {
    if (st?.role === 'coordinator') {
      return <text x={width / 2} y={top + 11} className="dc-node-hint">no distribution yet</text>
    }
    return null
  }
  // count > 0 implies a runtime entry exists.
  const { cols, cell, gridH } = grid(count, width)
  const bm = st!.bitmap || []
  const cells: ReactElement[] = []
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
    st!.progress != null
      ? Math.round(st!.progress * 100)
      : Math.round(((st!.held || 0) / count) * 100)
  const label =
    st!.role === 'coordinator'
      ? `${pct}% distributed`
      : `${st!.held || 0}/${count}${st!.complete ? ' ✓' : st!.alive === false ? ' · down' : ''}`
  return (
    <g>
      {cells}
      <text x={width / 2} y={top + TOP_PAD + gridH + 12} className="dc-node-label">{label}</text>
    </g>
  )
}
