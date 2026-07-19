// Download Coordinator — frontend module (owns both service_types).
//
// The rendering registration for the custom service type: the Edit tabs it injects, its
// custom diagram body (bitmap grid + aggregate %), its live chain/source edges (star →
// mesh), and its runtime poll. SystemDiagram + NodeEditModal + App stay type-agnostic and
// drive all of this through these hooks.
import type { Manifest } from '../../types/manifest'
import type { CustomStateMap, CustomTypeModule, DiagramEdgeSpec } from '../../types/customTypes'
import CoordinatorTab from './CoordinatorTab'
import { DiagramBody, bodyHeight } from './DiagramBody'
import type { DcNodeState } from './DiagramBody'

// Live chunk-source edges: distinct (source → puller) among the most recent transfers the
// coordinator recorded. Early on every edge originates at the coordinator (a star); as
// workers finish chunks and seed peers, worker→worker edges appear (the mesh).
function diagramEdges({ manifest, customState }: { manifest: Manifest; customState: CustomStateMap }): DiagramEdgeSpec[] {
  const edges: DiagramEdgeSpec[] = []
  const seen = new Set<string>()
  for (const node of manifest.nodes) {
    if (node.service_type !== 'download_coordinator') continue
    const recent = (customState[node.id] as DcNodeState | undefined)?.recent
    if (!recent || !recent.length) continue
    for (const r of recent.slice(-40)) {
      if (!r || r.from === r.to) continue
      const key = `${r.from}->${r.to}`
      if (seen.has(key)) continue
      seen.add(key)
      edges.push({ from: r.from, to: r.to, className: 'custom-edge', label: `chunk source: ${r.from} → ${r.to}` })
    }
  }
  return edges
}

export default {
  serviceTypes: ['download_coordinator', 'download_worker'],

  // Tabs injected into NodeEditModal between the kind tabs and Shutdown/Delete.
  editTabs(node) {
    if (node.service_type === 'download_coordinator') {
      return [{ id: 'distribution', label: 'Distribution', Component: CoordinatorTab }]
    }
    if (node.service_type === 'download_worker') {
      return [{ id: 'worker', label: 'Worker', Component: CoordinatorTab }]
    }
    return []
  },

  // Live runtime poll: App fetches this and merges { nodes: { [id]: state } } into the
  // customState map (consumed by the tab + the diagram body/edges below).
  runtime: {
    url: (sys) => `/api/custom/download-coordinator/state?system=${encodeURIComponent(sys)}`,
  },

  // Endpoint visibility/lock policy for this type's own routes (consulted by the
  // generic endpointPolicy seam). None of the /dc/* routes are an external client
  // API — they're the control plane the Distribution tab + diagram drive:
  //   - /dc/distribute is owned by the tab → hidden entirely (and locked).
  //   - /dc/sources, /dc/state, /dc/worker are internal → off the load balancer,
  //     shown in the Endpoints tab badged "internal" and locked from edit/delete.
  // Returning null falls through to the generic classification (/health, etc.).
  endpointPolicy(node, p) {
    if (node.service_type === 'download_coordinator') {
      if (p === '/dc/distribute') return { visibility: 'hidden', locked: true }
      if (p === '/dc/sources' || p === '/dc/state') return { visibility: 'internal', locked: true }
    } else if (node.service_type === 'download_worker') {
      if (p === '/dc/worker') return { visibility: 'internal', locked: true }
    }
    return null
  },

  // Custom diagram rendering, all keyed off service_type via the registry.
  DiagramBody,
  diagramHeight: bodyHeight,
  diagramEdges,
} satisfies CustomTypeModule
