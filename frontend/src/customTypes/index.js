// Frontend registry of custom service types — the rendering half of the Part-1
// mechanism (the backend half is server/customTypes/). Keyed by manifest
// `service_type`. Consumed by:
//   - CreateService.jsx  → lists addable types (via GET /api/custom-types)
//   - NodeEditModal.jsx  → injects custom Edit tabs (editTabs)
//   - SystemDiagram.jsx  → renders a custom node body + edges (DiagramBody/diagramEdges)
//   - App.jsx            → polls each type's runtime endpoint into a customState map
//
// A module may own several related service_types (e.g. the coordinator + its workers).
// Adding the next type = drop a module here and list it below; no seam code changes.
import consumerGroup from './consumerGroup/index.jsx'
import downloadCoordinator from './downloadCoordinator/index.jsx'
import llmWorker from './llmWorker/index.jsx'

const MODULES = [consumerGroup, downloadCoordinator, llmWorker]

const TYPES = {}
for (const m of MODULES) {
  for (const st of m.serviceTypes) TYPES[st] = m
}

export const CUSTOM_TYPES = TYPES

// The module that renders a given node, or null for a plain (non-custom) node.
export function customTypeOf(node) {
  return node && node.service_type ? TYPES[node.service_type] || null : null
}

// Distinct runtime polls across all registered modules, for App's generic customState
// fetch. Each is { url(systemId) -> string } returning { ok, nodes: { [id]: state } }.
export const CUSTOM_RUNTIMES = [...new Set(MODULES)].filter((m) => m.runtime).map((m) => m.runtime)
