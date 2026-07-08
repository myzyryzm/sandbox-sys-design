// LLM Worker — frontend module.
//
// The rendering registration for the custom service type: the Edit tab (live state +
// live tunables + on_cache_evict hook authoring), the custom diagram body (per-sequence
// decode progress), and the runtime poll. The worker→redis edge is STATIC in the
// manifest (written by onAdd), so no diagramEdges hook is needed.
import WorkerTab from './WorkerTab.jsx'
import { DiagramBody, bodyHeight } from './DiagramBody.jsx'

export default {
  serviceTypes: ['llm_worker'],

  // Tab injected into NodeEditModal between the kind tabs and Shutdown/Delete.
  editTabs(node) {
    if (node.service_type === 'llm_worker') {
      return [{ id: 'llm', label: 'LLM Worker', Component: WorkerTab }]
    }
    return []
  },

  // Live runtime poll: App fetches this and merges { nodes: { [id]: { live, config,
  // hook } } } into the customState map (consumed by the tab + diagram body).
  runtime: {
    url: (sys) => `/api/custom/llm-worker/state?system=${encodeURIComponent(sys)}`,
  },

  // /llm/state is the control-plane read the tab + diagram poll — internal (off the
  // load balancer's public surface) and locked from edit/delete. Everything else
  // falls through to the generic classification (/health, /metrics, …).
  endpointPolicy(node, p) {
    if (node.service_type === 'llm_worker' && p === '/llm/state') {
      return { visibility: 'internal', locked: true }
    }
    return null
  },

  DiagramBody,
  diagramHeight: bodyHeight,
}
