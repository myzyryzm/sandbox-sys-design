// LLM Worker — frontend module.
//
// The rendering registration for the custom service type: the Edit tabs (live state +
// live tunables + on_cache_evict hook authoring, plus the Scaling tab: scaler state,
// autoscaling policy and manual worker count), the custom diagram body (per-sequence
// decode progress; the scaler card shows utilization + its latest decision), and the
// runtime poll. The worker→redis edge is STATIC in the manifest (written by onAdd),
// so no diagramEdges hook is needed. The `<name>-scaler` sidecar node (scalerOf)
// renders at the top of the group's stack carrying the group's Edit button.
import WorkerTab from './WorkerTab.jsx'
import ScalingTab from './ScalingTab.jsx'
import { DiagramBody, bodyHeight } from './DiagramBody.jsx'

export default {
  serviceTypes: ['llm_worker', 'llm_scaler'],

  // Workers stack into a dotted worker-group box (SystemDiagram's generalized
  // group rendering keys off this predicate).
  workerGroup: (node) => node.service_type === 'llm_worker',

  // Tabs injected into NodeEditModal between the kind tabs and Shutdown/Delete. Only
  // the group BASE gets them (instances have no Edit button; the scaler's policy is
  // edited from its base's Scaling tab).
  editTabs(node) {
    if (node.service_type === 'llm_worker' && !node.instanceOf) {
      return [
        { id: 'llm', label: 'LLM Worker', Component: WorkerTab },
        { id: 'scaling', label: 'Scaling', Component: ScalingTab },
      ]
    }
    return []
  },

  // Live runtime poll: App fetches this and merges { nodes: { [id]: { live, config,
  // hook, scaler?, policy? } } } into the customState map (consumed by the tabs +
  // diagram bodies; the scaler node's entry is { live }).
  runtime: {
    url: (sys) => `/api/custom/llm-worker/state?system=${encodeURIComponent(sys)}`,
  },

  // /llm/state (worker) and /state (scaler) are the control-plane reads the tabs +
  // apply loop poll — internal (off the load balancer's public surface) and locked
  // from edit/delete. Everything else falls through to the generic classification
  // (/health, /metrics, …).
  endpointPolicy(node, p) {
    if (node.service_type === 'llm_worker' && p === '/llm/state') {
      return { visibility: 'internal', locked: true }
    }
    if (node.service_type === 'llm_scaler' && p === '/state') {
      return { visibility: 'internal', locked: true }
    }
    return null
  },

  DiagramBody,
  diagramHeight: bodyHeight,
}
