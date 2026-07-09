// Kafka Consumer Group — frontend module.
//
// The rendering registration for the consumer-group custom type (created from a Kafka
// node's Consumers tab, not the add-service modal): the Scaling tab (live group state +
// the scaler's policy + manual member count), the per-member diagram body (assigned
// partitions), and the runtime poll. The base renders as an LLM-worker-style group —
// dotted box, member cards stacked in columns — via the shared workerGroup predicate
// SystemDiagram reads; its `<base>-scaler` sidecar node (scalerOf) renders at the top
// of the stack as the group header, carrying the group's Edit button.
import ScalingTab from './ScalingTab.jsx'
import { DiagramBody, bodyHeight } from './DiagramBody.jsx'

export default {
  serviceTypes: ['consumer_group', 'consumer_scaler'],

  // Members of this type stack into a dotted worker-group box (SystemDiagram's
  // generalized llm-group rendering keys off this predicate).
  workerGroup: (node) => node.service_type === 'consumer_group',

  // Tab injected into NodeEditModal between the kind tabs and Shutdown/Delete. Only
  // the group BASE gets it (instances have no Edit button; the scaler's policy is
  // edited from its base's tab).
  editTabs(node) {
    if (node.service_type === 'consumer_group' && !node.instanceOf) {
      return [{ id: 'scaling', label: 'Scaling', Component: ScalingTab }]
    }
    return []
  },

  // Live runtime poll: App fetches this and merges { nodes: { [id]: { live?, policy?,
  // partitions? } } } into the customState map (consumed by the tab + diagram body).
  runtime: {
    url: (sys) => `/api/custom/consumer-group/state?system=${encodeURIComponent(sys)}`,
  },

  // The scaler's /state is the control-plane read the tab + apply loop poll — internal
  // (off the load balancer's public surface) and locked from edit/delete.
  endpointPolicy(node, p) {
    if (node.service_type === 'consumer_scaler' && p === '/state') {
      return { visibility: 'internal', locked: true }
    }
    return null
  },

  DiagramBody,
  diagramHeight: bodyHeight,
}
