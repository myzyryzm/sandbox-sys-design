// LLM Persistence Readers — frontend module.
//
// The rendering registration for the custom service type: the Readers edit tab
// (registry entry + live member state + manual member count), the runtime poll, and
// the endpoint policy hiding the control-plane route. Creation happens from the LLM
// worker's Persistence tab (customTypes/llmWorker/PersistenceTab.jsx), not the
// add-service modal — the backend type is `hidden`. The reader→stream and reader→db
// edges are STATIC in the manifest (written by onAdd), so no diagramEdges hook; the
// default service card + metric rows (persisted / active) cover the diagram body.
import ReadersTab from './ReadersTab'

export default {
  serviceTypes: ['persistence_reader'],

  // Readers stack into a dotted worker-group box (SystemDiagram's generalized group
  // rendering keys off this predicate); scaler-less, so the base carries the Edit button.
  workerGroup: (node) => node.service_type === 'persistence_reader',

  // Tabs injected into NodeEditModal. Only the group BASE gets them (instances have
  // no Edit button).
  editTabs(node) {
    if (node.service_type === 'persistence_reader' && !node.instanceOf) {
      return [{ id: 'readers', label: 'Readers', Component: ReadersTab }]
    }
    return []
  },

  // Live runtime poll: App fetches this and merges { nodes: { [id]: { registry?,
  // live? } } } into the customState map (consumed by the tab).
  runtime: {
    url: (sys) => `/api/custom/persistence-reader/state?system=${encodeURIComponent(sys)}`,
  },

  // /reader/state is the control-plane read the tab + state route poll — never part
  // of any client surface (the /workers + /assignments precedent).
  endpointPolicy(node, p) {
    if (node.service_type === 'persistence_reader' && p === '/reader/state') {
      return { visibility: 'hidden', locked: true }
    }
    return null
  },
}
