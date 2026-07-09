// Registry of custom service types (the reusable Part-1 mechanism).
//
// Each entry plugs a typed service into the existing add-service flow WITHOUT forking
// it: `onAdd` scaffolds the typed node(s) via scaffold.js, and `routes` contributes
// type-specific control endpoints (namespaced under /api/custom/<type>/...). The
// generic dispatcher (customServices.js) lists these for the add-service modal and
// routes "create" to the right type.
//
// Adding the next custom service type = add one module here + one module in the
// frontend registry (src/customTypes/). Nothing in the modal, diagram, edit-tab, or
// manifest core changes.
import consumerGroup from './consumerGroup.js'
import downloadCoordinator from './downloadCoordinator.js'
import llmWorker from './llmWorker.js'

export const CUSTOM_TYPES = {
  [consumerGroup.serviceType]: consumerGroup,
  [downloadCoordinator.serviceType]: downloadCoordinator,
  [llmWorker.serviceType]: llmWorker,
}
