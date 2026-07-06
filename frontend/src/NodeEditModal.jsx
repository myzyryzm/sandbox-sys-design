import { useState } from 'react'
import EndpointsModal from './EndpointsModal.jsx'
import GrpcServiceModal from './GrpcServiceModal.jsx'
import DbSchema from './DbSchema.jsx'
import DbCdc from './DbCdc.jsx'
import DbSeed from './DbSeed.jsx'
import EventStreamModal from './EventStreamModal.jsx'
import NodeOutageModal from './NodeOutageModal.jsx'
import ConfirmDelete from './ConfirmDelete.jsx'
import ClientScenarioTab from './ClientScenarioTab.jsx'
import WsClientMethodsTab from './WsClientMethodsTab.jsx'
import ConsumerTab from './ConsumerTab.jsx'
import ServiceCallsTab from './ServiceCallsTab.jsx'
import { customTypeOf } from './customTypes/index.js'

/**
 * Single "Edit" modal for a service / database / event-stream node. It replaces the
 * cluster of header icons that used to sit on each node (≡ endpoints, ⇄ gRPC, ⏻
 * shutdown, ✕ delete) with one tabbed popup: every former icon is now a tab. Each tab
 * renders the existing per-action modal in its `embedded` mode (body only — this
 * component owns the overlay, card and header).
 *
 * Tabs are derived from the node's kind, mirroring exactly which icons it used to show:
 *   - service          → Endpoints · gRPC · Calls · Shutdown · Delete
 *   - external service  → Endpoints · Calls · Shutdown · Delete   (no gRPC — third party)
 *   - client            → Functions · Delete   (a caller: no container, serves nothing)
 *                         (a websocket pool client also gets a read-only WebSocket tab)
 *
 * The Calls tab is read-only: it lists the service's API methods and, on click, traces
 * one on the MAIN diagram (via `onTraceMethod`), so it also closes this modal.
 *   - database          → Schema (or Replica) · Shutdown · Delete
 *   - event stream      → Topics · Consumers · Shutdown · Delete
 *
 * `busy` is lifted from the active tab (via each child's `onBusyChange`) so we can lock
 * tab-switching and dismissal while a delete / shutdown / rebuild is in flight.
 */
export default function NodeEditModal({ systemId, node, manifest, current, onClose, onLaunch, onTraceMethod }) {
  const isService = node.type === 'service'
  const isExternal = node.type === 'external_service'
  const isClient = node.type === 'client'
  const isDatabase = node.origin === 'create-database'
  const isEventStream = node.origin === 'create-event-stream'
  const isSecondary = !!node.replicaOf
  // Prometheus is shared infra: its only action is the visual Delete (remove the diagram
  // node, keep the container). No feature tabs, and NO Shutdown — never offer to stop the
  // container every other node's metrics depend on.
  const isPrometheus = node.type === 'prometheus'

  const tabs = []
  if (isService) {
    tabs.push({ id: 'endpoints', label: 'Endpoints' })
    tabs.push({ id: 'grpc', label: 'gRPC' })
    tabs.push({ id: 'calls', label: 'Calls' })
  } else if (isExternal) {
    // External services expose an HTTP API (the third party's endpoints) but never gRPC
    // contracts. The Functions "trigger bank" is client-only, so they have no Functions tab —
    // but an external service still calls back into the system through its OWN endpoints'
    // `downstream` (a webhook/callback, e.g. payments-api.completePayment -> service-1), which the
    // Calls tab traces like any other call.
    tabs.push({ id: 'endpoints', label: 'Endpoints' })
    tabs.push({ id: 'calls', label: 'Calls' })
  } else if (isClient) {
    // A client serves nothing and has no container — just its own functions: authorable
    // multi-step python functions that call the system through the load balancer
    // (clients/<module>.py, ClientScenarioTab). A websocket client ALSO has the two
    // BUILT-IN methods of its generated host pool script (ws-clients/<id>.mjs, shown
    // read-only under a separate "WebSocket" tab) — but it authors HTTP functions just
    // like any other client.
    if (node.origin === 'create-websockets') {
      tabs.push({ id: 'wsmethods', label: 'WebSocket', Component: WsClientMethodsTab })
    }
    tabs.push({ id: 'functions', label: 'Functions', Component: ClientScenarioTab })
  } else if (isDatabase) {
    tabs.push({ id: 'schema', label: isSecondary ? 'Replica' : 'Schema' })
    // CDC + Seed (postgres/mongodb primaries only; replicas are read-only).
    if (!isSecondary && (node.type === 'postgres' || node.type === 'mongodb')) {
      tabs.push({ id: 'cdc', label: 'CDC' })
      tabs.push({ id: 'seed', label: 'Seed' })
    }
  } else if (isEventStream) {
    tabs.push({ id: 'topics', label: 'Topics' })
    // Consumer functions: internal services that consume this cluster's topics. Rendered via the
    // generic Component-tab path below (no `switch` case needed).
    tabs.push({ id: 'consumers', label: 'Consumers', Component: ConsumerTab })
  }
  // Custom service types inject their own tab(s) (e.g. a Download Coordinator's
  // Distribution tab) between the kind tabs and the universal Shutdown/Delete.
  for (const t of customTypeOf(node)?.editTabs?.(node) || []) {
    tabs.push({ id: `custom:${t.id}`, label: t.label, Component: t.Component })
  }
  // A client has no container, so there's nothing to shut down; Prometheus is shared infra
  // that must never be shut down from here.
  if (!isClient && !isPrometheus) tabs.push({ id: 'shutdown', label: 'Shutdown' })
  tabs.push({ id: 'delete', label: 'Delete', danger: true })

  const [active, setActive] = useState(tabs[0].id)
  const [busy, setBusy] = useState(false)

  const dismiss = busy ? undefined : onClose

  function renderTab() {
    // Custom service-type tabs render their own component (embedded). They get the full
    // prop set so each can fetch/control what it needs.
    const custom = tabs.find((t) => t.id === active && t.Component)
    if (custom) {
      return (
        <custom.Component
          embedded
          systemId={systemId}
          node={node}
          current={current}
          manifest={manifest}
          onClose={onClose}
          onLaunch={onLaunch}
          onBusyChange={setBusy}
        />
      )
    }
    switch (active) {
      case 'endpoints':
        return (
          <EndpointsModal
            embedded
            systemId={systemId}
            service={node.id}
            node={node}
            onClose={onClose}
            onLaunch={onLaunch}
            onBusyChange={setBusy}
          />
        )
      case 'grpc':
        return (
          <GrpcServiceModal
            embedded
            systemId={systemId}
            node={node}
            onClose={onClose}
            onLaunch={onLaunch}
            onBusyChange={setBusy}
          />
        )
      case 'schema':
        return (
          <DbSchema
            embedded
            systemId={systemId}
            node={node}
            manifest={manifest}
            onClose={onClose}
            onLaunch={onLaunch}
            onBusyChange={setBusy}
          />
        )
      case 'cdc':
        return (
          <DbCdc
            embedded
            systemId={systemId}
            node={node}
            manifest={manifest}
            onClose={onClose}
            onLaunch={onLaunch}
            onBusyChange={setBusy}
          />
        )
      case 'seed':
        return (
          <DbSeed
            embedded
            systemId={systemId}
            node={node}
            onClose={onClose}
            onBusyChange={setBusy}
          />
        )
      case 'topics':
        return (
          <EventStreamModal
            embedded
            systemId={systemId}
            node={node}
            onClose={onClose}
            onLaunch={onLaunch}
          />
        )
      case 'calls':
        return (
          <ServiceCallsTab
            embedded
            systemId={systemId}
            service={node.id}
            node={node}
            onTrace={(ep) => onTraceMethod?.(ep)}
            onBusyChange={setBusy}
          />
        )
      case 'shutdown':
        return (
          <NodeOutageModal
            embedded
            systemId={systemId}
            node={node}
            current={current}
            onClose={onClose}
            onBusyChange={setBusy}
          />
        )
      case 'delete':
        return (
          <ConfirmDelete
            embedded
            systemId={systemId}
            node={node}
            manifest={manifest}
            onClose={onClose}
            onBusyChange={setBusy}
          />
        )
      default:
        return null
    }
  }

  return (
    <div className="modal-overlay" onClick={dismiss}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>
            Edit · <code>{node.label}</code>
          </h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <div className="modal-tabs" role="tablist">
          {tabs.map((t) => (
            <button
              key={t.id}
              type="button"
              role="tab"
              aria-selected={active === t.id}
              className={[
                'modal-tab',
                active === t.id ? 'active' : '',
                t.danger ? 'danger' : '',
              ].filter(Boolean).join(' ')}
              disabled={busy && active !== t.id}
              onClick={() => setActive(t.id)}
            >
              {t.label}
            </button>
          ))}
        </div>

        {renderTab()}
      </div>
    </div>
  )
}
