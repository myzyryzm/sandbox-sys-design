import { useEffect, useState } from 'react'
import { endpointPolicy, localPathOf } from './endpointPolicy'
import type { ManifestNode } from './types/manifest'
import type { DiscoveredEndpoint } from './types/registries'

/**
 * Read-only "Calls" tab for a service (in-system or external). Lists the service's
 * HTTP API methods by name; clicking one asks the parent to trace it on the MAIN
 * diagram — the service and only the nodes that one method calls light up
 * (service → each downstream), with the request/response schema drawn as in/out
 * arrows on the service node.
 *
 * No mutations — every fact comes from /api/endpoints. gRPC is out of scope for v1.
 */

// The human label for an endpoint's request/response schema: the referenced bank
// model name when set, else a generic "request"/"response" when an inline schema
// exists, else '' (nothing to show — that arrow is hidden).
function schemaLabel(
  model: string | null | undefined,
  inline: Record<string, unknown> | undefined,
  fallback: string,
): string {
  if (model) return model
  if (inline && Object.keys(inline).length) return fallback
  return ''
}

interface ServiceCallsTabProps {
  systemId: string
  service: string
  node: ManifestNode
  onTrace?: (e: DiscoveredEndpoint) => void
  embedded?: boolean
  onClose?: () => void
  // Accepted for parity with the other embedded tabs; this tab never mutates, so it
  // stays "not busy" and never locks tab-switching.
  onBusyChange?: (busy: boolean) => void
}

export default function ServiceCallsTab({
  systemId,
  service,
  node,
  onTrace,
  embedded = false,
  onClose,
  onBusyChange,
}: ServiceCallsTabProps) {
  void onBusyChange
  const [endpoints, setEndpoints] = useState<DiscoveredEndpoint[] | null>(null) // null = loading

  useEffect(() => {
    let cancelled = false
    fetch(`/api/endpoints?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json() as Promise<{ endpoints?: DiscoveredEndpoint[] }>)
      .then((d) => {
        if (!cancelled) setEndpoints((d.endpoints || []).filter((e) => e.service === service))
      })
      .catch(() => {
        if (!cancelled) setEndpoints([])
      })
    return () => {
      cancelled = true
    }
  }, [systemId, service])

  // Drop hidden routes (e.g. an external service's /health); keep public + internal.
  const visible =
    endpoints === null ? null : endpoints.filter((e) => endpointPolicy(e, node).visibility !== 'hidden')

  const body = (
    <>
      <p className="sim-desc">
        Pick a method to trace its call path on the diagram — the service and the nodes it
        calls light up; everything else dims. The request and response schemas show as
        arrows into and out of the service.
      </p>
      {visible === null ? (
        <p className="sim-desc">Loading…</p>
      ) : visible.length === 0 ? (
        <p className="sim-desc">No API methods.</p>
      ) : (
        <ul className="calls-list">
          {visible.map((e) => {
            const key = `${e.method} ${e.path}`
            const name = e.alias || `${e.method} ${localPathOf(e)}`
            const req = schemaLabel(e.requestModel, e.request, 'request')
            const res = schemaLabel(e.responseModel, e.response, 'response')
            const downstream = (e.downstream || []).filter(Boolean)
            return (
              <li
                key={key}
                className="calls-row"
                onClick={() => onTrace?.(e)}
                title="Trace this method on the diagram"
              >
                <div className="calls-row-head">
                  <span className="calls-name">{name}</span>
                  <span className="calls-route">
                    <span className="endpoint-method">{e.method}</span> {e.path}
                  </span>
                </div>
                <div className="calls-row-meta">
                  <span className="calls-calls">
                    calls: {downstream.length ? downstream.join(', ') : 'nothing'}
                  </span>
                  {(req || res) && (
                    <span className="calls-schemas">
                      {req && <code>req: {req}</code>}
                      {res && <code>res: {res}</code>}
                    </span>
                  )}
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={(ev) => ev.stopPropagation()}>
        <header className="modal-head">
          <h2>
            Calls · <code>{service}</code>
          </h2>
          <button className="modal-close" onClick={onClose}>
            ✕
          </button>
        </header>
        {body}
      </div>
    </div>
  )
}
