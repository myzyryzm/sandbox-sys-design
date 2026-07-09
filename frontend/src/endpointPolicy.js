// Endpoint visibility / lock policy — the single, generic place that decides which
// of a service's live routes are part of its EXTERNAL client surface and which are
// internal operational plumbing.
//
//   visibility: 'public'   — an external client API: shown on the load balancer and
//                            editable/deletable in the per-service Endpoints tab.
//               'internal' — operational/control-plane route (not for outside clients):
//                            hidden from the load balancer, but still listed in the
//                            Endpoints tab with an "internal" badge.
//               'hidden'   — not a manageable endpoint at all: never listed anywhere.
//   locked: no edit/delete in the management UI (a built-in/owned route).
//
// Only GENERIC operational routes (every service has /health; resilience-wrapped
// services expose /resilience/state) are classified here. A custom service type
// classifies ITS OWN routes through the registry seam — `customTypeOf(node).endpointPolicy`
// — so no type-specific paths (e.g. a Download Coordinator's /dc/*) leak into this
// generic layer.
import { customTypeOf } from './customTypes/index.js'

// The service-local path of a discovered endpoint, whose `.path` is LB-prefixed as
// `/<service><local>`. Strips the prefix and any trailing slash: `/health/` -> `/health`.
export function localPathOf(e) {
  const prefix = `/${e.service}`
  let p = e.path && e.path.startsWith(prefix) ? e.path.slice(prefix.length) : e.path || '/'
  return p.replace(/\/+$/, '') || '/'
}

// Generic operational routes that are never part of the external client surface.
// `/discovery/*` is the etcd listener's service-discovery view (sandbox-etcd skill,
// authored into the listener's app.py) — internal plumbing, not a client API.
function genericInternal(p) {
  return p === '/health' || p.startsWith('/resilience/') || p.startsWith('/discovery/')
}

// Resolve the policy for one endpoint. `ownerNode` is the manifest node that SERVES
// the endpoint (so its custom service type, if any, gets first say over its routes).
export function endpointPolicy(endpoint, ownerNode) {
  const p = localPathOf(endpoint)
  const custom = ownerNode && customTypeOf(ownerNode)?.endpointPolicy?.(ownerNode, p, endpoint)
  if (custom) return { visibility: 'public', locked: false, ...custom }
  // An external service's /health is just noise from the cloned template — it's not
  // scraped and doesn't drive any color — so hide it entirely (an in-system service
  // keeps /health listed below as a documented, locked internal route).
  if (ownerNode?.external && p === '/health') return { visibility: 'hidden', locked: true }
  if (genericInternal(p)) return { visibility: 'internal', locked: true }
  // User-marked internal: a normally-public route the user has taken off the load
  // balancer's surface. Unlike the built-in operational routes above it stays editable
  // (so the flag can be flipped back) — it's just badged and dropped from the LB.
  if (endpoint?.internal === true) return { visibility: 'internal', locked: false }
  return { visibility: 'public', locked: false }
}

// Part of the external client surface (what the load balancer should advertise).
// An external service's endpoints belong to a third party, not to us, so they're
// never advertised on our load balancer — they're still managed in that node's own
// Endpoints tab, just kept off the system's public surface.
export const isExternalEndpoint = (e, owner) =>
  !owner?.external && endpointPolicy(e, owner).visibility === 'public'
