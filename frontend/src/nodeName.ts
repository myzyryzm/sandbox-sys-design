// A node's name doubles as its permanent id: the compose service name, the nginx
// route, the on-disk folder, the manifest node id, and the Prometheus job. Because
// those can't be renamed once the stack is built, the name is fixed at creation and
// must be url/dns-safe and unique within the system.
//
// This mirrors NAME_RE in frontend/server/scaffold.js (the server is the source of
// truth and rejects anything invalid) — we validate here only to give immediate,
// friendly feedback before the request is sent. Keep the two in sync.
export const NODE_NAME_RE = /^[a-z][a-z0-9-]*$/

// Returns a human-readable error string if `name` isn't a valid node id, else null.
export function nodeNameError(name: string | null | undefined): string | null {
  const n = (name || '').trim()
  if (!n) return 'Name is required'
  if (/\s/.test(n)) return 'Name can’t contain spaces'
  if (n.length > 40) return 'Name is too long (40 characters max)'
  if (!NODE_NAME_RE.test(n)) return 'Use lowercase letters, digits and hyphens; start with a letter'
  return null
}

// Shared hint shown under every "name" field, so the permanence + format rules are
// visible up front rather than discovered via a rejection.
export const NODE_NAME_HINT =
  'Permanent id — lowercase letters, digits and hyphens, no spaces. Must be unique and can’t be changed later.'
