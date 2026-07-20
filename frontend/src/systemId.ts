// A system's id doubles as its permanent identity: the on-disk systems/<id>/
// folder, the docker-compose project name, and the /systems/<id> URL. Like node
// names, it can't be renamed once created.
//
// This mirrors SYSTEM_ID_RE in frontend/server/systemsApi.js (the server is the
// source of truth and rejects anything invalid) — we validate here only to give
// immediate, friendly feedback before the request is sent. Keep the two in sync.
// Note it's deliberately looser than NODE_NAME_RE: a leading digit is allowed.
export const SYSTEM_ID_RE = /^[a-z0-9][a-z0-9-]*$/

// Returns a human-readable error string if `id` isn't a valid system id, else null.
export function systemIdError(id: string | null | undefined): string | null {
  const n = (id || '').trim()
  if (!n) return 'Name is required'
  if (/\s/.test(n)) return 'Name can’t contain spaces'
  if (n.length > 40) return 'Name is too long (40 characters max)'
  if (!SYSTEM_ID_RE.test(n)) return 'Use lowercase letters, digits and hyphens'
  return null
}

export const SYSTEM_ID_HINT =
  'Permanent id — lowercase letters, digits and hyphens, no spaces. Becomes the systems/<id>/ folder and the URL; can’t be changed later.'
