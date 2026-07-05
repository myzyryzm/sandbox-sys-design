// Shared helpers for locating and validating sandbox systems on disk.
//
// Both the Claude terminal plugin (terminal.js) and the create-database plugin
// (databases.js) need to turn a browser-supplied `?system=`/`system` value into
// a trusted path under systems/. Keep that logic in one place so the validation
// (and the path-traversal guard) can't drift between the two.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// frontend/server/systems.js -> repo root is two levels up.
export const repoRoot = path.resolve(__dirname, '../..')
export const systemsDir = path.join(repoRoot, 'systems')

// Absolute path to a system's directory. Does not check existence.
export function systemDir(id) {
  return path.join(systemsDir, id)
}

// A system id must be a real directory under systems/ with a manifest.json.
// This both validates the browser-supplied value and stops path tricks (the
// startsWith guard rejects ids like "../foo") before we read or spawn anything.
export function isValidSystem(id) {
  if (!id || !/^[a-z0-9][a-z0-9-]*$/.test(id)) return false
  const dir = systemDir(id)
  return dir.startsWith(systemsDir + path.sep) && fs.existsSync(path.join(dir, 'manifest.json'))
}

// Where to drop the next frontend-generated node (a database or a service).
// New nodes land in the MIDDLE of the current space — the center of the bounding
// box of every in-system node — instead of the top-left corner, and fan out in
// rows of three so successive adds don't stack on the exact same spot. External
// services are excluded here: they live OUTSIDE the system boundary and get their
// own column via nextExternalPosition.
export function nextNodePosition(manifest) {
  const nodes = (manifest.nodes || []).filter((n) => !n.external)
  const i = nodes.filter((n) => n.origin).length
  const positioned = nodes.filter((n) => n.position)
  const xs = positioned.map((n) => n.position.x)
  const ys = positioned.map((n) => n.position.y)
  const cx = positioned.length ? (Math.min(...xs) + Math.max(...xs)) / 2 : 80
  const cy = positioned.length ? (Math.min(...ys) + Math.max(...ys)) / 2 : 160
  return { x: Math.round(cx + (i % 3) * 300), y: Math.round(cy + Math.floor(i / 3) * 180) }
}

// Where to drop the next external service. External services are drawn OUTSIDE the
// system boundary box, so they get a dedicated column to the right of every
// in-system node, stacked downward as more are added. Computed off the current
// rightmost in-system node so they always clear the boundary at creation time.
export function nextExternalPosition(manifest) {
  const nodes = manifest.nodes || []
  const externals = nodes.filter((n) => n.external).length
  const maxInternalX = Math.max(0, ...nodes.filter((n) => !n.external).map((n) => n.position?.x || 0))
  return { x: maxInternalX + 360, y: 120 + externals * 170 }
}

// Where to drop the next client. Clients are callers, so they sit in a column to the
// LEFT of the system boundary (external services are on the right) — giving the
// diagram a left-to-right story: clients → [system] → external services. The x can be
// negative; the diagram's viewBox is sized to the true bounding box.
export function nextClientPosition(manifest) {
  const nodes = manifest.nodes || []
  const clients = nodes.filter((n) => n.type === 'client').length
  const minInternalX = Math.min(80, ...nodes.filter((n) => !n.external).map((n) => n.position?.x || 0))
  return { x: minInternalX - 360, y: 120 + clients * 170 }
}
