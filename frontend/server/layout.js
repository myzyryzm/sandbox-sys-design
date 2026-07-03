// Vite dev-server plugin: persist diagram LAYOUT (node positions + the system
// boundary rectangle) into the manifest, so a hand-arranged diagram survives reloads.
//
//   POST /api/layout  { system, positions?: { [nodeId]: {x,y} }, boundary?: {x,y,w,h} }
//                     -> { ok }
//
// "Drag mode" in the frontend lets the user move every node and move/resize the dotted
// system boundary box. On drop the diagram POSTs the new coordinates here; we update
// systems/<id>/manifest.json in place (plain JSON, the same read-modify-write pattern
// every other manifest plugin uses) and the frontend's manifest poll picks it up. No
// docker rebuild — positions are pure render state.
//
// Security: `system` is validated against systems/; node ids are whitelisted against the
// LIVE manifest (never trusted from the request); every coordinate is coerced to a finite
// number; only the generated manifest.json is written.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'
import { readJsonBody, bad } from './databases.js'

// Smallest the boundary box may be dragged down to, so a stray resize can't collapse it
// to an unclickable sliver.
const MIN_BOUNDARY = 40

const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null)

function manifestPath(system) {
  return path.join(systemDir(system), 'manifest.json')
}

function handleSave(body) {
  const { system, positions, boundary } = body || {}
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)

  const manifest = JSON.parse(fs.readFileSync(manifestPath(system), 'utf8'))
  const byId = new Map((manifest.nodes || []).map((n) => [n.id, n]))
  let touched = false

  if (positions && typeof positions === 'object') {
    for (const [id, pos] of Object.entries(positions)) {
      const node = byId.get(id) // whitelist against the live manifest, not the request
      if (!node || !pos || typeof pos !== 'object') continue
      const x = num(pos.x)
      const y = num(pos.y)
      if (x === null || y === null) continue
      node.position = { x, y }
      touched = true
    }
  }

  if (boundary && typeof boundary === 'object') {
    const x = num(boundary.x)
    const y = num(boundary.y)
    const w = num(boundary.w)
    const h = num(boundary.h)
    if (x !== null && y !== null && w !== null && h !== null) {
      manifest.boundary = { x, y, w: Math.max(MIN_BOUNDARY, w), h: Math.max(MIN_BOUNDARY, h) }
      touched = true
    }
  }

  if (touched) {
    fs.writeFileSync(manifestPath(system), JSON.stringify(manifest, null, 2) + '\n')
  }
  return { ok: true }
}

export default function layout() {
  return {
    name: 'layout',
    configureServer(server) {
      const send = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }
      server.middlewares.use('/api/layout', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          send(res, 200, handleSave(await readJsonBody(req)))
        } catch (err) {
          send(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
