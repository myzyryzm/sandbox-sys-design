// Vite dev-server plugin: the generic entry point for custom service types.
//
//   GET  /api/custom-types
//     -> { ok, types: [{ serviceType, displayName, description }] }  (for the add modal)
//   POST /api/custom-services  { system, serviceType, name }
//     -> validates, then dispatches to CUSTOM_TYPES[serviceType].onAdd (which scaffolds
//        the typed node via scaffold.js and rebuilds). Returns { ok, node, log }.
//   + mounts each registered type's namespaced control routes (/api/custom/<type>/...).
//
// Mirrors services.js' validation/plugin shape; the per-type recipe lives in
// customTypes/. This file knows nothing about any specific type — adding one needs no
// change here.
import fs from 'node:fs'
import path from 'node:path'
import { CUSTOM_TYPES } from './customTypes/index.js'
import { bad, NAME_RE, readJsonBody } from './scaffold.js'
import { isValidSystem, systemDir } from './systems.js'

function listTypes() {
  return {
    ok: true,
    types: Object.values(CUSTOM_TYPES).map((t) => ({
      serviceType: t.serviceType,
      displayName: t.displayName,
      description: t.description,
    })),
  }
}

function validateCreate(body) {
  const { system, serviceType, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const type = CUSTOM_TYPES[serviceType]
  if (!type) throw bad(`unknown service type "${serviceType}"`)
  if (typeof name !== 'string' || !NAME_RE.test(name) || name.length > 40) {
    throw bad('name must be lowercase letters, digits and hyphens (start with a letter)')
  }
  const dir = systemDir(system)
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8'))
  if (manifest.nodes.some((n) => n.id === name)) {
    throw bad(`a node named "${name}" already exists in this system`)
  }
  if (fs.existsSync(path.join(dir, name))) throw bad(`systems/${system}/${name}/ already exists`)
  return { system, serviceType, name, manifest, type }
}

export default function customServices() {
  return {
    name: 'custom-services',
    configureServer(server) {
      const json = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }

      server.middlewares.use('/api/custom-types', (req, res, next) => {
        if (req.method !== 'GET') return next()
        try {
          return json(res, 200, listTypes())
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })

      server.middlewares.use('/api/custom-services', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          const { system, name, manifest, serviceType, type } = validateCreate(await readJsonBody(req))
          return json(res, 200, await type.onAdd({ system, name, manifest, serviceType }))
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })

      // Mount each type's namespaced control routes. A route is { path, handler },
      // where handler(req, res, next, ctx) gets a small ctx { json, readJsonBody }.
      for (const type of Object.values(CUSTOM_TYPES)) {
        for (const route of type.routes || []) {
          server.middlewares.use(route.path, (req, res, next) =>
            route.handler(req, res, next, { json, readJsonBody }),
          )
        }
      }
    },
  }
}
