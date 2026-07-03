// Vite dev-server plugin: add a generic service to the active system.
//
// POST /api/services  { system, name }
//
// Clones the generic service template (the hand-instrumented FastAPI app with
// /health + /metrics — the same shape as the base service-1) into a new service
// folder, wires it into docker-compose.yml, an nginx /<name>/ route, and
// prometheus.yml, adds a service node to manifest.json, then rebuilds the stack.
//
// The actual scaffolding primitives live in scaffold.js, shared with the
// custom-service-type mechanism (customTypes/) so the two never fork. This file is
// just the generic-service recipe: which template, and the plain service node it
// builds.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { systemDir, isValidSystem, nextNodePosition } from './systems.js'
import {
  bad, NAME_RE, readJsonBody, serviceMetrics, serviceHealth,
  cloneTemplate, addComposeService, addNginxRoute, addScrapeJob, addManifestNode, rebuild,
} from './scaffold.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// Canonical generic service (FastAPI with hand-written /health + /metrics).
// service-1 in each system is itself a copy of this.
const TEMPLATE_DIR = path.join(__dirname, 'templates', 'service')
const SERVICE_FILES = ['app.py', 'requirements.txt', 'Dockerfile']

function validate(body) {
  const { system, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof name !== 'string' || !NAME_RE.test(name) || name.length > 40) {
    throw bad('name must be lowercase letters, digits and hyphens (start with a letter)')
  }
  const dir = systemDir(system)
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8'))
  if (manifest.nodes.some((n) => n.id === name)) {
    throw bad(`a node named "${name}" already exists in this system`)
  }
  if (fs.existsSync(path.join(dir, name))) throw bad(`systems/${system}/${name}/ already exists`)
  return { system, name, manifest }
}

export async function handleCreate(body) {
  const { system, name, manifest } = validate(body)

  // 1. clone the generic service template into systems/<id>/<name>/
  cloneTemplate(system, name, TEMPLATE_DIR, SERVICE_FILES)

  // 2-5. compose, nginx route, prometheus, manifest
  addComposeService(system, name, { build: `./${name}` }, ` Service "${name}" — added by Add service (generic FastAPI backend)`)
  addNginxRoute(system, name)
  addScrapeJob(system, name, 8000, ` Service "${name}" — added by Add service`)
  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'service',
    origin: 'create-service',
    position: nextNodePosition(manifest),
    metrics: serviceMetrics(name),
    health: serviceHealth(name),
  })

  // 6. rebuild (frontend-safe)
  const log = process.env.CREATE_SVC_SKIP_REBUILD === '1' ? '(rebuild skipped)' : await rebuild(system, name)
  return { ok: true, node, log }
}

export default function createService() {
  return {
    name: 'create-service',
    configureServer(server) {
      server.middlewares.use('/api/services', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          const body = await readJsonBody(req)
          const result = await handleCreate(body)
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(result))
        } catch (err) {
          res.statusCode = err.statusCode || 500
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ ok: false, error: err.message }))
        }
      })
    },
  }
}
