// Vite dev-server plugin: add an EXTERNAL service to the active system.
//
// POST /api/external-services  { system, name }
//
// An external service simulates a third-party API that in-system services call
// out to (e.g. a payment gateway, an email provider). It is a real container —
// the same hand-instrumented FastAPI template a generic service uses — so the
// calls actually work over the docker network, but it is treated as living
// OUTSIDE the system boundary:
//
//   - NOT scraped by Prometheus (no scrape job) and carries no health block, so
//     the diagram never colors it by health — it stays neutral, outside the box.
//   - `type: 'external_service'`, which the gRPC layer gates against — an external
//     service can't serve or consume gRPC contracts (grpc.js requires
//     type === 'service'). It can still be the TARGET of a circuit breaker, since
//     resilience policies only require the SOURCE to be an in-system service.
//   - It DOES get an nginx route, like any service: that's how the host-side
//     endpoint discovery (endpoints.js, via the LB) can read its OpenAPI and how
//     callers reach it. Its endpoints are kept off the load balancer's advertised
//     surface (endpointPolicy.isExternalEndpoint) — they belong to the third party,
//     not to us.
//
// Shares every scaffolding primitive with the generic-service recipe (scaffold.js)
// — it only differs by skipping the scrape job, marking the node external, and
// placing it in the external column. Composing, not forking.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { systemDir, isValidSystem, nextExternalPosition } from './systems.js'
import {
  bad, NAME_RE, readJsonBody,
  cloneTemplate, addComposeService, addNginxRoute, addManifestNode, rebuild,
} from './scaffold.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// Reuse the canonical generic FastAPI service template — an external service is a
// real container that serves HTTP endpoints, it just lives outside our system.
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

  // 2-3. compose + nginx route. NO prometheus scrape job: an external service is
  // not part of our observability surface.
  addComposeService(system, name, { build: `./${name}` }, ` External service "${name}" — added by Add external service (simulates a third-party API)`)
  addNginxRoute(system, name)

  // 4. manifest node — external: no health, no metrics, marked external so the
  // diagram draws it outside the system boundary and keeps it off the LB surface.
  const node = addManifestNode(system, manifest, {
    id: name,
    label: name,
    type: 'external_service',
    origin: 'create-external-service',
    external: true,
    position: nextExternalPosition(manifest),
    metrics: [],
  })

  // 5. rebuild (frontend-safe). rebuild() also reloads nginx (needed for the new
  // /<name>/ route) and restarts prometheus (a harmless no-op — no scrape job).
  const log = process.env.CREATE_EXT_SKIP_REBUILD === '1' ? '(rebuild skipped)' : await rebuild(system, name)
  return { ok: true, node, log }
}

export default function externalServices() {
  return {
    name: 'external-services',
    configureServer(server) {
      server.middlewares.use('/api/external-services', async (req, res, next) => {
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
