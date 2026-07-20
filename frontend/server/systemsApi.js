// Vite dev-server plugin: the system roster — list, create, activate.
//
// GET  /api/systems                     → { ok, active, systems: [{ id, name, active }] }
// POST /api/systems           { id }    → scaffold systems/<id>/ (files only, no docker)
// POST /api/systems/activate  { system} → port arbitration: down the previously active
//                                         stack, `docker compose up --build -d` this one,
//                                         record it in .run/active_system
//
// Create is the server-side port of the old create_new.sh: the smallest runnable
// system (nginx LB → service-1, scraped by Prometheus), with service-1 copied from
// the same canonical template "Add service" clones so a fresh system and an
// Add-service node stay byte-identical. It writes files only — the entry screen
// navigates to /systems/<id> right after, and that page's activate call owns the
// docker wait, so create/click/deep-link all start a stack through ONE code path.
//
// Activate is the server-side port of start.sh's docker half. It must NEVER run
// ./start.sh itself — that script restarts the dev server this code runs inside.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'
import { repoRoot, systemsDir, systemDir, isValidSystem } from './systems.js'
import {
  HttpError, bad, readJsonBody, serviceMetrics, serviceHealth, cloneTemplate, withSystemLock,
} from './scaffold.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const pexec = promisify(execFile)

// Canonical generic service (FastAPI with hand-written /health + /metrics) —
// the same template services.js clones for "Add service".
const TEMPLATE_DIR = path.join(__dirname, 'templates', 'service')
const SERVICE_FILES = ['app.py', 'requirements.txt', 'Dockerfile']

// A system id doubles as folder name and docker-compose project name. Same rule
// as isValidSystem (systems.js) — a leading digit is allowed, unlike node names
// (NAME_RE). Client mirror: src/systemId.ts.
const SYSTEM_ID_RE = /^[a-z0-9][a-z0-9-]*$/

// ---------------------------------------------------------------------------
// .run/active_system — the same marker start.sh/stop.sh maintain: which system
// currently holds the shared host ports (8080 lb / 9090 prometheus).
// ---------------------------------------------------------------------------

const ACTIVE_FILE = path.join(repoRoot, '.run', 'active_system')

function readActive() {
  try {
    const id = fs.readFileSync(ACTIVE_FILE, 'utf8').trim()
    return id || null
  } catch {
    return null
  }
}

function writeActive(id) {
  fs.mkdirSync(path.dirname(ACTIVE_FILE), { recursive: true })
  fs.writeFileSync(ACTIVE_FILE, id + '\n')
}

function clearActive() {
  fs.rmSync(ACTIVE_FILE, { force: true })
}

// "my-system" -> "My System" (same derivation the old create_new.sh used).
function titleCase(id) {
  return id.split('-').filter(Boolean).map((w) => w[0].toUpperCase() + w.slice(1)).join(' ')
}

// ---------------------------------------------------------------------------
// GET /api/systems — list
// ---------------------------------------------------------------------------

function handleList() {
  const active = readActive()
  const systems = fs.readdirSync(systemsDir).filter(isValidSystem).sort().map((id) => {
    let name = titleCase(id)
    try {
      name = JSON.parse(fs.readFileSync(path.join(systemDir(id), 'manifest.json'), 'utf8')).name || name
    } catch {
      // unreadable manifest — fall back to the derived name
    }
    return { id, name, active: id === active }
  })
  return { ok: true, active, systems }
}

// ---------------------------------------------------------------------------
// POST /api/systems — create (files only; the caller activates separately)
// ---------------------------------------------------------------------------

function composeSeed(id) {
  return `# Self-contained compose file for the \`${id}\` system.
# Run from inside systems/${id}/:  docker compose up --build
#
# Topology: nginx LB  ->  service-1 (generic FastAPI), with Prometheus scraping
# the service directly. service-1 is a generic service (the same shape "Add
# service" creates): it exposes /health and /metrics and is reached through the
# LB at the /service-1/ prefix. More services slot in the same way — each gets
# its own compose service, an nginx /<id>/ route, and a Prometheus scrape job.
#
# Restartable: \`docker compose down\` then \`up\` cleanly recreates everything.
# Prometheus data is intentionally NOT persisted.

services:
  lb:
    image: nginx:1.27-alpine
    ports:
      - "8080:80"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro

  prometheus:
    image: prom/prometheus:v3.1.0
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro

  service-1:
    # Service "service-1" — generic FastAPI backend (the same shape "Add service" creates)
    build: ./service-1
`
}

const NGINX_SEED = `# nginx as the load balancer / router in front of the services.
#
# Each service is reached at its own /<service-id>/ prefix, which nginx strips
# before proxying (the trailing slash on proxy_pass), so the browser can call
# e.g. /service-1/health and it lands on service-1's /health. nginx matches the
# longest prefix, so adding more services never collides.
#
# The "Add service" button inserts a new upstream and a new location at the
# markers below — keep the marker comments in place.

events {}

http {
    # === upstreams (one per service; add \`server\` lines for replicas) ===
    upstream service-1 { server service-1:8000; }
    # === end upstreams ===

    server {
        listen 80;

        # === locations (one per service) ===
        location /service-1/ {
            proxy_pass http://service-1/;
            proxy_set_header Host              $host;
            proxy_set_header X-Real-IP         $remote_addr;
            proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
        # === end locations ===
    }
}
`

const PROMETHEUS_SEED = `global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  # Scrape each service container DIRECTLY (not through the nginx LB), so the
  # metrics reflect the service itself. \`service-1:8000\` resolves on the compose
  # network. The job name is the service id, which the manifest's health/metric
  # queries key off of: up{job="service-1"}. "Add service" appends a job here.
  [
    # Service "service-1" — generic FastAPI backend
    { job_name: service-1, static_configs: [ { targets: [ service-1:8000 ] } ] }
  ]
`

function readmeSeed(id, name) {
  const date = new Date().toISOString().slice(0, 10)
  return `# ${name}

System id: \`${id}\` — created from the web app on ${date}.

At creation this is the smallest runnable system: one nginx LB → \`service-1\`
(a generic FastAPI service exposing \`/health\` + \`/metrics\`), scraped by
Prometheus. No database, no downstream edges, no custom service types. Grow it
from the web app (Add service / Add database / custom types) or by editing the
files below — the shared frontend renders whatever the manifest describes, no
frontend edits needed.

## Run it

Open http://localhost:5173/ and pick this system, or from the repo root:

\`\`\`bash
./start.sh ${id}
./stop.sh  ${id}
\`\`\`

Only one system holds the shared host ports (8080/9090) at a time, so starting
this one stops whichever system was previously active.

## What to change as this grows

- \`manifest.json\` — topology (\`nodes\`/\`edges\`), per-node \`metrics[]\` PromQL,
  and \`health\` rules. This is what the diagram renders.
- \`service-1/app.py\` — the service logic and the hand-written metrics it exposes.
- \`docker-compose.yml\` — add services (replicas, a DB, a cache, exporters…).
- \`nginx/nginx.conf\` — per-service \`/<id>/\` routes and \`upstream\` blocks.
- \`prometheus/prometheus.yml\` — scrape targets for any new services.
`
}

function manifestSeed(id, name) {
  return {
    system_id: id,
    name,
    prometheus_base: '/api/prometheus',
    poll_interval_ms: 4000,
    nodes: [
      {
        id: 'lb',
        label: 'nginx LB',
        type: 'load_balancer',
        position: { x: 80, y: 160 },
        metrics: [],
      },
      {
        id: 'service-1',
        label: 'service-1',
        type: 'service',
        origin: 'create-service',
        position: { x: 80, y: 380 },
        metrics: serviceMetrics('service-1'),
        health: serviceHealth('service-1'),
      },
    ],
    edges: [],
  }
}

function handleCreate(body) {
  const { id } = body
  if (typeof id !== 'string' || !SYSTEM_ID_RE.test(id) || id.length > 40) {
    throw bad('id must be lowercase letters, digits and hyphens (start with a letter or digit)')
  }
  const dir = systemDir(id)
  try {
    fs.mkdirSync(dir) // non-recursive: doubles as the atomic "doesn't exist yet" gate
  } catch (err) {
    if (err.code === 'EEXIST') throw bad(`system "${id}" already exists`)
    throw err
  }
  const name = titleCase(id)
  fs.mkdirSync(path.join(dir, 'nginx'))
  fs.mkdirSync(path.join(dir, 'prometheus'))
  cloneTemplate(id, 'service-1', TEMPLATE_DIR, SERVICE_FILES)
  fs.writeFileSync(path.join(dir, 'docker-compose.yml'), composeSeed(id))
  fs.writeFileSync(path.join(dir, 'nginx', 'nginx.conf'), NGINX_SEED)
  fs.writeFileSync(path.join(dir, 'prometheus', 'prometheus.yml'), PROMETHEUS_SEED)
  fs.writeFileSync(path.join(dir, 'endpoints.json'), '{}\n')
  fs.writeFileSync(path.join(dir, 'README.md'), readmeSeed(id, name))
  // manifest.json LAST — the system only becomes isValidSystem-visible once complete.
  fs.writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify(manifestSeed(id, name), null, 2) + '\n')
  return { ok: true, id, name }
}

// ---------------------------------------------------------------------------
// POST /api/systems/activate — port arbitration
// ---------------------------------------------------------------------------

const DOCKER_OPTS = { cwd: repoRoot, timeout: 600_000, maxBuffer: 16 * 1024 * 1024 }
const composeFile = (id) => path.join(systemDir(id), 'docker-compose.yml')

async function compose(id, ...args) {
  const r = await pexec('docker', ['compose', '-f', composeFile(id), ...args], DOCKER_OPTS)
  return r.stdout + r.stderr
}

// A switch touches TWO systems (down the old, up the new), so the per-system
// withSystemLock isn't enough on its own — this chains whole activations so two
// concurrent switches can't interleave their down/up sequences.
let _activation = Promise.resolve()
function withActivationLock(fn) {
  const run = () => fn()
  const next = _activation.then(run, run)
  _activation = next.then(() => {}, () => {})
  return next
}

function handleActivate(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  return withActivationLock(async () => {
    const prev = readActive()
    let log = ''

    if (prev === system) {
      // Fast path: the marker already names this system — but verify the stack is
      // really up (a reboot leaves a stale marker) before trusting it.
      try {
        const ps = await compose(system, 'ps', '--services', '--status', 'running')
        if (ps.split('\n').map((s) => s.trim()).includes('lb')) {
          return { ok: true, system, already: true }
        }
      } catch {
        // docker unreachable or stack gone — fall through to a full up
      }
    }

    if (prev && prev !== system && fs.existsSync(composeFile(prev))) {
      // Free the shared host ports. Best-effort, like start.sh's `down || true` —
      // if docker itself is down, the `up` below fails loudly anyway.
      try {
        log += await withSystemLock(prev, () => compose(prev, 'down'))
      } catch (err) {
        log += `\n(down of previous system "${prev}" failed: ${err.stderr || err.message})\n`
      }
    }

    try {
      // The per-system lock serializes against any concurrent rebuild() of the
      // same system. --build matches start.sh: picks up code edited while inactive.
      log += await withSystemLock(system, () => compose(system, 'up', '--build', '-d'))
    } catch (err) {
      // The old stack is already down — nothing owns the ports, so the marker
      // must not claim otherwise.
      clearActive()
      const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
      throw new HttpError(500, `docker compose up failed:\n${detail}`)
    }

    writeActive(system)
    return { ok: true, system, log }
  })
}

export default function systemsApi() {
  return {
    name: 'systems-api',
    configureServer(server) {
      server.middlewares.use('/api/systems', async (req, res, next) => {
        const sub = (req.url || '').split('?')[0]
        const send = (result) => {
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(result))
        }
        try {
          if (req.method === 'GET' && (sub === '/' || sub === '')) return send(handleList())
          if (req.method === 'POST' && (sub === '/' || sub === '')) return send(handleCreate(await readJsonBody(req)))
          if (req.method === 'POST' && sub === '/activate') return send(await handleActivate(await readJsonBody(req)))
          return next()
        } catch (err) {
          res.statusCode = err.statusCode || 500
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ ok: false, error: err.message }))
        }
      })
    },
  }
}
