// Mechanical whole-system reset for Interview mode: rewrite systems/<id>/ back to the
// smallest observable canvas — the nginx lb + Prometheus, NO services — delete every
// other node folder and registry, and reconcile docker. The seed files mirror what
// create_new.sh generates minus service-1, regenerated IN PLACE so the running dev
// server (and the browser's 3s manifest poll) never see the system disappear.
//
// Unlike create_new.sh the seed manifest INCLUDES the Prometheus node (the exact
// shape /api/prom-node adds): the compose stack runs Prometheus anyway, and the
// frontend only polls metrics at all when a type:"prometheus" node exists — so the
// reset canvas is immediately observable instead of dead until the user adds it.
//
// NOT a plugin: interview.js calls resetSystem() from /api/interview/reset and
// /api/interview/start. Frontend-safe docker path only (up -d --remove-orphans +
// force-recreate lb + restart prometheus) — NEVER ./start.sh.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { repoRoot, systemDir, isValidSystem } from './systems.js'
import { bad, HttpError, withSystemLock, reloadNginx, composePath } from './scaffold.js'
import { prometheusNode } from './prometheus.js'

const pexec = promisify(execFile)

// Everything NOT in this list is deleted from systems/<id>/ — node folders (services,
// databases, cdc workers, grpc/, clients/, ws-*/…), endtoend-runs/, and every registry
// (models/scenarios/consumers/endtoend/etcd/interview/….json). Deleting beats writing
// empties: every registry reader already tolerates an absent file with an empty default.
const KEEP = new Set([
  'manifest.json',
  'docker-compose.yml',
  'endpoints.json',
  'nginx',
  'prometheus',
  'README.md',
])

function titleCase(id) {
  return id
    .split('-')
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(' ')
}

function seedManifest(system) {
  let systemId = system
  let name = titleCase(system)
  try {
    const old = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
    if (old.system_id) systemId = old.system_id
    if (old.name) name = old.name
  } catch {
    /* keep the id-derived fallbacks */
  }
  const manifest = {
    system_id: systemId,
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
      // Fixed position (not nextNodePosition, which would stack it on the lb in a
      // two-node manifest).
      { ...prometheusNode({ nodes: [] }), position: { x: 380, y: 160 } },
    ],
    edges: [],
  }
  // No `boundary`: the diagram auto-fits one around the internal nodes when absent.
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
}

function seedCompose(system) {
  const compose = `# Self-contained compose file for the \`${system}\` system.
# Run from inside systems/${system}/:  docker compose up --build
#
# Topology: nginx LB + Prometheus only — the empty interview canvas. Services,
# databases and streams added from the web app slot in as their own compose
# services, nginx /<id>/ routes, and Prometheus scrape jobs.
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
`
  fs.writeFileSync(path.join(systemDir(system), 'docker-compose.yml'), compose)
}

function seedNginx(system) {
  const conf = `# nginx as the load balancer / router in front of the services.
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
    # === end upstreams ===

    server {
        listen 80;

        # === locations (one per service) ===
        # === end locations ===
    }
}
`
  fs.mkdirSync(path.join(systemDir(system), 'nginx'), { recursive: true })
  fs.writeFileSync(path.join(systemDir(system), 'nginx', 'nginx.conf'), conf)
}

function seedPrometheus(system) {
  // Only the prometheus self-scrape job, in the exact shape /api/prom-node's
  // addScrapeJob writes (flow seq so later addIn splices keep working).
  const yml = `global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  # Scrape each service container DIRECTLY (not through the nginx LB), so the
  # metrics reflect the service itself. The job name is the service id, which the
  # manifest's health/metric queries key off of: up{job="<service>"}. "Add
  # service" appends a job here.
  [
    # prometheus — self-scrape (Prometheus node on the diagram)
    { job_name: prometheus, static_configs: [ { targets: [ prometheus:9090 ] } ] }
  ]
`
  fs.mkdirSync(path.join(systemDir(system), 'prometheus'), { recursive: true })
  fs.writeFileSync(path.join(systemDir(system), 'prometheus', 'prometheus.yml'), yml)
}

export async function resetSystem(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const dir = systemDir(system)

  // A. Rewrite the seed files in place (all synchronous, manifest first — it must
  // never be absent: isValidSystem and the browser's manifest poll both read it).
  seedManifest(system)
  seedCompose(system)
  seedNginx(system)
  seedPrometheus(system)
  fs.writeFileSync(path.join(dir, 'endpoints.json'), '{}\n')

  // B. Delete everything else.
  for (const entry of fs.readdirSync(dir)) {
    if (!KEEP.has(entry)) fs.rmSync(path.join(dir, entry), { recursive: true, force: true })
  }

  // C. Reconcile docker under the per-system lock (serializes against any concurrent
  // rebuild). --remove-orphans drops every container whose service no longer exists
  // in the rewritten compose file; the lb is then force-recreated (reloadNginx) so it
  // re-resolves the rewritten nginx.conf bind mount, and prometheus restarted so it
  // re-reads the rewritten scrape config.
  return withSystemLock(system, async () => {
    const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
    let log = ''
    try {
      const up = await pexec(
        'docker',
        ['compose', '-f', composePath(system), 'up', '-d', '--remove-orphans'],
        opts,
      )
      log += up.stdout + up.stderr
    } catch (err) {
      const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
      throw new HttpError(500, `docker compose failed during reset:\n${detail}`)
    }
    log += await reloadNginx(system)
    try {
      const r = await pexec(
        'docker',
        ['compose', '-f', composePath(system), 'restart', 'prometheus'],
        opts,
      )
      log += r.stdout + r.stderr
    } catch (err) {
      const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
      throw new HttpError(500, `prometheus restart failed during reset:\n${detail}`)
    }
    return log
  })
}
