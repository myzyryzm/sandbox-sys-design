// Vite dev-server plugin: add / remove the Prometheus NODE on the diagram.
//
//   POST   /api/prom-node   { system }        -> add the prometheus node
//   DELETE /api/prom-node   { system, id }    -> remove it (visual toggle)
//
// Prometheus already runs as a real container in every system's docker-compose.yml
// (it's shared infra: every node's metric/health poll goes through it, and every
// rebuild path runs `docker compose restart prometheus`). So this is NOT a
// create/destroy of the container — it only toggles whether Prometheus shows up on
// the diagram as a manifest node (and self-scrapes so that node has live metrics).
//
// Add: reject if one already exists (single-instance), append a `prometheus` self-
// scrape job so `up{job="prometheus"}` + the tsdb/http self-metrics resolve, append
// the manifest node, then restart the prometheus container to load the new job.
// Delete: strip the self-scrape job + the node, and best-effort restart. The compose
// `prometheus` service is NEVER touched — the container stays up either way.
//
// The route base deliberately avoids the `/api/prometheus` prefix: that's the Vite
// proxy to :9090 (vite.config.js), which matches by prefix and would otherwise
// swallow this endpoint.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemsDir, systemDir, isValidSystem, nextNodePosition } from './systems.js'
import { bad, HttpError, readJsonBody, serviceHealth, addScrapeJob, addManifestNode } from './scaffold.js'

const pexec = promisify(execFile)

const PROM_ID = 'prometheus'
const PROM_PORT = 9090

// The single source of truth for the on-diagram Prometheus node. The seed data in the
// existing systems' manifests mirrors this shape.
function prometheusNode(manifest) {
  return {
    id: PROM_ID,
    label: 'Prometheus',
    type: 'prometheus',
    origin: 'create-prometheus',
    position: nextNodePosition(manifest),
    metrics: [
      { label: 'targets up', query: 'sum(up)', unit: '' },
      { label: 'series', query: 'prometheus_tsdb_head_series', unit: '' },
      { label: 'ingest', query: 'rate(prometheus_tsdb_head_samples_appended_total[1m])', unit: '/s' },
      { label: 'api req', query: 'sum(rate(prometheus_http_requests_total[1m]))', unit: '/s' },
    ],
    health: serviceHealth(PROM_ID), // up{job="prometheus"}: green when self-scrape is up
  }
}

function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(path.join(systemDir(system), 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n')
}
function hasPrometheusNode(manifest) {
  return (manifest.nodes || []).some((n) => n.type === 'prometheus')
}

// Drop the `prometheus` self-scrape job from prometheus.yml (comment-preserving),
// mirroring removeScrapeJob in remove.js (which isn't exported).
function removeScrapeJob(system, id) {
  const file = path.join(systemDir(system), 'prometheus', 'prometheus.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const sc = doc.get('scrape_configs')
  const i = sc?.items?.findIndex((it) => String(it.get('job_name')) === id) ?? -1
  if (i >= 0) sc.delete(i)
  fs.writeFileSync(file, doc.toString())
}

// Restart just the prometheus container so a scrape-config change takes effect. Never
// builds or touches any other service (the frontend-safe docker path — never ./start.sh).
async function restartPrometheus(system) {
  if (process.env.PROM_NODE_SKIP_REBUILD === '1') return '(restart skipped)'
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 120_000, maxBuffer: 16 * 1024 * 1024 }
  const r = await pexec('docker', ['compose', '-f', compose, 'restart', 'prometheus'], opts)
  return r.stdout + r.stderr
}

export async function handleAdd(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  if (hasPrometheusNode(manifest)) {
    throw new HttpError(409, 'Prometheus is already on the diagram (only one is allowed).')
  }
  // Self-scrape so the node's metrics/health have data, then append the node.
  addScrapeJob(system, PROM_ID, PROM_PORT, ` ${PROM_ID} — self-scrape (Prometheus node on the diagram)`)
  const node = addManifestNode(system, manifest, prometheusNode(manifest))
  let log
  try {
    log = await restartPrometheus(system) // load the new self-scrape job
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose restart prometheus failed:\n${detail}`)
  }
  return { ok: true, node, log }
}

export async function handleRemove(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = readManifest(system)
  // Remove the self-scrape job + the node (+ any stray edges). The compose service stays.
  removeScrapeJob(system, PROM_ID)
  manifest.nodes = (manifest.nodes || []).filter((n) => n.type !== 'prometheus')
  manifest.edges = (manifest.edges || []).filter((e) => e.from !== PROM_ID && e.to !== PROM_ID)
  writeManifest(system, manifest)
  // Restart is best-effort here: the manifest edit is what toggles the diagram off, and
  // a dropped self-scrape job is harmless until the next restart anyway.
  let log = ''
  try {
    log = await restartPrometheus(system)
  } catch (err) {
    log = `(restart skipped: ${err.message})`
  }
  return { ok: true, removed: PROM_ID, log }
}

export default function prometheusNodePlugin() {
  return {
    name: 'prometheus-node',
    configureServer(server) {
      server.middlewares.use('/api/prom-node', async (req, res, next) => {
        if (req.method !== 'POST' && req.method !== 'DELETE') return next()
        try {
          const body = await readJsonBody(req)
          const result = req.method === 'POST' ? await handleAdd(body) : await handleRemove(body)
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
