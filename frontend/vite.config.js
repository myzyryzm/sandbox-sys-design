import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import claudeTerminal from './server/terminal.js'
import createDatabase from './server/databases.js'
import createService from './server/services.js'
import externalServices from './server/externalServices.js'
import clients from './server/clients.js'
import scenarios from './server/scenarios.js'
import consumers from './server/consumers.js'
import customServices from './server/customServices.js'
import removeComponent from './server/remove.js'
import endpoints from './server/endpoints.js'
import models from './server/models.js'
import dbSchema from './server/dbschema.js'
import dbSeed from './server/dbseed.js'
import simulate from './server/simulate.js'
import skills from './server/skills.js'
import eventStreams from './server/eventstreams.js'
import grpc from './server/grpc.js'
import createReplica from './server/replicas.js'
import cdc from './server/cdc.js'
import resilience from './server/resilience.js'
import outage from './server/outage.js'
import layout from './server/layout.js'
import endtoend from './server/endtoend.js'
import websockets from './server/websockets.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// The shared `systems/` directory lives one level up from `frontend/`.
const systemsDir = path.resolve(__dirname, '../systems')

/**
 * Tiny dev-server middleware that serves files out of ../systems under the
 * `/systems/*` URL prefix. This is how the browser loads a system's
 * manifest.json without any CORS setup or a separate static server — it's
 * same-origin with the Vite dev server. Adding a new system (a new
 * systems/<id>/ folder) is served automatically; no frontend changes needed.
 */
function serveSystems() {
  return {
    name: 'serve-systems',
    configureServer(server) {
      server.middlewares.use('/systems', (req, res, next) => {
        const rel = decodeURIComponent((req.url || '').split('?')[0])
        const filePath = path.join(systemsDir, rel)

        // Prevent path traversal outside the systems directory.
        if (!filePath.startsWith(systemsDir)) {
          res.statusCode = 403
          return res.end('Forbidden')
        }
        fs.readFile(filePath, (err, data) => {
          if (err) return next()
          if (filePath.endsWith('.json')) {
            res.setHeader('Content-Type', 'application/json')
          }
          res.end(data)
        })
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), serveSystems(), claudeTerminal(), createDatabase(), createService(), externalServices(), clients(), scenarios(), consumers(), customServices(), removeComponent(), endpoints(), models(), dbSchema(), dbSeed(), simulate(), skills(), eventStreams(), grpc(), createReplica(), cdc(), resilience(), outage(), layout(), endtoend(), websockets()],
  server: {
    proxy: {
      // Browser -> /api/prometheus/api/v1/query?...  proxied to Prometheus.
      // The /api/prometheus prefix is stripped so Prometheus sees /api/v1/query.
      '/api/prometheus': {
        target: 'http://localhost:9090',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api\/prometheus/, ''),
      },
    },
  },
})
