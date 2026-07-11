// Vite dev-server plugin: GLOBAL (cross-system) app settings.
//
//   GET  /api/settings           -> { ok, settings }
//   POST /api/settings  { prefixColors?, dangerouslySkipPermissions? } -> { ok, settings }
//
// Unlike the per-system registries under systems/<id>/, these are app-wide, so they live in
// a single settings.json at the repo root (gitignored — machine-local config). Two settings
// today, more later:
//   - prefixColors: the diagram's row-prefix badge/edge colors (see frontend/src/prefixColors.js).
//   - dangerouslySkipPermissions: when true, terminal.js adds --dangerously-skip-permissions to
//     every launched `claude` session.
//
// Security: every incoming value is strictly validated before it is written — colors must be
// #rrggbb (they're injected straight into CSS/SVG, so this blocks style/markup injection) and
// the flag must be a boolean. Unknown keys are dropped (only the known shape is persisted). This
// mirrors the "backend validates all browser input; never let the browser escalate" posture the
// rest of the plugins follow — the permission flag in particular is read server-side, never from
// a browser query param.
import fs from 'node:fs'
import path from 'node:path'
import { repoRoot } from './systems.js'
import { readJsonBody, bad } from './scaffold.js'

const SETTINGS_PATH = path.join(repoRoot, 'settings.json')

// Keep in sync with DEFAULT_PREFIX_COLORS in frontend/src/prefixColors.js and the fallbacks in
// styles.css / SystemDiagram.jsx.
const DEFAULT_PREFIX_COLORS = {
  http: '#38ffbd',
  function: '#6ea8fe',
  consumer: '#e0a44f',
  grpc: '#b18cf2',
  etcdKey: '#ff9eed',
  etcdEdge: '#5aa0c0',
  redisKey: '#ff6b5e',
}
const COLOR_ROLES = Object.keys(DEFAULT_PREFIX_COLORS)
const HEX_RE = /^#[0-9a-fA-F]{6}$/

function defaults() {
  return { prefixColors: { ...DEFAULT_PREFIX_COLORS }, dangerouslySkipPermissions: false }
}

// Read settings.json, merged over defaults so a missing/partial/corrupt file still yields a full,
// valid object. Exported so terminal.js can read the flag at connect time.
export function readSettings() {
  const base = defaults()
  try {
    const raw = JSON.parse(fs.readFileSync(SETTINGS_PATH, 'utf8'))
    return {
      prefixColors: { ...base.prefixColors, ...normalizeColors(raw.prefixColors) },
      dangerouslySkipPermissions: raw.dangerouslySkipPermissions === true,
    }
  } catch {
    return base // no file yet, or unreadable/invalid — fall back to defaults
  }
}

// Keep only known roles whose value is a valid #rrggbb hex; drop everything else.
function normalizeColors(colors) {
  const out = {}
  if (colors && typeof colors === 'object') {
    for (const role of COLOR_ROLES) {
      if (HEX_RE.test(colors[role])) out[role] = colors[role].toLowerCase()
    }
  }
  return out
}

function writeSettings(next) {
  fs.writeFileSync(SETTINGS_PATH, JSON.stringify(next, null, 2) + '\n')
}

// Merge a POSTed patch onto the current settings after strict validation. Rejects malformed
// values loudly (400) so a bad color can't silently no-op or reach the DOM/CLI unchecked.
function applyPatch(body) {
  const current = readSettings()
  const next = { prefixColors: { ...current.prefixColors }, dangerouslySkipPermissions: current.dangerouslySkipPermissions }

  if (body.prefixColors !== undefined) {
    if (!body.prefixColors || typeof body.prefixColors !== 'object') throw bad('prefixColors must be an object')
    for (const [role, value] of Object.entries(body.prefixColors)) {
      if (!COLOR_ROLES.includes(role)) throw bad(`unknown color role "${role}"`)
      if (!HEX_RE.test(value)) throw bad(`color "${role}" must be a #rrggbb hex`)
      next.prefixColors[role] = value.toLowerCase()
    }
  }

  if (body.dangerouslySkipPermissions !== undefined) {
    if (typeof body.dangerouslySkipPermissions !== 'boolean') throw bad('dangerouslySkipPermissions must be a boolean')
    next.dangerouslySkipPermissions = body.dangerouslySkipPermissions
  }

  writeSettings(next)
  return next
}

export default function settings() {
  return {
    name: 'settings',
    configureServer(server) {
      const send = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }
      server.middlewares.use('/api/settings', async (req, res, next) => {
        try {
          if (req.method === 'GET') return send(res, 200, { ok: true, settings: readSettings() })
          if (req.method === 'POST') {
            const body = await readJsonBody(req)
            return send(res, 200, { ok: true, settings: applyPatch(body || {}) })
          }
          return next()
        } catch (err) {
          send(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
