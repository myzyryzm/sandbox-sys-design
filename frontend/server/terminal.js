// Vite dev-server plugin: a WebSocket-backed Claude Code terminal.
//
// It attaches a WebSocket endpoint (`/term`) to the dev server's existing HTTP
// server and, per connection, spawns an interactive `claude` session inside a
// real pseudo-terminal (node-pty). The browser's xterm.js terminal streams
// keystrokes in and screen output out over that socket. This is how you add or
// modify components of the running system from inside the web app.
//
// Mirrors the serveSystems() plugin in vite.config.js: same origin, same port,
// no CORS, one `npm run dev` process.
import fs from 'node:fs'
import path from 'node:path'
import { WebSocketServer } from 'ws'
import nodePty from 'node-pty'
import { repoRoot, systemsDir, isValidSystem } from './systems.js'
import { readSettings } from './settings.js'

// Completion sentinel. Queue-launched sessions (mode === 'new') are instructed to
// print DONE_TOKEN as their very last action; we watch the PTY output for it and
// tell the browser, which auto-advances the edit queue. Interactive `claude` never
// exits when a task finishes, so this is the only reliable "task done" signal.
const DONE_TOKEN = '<<<SANDBOX_QUEUE_DONE>>>'

// Model + reasoning effort for every editing session we launch. These sessions do
// real judgment work (authoring FastAPI routes, DB schemas, .proto servicers) with
// the full manifest inlined, so we pin the strongest model at max thinking. Set
// explicitly (not left to the host's global settings) so the sandbox behaves the
// same for anyone running it. `opus[1m]` keeps the 1M-token context so a large
// manifest + skill files don't crowd out the actual task.
const MODEL_ARGS = ['--model', 'opus[1m]', '--effort', 'xhigh']
const DONE_INSTRUCTION = `

--- EDIT QUEUE PROTOCOL ---
This session was launched from the web app's edit queue. When (and only when) you have
FULLY completed and verified the requested change, print this exact line on its own line
as your VERY LAST action, with nothing after it:
${DONE_TOKEN}
Do NOT print it (or mention it) at any other time — not while reasoning, not if you stop
to ask a question, and not if you cannot finish. It is the signal that starts the next
queued edit, so emit it once, only when the work is truly done.`

// Remove ANSI/VT escape sequences so the plain-ASCII token survives the TUI's
// colouring/redraws when we scan the output stream for it. Char-code based so there
// are no control chars in this source file.
function stripAnsi(s) {
  let out = ''
  for (let i = 0; i < s.length; i++) {
    if (s.charCodeAt(i) === 27) {
      // ESC: drop the escape sequence. CSI (ESC [) / OSC (ESC ]) consume params up to
      // a final byte; any other two-char escape just drops the following char.
      const next = s[i + 1]
      if (next === '[') {
        i += 2
        while (i < s.length && (s.charCodeAt(i) < 64 || s.charCodeAt(i) > 126)) i++
      } else if (next === ']') {
        i += 2
        while (i < s.length && s.charCodeAt(i) !== 7 && s.charCodeAt(i) !== 27) i++
      } else {
        i += 1
      }
      continue
    }
    out += s[i]
  }
  return out
}

/**
 * Build the `--append-system-prompt` text that makes the session aware of its
 * role and the exact system it's attached to. Generated fresh on every connect
 * so the inlined manifest is always current.
 */
function buildSystemPrompt(id) {
  let manifest = '(manifest.json could not be read)'
  try {
    manifest = fs.readFileSync(path.join(systemsDir, id, 'manifest.json'), 'utf8').trim()
  } catch {
    /* fall through with the placeholder */
  }

  return `You are embedded in the "Distributed Systems Sandbox" as a component-builder.
You are attached to the system "${id}" (its files are under systems/${id}/; your
working directory is the repo root). Your job: add or modify this system's components
when the user asks, then apply the changes.

IMPORTANT: you run INSIDE the web app's dev server (npm run dev), which reads these
files live. Never run ./start.sh — it would tear down the dev server. Rebuild a changed
service directly with docker compose (see below).

For endpoint work (adding, editing, or deleting a route on a service), use the
**sandbox-endpoint** skill — it covers the FastAPI app, the endpoints.json registry that
drives the diagram trace, and the rebuild/verify steps. For database work (adding, updating,
or deleting a datastore, **or adding/removing a primary→secondary read replica**), use the
**sandbox-database** skill — it covers the compose service + exporter, the Prometheus scrape job,
the manifest node, replica streaming/read-only wiring, and the rebuild/verify steps. For event
stream work (adding, updating, or deleting a Kafka cluster, its topics, and which services
produce/consume them), use the **sandbox-event-stream** skill — it covers the broker + exporter,
the scrape job, the manifest node + producer/consumer edges, and the streams.json topic registry.
For gRPC contract authoring (defining a .proto, running protoc, and generating the system's single
shared servicer), use the **sandbox-grpc-contract** skill; for attaching an existing contract to a
service as server and/or client (wiring the shared servicer / a client stub plus the manifest grpc
block + editable targets), use the **sandbox-grpc-attach** skill. For authoring a CLIENT FUNCTION
(implementing a client's named, argument-taking function as real Python in
systems/${id}/clients/<client>.py — calling the system through the load balancer with the lb helper,
with whatever branching/looping it needs), use the **sandbox-client-scenario** skill — pure Python, no docker.
For RUNNING an end-to-end test process (executing a process defined in systems/${id}/endtoend.json —
calling its client methods at their rates for a duration, watching its failure/constraint conditions,
and reporting a verdict), use the **sandbox-end-to-end-process** skill — it drives the running system
and writes a run report; it changes no components.

Other key files in systems/${id}/:
- manifest.json   Topology + per-node PromQL; the frontend draws the live diagram from
  it, so editing it changes what the user sees. nodes[]: { id, label, type, position,
  metrics:[{label, query, unit, scale?}], health?:{ query, rules:[{color, when}] } }
  ("when" is a tiny "value <op> number" expression, first match wins). edges[]: { from,
  to } node ids. prometheus_base is "/api/prometheus"; poll_interval_ms sets the refresh.
- docker-compose.yml   One container per node (services, databases + exporters, lb,
  prometheus). Rebuild one service:
    docker compose -f systems/${id}/docker-compose.yml up -d --build <service>
- nginx/nginx.conf   Load balancer; each service is routed at its own /<service>/ prefix.
  Reload after route changes: docker compose -f systems/${id}/docker-compose.yml exec -T lb nginx -s reload
- prometheus/prometheus.yml   Scrape targets (scrapes services/exporters directly, not
  through the lb). Restart after changes: docker compose -f systems/${id}/docker-compose.yml restart prometheus
Keep new metrics consistent between a service's app.py and manifest.json's PromQL. Reach
a service through the lb at http://localhost:8080/<service><path>.

Current systems/${id}/manifest.json:
${manifest}`
}

export default function claudeTerminal() {
  return {
    name: 'claude-terminal',
    configureServer(server) {
      // noServer: we drive the upgrade ourselves so we don't steal Vite's HMR
      // WebSocket — we only claim the `/term` path.
      const wss = new WebSocketServer({ noServer: true })

      server.httpServer?.on('upgrade', (req, socket, head) => {
        let pathname
        try {
          pathname = new URL(req.url, 'http://localhost').pathname
        } catch {
          return
        }
        if (pathname !== '/term') return // leave Vite HMR (and everything else) alone
        wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req))
      })

      wss.on('connection', (ws, req) => {
        const url = new URL(req.url, 'http://localhost')
        const id = url.searchParams.get('system')

        if (!isValidSystem(id)) {
          ws.send(`\r\n[terminal] unknown system "${id}". Closing.\r\n`)
          ws.close()
          return
        }

        // Optional per-endpoint session: the endpoint-authoring modal passes a
        // session id (a UUID it generated and saved with the endpoint) plus a
        // mode. `new` starts a fresh Claude session with that exact id and a
        // pre-filled prompt; `resume` continues an existing one. Without a valid
        // session we fall back to the general "edit this system" session.
        const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/
        const sessionRaw = url.searchParams.get('session')
        const session = sessionRaw && UUID_RE.test(sessionRaw) ? sessionRaw : null
        const mode = url.searchParams.get('mode')
        const prompt = (url.searchParams.get('prompt') || '').slice(0, 8000)

        let args
        if (session && mode === 'new') {
          // Only launched sessions get the queue protocol, so ad-hoc "Edit with
          // Claude" sessions never print the sentinel.
          args = ['--session-id', session, '--append-system-prompt', buildSystemPrompt(id) + DONE_INSTRUCTION]
          if (prompt) args.push(prompt) // positional prompt -> seeded into the session
        } else if (session && mode === 'resume') {
          args = ['--resume', session]
        } else {
          args = ['--append-system-prompt', buildSystemPrompt(id)]
        }
        // Pin model + effort ahead of the mode-specific flags for every session. When the
        // global "dangerously skip permissions" setting is on, prepend the flag too, so it
        // applies to every mode (new / resume / ad-hoc) and stays BEFORE any positional prompt.
        // Read server-side (not from a browser query param) so this escalation can't be forced
        // from the client.
        const permArgs = readSettings().dangerouslySkipPermissions ? ['--dangerously-skip-permissions'] : []
        args = [...MODEL_ARGS, ...permArgs, ...args]

        const pty = nodePty.spawn('claude', args, {
          name: 'xterm-256color',
          cwd: repoRoot,
          env: { ...process.env, TERM: 'xterm-256color' },
          cols: 80,
          rows: 24,
        })

        // Watch the output for the completion sentinel a queue-launched session prints
        // when it finishes. We keep a small ANSI-stripped rolling buffer (the token can
        // straddle two PTY chunks) and fire once, as a BINARY control frame so it never
        // collides with the normal string PTY frames the browser writes to xterm.
        let doneBuf = ''
        let doneSent = false
        pty.onData((data) => {
          if (ws.readyState === ws.OPEN) ws.send(data)
          if (doneSent || !(session && mode === 'new')) return
          doneBuf = (doneBuf + stripAnsi(data)).slice(-256)
          if (doneBuf.includes(DONE_TOKEN)) {
            doneSent = true
            doneBuf = ''
            if (ws.readyState === ws.OPEN) {
              ws.send(Buffer.from(JSON.stringify({ type: 'done', session })))
            }
          }
        })

        pty.onExit(() => {
          if (ws.readyState === ws.OPEN) ws.close()
        })

        ws.on('message', (raw) => {
          let msg
          try {
            msg = JSON.parse(raw.toString())
          } catch {
            return
          }
          if (msg.type === 'input') {
            pty.write(msg.data)
          } else if (msg.type === 'resize' && msg.cols > 0 && msg.rows > 0) {
            pty.resize(msg.cols, msg.rows)
          }
        })

        ws.on('close', () => {
          try {
            pty.kill()
          } catch {
            /* already gone */
          }
        })
      })

      server.httpServer?.on('close', () => wss.close())
    },
  }
}
