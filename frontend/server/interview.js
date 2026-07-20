// Vite dev-server plugin: Interview mode — a mock system-design interview driven by a
// headless `claude -p` chat session (the browser renders it as a chat panel, NOT the
// xterm terminal). The interviewer presents a question from the curated bank
// (interviewQuestions.js), scopes functional / non-functional requirements over a
// back-and-forth, records them via the routes below (they render as text boxes on the
// diagram), and then drives the normal design flows itself. Starting an interview
// WIPES the system to the empty canvas (interviewReset.js) — the frontend warns first.
//
//   GET    /api/interview?system=<id>
//     -> { ok, interview: <interview.json or null>, turnInFlight, skipPermissions }
//        `skipPermissions` is the server-side dangerouslySkipPermissions setting: the
//        headless session cannot answer permission prompts, so the panel hard-gates
//        Start on it.
//   POST   /api/interview/reset    { system }            -> mechanical wipe only (curl-testable)
//   POST   /api/interview/start    { system }            -> wipe + pick a question + fresh interview.json
//   POST   /api/interview/message  { system, text }      -> ONE chat turn; streams application/x-ndjson
//        lines: {type:'assistant_text',text} | {type:'tool_use',name,summary} |
//        {type:'result',ok} | {type:'error',message}. One turn at a time per system.
//   POST   /api/interview/stop-turn { system }           -> kill an in-flight turn
//   POST   /api/interview/requirements { system, kind:'functional'|'nonfunctional',
//                                        op:'add'|'update'|'remove', id?, text?, processId? }
//        The write API the SESSION curls as requirements converge; processId links a
//        requirement to its generated endtoend.json test process.
//   POST   /api/interview/phase    { system, phase }     -> functional | nonfunctional | design
//   POST   /api/interview/layout   { system, frBox?, nfrBox? }  -> diagram text-box rects
//   POST   /api/interview/end      { system }            -> status flip only; artifacts stay
//
// interview.json is the single source of truth (question, conversationId, phase,
// requirement lists, box layout, chat transcript) — the panel restores everything from
// one GET after a reload. All writes go through this backend; every read-modify-write
// is synchronous (no await between read and write), so writes can't tear. The claude
// CLI keeps the actual conversation state on disk keyed by conversationId, so turns
// survive dev-server restarts via --resume.
import fs from 'node:fs'
import path from 'node:path'
import readline from 'node:readline'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { repoRoot, systemDir, isValidSystem } from './systems.js'
import { bad, HttpError, readJsonBody } from './scaffold.js'
import { readSettings } from './settings.js'
import { MODEL_ARGS } from './terminal.js'
import { resetSystem } from './interviewReset.js'
import { pickQuestion } from './interviewQuestions.js'
import { isRunActive, clearRunState } from './endtoend.js'
import { clearOutages } from './outage.js'

const conflict = (msg) => new HttpError(409, msg)

const MAX_MESSAGE = 8000 // mirrors terminal.js' prompt cap
const MAX_REQ_TEXT = 500 // mirrors endtoend.js' condition cap
const MAX_REQ_ROWS = 20
const PHASES = ['functional', 'nonfunctional', 'design']
const MIN_BOX = 120
const DEFAULT_LAYOUT = {
  frBox: { x: 560, y: 60, w: 300, h: 190 },
  nfrBox: { x: 560, y: 280, w: 300, h: 190 },
}

// system id -> { child, startedAt }. One chat turn at a time per system (matches the
// single-writer model: while an interview is active, the interviewer session is the
// only thing mutating the system).
const turns = new Map()

// --- registry (systems/<id>/interview.json) --------------------------------------

function interviewFile(system) {
  return path.join(systemDir(system), 'interview.json')
}
function readInterview(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(interviewFile(system), 'utf8'))
    return raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : null
  } catch {
    return null
  }
}
function writeInterview(system, data) {
  fs.writeFileSync(interviewFile(system), JSON.stringify(data, null, 2) + '\n')
}

// Write-through transcript append: one entry per chat event, persisted as it happens,
// so a reloaded panel (or a crashed dev server) loses at most the in-flight tail.
function appendMessage(system, role, text) {
  const interview = readInterview(system)
  if (!interview) return
  if (!Array.isArray(interview.messages)) interview.messages = []
  interview.messages.push({ role, text, ts: new Date().toISOString() })
  writeInterview(system, interview)
}

function markCliSessionStarted(system) {
  const interview = readInterview(system)
  if (!interview || interview.cliSessionStarted) return
  interview.cliSessionStarted = true
  writeInterview(system, interview)
}

function processExists(system, id) {
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'endtoend.json'), 'utf8'))
    return Array.isArray(raw?.processes) && raw.processes.some((p) => p && p.id === id)
  } catch {
    return false
  }
}

// --- start / reset ---------------------------------------------------------------

// Refuse to wipe a system that's mid-anything: an in-flight chat turn or a running
// end-to-end process would be left pointing at deleted containers.
function guardIdle(system) {
  if (turns.has(system)) throw conflict('an interview chat turn is in flight — wait for it or stop it first')
  if (isRunActive(system)) throw conflict('an end-to-end process is running in this system — stop it first')
}

async function handleReset(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  guardIdle(system)
  clearRunState(system)
  clearOutages(system)
  const log = await resetSystem(system)
  return { ok: true, log }
}

async function handleStart(body, req) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  guardIdle(system)
  // Remember the outgoing question (if any) before the reset deletes interview.json,
  // so back-to-back interviews get a different one.
  const excludeId = readInterview(system)?.question?.id
  clearRunState(system)
  clearOutages(system)
  const log = await resetSystem(system)
  const interview = {
    status: 'active',
    question: pickQuestion(excludeId),
    conversationId: randomUUID(),
    // Recorded so the interviewer session knows where to curl on EVERY turn — the
    // system prompt (which also carries it) is only sent on the first one.
    apiBase: `http://${req.headers.host || 'localhost:5173'}`,
    startedAt: new Date().toISOString(),
    endedAt: null,
    cliSessionStarted: false,
    phase: 'functional',
    functionalRequirements: [],
    nonFunctionalRequirements: [],
    layout: JSON.parse(JSON.stringify(DEFAULT_LAYOUT)),
    messages: [],
  }
  writeInterview(system, interview)
  return { ok: true, interview, log }
}

// --- interview state routes (the session's write API) ----------------------------

function requireInterview(system) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const interview = readInterview(system)
  if (!interview) throw bad('no interview exists in this system')
  return interview
}

function handleRequirements(body) {
  const { system, kind, op, id } = body
  const interview = requireInterview(system)
  if (interview.status !== 'active') throw conflict('the interview has ended')
  const key =
    kind === 'functional' ? 'functionalRequirements'
    : kind === 'nonfunctional' ? 'nonFunctionalRequirements'
    : null
  if (!key) throw bad('kind must be "functional" or "nonfunctional"')
  const list = Array.isArray(interview[key]) ? interview[key] : []

  if (op === 'add') {
    const text = typeof body.text === 'string' ? body.text.trim() : ''
    if (!text) throw bad('text is required')
    if (text.length > MAX_REQ_TEXT) throw bad(`requirement text is too long (max ${MAX_REQ_TEXT})`)
    if (list.length >= MAX_REQ_ROWS) throw bad(`at most ${MAX_REQ_ROWS} ${kind} requirements`)
    const prefix = kind === 'functional' ? 'fr' : 'nfr'
    const next =
      1 + list.reduce((m, r) => Math.max(m, Number(String(r?.id || '').split('-')[1]) || 0), 0)
    list.push({ id: `${prefix}-${next}`, text, processId: null, ts: new Date().toISOString() })
  } else if (op === 'update') {
    const row = list.find((r) => r && r.id === id)
    if (!row) throw bad(`no ${kind} requirement "${id}"`)
    if (body.text !== undefined) {
      const text = typeof body.text === 'string' ? body.text.trim() : ''
      if (!text) throw bad('text cannot be empty')
      if (text.length > MAX_REQ_TEXT) throw bad(`requirement text is too long (max ${MAX_REQ_TEXT})`)
      row.text = text
    }
    if (body.processId !== undefined) {
      if (body.processId === null) {
        row.processId = null
      } else {
        if (typeof body.processId !== 'string' || !processExists(system, body.processId)) {
          throw bad(`processId "${body.processId}" is not an end-to-end process in this system`)
        }
        row.processId = body.processId
      }
    }
  } else if (op === 'remove') {
    const i = list.findIndex((r) => r && r.id === id)
    if (i < 0) throw bad(`no ${kind} requirement "${id}"`)
    list.splice(i, 1)
  } else {
    throw bad('op must be "add", "update" or "remove"')
  }

  interview[key] = list
  writeInterview(system, interview)
  return { ok: true, interview }
}

function handlePhase(body) {
  const { system, phase } = body
  const interview = requireInterview(system)
  if (interview.status !== 'active') throw conflict('the interview has ended')
  if (!PHASES.includes(phase)) throw bad(`phase must be one of: ${PHASES.join(', ')}`)
  interview.phase = phase
  writeInterview(system, interview)
  return { ok: true, interview }
}

function handleLayout(body) {
  const { system } = body
  const interview = requireInterview(system)
  if (!interview.layout) interview.layout = JSON.parse(JSON.stringify(DEFAULT_LAYOUT))
  const num = (v, fallback) => {
    const n = Number(v)
    return Number.isFinite(n) ? Math.round(n) : fallback
  }
  for (const box of ['frBox', 'nfrBox']) {
    const r = body[box]
    if (!r || typeof r !== 'object') continue
    const cur = interview.layout[box] || DEFAULT_LAYOUT[box]
    interview.layout[box] = {
      x: num(r.x, cur.x),
      y: num(r.y, cur.y),
      w: Math.max(MIN_BOX, num(r.w, cur.w)),
      h: Math.max(MIN_BOX, num(r.h, cur.h)),
    }
  }
  writeInterview(system, interview)
  return { ok: true, layout: interview.layout }
}

function handleEnd(body) {
  const { system } = body
  const interview = requireInterview(system)
  if (turns.has(system)) throw conflict('a chat turn is in flight — wait for it or stop it first')
  interview.status = 'ended'
  interview.endedAt = new Date().toISOString()
  writeInterview(system, interview)
  return { ok: true, interview }
}

function handleStopTurn(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const t = turns.get(system)
  if (t) {
    try {
      t.child.kill('SIGTERM')
    } catch {
      /* already gone */
    }
    turns.delete(system)
  }
  return { ok: true, stopped: Boolean(t) }
}

// --- the chat turn ---------------------------------------------------------------

// Deliberately thin: durable behavior lives in the sandbox-interview skill, because
// this system prompt is only sent on the FIRST turn (--resume re-sends nothing).
function buildInterviewerPrompt(system, interview) {
  return `You are the INTERVIEWER in a mock system-design interview inside the "Distributed
Systems Sandbox" web app. The **sandbox-interview** skill is the canonical procedure for
every part of this job (requirement scoping, the interview state API, the design phase,
test authoring) — read and follow it before doing anything else.

You are attached to the system "${system}" (files under systems/${system}/; your working
directory is the repo root). The web app's backend is at ${interview.apiBase} — record
interview state through its /api/interview routes as the skill describes.

The interview question (adapted from ${interview.question?.source?.name || 'the question bank'}):
${JSON.stringify(interview.question, null, 2)}

You are rendered as a CHAT, not a terminal: reply in short conversational prose (no
markdown headers, no tables, no walls of text) and ask ONE question at a time, then stop
and wait for the candidate's reply. This session is resumed headlessly per chat turn and
this system prompt is NOT re-sent on later turns — durable state lives in
systems/${system}/interview.json and the skill.`
}

// One-line, human-readable description of a tool call for the chat's status rows.
function toolSummary(name, input) {
  const inp = input && typeof input === 'object' ? input : {}
  let detail = ''
  if (name === 'Bash') {
    detail = inp.description || String(inp.command || '').replace(/\s+/g, ' ')
  } else if (['Edit', 'Write', 'Read', 'NotebookEdit'].includes(name)) {
    const p = inp.file_path || inp.notebook_path || ''
    detail = p ? path.relative(repoRoot, p) : ''
  } else if (name === 'Skill') {
    detail = inp.skill || ''
  } else if (name === 'Glob' || name === 'Grep') {
    detail = inp.pattern || ''
  }
  const label = detail ? `${name}: ${detail}` : name
  return `⚙ ${label.slice(0, 120)}`
}

async function handleMessage(req, res) {
  const body = await readJsonBody(req)
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const text = typeof body.text === 'string' ? body.text.trim().slice(0, MAX_MESSAGE) : ''
  if (!text) throw bad('text is required')
  const interview = readInterview(system)
  if (!interview || interview.status !== 'active') throw conflict('no active interview in this system')
  if (turns.has(system)) throw conflict('a chat turn is already in flight')

  appendMessage(system, 'user', text)

  // Headless print mode: stream-json (requires --verbose) gives one JSON line per
  // event. The first turn creates the CLI session under our conversationId with the
  // interviewer system prompt; later turns --resume it. Permissions can't prompt in
  // -p mode, so the skip flag comes from the same server-side setting terminal.js
  // uses (the panel hard-gates Start on it).
  const permArgs = readSettings().dangerouslySkipPermissions ? ['--dangerously-skip-permissions'] : []
  const base = ['-p', '--output-format', 'stream-json', '--verbose', ...MODEL_ARGS, ...permArgs]
  const firstTurn = !interview.cliSessionStarted
  const newArgs = [
    ...base,
    '--session-id',
    interview.conversationId,
    '--append-system-prompt',
    buildInterviewerPrompt(system, interview),
    text,
  ]
  const resumeArgs = [...base, '--resume', interview.conversationId, text]

  res.statusCode = 200
  res.setHeader('Content-Type', 'application/x-ndjson')
  res.setHeader('Cache-Control', 'no-cache')
  res.flushHeaders?.()
  req.setTimeout?.(0)

  const send = (obj) => {
    try {
      res.write(JSON.stringify(obj) + '\n')
    } catch {
      /* browser went away — the turn keeps running and persisting */
    }
  }

  // A retried kickoff (first turn failed after creating the CLI session but before a
  // result landed) hits "session id already in use" — fall back to --resume once.
  let retriedAsResume = false

  const launch = (args) => {
    const child = spawn('claude', args, { cwd: repoRoot, env: { ...process.env } })
    turns.set(system, { child, startedAt: Date.now() })

    let sawResult = false
    let stderrTail = ''
    child.stderr.on('data', (d) => {
      stderrTail = (stderrTail + d).slice(-2000)
    })

    const rl = readline.createInterface({ input: child.stdout })
    rl.on('line', (line) => {
      let evt
      try {
        evt = JSON.parse(line)
      } catch {
        console.warn('[interview] skipping unparseable stream-json line:', line.slice(0, 120))
        return
      }
      if (evt.type === 'assistant' && Array.isArray(evt.message?.content)) {
        for (const block of evt.message.content) {
          if (block.type === 'text' && block.text?.trim()) {
            send({ type: 'assistant_text', text: block.text })
            appendMessage(system, 'assistant', block.text)
          } else if (block.type === 'tool_use') {
            const summary = toolSummary(block.name, block.input)
            send({ type: 'tool_use', name: block.name, summary })
            appendMessage(system, 'status', summary)
          }
        }
      } else if (evt.type === 'result') {
        // The final result duplicates the last assistant text — emit only the verdict.
        sawResult = true
        const ok = evt.subtype === 'success'
        if (ok) markCliSessionStarted(system)
        send(ok ? { type: 'result', ok: true } : { type: 'result', ok: false, error: evt.subtype || 'error' })
      }
      // system/init and user (tool-result) events are noise here — skipped.
    })

    child.on('error', (err) => {
      turns.delete(system)
      const message = `could not launch claude: ${err.message}`
      send({ type: 'error', message })
      appendMessage(system, 'status', `⚠ turn failed — ${message}`)
      res.end()
    })

    child.on('close', (code) => {
      // Only this launch's entry (stop-turn may have cleared it already).
      if (turns.get(system)?.child === child) turns.delete(system)
      if (!sawResult) {
        if (firstTurn && !retriedAsResume && /already in use|already exists/i.test(stderrTail)) {
          retriedAsResume = true
          launch(resumeArgs)
          return
        }
        const detail = stderrTail.trim().slice(-400)
        const message = `claude exited (code ${code}) without a result${detail ? `: ${detail}` : ''}`
        send({ type: 'error', message })
        appendMessage(system, 'status', `⚠ turn failed — ${message}`)
      }
      res.end()
    })
  }

  launch(firstTurn ? newArgs : resumeArgs)
  // NOTE: no res.on('close') kill — if the browser reloads mid-turn, the turn keeps
  // running and persisting to interview.json; the reloaded panel sees turnInFlight
  // and catches up from the transcript. Only /stop-turn kills a turn.
}

// --- plugin ----------------------------------------------------------------------

export default function interview() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  const wrap = (fn) => async (req, res) => {
    try {
      json(res, 200, await fn(await readJsonBody(req), req))
    } catch (err) {
      json(res, err.statusCode || 500, { ok: false, error: err.message })
    }
  }
  return {
    name: 'interview',
    configureServer(server) {
      // Sub-routes first — Connect matches by prefix, so /api/interview would
      // otherwise swallow them.
      const post = (route, fn) =>
        server.middlewares.use(route, (req, res, next) =>
          req.method === 'POST' ? wrap(fn)(req, res) : next(),
        )
      post('/api/interview/reset', handleReset)
      post('/api/interview/start', handleStart)
      post('/api/interview/stop-turn', handleStopTurn)
      post('/api/interview/requirements', handleRequirements)
      post('/api/interview/phase', handlePhase)
      post('/api/interview/layout', handleLayout)
      post('/api/interview/end', handleEnd)

      // The streaming turn writes its own NDJSON response — not wrapped in json().
      server.middlewares.use('/api/interview/message', (req, res, next) => {
        if (req.method !== 'POST') return next()
        handleMessage(req, res).catch((err) => {
          json(res, err.statusCode || 500, { ok: false, error: err.message })
        })
      })

      server.middlewares.use('/api/interview', (req, res, next) => {
        if (req.method !== 'GET') return next()
        try {
          const system = new URL(req.url, 'http://localhost').searchParams.get('system')
          if (!isValidSystem(system)) return json(res, 400, { ok: false, error: 'unknown system' })
          return json(res, 200, {
            ok: true,
            interview: readInterview(system),
            turnInFlight: turns.has(system),
            skipPermissions: readSettings().dangerouslySkipPermissions === true,
          })
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
