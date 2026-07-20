import './InterviewPanel.css'
import { useEffect, useRef, useState } from 'react'

// The Interview-mode chat panel: a right-side drawer where a headless `claude -p`
// session (one spawn per turn, resumed by conversation id — frontend/server/interview.js)
// plays the interviewer. The panel renders the persisted transcript (interview.messages)
// plus the live NDJSON stream of the in-flight turn, the scoped requirements with their
// generate/run/verdict state, and the start/end lifecycle. All interview STATE lives in
// systems/<id>/interview.json — App polls GET /api/interview and passes it down, so a
// page reload restores everything.
//
// One-writer rule: while an interview is active the interviewer session is the only
// thing mutating the system. "Generate test" therefore sends a canned CHAT message
// (the session authors the endtoend process itself) instead of enqueueing a separate
// edit session; only RUNNING an authored test uses the normal edit queue (onLaunch),
// because a run drives the system without designing it.
import { buildEndToEndRunPrompt } from './endToEndBank'
import type { EndToEndProcess } from './types/registries'
import type { LaunchSession } from './types/customTypes'

const RUN_DURATION_SECONDS = 60

// ─── systems/<id>/interview.json (polled by App, passed down) ───────────────

export interface InterviewMessage {
  role: 'user' | 'assistant' | 'status' | (string & {})
  text: string
}

export interface InterviewRequirement {
  id: string
  text: string
  // Linked end-to-end process (the requirement's authored "unit test").
  processId?: string | null
}

export interface InterviewQuestion {
  title?: string
  statement?: string
  source?: { url?: string; name?: string }
}

export interface InterviewState {
  status?: string // 'active' | 'ended'
  phase?: string
  conversationId?: string
  question?: InterviewQuestion
  messages?: InterviewMessage[]
  functionalRequirements?: InterviewRequirement[]
  nonFunctionalRequirements?: InterviewRequirement[]
  // Diagram render state for the requirement text boxes (persisted via
  // POST /api/interview/layout, read by SystemDiagram).
  layout?: {
    frBox?: { x: number; y: number; w: number; h: number }
    nfrBox?: { x: number; y: number; w: number; h: number }
  } | null
}

// One NDJSON event of the streamed interviewer turn.
interface TurnEvent {
  type?: string
  text?: string
  summary?: string
  message?: string
  ok?: boolean
  error?: string
}

// Minimal chat-text renderer: **bold** and `code` spans (the interviewer writes light
// markdown even when asked not to). Everything else stays plain text (pre-wrap).
function renderChatText(text: string) {
  const parts = String(text).split(/(\*\*[^*]+\*\*|`[^`]+`)/g)
  return parts.map((p, i) => {
    if (p.startsWith('**') && p.endsWith('**')) return <strong key={i}>{p.slice(2, -2)}</strong>
    if (p.startsWith('`') && p.endsWith('`')) return <code key={i}>{p.slice(1, -1)}</code>
    return p
  })
}

interface InterviewPanelProps {
  systemId: string
  interview?: InterviewState | null
  turnInFlight?: boolean
  skipPermissions?: boolean
  onRefresh?: () => void
  onLaunch: LaunchSession
  onClose: () => void
}

export default function InterviewPanel({
  systemId,
  interview,
  turnInFlight,
  skipPermissions,
  onRefresh,
  onLaunch,
  onClose,
}: InterviewPanelProps) {
  const [draft, setDraft] = useState('')
  // Local turn-in-flight (this tab is streaming a turn). The polled `turnInFlight`
  // covers the reload-mid-turn case where no local stream exists.
  const [sending, setSending] = useState(false)
  const sendingRef = useRef(false)
  // The rendered transcript. Reconciled from the polled interview.messages whenever no
  // local turn is streaming; appended-to locally (user bubble + streamed events) during
  // one, so the chat feels live while the server file stays the source of truth.
  const [messages, setMessages] = useState<InterviewMessage[]>(interview?.messages || [])
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false) // start/end lifecycle actions
  const [confirmStart, setConfirmStart] = useState(false)
  const [confirmEnd, setConfirmEnd] = useState(false)
  // endtoend.json processes (their lastRun carries a linked requirement's verdict).
  const [processes, setProcesses] = useState<EndToEndProcess[]>([])
  const scrollRef = useRef<HTMLDivElement | null>(null)

  const active = interview?.status === 'active'
  const locked = sending || turnInFlight

  useEffect(() => {
    sendingRef.current = sending
  }, [sending])

  // Reconcile the transcript from the server between turns (and while a reloaded tab
  // watches a turn it didn't start — the write-through appends surface via the poll).
  useEffect(() => {
    if (!sendingRef.current) setMessages(interview?.messages || [])
  }, [interview?.messages?.length, interview?.conversationId, interview?.status])

  // Verdict poll: the linked processes' lastRun (PASS/FAIL) for the requirement rows.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(`/api/endtoend?system=${systemId}`)
        const data = (await res.json()) as { ok?: boolean; processes?: EndToEndProcess[] }
        if (!cancelled && data.ok) setProcesses(data.processes || [])
      } catch {
        /* keep the last good list */
      }
    }
    tick()
    const id = setInterval(tick, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [systemId])

  // Keep the newest message in view.
  useEffect(() => {
    const el = scrollRef.current
    if (el) el.scrollTop = el.scrollHeight
  }, [messages.length])

  async function sendMessage(text: string) {
    const trimmed = text.trim()
    if (!trimmed || sendingRef.current) return
    setSending(true)
    sendingRef.current = true
    setError(null)
    setMessages((m) => [...m, { role: 'user', text: trimmed }])
    setDraft('')
    try {
      const res = await fetch('/api/interview/message', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, text: trimmed }),
      })
      if (!res.ok || !res.body) {
        const d = (await res.json().catch(() => ({}))) as { error?: string }
        throw new Error(d.error || `HTTP ${res.status}`)
      }
      const reader = res.body.getReader()
      const dec = new TextDecoder()
      let buf = ''
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })
        let i
        while ((i = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, i)
          buf = buf.slice(i + 1)
          if (!line.trim()) continue
          let evt: TurnEvent
          try {
            evt = JSON.parse(line) as TurnEvent
          } catch {
            continue
          }
          if (evt.type === 'assistant_text') {
            setMessages((m) => [...m, { role: 'assistant', text: evt.text || '' }])
          } else if (evt.type === 'tool_use') {
            setMessages((m) => [...m, { role: 'status', text: evt.summary || '' }])
          } else if (evt.type === 'error') {
            setError(evt.message || 'turn error')
          } else if (evt.type === 'result' && !evt.ok) {
            setError(`The turn ended with an error (${evt.error}). Send again to retry.`)
          }
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setSending(false)
      sendingRef.current = false
      onRefresh?.()
    }
  }

  async function startInterview() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/interview/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId }),
      })
      const d = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      setConfirmStart(false)
      setMessages([])
      onRefresh?.()
      // Kick off the first interviewer turn as an honest, visible user message.
      sendMessage('Begin the interview.')
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function endInterview() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/interview/end', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId }),
      })
      const d = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      setConfirmEnd(false)
      onRefresh?.()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  async function stopTurn() {
    try {
      await fetch('/api/interview/stop-turn', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId }),
      })
    } catch {
      /* the stream's close will surface the state */
    }
    onRefresh?.()
  }

  function generateTest(req: InterviewRequirement, kind: string) {
    sendMessage(
      `Generate an end-to-end test for requirement ${req.id}: "${req.text}". ` +
        `Author it as an end-to-end process (POST /api/endtoend — create any missing client ` +
        `or client function first if feasible), then link it with POST /api/interview/requirements ` +
        `{"system":"${systemId}","kind":"${kind}","op":"update","id":"${req.id}","processId":"<the new process id>"}. ` +
        `If the design doesn't yet have what the test needs, explain what's missing and leave it pending.`,
    )
  }

  // Same start flow as EndToEndModal.startProcess: mark running, then enqueue the
  // normal sandbox-end-to-end-process run session.
  async function runTest(proc: EndToEndProcess) {
    setBusy(true)
    setError(null)
    try {
      const sessionId = crypto.randomUUID()
      const res = await fetch('/api/endtoend/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          id: proc.id,
          duration_seconds: RUN_DURATION_SECONDS,
        }),
      })
      const d = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      onLaunch(
        {
          sessionId,
          mode: 'new',
          prompt: buildEndToEndRunPrompt({
            systemId,
            processId: proc.id,
            processName: proc.name,
            durationSeconds: RUN_DURATION_SECONDS,
            apiBase: location.origin,
          }),
        },
        { kind: 'e2e', target: proc.id, title: proc.name },
      )
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }

  function requirementRow(req: InterviewRequirement, kind: string) {
    const proc = req.processId ? processes.find((p) => p.id === req.processId) : null
    const verdict = proc?.lastRun?.verdict || null
    return (
      <div className="iv-req-row" key={req.id}>
        <span
          className={`iv-req-dot ${
            verdict ? (verdict === 'PASS' ? 'pass' : 'fail') : proc ? 'ready' : 'none'
          }`}
          title={
            verdict
              ? `last run: ${verdict}`
              : proc
                ? 'test authored — not run yet'
                : 'no test yet'
          }
        />
        <span className="iv-req-id">{req.id}</span>
        <span className="iv-req-text">{req.text}</span>
        {active && !proc && (
          <button
            className="iv-req-btn"
            disabled={locked || busy}
            title="Ask the interviewer to author this requirement's end-to-end test"
            onClick={() => generateTest(req, kind)}
          >
            Generate test
          </button>
        )}
        {proc && (
          <>
            {verdict && (
              <span className={`sim-status ${verdict === 'PASS' ? 'on' : 'off'}`}>{verdict}</span>
            )}
            <button
              className="iv-req-btn"
              disabled={busy || locked}
              title={`Run this test for ${RUN_DURATION_SECONDS}s (queued like any end-to-end run)`}
              onClick={() => runTest(proc)}
            >
              Run
            </button>
          </>
        )}
      </div>
    )
  }

  return (
    <div className="interview-panel">
      <header className="iv-head">
        <h2>🎙 Interview</h2>
        {interview?.question && (
          <span className="iv-question" title={interview.question.statement}>
            {interview.question.title}
            {' — '}
            <a href={interview.question.source?.url} target="_blank" rel="noreferrer">
              adapted from {interview.question.source?.name}
            </a>
          </span>
        )}
        {interview && <span className={`iv-phase iv-phase-${interview.phase}`}>{active ? interview.phase : 'ended'}</span>}
        <button className="modal-close" onClick={onClose}>
          ✕
        </button>
      </header>

      {!skipPermissions && (
        <div className="iv-banner">
          The interview runs Claude headlessly — it cannot answer permission prompts. Enable
          “Dangerously skip permissions” in ⚙ Settings first.
        </div>
      )}

      {!interview || interview.status === 'ended' ? (
        <div className="iv-idle">
          {interview?.status === 'ended' && (
            <p className="sim-desc">
              This interview has ended — its transcript and requirements stay below. Starting a
              new one clears the whole system again.
            </p>
          )}
          {!confirmStart ? (
            <button
              className="primary"
              disabled={busy || !skipPermissions}
              onClick={() => setConfirmStart(true)}
            >
              Start interview
            </button>
          ) : (
            <div className="iv-confirm">
              <p>
                Starting an interview <strong>wipes the current system</strong>: every service,
                database, stream, client, model, endpoint and test is deleted and the canvas
                resets to an empty lb + Prometheus. This cannot be undone. A random question is
                then drawn for you.
              </p>
              <button className="primary" disabled={busy} onClick={startInterview}>
                {busy ? 'Resetting system…' : 'Wipe it — start the interview'}
              </button>
              <button disabled={busy} onClick={() => setConfirmStart(false)}>
                Cancel
              </button>
            </div>
          )}
        </div>
      ) : null}

      {interview && (
        <>
          <div className="iv-messages" ref={scrollRef}>
            {messages.map((m, i) =>
              m.role === 'status' ? (
                <div className="iv-msg-status" key={i}>
                  {m.text}
                </div>
              ) : (
                <div className={`iv-msg iv-msg-${m.role}`} key={i}>
                  {renderChatText(m.text)}
                </div>
              ),
            )}
            {locked && <div className="iv-msg-status iv-thinking">interviewer is working…</div>}
          </div>

          {((interview.functionalRequirements?.length ?? 0) > 0 ||
            (interview.nonFunctionalRequirements?.length ?? 0) > 0) && (
            <div className="iv-reqs">
              {(interview.functionalRequirements?.length ?? 0) > 0 && (
                <>
                  <div className="iv-reqs-title">Functional requirements</div>
                  {interview.functionalRequirements!.map((r) => requirementRow(r, 'functional'))}
                </>
              )}
              {(interview.nonFunctionalRequirements?.length ?? 0) > 0 && (
                <>
                  <div className="iv-reqs-title">Non-functional requirements</div>
                  {interview.nonFunctionalRequirements!.map((r) =>
                    requirementRow(r, 'nonfunctional'),
                  )}
                </>
              )}
            </div>
          )}

          {error && <div className="iv-error">{error}</div>}

          {active && (
            <div className="iv-input-row">
              <textarea
                value={draft}
                placeholder={locked ? 'wait for the interviewer…' : 'type a reply…'}
                disabled={locked}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault()
                    sendMessage(draft)
                  }
                }}
              />
              <div className="iv-input-actions">
                <button className="primary" disabled={locked || !draft.trim()} onClick={() => sendMessage(draft)}>
                  Send
                </button>
                {locked && (
                  <button className="iv-stop" title="Kill the in-flight turn" onClick={stopTurn}>
                    stop turn
                  </button>
                )}
              </div>
            </div>
          )}

          {active && (
            <div className="iv-foot">
              {!confirmEnd ? (
                <button disabled={busy || locked} onClick={() => setConfirmEnd(true)}>
                  End interview
                </button>
              ) : (
                <>
                  <span className="sim-desc">End it? The design and transcript stay.</span>
                  <button className="primary" disabled={busy} onClick={endInterview}>
                    End
                  </button>
                  <button disabled={busy} onClick={() => setConfirmEnd(false)}>
                    Keep going
                  </button>
                </>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}
