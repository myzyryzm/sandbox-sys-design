import { useEffect, useRef, useState } from 'react'
import { Terminal as XTerm } from '@xterm/xterm'
import { FitAddon } from '@xterm/addon-fit'
import '@xterm/xterm/css/xterm.css'

/**
 * Browser terminal wired to a Claude Code session for the given system.
 *
 * Streams over the dev server's `/term` WebSocket (see frontend/server/terminal.js):
 *   - keystrokes  -> { type: 'input', data }
 *   - resizes     -> { type: 'resize', cols, rows }
 *   - server sends raw PTY output, written straight to xterm.
 *
 * An optional `session` ({ sessionId, mode, prompt }) attaches the connection to a
 * specific Claude session: mode 'new' starts `claude --session-id <id>` with the
 * prompt pre-filled (used by the endpoint-authoring modal); mode 'resume' continues
 * `claude --resume <id>`. The connect effect re-runs when the session changes, so
 * launching an endpoint reconnects to its session. `onLaunched` fires once the
 * socket opens so the parent can flip a 'new' session to 'resume' (re-running
 * --session-id on the same id would error).
 */
export default function Terminal({ systemId, session = null, onLaunched, onSessionDone }) {
  const containerRef = useRef(null)
  const [status, setStatus] = useState('connecting')

  const sessionId = session?.sessionId || null
  const mode = session?.mode || null
  const prompt = session?.prompt || ''

  // Keep the latest onSessionDone callback in a ref: the connect effect only re-runs
  // when system/session change, so reading the prop directly would go stale.
  const onDoneRef = useRef(onSessionDone)
  useEffect(() => {
    onDoneRef.current = onSessionDone
  })

  useEffect(() => {
    const term = new XTerm({
      cursorBlink: true,
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
      fontSize: 13,
      theme: { background: '#0b0d12', foreground: '#e6e6e6' },
    })
    const fit = new FitAddon()
    term.loadAddon(fit)
    term.open(containerRef.current)
    fit.fit()

    let ws = null
    let cancelled = false

    const sendResize = () => {
      try {
        fit.fit()
      } catch {
        return
      }
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'resize', cols: term.cols, rows: term.rows }))
      }
    }

    const connect = () => {
      if (cancelled) return
      const proto = location.protocol === 'https:' ? 'wss' : 'ws'
      const params = new URLSearchParams({ system: systemId })
      if (sessionId && mode) {
        params.set('session', sessionId)
        params.set('mode', mode)
        if (mode === 'new' && prompt) params.set('prompt', prompt)
      }
      ws = new WebSocket(`${proto}://${location.host}/term?${params.toString()}`)
      // Terminal output arrives as string frames; the server's completion signal is a
      // single BINARY control frame — request it as an ArrayBuffer so we can tell them apart.
      ws.binaryType = 'arraybuffer'

      ws.onopen = () => {
        setStatus('connected')
        sendResize()
        term.focus()
        // Now that the session id exists, demote a 'new' launch to 'resume' so a
        // later remount resumes it instead of trying to re-create the id.
        if (sessionId && mode === 'new' && onLaunched) onLaunched(sessionId)
      }
      ws.onmessage = (e) => {
        if (typeof e.data === 'string') {
          term.write(e.data)
          return
        }
        // Binary control frame from the server (e.g. the completion sentinel).
        try {
          const msg = JSON.parse(new TextDecoder().decode(e.data))
          if (msg?.type === 'done') onDoneRef.current?.(sessionId)
        } catch {
          /* ignore malformed control frames */
        }
      }
      ws.onclose = () => setStatus('disconnected')
      ws.onerror = () => setStatus('disconnected')
    }

    // Defer the connect one tick. React StrictMode (always active here — the app
    // runs under `npm run dev`) mounts → unmounts → remounts synchronously; the
    // delay lets the phantom first mount's cleanup cancel BEFORE we ever open a
    // socket / spawn a `claude` pty. Otherwise a 'new' session would be created
    // and immediately torn down, so the later `--resume` fails with
    // "No conversation found with session ID …".
    const timer = setTimeout(connect, 0)

    const dataSub = term.onData((data) => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'input', data }))
      }
    })

    const ro = new ResizeObserver(sendResize)
    ro.observe(containerRef.current)

    return () => {
      cancelled = true
      clearTimeout(timer)
      ro.disconnect()
      dataSub.dispose()
      if (ws) ws.close()
      term.dispose()
    }
    // Reconnect only when the SYSTEM or SESSION changes — NOT when `mode` flips
    // (the new->resume demotion above must not tear down the live session).
    // `mode`/`prompt` are read at connect time, set atomically with `sessionId`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [systemId, sessionId])

  return (
    <div className="terminal-wrap">
      <div className="terminal-status" data-status={status}>
        claude · <code>{systemId}</code>
        {sessionId ? <> · {mode === 'resume' ? 'resumed' : 'session'} <code>{sessionId.slice(0, 8)}</code></> : null}
        {' '}· {status}
      </div>
      <div className="terminal-host" ref={containerRef} />
    </div>
  )
}
