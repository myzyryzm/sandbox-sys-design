import { useEffect, useState } from 'react'
import { Link, Navigate, useParams } from 'react-router-dom'
import App from './App'
import { systemIdError } from './systemId'
import './SystemPage.css'

/**
 * Route wrapper for /systems/:systemId. Before mounting the diagram app it
 * ensures the system's docker stack actually owns the shared host ports
 * (8080 lb / 9090 prometheus) via POST /api/systems/activate — which downs the
 * previously active system if a different one was running. Deep links, entry-
 * screen clicks and refreshes all start a stack through this one code path;
 * the server fast-paths the "already running" case so re-mounts are cheap.
 */
export default function SystemPage() {
  const { systemId } = useParams()
  const [phase, setPhase] = useState<'starting' | 'ready' | 'error'>('starting')
  const [error, setError] = useState<string | null>(null)
  // Bumping this re-runs the activate effect (the Retry button).
  const [attempt, setAttempt] = useState(0)

  const badId = !systemId || systemIdError(systemId) !== null

  useEffect(() => {
    if (badId) return
    let cancelled = false
    setPhase('starting')
    setError(null)
    fetch('/api/systems/activate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ system: systemId }),
    })
      .then(async (res) => {
        const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
        if (!cancelled) setPhase('ready')
      })
      .catch((err) => {
        if (cancelled) return
        setPhase('error')
        setError(err instanceof Error ? err.message : String(err))
      })
    return () => { cancelled = true }
  }, [systemId, badId, attempt])

  if (badId) return <Navigate to="/" replace />

  if (phase === 'starting') {
    return (
      <div className="system-page-state">
        <div className="system-page-card">
          <h1>Starting <code>{systemId}</code>…</h1>
          <p className="system-page-note">
            docker compose up --build — a first build can take a minute. The previously
            active system (if any) is being stopped to free the shared ports.
          </p>
          <div className="system-page-spinner" aria-hidden="true" />
        </div>
      </div>
    )
  }

  if (phase === 'error') {
    return (
      <div className="system-page-state">
        <div className="system-page-card">
          <h1>Couldn’t start <code>{systemId}</code></h1>
          <p className="system-page-error">{error}</p>
          <div className="system-page-actions">
            <button className="header-btn no-auto" onClick={() => setAttempt((n) => n + 1)}>Retry</button>
            <Link className="back-link" to="/">← Systems</Link>
          </div>
        </div>
      </div>
    )
  }

  // key: App's state (manifest, polls, modals, terminal…) is all per-system —
  // remount from scratch when the id changes rather than reconciling.
  return <App systemId={systemId!} key={systemId} />
}
