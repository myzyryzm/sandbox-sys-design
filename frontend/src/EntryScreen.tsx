import { useEffect, useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { systemIdError, SYSTEM_ID_HINT } from './systemId'
import './EntryScreen.css'

/**
 * The landing page at "/": pick a system to load, or create a new one.
 *
 * Opening a system just navigates to /systems/<id> — SystemPage owns the
 * activate call (and its "Starting…" overlay), so clicks, deep links and
 * refreshes all start a stack through one code path. Creating a system is a
 * fast file-only POST /api/systems; the navigation afterwards builds and
 * starts it like any other selection.
 */
interface SystemInfo {
  id: string
  name: string
  active: boolean
}

export default function EntryScreen() {
  const navigate = useNavigate()
  const [systems, setSystems] = useState<SystemInfo[] | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [name, setName] = useState('')
  const [busy, setBusy] = useState(false)
  const [createError, setCreateError] = useState<string | null>(null)

  function load() {
    setLoadError(null)
    fetch('/api/systems')
      .then((r) => r.json() as Promise<{ ok?: boolean; error?: string; systems?: SystemInfo[] }>)
      .then((d) => {
        if (!d.ok) throw new Error(d.error || 'failed to list systems')
        setSystems(d.systems || [])
      })
      .catch((err) => setLoadError(err instanceof Error ? err.message : String(err)))
  }

  useEffect(load, [])

  const trimmed = name.trim()
  const nameErr = systemIdError(trimmed) ||
    (systems?.some((s) => s.id === trimmed) ? `"${trimmed}" already exists` : null)

  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setBusy(true)
    setCreateError(null)
    try {
      const res = await fetch('/api/systems', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: trimmed }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      // SystemPage's "Starting…" overlay owns the docker build/up wait.
      navigate(`/systems/${trimmed}`)
    } catch (err) {
      setBusy(false)
      setCreateError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="entry-screen">
      <header className="entry-head">
        <h1>Distributed Systems Sandbox</h1>
        <p className="entry-subtitle">
          Pick a system to load — its docker stack claims the shared ports (8080 lb /
          9090 prometheus), stopping whichever system was previously active.
        </p>
      </header>

      {loadError && (
        <div className="entry-error">
          <p className="error">{loadError}</p>
          <button className="header-btn no-auto" onClick={load}>Retry</button>
        </div>
      )}

      {!loadError && systems === null && <p className="entry-loading">Loading systems…</p>}

      {systems && (
        <ul className="entry-list">
          {systems.map((s) => (
            <li key={s.id}>
              <Link className="entry-row" to={`/systems/${s.id}`}>
                <span className="entry-name">{s.name}</span>
                <span className="system-id">{s.id}</span>
                {s.active && <span className="entry-active">active</span>}
              </Link>
            </li>
          ))}
          {systems.length === 0 && (
            <li className="entry-empty">No systems yet — create one below.</li>
          )}
        </ul>
      )}

      {systems && (
        <button className="entry-new header-btn no-auto" onClick={() => setShowCreate(true)}>
          ＋ New system
        </button>
      )}

      {showCreate && (
        <div className="modal-overlay" onClick={busy ? undefined : () => setShowCreate(false)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <header className="modal-head">
              <h2>New system</h2>
              <button className="modal-close" onClick={() => setShowCreate(false)} disabled={busy}>✕</button>
            </header>

            <form onSubmit={submit}>
              <p className="sim-desc">
                Creates the smallest runnable system — an nginx load balancer in front of
                one generic FastAPI service, scraped by Prometheus — then starts it. Grow
                it from the diagram (Add service / database / stream / …).
              </p>

              <label className="form-row">
                <span>Name</span>
                <input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="my-system"
                  autoFocus
                  disabled={busy}
                />
              </label>
              {trimmed && nameErr
                ? <small className="field-error">{nameErr}</small>
                : <small className="form-hint">{SYSTEM_ID_HINT}</small>}

              {createError && <p className="modal-error">{createError}</p>}

              <div className="modal-actions">
                <button type="button" onClick={() => setShowCreate(false)} disabled={busy}>Cancel</button>
                <button type="submit" className="primary" disabled={busy || !!nameErr}>
                  {busy ? 'Creating…' : 'Create system'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  )
}
