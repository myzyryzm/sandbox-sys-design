import { useCallback, useEffect, useState } from 'react'
import { buildEndToEndRunPrompt } from './endToEndBank.js'

/**
 * Top-level "End-to-End" modal. Define whole test PROCESSES and run them.
 *
 * A process is: a name, a client_list (client methods to call + how often, in seconds), a
 * failure_list (freeform "a bug occurred if this happens") and a constraint_list (freeform
 * invariants that must never be violated). Defining one is pure data entry (persisted to
 * systems/<id>/endtoend.json via /api/endtoend). RUNNING one hands off to a launched Claude
 * session (the sandbox-end-to-end-process skill via onLaunch/enqueueSession) that coordinates the
 * entire run — calling the methods at their rates for a chosen duration, synthesizing the
 * arguments the form never collects, watching the conditions, and writing a run report.
 *
 * The backend's in-memory run-state is the single source of truth: this modal polls /api/endtoend
 * to toggle each row between Start and Stop, and the launched session polls the same endpoint to
 * know when to halt. One run at a time per system.
 */

// A client function's display signature, e.g. "checkout(order_id: string)".
function sig(fn) {
  return `${fn.name}(${(fn.args || []).map((a) => `${a.name}: ${a.type}`).join(', ')})`
}

function blankForm() {
  return {
    name: '',
    client_list: [{ client: '', method: '', intervalSeconds: 5 }],
    websocket_list: [],
    failure_list: [],
    constraint_list: [],
  }
}

export default function EndToEndModal({ systemId, manifest, scenarios, onLaunch, onClose }) {
  const [data, setData] = useState({ processes: [], run: { running: false } })
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  // Define / edit form.
  const [adding, setAdding] = useState(false)
  const [editingId, setEditingId] = useState(null)
  const [form, setForm] = useState(blankForm)

  // Per-row transient UI.
  const [startingId, setStartingId] = useState(null) // the process showing its duration sub-form
  const [duration, setDuration] = useState(30)
  const [confirmDelete, setConfirmDelete] = useState(null)

  const run = data.run || { running: false }
  const processes = data.processes || []

  // Dropdown of every client function in the system (from the polled scenarios registry).
  const methodOptions = (scenarios || []).map((f) => ({
    value: `${f.client}::${f.name}`,
    label: `${f.client} · ${sig(f)}`,
  }))
  const validValues = new Set(methodOptions.map((o) => o.value))
  const noMethods = methodOptions.length === 0

  // Websocket clients (pool scripts driven with a configurable client count).
  const wsClientOptions = (manifest?.nodes || [])
    .filter((n) => n.type === 'client' && n.origin === 'create-websockets')
    .map((n) => n.id)
  const noWsClients = wsClientOptions.length === 0

  const load = useCallback(async () => {
    try {
      const res = await fetch(`/api/endtoend?system=${encodeURIComponent(systemId)}`)
      const d = await res.json()
      if (d.ok) setData({ processes: d.processes || [], run: d.run || { running: false } })
    } catch {
      /* keep the last good state */
    }
  }, [systemId])

  // Own poll, so App needs no new state. ~1.5s keeps the Start/Stop toggle live.
  useEffect(() => {
    load()
    const id = setInterval(load, 1500)
    return () => clearInterval(id)
  }, [load])

  // --- form helpers ---
  const addMethod = () =>
    setForm((f) => ({ ...f, client_list: [...f.client_list, { client: '', method: '', intervalSeconds: 5 }] }))
  const updateMethod = (i, patch) =>
    setForm((f) => ({ ...f, client_list: f.client_list.map((r, j) => (j === i ? { ...r, ...patch } : r)) }))
  const removeMethod = (i) =>
    setForm((f) => ({ ...f, client_list: f.client_list.filter((_, j) => j !== i) }))

  const addWsPool = () =>
    setForm((f) => ({ ...f, websocket_list: [...f.websocket_list, { client: wsClientOptions[0] || '', clientCount: 10, messagesPerSecond: 1 }] }))
  const updateWsPool = (i, patch) =>
    setForm((f) => ({ ...f, websocket_list: f.websocket_list.map((r, j) => (j === i ? { ...r, ...patch } : r)) }))
  const removeWsPool = (i) =>
    setForm((f) => ({ ...f, websocket_list: f.websocket_list.filter((_, j) => j !== i) }))

  const addCond = (key) => setForm((f) => ({ ...f, [key]: [...f[key], ''] }))
  const updateCond = (key, i, val) =>
    setForm((f) => ({ ...f, [key]: f[key].map((s, j) => (j === i ? val : s)) }))
  const removeCond = (key, i) => setForm((f) => ({ ...f, [key]: f[key].filter((_, j) => j !== i) }))

  function startAdd() {
    setForm(blankForm())
    setEditingId(null)
    setError(null)
    setAdding(true)
  }
  function startEdit(p) {
    setForm({
      name: p.name || '',
      client_list: (p.client_list || []).map((r) => ({ ...r })),
      websocket_list: (p.websocket_list || []).map((r) => ({ ...r })),
      failure_list: [...(p.failure_list || [])],
      constraint_list: [...(p.constraint_list || [])],
    })
    setEditingId(p.id)
    setError(null)
    setConfirmDelete(null)
    setAdding(true)
  }
  function cancelForm() {
    setAdding(false)
    setEditingId(null)
    setError(null)
  }

  async function submit() {
    setError(null)
    const name = form.name.trim()
    if (!name) return setError('Process name is required')

    const client_list = form.client_list
      .filter((r) => r.client && r.method)
      .map((r) => ({ client: r.client, method: r.method, intervalSeconds: Number(r.intervalSeconds) }))
    const websocket_list = form.websocket_list
      .filter((r) => r.client)
      .map((r) => ({
        client: r.client,
        clientCount: Number(r.clientCount),
        messagesPerSecond: Number(r.messagesPerSecond) || 1,
      }))
    if (client_list.length === 0 && websocket_list.length === 0) {
      return setError('Add at least one client method or websocket client pool')
    }
    for (const r of client_list) {
      if (!Number.isInteger(r.intervalSeconds) || r.intervalSeconds < 1 || r.intervalSeconds > 60) {
        return setError(`Rate for ${r.client}.${r.method} must be a whole number of seconds between 1 and 60`)
      }
    }
    for (const r of websocket_list) {
      if (!Number.isInteger(r.clientCount) || r.clientCount < 1 || r.clientCount > 200) {
        return setError(`Client count for ${r.client} must be a whole number between 1 and 200`)
      }
      if (!Number.isInteger(r.messagesPerSecond) || r.messagesPerSecond < 1 || r.messagesPerSecond > 20) {
        return setError(`Messages/s for ${r.client} must be a whole number between 1 and 20`)
      }
    }
    const failure_list = form.failure_list.map((s) => s.trim()).filter(Boolean)
    const constraint_list = form.constraint_list.map((s) => s.trim()).filter(Boolean)

    setBusy(true)
    try {
      const res = await fetch('/api/endtoend', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          id: editingId || undefined,
          name,
          client_list,
          websocket_list,
          failure_list,
          constraint_list,
        }),
      })
      const d = await res.json().catch(() => ({}))
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      cancelForm()
      await load()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function removeProcess(p) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/endtoend', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: p.id }),
      })
      const d = await res.json().catch(() => ({}))
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      setConfirmDelete(null)
      await load()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function startProcess(p) {
    const n = Number(duration)
    if (!Number.isInteger(n) || n < 1 || n > 600) {
      return setError('Choose a whole number of seconds between 1 and 600')
    }
    setBusy(true)
    setError(null)
    try {
      const sessionId = crypto.randomUUID()
      const res = await fetch('/api/endtoend/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: p.id, duration_seconds: n }),
      })
      const d = await res.json().catch(() => ({}))
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      onLaunch(
        {
          sessionId,
          mode: 'new',
          prompt: buildEndToEndRunPrompt({
            systemId,
            processId: p.id,
            processName: p.name,
            durationSeconds: n,
            apiBase: location.origin,
          }),
        },
        { kind: 'e2e', target: p.id, title: p.name },
      )
      setStartingId(null)
      onClose() // reveal the terminal running the process (same as the other launch flows)
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
  }

  async function stopProcess(p) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/endtoend/stop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: p.id }),
      })
      const d = await res.json().catch(() => ({}))
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      await load()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>End-to-end processes</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <p className="sim-desc">
          Define a whole process — a set of <strong>client methods</strong> to drive at chosen
          rates, the <strong>constraints</strong> Claude must uphold (rules of the valid world it
          seeds and the legal inputs it uses) and the <strong>failures</strong> that mean the system
          is broken or poorly designed. <strong>Start</strong> hands off to a Claude session that
          seeds any out-of-scope data, runs it for a duration, probes for those failure states, and
          reports a verdict.
        </p>

        {/* ---- Existing processes ---- */}
        {processes.length === 0 ? (
          <p className="sim-desc">No processes defined yet.</p>
        ) : (
          <ul className="endpoint-list">
            {processes.map((p) => {
              const isThis = run.running && run.id === p.id
              const otherRunning = run.running && run.id !== p.id
              const confirming = confirmDelete === p.id
              const starting = startingId === p.id
              return (
                <li key={p.id} className="endpoint-list-row">
                  <span className="scenario-fn-sig">
                    {p.name}
                    {p.lastRun && (
                      <span className={`sim-status ${p.lastRun.verdict === 'PASS' ? 'on' : 'off'}`} style={{ marginLeft: 8 }}>
                        {p.lastRun.verdict || 'ran'}
                      </span>
                    )}
                  </span>
                  <span className="scenario-stepcount">
                    {(p.client_list || []).length} method{(p.client_list || []).length === 1 ? '' : 's'}
                    {(p.websocket_list || []).length ? ` · ${p.websocket_list.length} ws pool${p.websocket_list.length === 1 ? '' : 's'}` : ''}
                    {(p.failure_list || []).length ? ` · ${p.failure_list.length} fail` : ''}
                    {(p.constraint_list || []).length ? ` · ${p.constraint_list.length} constraint` : ''}
                  </span>

                  {isThis ? (
                    <span className="endpoint-list-actions">
                      <span className="sim-status on">running · ~{run.remaining_seconds}s</span>
                      <button className="danger" disabled={busy} onClick={() => stopProcess(p)}>Stop</button>
                    </span>
                  ) : starting ? (
                    <span className="endpoint-list-actions">
                      <input
                        type="number"
                        min={1}
                        max={600}
                        value={duration}
                        disabled={busy}
                        onChange={(e) => setDuration(e.target.value)}
                        title="run duration in seconds"
                        style={{ width: 70 }}
                      />
                      <span className="grpc-optional">s</span>
                      <button className="primary" disabled={busy} onClick={() => startProcess(p)}>Run</button>
                      <button className="link" disabled={busy} onClick={() => setStartingId(null)}>Cancel</button>
                    </span>
                  ) : confirming ? (
                    <span className="endpoint-list-actions">
                      <span className="endpoint-confirm">Delete process?</span>
                      <button className="link" disabled={busy} onClick={() => removeProcess(p)}>Yes</button>
                      <button className="link" disabled={busy} onClick={() => setConfirmDelete(null)}>No</button>
                    </span>
                  ) : (
                    <span className="endpoint-list-actions">
                      <button
                        className="link"
                        disabled={busy || otherRunning}
                        title={otherRunning ? 'another process is running' : 'run this process'}
                        onClick={() => { setError(null); setStartingId(p.id) }}
                      >
                        Start
                      </button>
                      <button className="link" disabled={busy || run.running} onClick={() => startEdit(p)}>Edit</button>
                      <button className="link-danger" disabled={busy || run.running} onClick={() => setConfirmDelete(p.id)}>Delete</button>
                    </span>
                  )}
                </li>
              )
            })}
          </ul>
        )}

        {/* ---- Define / edit a process ---- */}
        {!adding ? (
          <div className="form-section">
            <button className="link" onClick={startAdd} disabled={busy}>＋ Define a process</button>
          </div>
        ) : (
          <div className="form-section">
            <div className="form-section-head">
              <span>{editingId ? 'Edit process' : 'New process'}</span>
            </div>

            <label className="form-row">
              <span>Name</span>
              <input
                value={form.name}
                onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                placeholder="e.g. Checkout under sustained load"
                disabled={busy}
                autoFocus
              />
            </label>

            {/* client_list */}
            <div className="form-section">
              <div className="form-section-head">
                <span>Client methods <em className="grpc-optional">(method + how often to call it, in seconds)</em></span>
                <button type="button" onClick={addMethod} disabled={busy || noMethods}>+ method</button>
              </div>
              {noMethods && (
                <p className="sim-desc">No client functions exist yet — add functions to a client (its Functions tab) first.</p>
              )}
              {form.client_list.map((r, i) => {
                const val = r.client && r.method ? `${r.client}::${r.method}` : ''
                const stale = val && !validValues.has(val)
                return (
                  <div className="field-row" key={i}>
                    <select
                      value={val}
                      disabled={busy}
                      onChange={(e) => {
                        const [client, method] = e.target.value.split('::')
                        updateMethod(i, { client: client || '', method: method || '' })
                      }}
                    >
                      <option value="">— pick a client method —</option>
                      {stale && <option value={val}>{r.client} · {r.method} (missing)</option>}
                      {methodOptions.map((o) => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))}
                    </select>
                    <input
                      type="number"
                      min={1}
                      max={60}
                      value={r.intervalSeconds}
                      disabled={busy}
                      onChange={(e) => updateMethod(i, { intervalSeconds: e.target.value })}
                      title="call every N seconds"
                      style={{ width: 70 }}
                    />
                    <span className="grpc-optional">s</span>
                    <button type="button" className="link-danger" onClick={() => removeMethod(i)} disabled={busy}>×</button>
                  </div>
                )
              })}
            </div>

            {/* websocket_list */}
            <div className="form-section">
              <div className="form-section-head">
                <span>WebSocket clients <em className="grpc-optional">(pool size to keep connected + messages/s each)</em></span>
                <button type="button" onClick={addWsPool} disabled={busy || noWsClients}>+ ws pool</button>
              </div>
              {noWsClients && form.websocket_list.length === 0 && (
                <p className="sim-desc">No websocket clients exist yet — add a websocket tier (＋ Add WebSockets) first.</p>
              )}
              {form.websocket_list.map((r, i) => {
                const stale = r.client && !wsClientOptions.includes(r.client)
                return (
                  <div className="field-row" key={i}>
                    <select
                      value={r.client}
                      disabled={busy}
                      onChange={(e) => updateWsPool(i, { client: e.target.value })}
                    >
                      <option value="">— pick a websocket client —</option>
                      {stale && <option value={r.client}>{r.client} (missing)</option>}
                      {wsClientOptions.map((id) => (
                        <option key={id} value={id}>{id}</option>
                      ))}
                    </select>
                    <input
                      type="number"
                      min={1}
                      max={200}
                      value={r.clientCount}
                      disabled={busy}
                      onChange={(e) => updateWsPool(i, { clientCount: e.target.value })}
                      title="how many pool clients to spawn"
                      style={{ width: 70 }}
                    />
                    <span className="grpc-optional">clients</span>
                    <input
                      type="number"
                      min={1}
                      max={20}
                      value={r.messagesPerSecond}
                      disabled={busy}
                      onChange={(e) => updateWsPool(i, { messagesPerSecond: e.target.value })}
                      title="messages per second each client sends"
                      style={{ width: 60 }}
                    />
                    <span className="grpc-optional">msg/s</span>
                    <button type="button" className="link-danger" onClick={() => removeWsPool(i)} disabled={busy}>×</button>
                  </div>
                )
              })}
            </div>

            {/* failure_list */}
            <div className="form-section">
              <div className="form-section-head">
                <span>Failures <em className="grpc-optional">(states that mean the system is broken or poorly designed)</em></span>
                <button type="button" onClick={() => addCond('failure_list')} disabled={busy}>+ failure</button>
              </div>
              {form.failure_list.map((s, i) => (
                <div className="field-row" key={i}>
                  <input
                    value={s}
                    disabled={busy}
                    onChange={(e) => updateCond('failure_list', i, e.target.value)}
                    placeholder="e.g. two payments for the same order both succeed (double charge)"
                  />
                  <button type="button" className="link-danger" onClick={() => removeCond('failure_list', i)} disabled={busy}>×</button>
                </div>
              ))}
            </div>

            {/* constraint_list */}
            <div className="form-section">
              <div className="form-section-head">
                <span>Constraints <em className="grpc-optional">(rules Claude must uphold — valid-world preconditions to seed &amp; legal inputs)</em></span>
                <button type="button" onClick={() => addCond('constraint_list')} disabled={busy}>+ constraint</button>
              </div>
              {form.constraint_list.map((s, i) => (
                <div className="field-row" key={i}>
                  <input
                    value={s}
                    disabled={busy}
                    onChange={(e) => updateCond('constraint_list', i, e.target.value)}
                    placeholder="e.g. checkout/refund only called on an order that exists"
                  />
                  <button type="button" className="link-danger" onClick={() => removeCond('constraint_list', i)} disabled={busy}>×</button>
                </div>
              ))}
            </div>

            {error && <p className="modal-error">{error}</p>}

            <div className="modal-actions">
              <button type="button" onClick={cancelForm} disabled={busy}>Cancel</button>
              <button type="button" className="primary" onClick={submit} disabled={busy}>
                {busy ? 'Saving…' : editingId ? 'Save' : 'Create process'}
              </button>
            </div>
          </div>
        )}

        {error && !adding && <p className="modal-error">{error}</p>}
      </div>
    </div>
  )
}
