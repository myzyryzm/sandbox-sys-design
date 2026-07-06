import { useCallback, useEffect, useState } from 'react'
import { isExternalEndpoint } from './endpointPolicy.js'
import { buildScenarioFunctionPrompt } from './scenarioBank.js'

/**
 * A client's "Functions" tab (embedded in NodeEditModal). A client's behavior is a set of
 * "functions" it OWNS: named, argument-taking Python functions (in systems/<id>/clients/<module>.py)
 * authored by Claude from a description, each calling the system through the load balancer.
 * Functions are local to each client — there is no shared bank and no attach-by-name; their
 * metadata lives in systems/<id>/scenarios.json keyed by their owner client (the diagram `steps`
 * are inferred from the code).
 *
 * Three sections:
 *   1. This client's functions — Resume / Edit / Delete each one.
 *   2. Define a function — name + argument signature + description → launches a Claude
 *      session (sandbox-client-scenario skill) that implements it in the client's .py.
 *   3. Run — pick one of this client's functions, supply argument values, run the script
 *      (POST /api/scenarios/run → python3 <module>.py --<name> …) and show each call's response.
 *
 * Clicking a function on the client node in the diagram traces its calls (handled there);
 * this tab is the authoring + run surface.
 */

const ARG_TYPES = ['string', 'number', 'boolean']
const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/

// A websocket pool client already shows two built-in method rows (see websockets.js
// CLIENT_METHODS) on the same diagram node as its authored HTTP functions — reserve
// those names so an authored function can't produce a colliding, ambiguous row.
const WS_BUILTIN_NAMES = ['send', 'onReceive']

function blankForm() {
  return { name: '', args: [], description: '' }
}

// A function's display signature, e.g. "checkout(userId: string, qty: number)".
function sig(fn) {
  return `${fn.name}(${(fn.args || []).map((a) => `${a.name}: ${a.type}`).join(', ')})`
}

// When editing, a new description entry is APPENDED to the existing one (an empty entry
// leaves it unchanged), so the description accumulates over successive edits.
function joinDescription(base, addition) {
  const b = (base || '').trim()
  const a = (addition || '').trim()
  if (!b) return a
  if (!a) return b
  return `${b}\n\n${a}`
}

export default function ClientScenarioTab({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }) {
  const client = node.id
  const moduleFile = `${client.replace(/-/g, '_')}.py` // the client's python script (clients/<module>.py)
  const [functions, setFunctions] = useState(null) // this client's own functions; null = loading
  const [endpoints, setEndpoints] = useState([]) // discovered endpoints (for the prompt)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  // Define / edit form.
  const [adding, setAdding] = useState(false)
  const [editingName, setEditingName] = useState(null) // name when editing an existing function
  const [editingDescription, setEditingDescription] = useState('') // existing description (read-only)
  const [form, setForm] = useState(blankForm)
  const [confirmName, setConfirmName] = useState(null) // function pending delete confirm

  // Run panel.
  const [runName, setRunName] = useState('')
  const [runArgs, setRunArgs] = useState({})
  const [results, setResults] = useState(null)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const load = useCallback(() => {
    return Promise.all([
      fetch(`/api/scenarios?system=${encodeURIComponent(systemId)}`).then((r) => r.json()).catch(() => ({})),
      fetch(`/api/endpoints?system=${encodeURIComponent(systemId)}`).then((r) => r.json()).catch(() => ({})),
    ]).then(([fns, eps]) => {
      // Keep only the functions this client owns (the registry holds every client's).
      const mine = fns.ok ? (fns.functions || []).filter((f) => f.client === client) : []
      setFunctions(mine)
      setEndpoints(eps.ok ? eps.endpoints || [] : [])
    })
  }, [systemId, client])

  useEffect(() => { load() }, [load])

  const editing = editingName !== null

  // The client-callable surface (LB-routable, in-system) — what the authoring prompt lists.
  const byId = Object.fromEntries((manifest?.nodes || []).map((n) => [n.id, n]))
  const callableEndpoints = endpoints.filter((e) => isExternalEndpoint(e, byId[e.service]))

  // --- form helpers ---
  const setField = (k) => (e) => setForm((f) => ({ ...f, [k]: e.target.value }))
  const addArg = () => setForm((f) => ({ ...f, args: [...f.args, { name: '', type: 'string' }] }))
  const updateArg = (i, patch) => setForm((f) => ({ ...f, args: f.args.map((r, j) => (j === i ? { ...r, ...patch } : r)) }))
  const removeArg = (i) => setForm((f) => ({ ...f, args: f.args.filter((_, j) => j !== i) }))

  function startAdd() {
    setForm(blankForm())
    setEditingName(null)
    setEditingDescription('')
    setError(null)
    setAdding(true)
  }

  function startEdit(fn) {
    setForm({ name: fn.name, args: (fn.args || []).map((a) => ({ ...a })), description: '' })
    setEditingName(fn.name)
    setEditingDescription(fn.description || '')
    setError(null)
    setConfirmName(null)
    setAdding(true)
  }

  function cancelForm() {
    setAdding(false)
    setEditingName(null)
    setEditingDescription('')
    setError(null)
  }

  async function submit() {
    setError(null)
    const name = editing ? editingName : form.name.trim()
    if (!editing) {
      if (!name) return setError('Function name is required')
      if (!IDENT_RE.test(name)) {
        return setError('Function name must start with a letter or underscore and use only letters, digits and underscores')
      }
      if (node.origin === 'create-websockets' && WS_BUILTIN_NAMES.includes(name)) {
        return setError(`"${name}" is a built-in websocket method — choose a different name`)
      }
      if ((functions || []).some((f) => f.name === name)) {
        return setError(`a function named "${name}" already exists`)
      }
    }
    // Collect args (skip fully blank rows; validate the rest).
    const args = []
    const seen = new Set()
    for (const r of form.args) {
      const an = (r.name || '').trim()
      if (!an) continue
      if (!IDENT_RE.test(an)) return setError(`argument "${an}" must be a valid identifier`)
      if (seen.has(an)) return setError(`duplicate argument "${an}"`)
      seen.add(an)
      args.push({ name: an, type: r.type })
    }
    if (!editing && !form.description.trim()) {
      return setError('Describe what the function does')
    }
    const description = editing ? joinDescription(editingDescription, form.description) : form.description

    const conversationId = crypto.randomUUID()
    setBusy(true)
    try {
      // 1. Persist the function shell for this client (Claude fills in the steps).
      const res = await fetch('/api/scenarios', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, client, name, args, description, conversationId }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)

      // 2. Launch the authoring session.
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildScenarioFunctionPrompt({ systemId, client, name, args, description, endpoints: callableEndpoints }),
      }, { kind: 'client', target: client, title: name })
      onClose()
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
  }

  function onDescriptionKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      submit()
    }
  }

  async function removeFunction(fn) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/scenarios', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, client, name: fn.name }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmName(null)
      if (runName === fn.name) { setRunName(''); setRunArgs({}); setResults(null) }
      await load()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  // --- run ---
  const runFn = (functions || []).find((f) => f.name === runName) || null

  function pickRun(name) {
    setRunName(name)
    setResults(null)
    const fn = (functions || []).find((f) => f.name === name)
    const seed = {}
    for (const a of fn?.args || []) seed[a.name] = a.type === 'boolean' ? false : ''
    setRunArgs(seed)
  }

  async function run() {
    if (!runFn) return
    setError(null)
    setBusy(true)
    setResults(null)
    try {
      const res = await fetch('/api/scenarios/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, client, name: runFn.name, args: runArgs }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setResults(data.results || [])
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  const body = (
    <>
      <p className="sim-desc">
        <strong>Functions</strong> <code>{client}</code> can run — each a named, argument-taking
        Python function in <code>clients/{moduleFile}</code> that calls the system through the load
        balancer (run like <code>python3 {moduleFile} --name …</code>). Authored from a description;
        click a function on this node in the diagram to trace its calls.
      </p>

      {/* ---- This client's functions ---- */}
      {functions === null ? (
        <p className="sim-desc">Loading…</p>
      ) : functions.length === 0 ? (
        <p className="sim-desc">No functions defined yet.</p>
      ) : (
        <ul className="endpoint-list">
          {functions.map((fn) => {
            const pending = !(fn.steps && fn.steps.length)
            const confirming = confirmName === fn.name
            return (
              <li key={fn.name} className="endpoint-list-row">
                <code className="scenario-fn-sig">{sig(fn)}</code>
                {pending ? (
                  <span className="scenario-pending" title="Steps not authored yet — open or resume the Claude session">pending</span>
                ) : (
                  <span className="scenario-stepcount">{fn.steps.length} step{fn.steps.length === 1 ? '' : 's'}</span>
                )}
                {confirming ? (
                  <span className="endpoint-list-actions">
                    <span className="endpoint-confirm">Delete function?</span>
                    <button className="link" disabled={busy} onClick={() => removeFunction(fn)}>Yes</button>
                    <button className="link" disabled={busy} onClick={() => setConfirmName(null)}>No</button>
                  </span>
                ) : (
                  <span className="endpoint-list-actions">
                    {fn.conversationId && (
                      <button
                        className="link"
                        disabled={busy}
                        title="Resume this function’s Claude session"
                        onClick={() => {
                          onLaunch({ sessionId: fn.conversationId, mode: 'resume', prompt: '' })
                          onClose()
                        }}
                      >
                        Resume
                      </button>
                    )}
                    <button className="link" disabled={busy} onClick={() => startEdit(fn)}>Edit</button>
                    <button className="link-danger" disabled={busy} onClick={() => setConfirmName(fn.name)}>Delete</button>
                  </span>
                )}
              </li>
            )
          })}
        </ul>
      )}

      {/* ---- Define / edit a function ---- */}
      {!adding ? (
        <div className="form-section">
          <button className="link" onClick={startAdd} disabled={busy || functions === null}>
            ＋ Define a function
          </button>
        </div>
      ) : (
        <div className="form-section">
          <div className="form-section-head">
            <span>{editing ? `Edit ${editingName}` : 'New function'}</span>
          </div>

          <label className="form-row">
            <span>Name</span>
            <input
              value={editing ? editingName : form.name}
              onChange={setField('name')}
              placeholder="checkout  (a function name — unique to this client)"
              disabled={busy || editing}
            />
          </label>

          <div className="form-section">
            <div className="form-section-head">
              <span>Arguments <em className="grpc-optional">(optional — values supplied at run time)</em></span>
              <button type="button" onClick={addArg} disabled={busy}>+ arg</button>
            </div>
            {form.args.map((r, i) => (
              <div className="field-row" key={i}>
                <input
                  value={r.name}
                  onChange={(e) => updateArg(i, { name: e.target.value })}
                  placeholder="argName"
                  disabled={busy}
                />
                <select value={r.type} onChange={(e) => updateArg(i, { type: e.target.value })} disabled={busy}>
                  {ARG_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                </select>
                <button type="button" className="link-danger" onClick={() => removeArg(i)} disabled={busy}>×</button>
              </div>
            ))}
          </div>

          {editing && editingDescription.trim() && (
            <div className="form-row form-row-stack">
              <span>Current description</span>
              <p className="endpoint-current-desc">{editingDescription}</p>
            </div>
          )}

          <label className="form-row form-row-stack">
            <span>{editing ? 'Add to description' : 'Describe'}</span>
            <textarea
              className="desc-input"
              value={form.description}
              onChange={setField('description')}
              onKeyDown={onDescriptionKeyDown}
              placeholder={editing
                ? 'Add to this function’s description — appended to the current one (Enter to submit, Shift+Enter for a newline)'
                : 'What should this function do? e.g. "create an order for the user, then pay for it" (Enter to submit, Shift+Enter for a newline)'}
              rows={3}
              disabled={busy}
              autoFocus
            />
          </label>

          <p className="sim-desc">
            {editing
              ? `Saving re-authors this function in a fresh Claude session (editing clients/${moduleFile}).`
              : `Creating opens a Claude session that writes this function in clients/${moduleFile}.`}
          </p>

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={cancelForm} disabled={busy}>Cancel</button>
            <button type="button" className="primary" onClick={submit} disabled={busy}>
              {busy ? 'Working…' : editing ? 'Save & open Claude' : 'Author & open Claude'}
            </button>
          </div>
        </div>
      )}

      {/* ---- Run a function ---- */}
      {(functions || []).length > 0 && (
        <div className="form-section">
          <div className="form-section-head">
            <span>Run a function</span>
          </div>
          <label className="form-row">
            <span>Function</span>
            <select value={runName} onChange={(e) => pickRun(e.target.value)} disabled={busy}>
              <option value="">— pick a function —</option>
              {(functions || []).map((f) => (
                <option key={f.name} value={f.name}>{sig(f)}</option>
              ))}
            </select>
          </label>

          {runFn && (runFn.args || []).map((a) => (
            <label className="form-row" key={a.name}>
              <span>{a.name} <em className="grpc-optional">{a.type}</em></span>
              {a.type === 'boolean' ? (
                <input
                  type="checkbox"
                  checked={!!runArgs[a.name]}
                  onChange={(e) => setRunArgs((v) => ({ ...v, [a.name]: e.target.checked }))}
                  disabled={busy}
                />
              ) : (
                <input
                  type={a.type === 'number' ? 'number' : 'text'}
                  value={runArgs[a.name] ?? ''}
                  onChange={(e) => setRunArgs((v) => ({ ...v, [a.name]: e.target.value }))}
                  disabled={busy}
                />
              )}
            </label>
          ))}

          {runFn && !(runFn.steps && runFn.steps.length) && (
            <p className="sim-desc">This function has no steps yet — author them first (Resume its session).</p>
          )}

          <div className="modal-actions">
            <button
              type="button"
              className="primary"
              onClick={run}
              disabled={busy || !runFn || !(runFn.steps && runFn.steps.length)}
            >
              {busy ? 'Running…' : '▶ Run'}
            </button>
          </div>
        </div>
      )}

      {error && !adding && <p className="modal-error">{error}</p>}

      {results && (
        <div className="scenario-results">
          <div className="scenario-results-head">Run results</div>
          {results.length === 0 ? (
            <p className="sim-desc">No steps ran.</p>
          ) : (
            <ol>
              {results.map((r) => (
                <li key={r.step} className={`scenario-result ${r.ok ? 'ok' : 'fail'}`}>
                  <div className="scenario-result-head">
                    <span className="scenario-result-call">{r.method} {r.path}</span>
                    <span className={`scenario-result-status ${r.ok ? 'ok' : 'fail'}`}>
                      {r.error ? 'error' : r.status}
                    </span>
                  </div>
                  {r.sentBody != null && (
                    <pre className="scenario-result-body">→ sent {JSON.stringify(r.sentBody)}</pre>
                  )}
                  {r.error ? (
                    <pre className="scenario-result-body">{r.error}</pre>
                  ) : (
                    <pre className="scenario-result-body">{
                      typeof r.response === 'string' ? r.response : JSON.stringify(r.response, null, 2)
                    }</pre>
                  )}
                </li>
              ))}
            </ol>
          )}
        </div>
      )}

      {!embedded && (
        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={busy}>Close</button>
        </div>
      )}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Functions · <code>{client}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
