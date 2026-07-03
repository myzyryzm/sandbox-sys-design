import { useCallback, useEffect, useState } from 'react'
import { modelImpact, buildModelUpdatePrompt } from './modelBank.js'

/**
 * Per-system "models bank". Lists reusable TypeScript model interfaces and lets the
 * user add / edit / delete them (GET/POST/DELETE /api/models). Endpoints reference
 * these by name from their request/response (see EndpointsModal). A model's name is
 * its immutable id; the definition is raw TypeScript that may reference other models
 * by name. Deleting a model still referenced by an endpoint is blocked server-side.
 *
 * Editing several EXISTING models is staged locally (no per-edit save). "Review & save"
 * shows which services/databases the changes affect (GET /api/model-usage + the model
 * reference graph), then a single Confirm persists every edit in one write and launches
 * ONE Claude session that propagates the new shapes to the affected endpoints/databases.
 * Creating a NEW model still saves immediately (a new model has no consumers to update).
 */

// A model name is a TypeScript identifier (mirrors MODEL_NAME_RE in server/models.js).
const MODEL_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/

function blankForm() {
  return { name: '', ts: '', description: '' }
}

const TS_PLACEHOLDER = `// id => primary key
// unique constraint on (customer_id, ref)
interface Order {
  id: string
  items: OrderItem[]   // reference another model by name
}`

export default function ModelsModal({ systemId, onClose, onLaunch, manifest }) {
  void manifest // available for future cross-checks; impact data comes from /api/model-usage
  const [models, setModels] = useState(null) // null = loading
  const [form, setForm] = useState(blankForm)
  const [editingName, setEditingName] = useState(null) // non-null while editing an existing model
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [confirmName, setConfirmName] = useState(null) // row pending delete confirm
  const [drafts, setDrafts] = useState({}) // staged edits to EXISTING models: { name: { ts, description } }
  const [usage, setUsage] = useState({}) // GET /api/model-usage — where each model is used
  const [review, setReview] = useState(false) // showing the impact-review panel

  const load = useCallback(() => {
    return fetch(`/api/models?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json())
      .then((d) => setModels(Array.isArray(d.models) ? d.models : []))
      .catch(() => setModels([]))
  }, [systemId])

  // Where each model is referenced (endpoints/databases) — powers the impact review.
  const loadUsage = useCallback(() => {
    return fetch(`/api/model-usage?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json())
      .then((d) => setUsage(d && d.usage ? d.usage : {}))
      .catch(() => setUsage({}))
  }, [systemId])

  useEffect(() => {
    load()
    loadUsage()
  }, [load, loadUsage])

  const set = (k) => (e) => setForm((f) => ({ ...f, [k]: e.target.value }))
  const editing = editingName !== null
  const dirtyCount = Object.keys(drafts).length

  // Other model names (excluding the one being edited) the definition can reference.
  const referenceable = (models || []).map((m) => m.name).filter((n) => n !== editingName)

  // Inline name check — don't nag before the user has typed anything.
  const nameErr = (() => {
    const n = form.name.trim()
    if (!n) return null
    if (!MODEL_NAME_RE.test(n)) {
      return 'Use a TypeScript identifier: start with a letter or underscore, then letters/digits/underscores'
    }
    if (n.length > 60) return 'Name is too long (60 characters max)'
    return null
  })()

  function startEdit(m) {
    // Re-editing a model with a staged draft resumes from the draft, not the saved text.
    const draft = drafts[m.name]
    setForm({
      name: m.name,
      ts: draft ? draft.ts : m.ts || '',
      description: draft ? draft.description : m.description || '',
    })
    setEditingName(m.name)
    setError(null)
    setConfirmName(null)
    setReview(false)
  }

  function resetForm() {
    setForm(blankForm())
    setEditingName(null)
    setError(null)
  }

  // Stage an edit to an existing model (no network). A no-op edit (equal to the saved
  // model) is dropped so badges/impact reflect only real changes.
  function stageEdit() {
    setError(null)
    if (!form.ts.trim()) return setError('Definition (TypeScript) is required')
    const name = editingName
    const original = (models || []).find((m) => m.name === name)
    const ts = form.ts
    const description = form.description
    setDrafts((d) => {
      const next = { ...d }
      if (original && original.ts === ts && (original.description || '') === (description || '')) {
        delete next[name]
      } else {
        next[name] = { ts, description }
      }
      return next
    })
    resetForm()
  }

  // Create a brand-new model immediately — it has no consumers yet, so nothing to propagate.
  async function createModel() {
    setError(null)
    const name = form.name.trim()
    if (!name) return setError('Name is required')
    if (nameErr) return setError(nameErr)
    if (!form.ts.trim()) return setError('Definition (TypeScript) is required')
    setBusy(true)
    try {
      const res = await fetch('/api/models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, name, ts: form.ts, description: form.description }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load()
      await loadUsage()
      resetForm()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  function submit() {
    if (editing) stageEdit()
    else createModel()
  }

  function openReview() {
    setError(null)
    loadUsage() // refresh so impact reflects any concurrent endpoint/db changes
    setReview(true)
  }

  // Persist every staged edit in one write, then hand propagation to a single Claude
  // session (unless nothing downstream is affected, in which case we just save).
  async function confirmSave() {
    setError(null)
    const edited = Object.keys(drafts)
    if (edited.length === 0) return setReview(false)
    const payload = edited.map((n) => ({ name: n, ts: drafts[n].ts, description: drafts[n].description }))
    setBusy(true)
    try {
      const res = await fetch('/api/models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, models: payload }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)

      const merged = (models || []).map((m) => (drafts[m.name] ? { ...m, ...drafts[m.name] } : m))
      const impact = modelImpact({ names: edited, models: merged, usage })

      // Documented-only topics resolve their TS automatically, so only ENFORCED topics
      // need a session to update producer/consumer validation.
      const enforcedStreams = (impact.streams || []).filter((s) => s.enforce)
      if (impact.endpoints.length === 0 && impact.databases.length === 0 && enforcedStreams.length === 0) {
        // Nothing downstream — just persist and refresh, stay open.
        setDrafts({})
        setReview(false)
        await load()
        await loadUsage()
        setBusy(false)
        return
      }

      onLaunch?.({
        sessionId: crypto.randomUUID(),
        mode: 'new',
        prompt: buildModelUpdatePrompt({ systemId, edits: payload, impact, allModels: merged }),
      }, { kind: 'model', target: 'models', title: edited.length === 1 ? edited[0] : 'update' })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  async function remove(m) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/models', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, name: m.name }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmName(null)
      if (editingName === m.name) resetForm()
      // Drop any staged edit for the removed model.
      setDrafts((d) => {
        if (!d[m.name]) return d
        const next = { ...d }
        delete next[m.name]
        return next
      })
      await load()
      await loadUsage()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  // Impact is computed only while reviewing (against the bank with drafts applied).
  const edited = Object.keys(drafts)
  const merged = (models || []).map((m) => (drafts[m.name] ? { ...m, ...drafts[m.name] } : m))
  const impact = review ? modelImpact({ names: edited, models: merged, usage }) : null

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Models bank</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <p className="sim-desc">
          Reusable <strong>TypeScript</strong> model interfaces for this system. Reference them by
          name from an endpoint’s request/response. A model’s <strong>name</strong> is its permanent
          id, and a definition can reference other models by name.
        </p>

        {/* Existing models */}
        {models === null ? (
          <p className="sim-desc">Loading…</p>
        ) : models.length === 0 ? (
          <p className="sim-desc">No models yet.</p>
        ) : (
          <ul className="model-list">
            {models.map((m) => {
              const confirming = confirmName === m.name
              const draft = drafts[m.name]
              const ts = draft ? draft.ts : m.ts
              const preview = (ts || '').replace(/\s+/g, ' ').trim()
              return (
                <li key={m.name} className="model-list-row">
                  <code className="model-name">{m.name}</code>
                  {draft && <span className="model-dirty-badge">modified</span>}
                  <span className="model-ts-preview" title={ts}>
                    {preview.length > 64 ? preview.slice(0, 64) + '…' : preview}
                  </span>
                  {confirming ? (
                    <span className="endpoint-list-actions">
                      <span className="endpoint-confirm">Delete?</span>
                      <button className="link" disabled={busy} onClick={() => remove(m)}>Yes</button>
                      <button className="link" disabled={busy} onClick={() => setConfirmName(null)}>No</button>
                    </span>
                  ) : (
                    <span className="endpoint-list-actions">
                      <button className="link" disabled={busy || review} onClick={() => startEdit(m)}>Edit</button>
                      <button className="link-danger" disabled={busy || review} onClick={() => setConfirmName(m.name)}>Delete</button>
                    </span>
                  )}
                </li>
              )
            })}
          </ul>
        )}

        {review ? (
          /* Impact review — what this batch of edits will change */
          <div className="form-section">
            <div className="form-section-head">
              <span>Review changes</span>
            </div>
            <p className="sim-desc">
              Saving updates {edited.length} model{edited.length === 1 ? '' : 's'} (
              {edited.join(', ')}) and applies the new shape to everything below.
            </p>

            {impact.endpoints.length === 0 && impact.databases.length === 0 && impact.streams.length === 0 ? (
              <p className="sim-desc">No services, databases or event-stream topics are affected — changes will just be saved.</p>
            ) : (
              <>
                {impact.endpoints.length > 0 && (
                  <div className="impact-group">
                    <div className="impact-group-head">Affected services</div>
                    <ul className="impact-list">
                      {impact.endpoints.map((e) => (
                        <li key={`${e.service}|${e.method}|${e.path}|${e.field}`}>
                          <code>{e.service}</code> {e.method} /{e.service}{e.path}{' '}
                          <span className="impact-field">({e.field})</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {impact.databases.length > 0 && (
                  <div className="impact-group">
                    <div className="impact-group-head">Affected databases</div>
                    <ul className="impact-list">
                      {impact.databases.map((d) => (
                        <li key={d.id}>
                          <code>{d.id}</code> ({d.engine})
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {impact.streams.length > 0 && (
                  <div className="impact-group">
                    <div className="impact-group-head">Affected event-stream topics</div>
                    <ul className="impact-list">
                      {impact.streams.map((s) => (
                        <li key={`${s.cluster}|${s.topic}`}>
                          <code>{s.cluster}</code> / {s.topic}{' '}
                          <span className="impact-field">({s.enforce ? 'enforced' : 'documented'})</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                <p className="form-hint">
                  A single Claude session will update each affected service / database / enforced
                  topic (via the sandbox-endpoint / sandbox-database / sandbox-event-stream skills)
                  and rebuild it. Documented-only topics need no change — their schema re-resolves
                  automatically.
                </p>
              </>
            )}

            {error && <p className="modal-error">{error}</p>}

            <div className="modal-actions">
              <button type="button" onClick={() => setReview(false)} disabled={busy}>Back</button>
              <button type="button" className="primary" onClick={confirmSave} disabled={busy}>
                {busy ? 'Saving…' : 'Confirm & save → apply'}
              </button>
            </div>
          </div>
        ) : (
          <>
            {/* Add / edit model */}
            <div className="form-section">
              <div className="form-section-head">
                <span>{editing ? `Edit model · ${editingName}` : 'New model'}</span>
                {editing && (
                  <button type="button" className="link" onClick={resetForm} disabled={busy}>＋ New</button>
                )}
              </div>

              <label className="form-row">
                <span>Name</span>
                <input
                  value={form.name}
                  onChange={set('name')}
                  placeholder="Order"
                  disabled={busy || editing}
                />
              </label>
              {editing ? (
                <small className="form-hint">Name is the model’s permanent id and can’t be changed. Delete &amp; re-add to rename.</small>
              ) : nameErr ? (
                <small className="field-error">{nameErr}</small>
              ) : (
                <small className="form-hint">A TypeScript identifier, unique in this system — it’s the model’s permanent id.</small>
              )}

              <label className="form-row form-row-stack">
                <span>Definition</span>
                <textarea
                  className="ts-input"
                  value={form.ts}
                  onChange={set('ts')}
                  placeholder={TS_PLACEHOLDER}
                  rows={8}
                  disabled={busy}
                  spellCheck={false}
                />
              </label>

              <small className="form-hint">
                <code>//</code> comments are kept and used as schema directives (primary key, foreign key,
                unique, index, length…) when building a database from this model.
              </small>

              {referenceable.length > 0 && (
                <p className="model-ref-legend">
                  Referenceable: {referenceable.join(', ')}
                </p>
              )}

              <label className="form-row">
                <span>Describe</span>
                <input
                  value={form.description}
                  onChange={set('description')}
                  placeholder="What is this model? (optional)"
                  disabled={busy}
                />
              </label>

              {editing && (
                <small className="form-hint">
                  Edits to existing models are staged — review what they affect, then save them all together.
                </small>
              )}

              {error && <p className="modal-error">{error}</p>}

              <div className="modal-actions">
                <button type="button" onClick={onClose} disabled={busy}>Close</button>
                <button type="button" className="primary" onClick={submit} disabled={busy || !!nameErr}>
                  {busy ? 'Saving…' : editing ? 'Stage edit' : 'Add model'}
                </button>
              </div>
            </div>

            {/* Review bar — appears once there are staged edits */}
            {dirtyCount > 0 && (
              <div className="form-section model-review-bar">
                <button type="button" className="primary" onClick={openReview} disabled={busy}>
                  Review &amp; save changes ({dirtyCount})
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}
