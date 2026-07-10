import { useCallback, useEffect, useState } from 'react'
import { buildGrpcUpdatePrompt, methodSig } from './grpcBank.js'

/**
 * gRPC contract bank (Part A) — pure SHAPE, model-bank workflow.
 *
 * Creating a NEW contract (first form method, or uploading a .proto whose
 * service name is new) persists immediately: the backend synthesizes/validates
 * the proto and generates the _pb2 bindings itself — no Claude session (a new
 * contract has no owner and no behavior yet).
 *
 * Everything on an EXISTING contract is STAGED locally (add/edit/delete a
 * method, re-upload, delete the contract), badged, then "Review & save" shows
 * the affected services (the contract's owning server + its callers, joined
 * from the manifest) and applies the whole batch in one POST /api/grpc-apply.
 * If any changed contract is attached, ONE propagation session updates the
 * owner's servicer + client call sites and rebuilds (sandbox-grpc-contract).
 *
 * Behavior text lives elsewhere: per-method descriptions are written from the
 * service Edit modal's gRPC tab (the serving side), not here.
 */

// Proto types offered in the field dropdowns; `repeated` variants for collections.
const PROTO_TYPES = [
  'string', 'bytes', 'int32', 'int64', 'uint32', 'uint64', 'bool', 'double', 'float',
  'repeated string', 'repeated bytes', 'repeated int32', 'repeated int64',
]

function blankForm() {
  return {
    contract: '',
    method: '',
    request: [{ name: '', type: 'string' }],
    response: [{ name: '', type: 'string' }],
    responseStreaming: false,
  }
}

// Turn dynamic {name,type} rows into a flat { name: type } object, dropping blanks.
function rowsToFields(rows) {
  const out = {}
  for (const r of rows) {
    const n = r.name.trim()
    if (n) out[n] = r.type
  }
  return out
}

function fieldsToRows(obj) {
  const rows = Object.entries(obj || {}).map(([name, type]) => ({ name, type }))
  return rows.length ? rows : [{ name: '', type: 'string' }]
}

const sameFields = (a, b) => JSON.stringify(a || {}) === JSON.stringify(b || {})

// Client-side mirror of the backend's upload check, only to route the submit:
// an uploaded proto whose service name already exists becomes a STAGED
// replace-proto draft; a new name creates immediately. (The backend re-checks.)
function protoServiceName(text) {
  const src = (text || '').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')
  const names = [...src.matchAll(/\bservice\s+([A-Za-z_]\w*)\s*\{/g)].map((m) => m[1])
  return names.length === 1 ? names[0] : null
}

// How a contract row shows who uses it.
function attachmentLabel(c) {
  if (c.servers.length > 1) return `served by ${c.servers.join(', ')} (custom)`
  if (c.server) return `served by ${c.server}`
  return 'unattached'
}

export default function GrpcContractsModal({ systemId, onClose, onLaunch }) {
  const [contracts, setContracts] = useState(null) // null = loading
  const [drafts, setDrafts] = useState({}) // staged edits: { name: {kind, ...} }
  const [form, setForm] = useState(blankForm)
  const [editingMethod, setEditingMethod] = useState(null) // method name being edited
  const [openName, setOpenName] = useState(null) // expanded contract row
  const [confirmName, setConfirmName] = useState(null) // contract pending delete-stage confirm
  const [review, setReview] = useState(false)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [tab, setTab] = useState('form') // 'form' (build a method) | 'upload' (.proto file)
  const [proto, setProto] = useState('') // uploaded / pasted .proto text

  const load = useCallback(() => {
    return fetch(`/api/grpc-contracts?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json())
      .then((d) => {
        const list = d.ok ? d.contracts : []
        setContracts(list)
        return list
      })
      .catch(() => {
        setContracts([])
        return []
      })
  }, [systemId])

  useEffect(() => {
    load()
  }, [load])

  const byName = new Map((contracts || []).map((c) => [c.name, c]))

  // A contract's method list with its staged draft applied (what review saves).
  function draftedMethods(c) {
    const d = drafts[c.name]
    if (!d || d.kind !== 'methods') return c.methods
    const deletes = new Set(d.deletes)
    const out = c.methods.filter((m) => !deletes.has(m.name)).map((m) => d.upserts[m.name] || m)
    for (const u of Object.values(d.upserts)) {
      if (!out.some((m) => m.name === u.name)) out.push(u)
    }
    return out
  }

  const set = (k) => (e) => setForm((f) => ({ ...f, [k]: e.target.value }))
  const updateRow = (key, i, patch) =>
    setForm((f) => ({ ...f, [key]: f[key].map((r, j) => (j === i ? { ...r, ...patch } : r)) }))
  const addRow = (key) => setForm((f) => ({ ...f, [key]: [...f[key], { name: '', type: 'string' }] }))
  const removeRow = (key, i) => setForm((f) => ({ ...f, [key]: f[key].filter((_, j) => j !== i) }))

  function resetForm() {
    setForm(blankForm())
    setEditingMethod(null)
    setError(null)
  }

  // Pre-fill the form so a new method joins an existing contract (staged).
  function addMethodTo(name) {
    setForm({ ...blankForm(), contract: name })
    setEditingMethod(null)
    setTab('form')
    setError(null)
  }

  // Load an existing (or staged) method into the form for a staged edit.
  function startEditMethod(c, m) {
    setForm({
      contract: c.name,
      method: m.name,
      request: fieldsToRows(m.request),
      response: fieldsToRows(m.response),
      responseStreaming: !!m.responseStreaming,
    })
    setEditingMethod(m.name)
    setTab('form')
    setError(null)
  }

  // Merge one staged change into a contract's draft, dropping no-op drafts.
  function stageMethodChange(name, mutate) {
    setDrafts((all) => {
      const cur = all[name]
      const d =
        cur && cur.kind === 'methods'
          ? { kind: 'methods', upserts: { ...cur.upserts }, deletes: [...cur.deletes] }
          : { kind: 'methods', upserts: {}, deletes: [] }
      mutate(d)
      const next = { ...all }
      if (!Object.keys(d.upserts).length && !d.deletes.length) delete next[name]
      else next[name] = d
      return next
    })
  }

  function submitForm() {
    setError(null)
    const contract = form.contract.trim()
    const method = form.method.trim()
    if (!/^[A-Z][A-Za-z0-9]*$/.test(contract)) return setError('Contract must be PascalCase (e.g. ChunkTransfer)')
    if (!/^[A-Z][A-Za-z0-9]*$/.test(method)) return setError('Method must be PascalCase (e.g. GetChunk)')
    const record = {
      name: method,
      request: rowsToFields(form.request),
      response: rowsToFields(form.response),
      responseStreaming: form.responseStreaming,
      formAuthored: true,
    }

    const existing = byName.get(contract)
    if (!existing) return createContract(contract, record)

    // Staged upsert on an existing contract (model-bank "Stage edit").
    if (drafts[contract]?.kind === 'delete') return setError(`"${contract}" is staged for deletion — undo that first`)
    if (drafts[contract]?.kind === 'replace-proto') return setError(`"${contract}" has a staged re-upload — form edits would be overwritten`)
    const saved = existing.methods.find((m) => m.name === method)
    if (saved && !editingMethod && !drafts[contract]?.upserts?.[method]) {
      return setError(`method "${method}" already exists — use its Edit action to stage a change`)
    }
    if (saved && !saved.formAuthored) {
      return setError(`method "${method}" came from an uploaded .proto — delete it or re-upload to reshape it`)
    }
    stageMethodChange(contract, (d) => {
      d.deletes = d.deletes.filter((n) => n !== method)
      const noop =
        saved &&
        sameFields(saved.request, record.request) &&
        sameFields(saved.response, record.response) &&
        !!saved.responseStreaming === record.responseStreaming
      if (noop) delete d.upserts[method]
      else d.upserts[method] = record
    })
    resetForm()
    setOpenName(contract)
  }

  // Create a brand-new contract immediately — it has no owner or callers yet,
  // so there is nothing to propagate (the backend runs protoc mechanically).
  async function createContract(contract, record) {
    setBusy(true)
    try {
      const res = await fetch('/api/grpc-contracts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, contract, methods: [record] }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load()
      resetForm()
      setOpenName(contract)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  // Stage a method delete (or drop a staged-only method).
  function deleteMethod(c, m) {
    setError(null)
    stageMethodChange(c.name, (d) => {
      delete d.upserts[m.name]
      const saved = c.methods.some((x) => x.name === m.name)
      if (saved && !d.deletes.includes(m.name)) d.deletes.push(m.name)
    })
    if (editingMethod === m.name) resetForm()
  }

  // Stage / unstage a whole-contract delete.
  function stageContractDelete(name) {
    setDrafts((all) => ({ ...all, [name]: { kind: 'delete' } }))
    setConfirmName(null)
    if (form.contract.trim() === name) resetForm()
  }
  function unstage(name) {
    setDrafts((all) => {
      const next = { ...all }
      delete next[name]
      return next
    })
  }

  // Load the chosen .proto into the editable textarea (the user can also paste).
  async function onProtoFile(e) {
    const file = e.target.files?.[0]
    if (!file) return
    setError(null)
    try {
      setProto(await file.text())
    } catch {
      setError('Could not read that file')
    }
  }

  // Upload: a NEW service name creates immediately (backend validates with real
  // protoc); an existing name stages a replace-proto draft for review.
  async function submitUpload() {
    setError(null)
    if (!proto.trim()) return setError('Choose or paste a .proto file first')
    const name = protoServiceName(proto)
    if (name && byName.has(name)) {
      setDrafts((all) => ({ ...all, [name]: { kind: 'replace-proto', protoFile: proto } }))
      setProto('')
      setOpenName(name)
      return
    }
    setBusy(true)
    try {
      const res = await fetch('/api/grpc-contracts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, protoFile: proto }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load()
      setProto('')
      setOpenName(data.contract)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  // Apply every staged draft in one batch; the backend returns the affected
  // services (impact) — if any, launch ONE propagation session.
  async function confirmSave() {
    setError(null)
    const names = Object.keys(drafts)
    if (!names.length) return setReview(false)
    const changes = names.map((name) => {
      const d = drafts[name]
      if (d.kind === 'methods') {
        return { contract: name, kind: 'methods', upserts: Object.values(d.upserts), deletes: d.deletes }
      }
      if (d.kind === 'replace-proto') return { contract: name, kind: 'replace-proto', protoFile: d.protoFile }
      return { contract: name, kind: 'delete' }
    })
    setBusy(true)
    try {
      const res = await fetch('/api/grpc-apply', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, changes }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)

      const impact = data.impact || { owners: [], clients: [] }
      const fresh = await load() // post-apply registry (carried descriptions, new methods)
      if (!impact.owners.length && !impact.clients.length) {
        // Nothing attached — pure shape + codegen, stay open.
        setDrafts({})
        setReview(false)
        setBusy(false)
        return
      }

      const freshByName = new Map(fresh.map((c) => [c.name, c]))
      const entries = names.map((name) => {
        const d = drafts[name]
        if (d.kind === 'delete') return { contract: name, kind: 'delete' }
        if (d.kind === 'replace-proto') {
          return { contract: name, kind: 'replace-proto', methods: freshByName.get(name)?.methods || [] }
        }
        const methods = freshByName.get(name)?.methods || []
        return {
          contract: name,
          kind: 'methods',
          upserts: Object.keys(d.upserts).map((n) => methods.find((m) => m.name === n)).filter(Boolean),
          deletes: d.deletes,
        }
      })
      onLaunch({
        sessionId: crypto.randomUUID(),
        mode: 'new',
        prompt: buildGrpcUpdatePrompt({ systemId, entries, impact }),
      }, {
        kind: 'grpc',
        target: 'grpc',
        title: names.length === 1 ? `update ${names[0]}` : `update ${names.length} contracts`,
      })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  const dirtyCount = Object.keys(drafts).length
  const editingExisting = byName.has(form.contract.trim())

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>gRPC contracts</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <p className="sim-desc">
          A contract is pure <strong>shape</strong> (a proto <code>service</code> + messages). Each is
          served by <strong>one</strong> owning service — attach it (and describe each method’s
          behavior) from that service’s Edit → gRPC tab.
        </p>

        {/* Existing contracts */}
        {contracts === null ? (
          <p className="sim-desc">Loading…</p>
        ) : contracts.length === 0 ? (
          <p className="sim-desc">No contracts yet.</p>
        ) : (
          <ul className="endpoint-list">
            {contracts.map((c) => {
              const open = openName === c.name
              const draft = drafts[c.name]
              const methods = draftedMethods(c)
              const confirming = confirmName === c.name
              return (
                <li key={c.name} className="grpc-contract">
                  <div className="grpc-contract-head">
                    <button
                      className="skill-toggle"
                      onClick={() => setOpenName(open ? null : c.name)}
                    >
                      <span className={`skill-caret${open ? ' open' : ''}`}>▸</span>
                      <code>{c.name}</code>
                    </button>
                    {draft && (
                      <span className="model-dirty-badge">
                        {draft.kind === 'delete' ? 'deleting' : draft.kind === 'replace-proto' ? 're-upload' : 'modified'}
                      </span>
                    )}
                    <span className="grpc-contract-meta">
                      {methods.length} method{methods.length === 1 ? '' : 's'} · {attachmentLabel(c)}
                    </span>
                    {draft ? (
                      <button className="link" disabled={busy} onClick={() => unstage(c.name)}>undo</button>
                    ) : confirming ? (
                      <span className="endpoint-list-actions">
                        <span className="endpoint-confirm">Delete?</span>
                        <button className="link" disabled={busy} onClick={() => stageContractDelete(c.name)}>Yes</button>
                        <button className="link" disabled={busy} onClick={() => setConfirmName(null)}>No</button>
                      </span>
                    ) : (
                      <span className="endpoint-list-actions">
                        <button className="link" disabled={busy} onClick={() => addMethodTo(c.name)}>+ method</button>
                        <button className="link-danger" disabled={busy} onClick={() => setConfirmName(c.name)}>Delete</button>
                      </span>
                    )}
                  </div>
                  {open && draft?.kind !== 'delete' && (
                    <div className="grpc-contract-body">
                      {draft?.kind === 'replace-proto' && (
                        <p className="sim-desc">Staged re-upload — the methods below are replaced by the new .proto on save.</p>
                      )}
                      {methods.map((m) => {
                        const staged = draft?.kind === 'methods' && !!draft.upserts[m.name]
                        return (
                          <div key={m.name} className="grpc-method">
                            <code>{m.name}</code>
                            <span className="grpc-sig" title={m.description || undefined}>{methodSig(m)}</span>
                            {staged && <span className="model-dirty-badge">staged</span>}
                            {draft?.kind !== 'replace-proto' && (
                              <span className="endpoint-list-actions">
                                {m.formAuthored ? (
                                  <button className="link" disabled={busy} onClick={() => startEditMethod(c, m)}>Edit</button>
                                ) : (
                                  <span className="grpc-contract-meta" title="This method came from an uploaded .proto — delete it or re-upload to reshape it.">uploaded</span>
                                )}
                                <button className="link-danger" disabled={busy} onClick={() => deleteMethod(c, m)}>Delete</button>
                              </span>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
                </li>
              )
            })}
          </ul>
        )}

        {review ? (
          /* Impact review — what this staged batch will change */
          <div className="form-section">
            <div className="form-section-head">
              <span>Review changes</span>
            </div>
            <p className="sim-desc">
              Saving applies {dirtyCount} staged contract change{dirtyCount === 1 ? '' : 's'} (
              {Object.keys(drafts).join(', ')}). The backend re-runs protoc and regenerates the
              bindings; nothing is saved unless every changed contract compiles.
            </p>
            {(() => {
              const affected = Object.keys(drafts)
                .map((name) => byName.get(name))
                .filter(Boolean)
                .filter((c) => c.servers.length || c.clients.length)
              return affected.length === 0 ? (
                <p className="sim-desc">No services serve or call these contracts — changes will just be saved.</p>
              ) : (
                <>
                  <div className="impact-group">
                    <div className="impact-group-head">Affected services</div>
                    <ul className="impact-list">
                      {affected.flatMap((c) => [
                        ...c.servers.map((s) => (
                          <li key={`${c.name}|s|${s}`}>
                            <code>{s}</code> serves {c.name}
                            {drafts[c.name]?.kind === 'delete' && <span className="impact-field"> (will be unwired)</span>}
                          </li>
                        )),
                        ...c.clients.map((cl) => (
                          <li key={`${c.name}|c|${cl.service}`}>
                            <code>{cl.service}</code> calls {c.name}
                          </li>
                        )),
                      ])}
                    </ul>
                  </div>
                  <p className="form-hint">
                    A single Claude session will update each owning service’s servicer (per its stored
                    method descriptions), each caller’s call sites, and rebuild them
                    (sandbox-grpc-contract skill).
                  </p>
                </>
              )
            })()}

            {error && <p className="modal-error">{error}</p>}

            <div className="modal-actions">
              <button type="button" onClick={() => setReview(false)} disabled={busy}>Back</button>
              <button type="button" className="primary" onClick={confirmSave} disabled={busy}>
                {busy ? 'Applying…' : 'Confirm & save → apply'}
              </button>
            </div>
          </div>
        ) : (
          <>
            {/* Author shape: build a method via the form, or upload a whole .proto. */}
            <div className="form-section">
              <div className="grpc-tabs">
                <button
                  type="button"
                  className={`grpc-tab${tab === 'form' ? ' active' : ''}`}
                  onClick={() => { setTab('form'); setError(null) }}
                  disabled={busy}
                >{editingMethod ? 'Edit method' : 'Add method'}</button>
                <button
                  type="button"
                  className={`grpc-tab${tab === 'upload' ? ' active' : ''}`}
                  onClick={() => { setTab('upload'); setError(null) }}
                  disabled={busy}
                >Upload .proto</button>
              </div>

              {tab === 'form' ? (
                <>
                  <label className="form-row">
                    <span>Contract</span>
                    <input value={form.contract} onChange={set('contract')} placeholder="ChunkTransfer" disabled={busy || !!editingMethod} />
                  </label>
                  <label className="form-row">
                    <span>Method</span>
                    <input value={form.method} onChange={set('method')} placeholder="GetChunk" disabled={busy || !!editingMethod} />
                  </label>

                  {['request', 'response'].map((key) => (
                    <div className="form-section" key={key}>
                      <div className="form-section-head">
                        <span>{key} fields <em className="grpc-optional">(blank = empty message)</em></span>
                        <button type="button" onClick={() => addRow(key)} disabled={busy}>+ field</button>
                      </div>
                      {form[key].map((r, i) => (
                        <div className="field-row" key={i}>
                          <input
                            value={r.name}
                            onChange={(e) => updateRow(key, i, { name: e.target.value })}
                            placeholder="field_name"
                            disabled={busy}
                          />
                          <select value={r.type} onChange={(e) => updateRow(key, i, { type: e.target.value })} disabled={busy}>
                            {PROTO_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                          </select>
                          <button type="button" className="link-danger" onClick={() => removeRow(key, i)} disabled={busy}>×</button>
                        </div>
                      ))}
                      {key === 'response' && (
                        <label className="grpc-check">
                          <input
                            type="checkbox"
                            checked={form.responseStreaming}
                            onChange={(e) => setForm((f) => ({ ...f, responseStreaming: e.target.checked }))}
                            disabled={busy}
                          />
                          <span>Streaming response (<code>returns (stream …)</code>)</span>
                        </label>
                      )}
                    </div>
                  ))}

                  <small className="form-hint">
                    {editingExisting
                      ? 'Edits to an existing contract are staged — review what they affect, then save them all together.'
                      : 'A new contract is created immediately (the backend synthesizes the .proto and runs protoc). Describe each method’s behavior later, when a service attaches it as server.'}
                  </small>
                </>
              ) : (
                <>
                  <p className="sim-desc">
                    Upload a complete proto3 <code>.proto</code> (one self-contained <code>service</code>).
                    It's validated with <code>protoc</code> before anything is created — if it doesn't
                    compile, the exact error is shown and nothing is registered. Re-uploading an existing
                    contract's service stages a replacement for review.
                  </p>
                  <label className="form-row">
                    <span>.proto file</span>
                    <input type="file" accept=".proto,text/plain" onChange={onProtoFile} disabled={busy} />
                  </label>
                  <label className="form-row">
                    <span>Contents</span>
                    <textarea
                      className="proto-input"
                      value={proto}
                      onChange={(e) => setProto(e.target.value)}
                      placeholder={'syntax = "proto3";\n\nservice ChunkTransfer {\n  rpc GetChunk (ChunkRequest) returns (stream ChunkFrame);\n}\n\nmessage ChunkRequest { int32 chunk_id = 1; }'}
                      rows={12}
                      spellCheck={false}
                      disabled={busy}
                    />
                  </label>
                </>
              )}

              {error && <p className="modal-error">{error}</p>}

              <div className="modal-actions">
                <button type="button" onClick={onClose} disabled={busy}>Close</button>
                {tab === 'form' ? (
                  <button type="button" className="primary" onClick={submitForm} disabled={busy}>
                    {busy ? 'Working…' : editingExisting ? 'Stage method' : 'Create contract'}
                  </button>
                ) : (
                  <button type="button" className="primary" onClick={submitUpload} disabled={busy}>
                    {busy ? 'Validating…' : 'Upload & validate'}
                  </button>
                )}
              </div>
            </div>

            {/* Review bar — appears once there are staged edits */}
            {dirtyCount > 0 && (
              <div className="form-section model-review-bar">
                <button type="button" className="primary" onClick={() => { setError(null); setReview(true) }} disabled={busy}>
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
