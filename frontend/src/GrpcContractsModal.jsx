import { useCallback, useEffect, useState } from 'react'

/**
 * gRPC contract authoring (Part A). Lists the system's contracts and lets the user
 * add a method to one (new or existing): an optional request/response field set
 * (proto types) + a streaming toggle + a free-text instruction. On submit it
 * persists the method to the per-system registry via POST /api/grpc-contracts
 * (with a fresh Claude session id), then launches that session to author the
 * .proto, run protoc, and generate the system's single shared servicer
 * (sandbox-grpc-contract skill). Re-opening a contract shows the original
 * instruction text — the provenance of the generated implementation.
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
    instruction: '',
  }
}

// Turn dynamic {name,type} rows into a flat { name: type } object, dropping blanks.
// Empty -> {} so the backend stores nothing and Claude infers from the instruction.
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

function shape(obj) {
  const keys = Object.keys(obj || {})
  return keys.length ? keys.map((k) => `${k}: ${obj[k]}`).join(', ') : 'inferred'
}

// A method's display signature. Form-authored methods carry request/response field
// maps; uploaded methods carry the message type names (requestType/responseType)
// instead, so fall back to those (with any stream markers) when the maps are empty.
function methodSig(m) {
  const reqHas = m.request && Object.keys(m.request).length
  const resHas = m.response && Object.keys(m.response).length
  const reqInner = reqHas ? shape(m.request) : `${m.requestStreaming ? 'stream ' : ''}${m.requestType || 'inferred'}`
  const resInner = resHas ? shape(m.response) : m.responseType || 'inferred'
  return `(${reqInner}) → ${m.responseStreaming ? 'stream ' : ''}(${resInner})`
}

function buildContractPrompt({ systemId, contract, method, request, response, responseStreaming, instruction }) {
  const fmt = (o) => (Object.keys(o).length ? JSON.stringify(o) : 'none (infer from the instruction)')
  return [
    `Use the sandbox-grpc-contract skill to author a gRPC method in the "${systemId}" system.`,
    ``,
    `Contract (proto service): ${contract}`,
    `Method: ${method}`,
    `Request fields (name -> proto type): ${fmt(request)}`,
    `Response fields (name -> proto type): ${fmt(response)}`,
    `Response streaming: ${responseStreaming ? 'yes (returns (stream ...))' : 'no'}`,
    ``,
    `What the method should do:`,
    instruction.trim() || '(no description given — infer something reasonable)',
    ``,
    `Write/append it to systems/${systemId}/grpc/${contract}.proto with sequential field`,
    `numbers, run protoc, and generate the SINGLE shared servicer`,
    `systems/${systemId}/grpc/${contract}_servicer.py (generated once for the whole system).`,
  ].join('\n')
}

// Prompt for the upload path: the .proto is already written + protoc-validated, so
// the session only generates the bindings + shared servicer (it must not re-author
// the proto).
function buildUploadPrompt({ systemId, contract, methods, instruction }) {
  const sig = (m) =>
    `  ${m.name}(${m.requestStreaming ? 'stream ' : ''}${m.requestType}) -> ${m.responseStreaming ? 'stream ' : ''}${m.responseType}`
  return [
    `Use the sandbox-grpc-contract skill to finish a gRPC contract in the "${systemId}" system.`,
    ``,
    `An existing .proto was just UPLOADED and already passed protoc validation; it is`,
    `written at systems/${systemId}/grpc/${contract}.proto. Do NOT rewrite or re-author the`,
    `.proto — it is the source of truth.`,
    ``,
    `Contract (proto service): ${contract}`,
    `Methods:`,
    ...(methods || []).map(sig),
    ``,
    `Do this:`,
    `1. Run protoc to generate systems/${systemId}/grpc/${contract}_pb2.py and`,
    `   ${contract}_pb2_grpc.py from the uploaded .proto.`,
    `2. Implement the SINGLE shared servicer systems/${systemId}/grpc/${contract}_servicer.py`,
    `   (generated once for the whole system) for the methods above.`,
    ``,
    `What the methods should do:`,
    (instruction || '').trim() || '(no description given — infer reasonable behavior from the .proto)',
  ].join('\n')
}

export default function GrpcContractsModal({ systemId, onClose, onLaunch }) {
  const [contracts, setContracts] = useState(null) // null = loading
  const [form, setForm] = useState(blankForm)
  const [openName, setOpenName] = useState(null) // expanded contract row
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [tab, setTab] = useState('form') // 'form' (build a method) | 'upload' (.proto file)
  const [proto, setProto] = useState('') // uploaded / pasted .proto text
  const [protoInstruction, setProtoInstruction] = useState('')

  const load = useCallback(() => {
    return fetch(`/api/grpc-contracts?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json())
      .then((d) => setContracts(d.ok ? d.contracts : []))
      .catch(() => setContracts([]))
  }, [systemId])

  useEffect(() => {
    load()
  }, [load])

  const set = (k) => (e) => setForm((f) => ({ ...f, [k]: e.target.value }))

  // Row editors for the request/response field lists.
  const updateRow = (key, i, patch) =>
    setForm((f) => ({ ...f, [key]: f[key].map((r, j) => (j === i ? { ...r, ...patch } : r)) }))
  const addRow = (key) =>
    setForm((f) => ({ ...f, [key]: [...f[key], { name: '', type: 'string' }] }))
  const removeRow = (key, i) =>
    setForm((f) => ({ ...f, [key]: f[key].filter((_, j) => j !== i) }))

  // Pre-fill the form's contract name so a new method joins an existing contract.
  function addMethodTo(name) {
    setForm((f) => ({ ...blankForm(), contract: name }))
    setError(null)
  }

  async function submit() {
    setError(null)
    if (!/^[A-Z][A-Za-z0-9]*$/.test(form.contract.trim())) {
      return setError('Contract must be PascalCase (e.g. ChunkTransfer)')
    }
    if (!/^[A-Z][A-Za-z0-9]*$/.test(form.method.trim())) {
      return setError('Method must be PascalCase (e.g. GetChunk)')
    }
    const conversationId = crypto.randomUUID()
    const request = rowsToFields(form.request)
    const response = rowsToFields(form.response)
    const body = {
      system: systemId,
      contract: form.contract.trim(),
      method: form.method.trim(),
      request,
      response,
      responseStreaming: form.responseStreaming,
      instruction: form.instruction,
      conversationId,
    }
    setBusy(true)
    try {
      const res = await fetch('/api/grpc-contracts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildContractPrompt({ ...body, request, response }),
      }, { kind: 'grpc', target: body.contract, title: body.method })
      onClose()
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
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

  // Upload path: the backend validates the whole .proto with real protoc and only
  // registers it if it compiles; on success we launch the session that generates the
  // bindings + shared servicer. A validation failure is shown verbatim (what's wrong).
  async function submitUpload() {
    setError(null)
    if (!proto.trim()) return setError('Choose or paste a .proto file first')
    const conversationId = crypto.randomUUID()
    setBusy(true)
    try {
      const res = await fetch('/api/grpc-contracts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          protoFile: proto,
          instruction: protoInstruction,
          conversationId,
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildUploadPrompt({
          systemId,
          contract: data.contract,
          methods: data.methods,
          instruction: protoInstruction,
        }),
      }, { kind: 'grpc', target: data.contract, title: 'upload proto' })
      onClose()
    } catch (err) {
      setBusy(false)
      setError(err.message)
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>gRPC contracts</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        {/* Existing contracts (re-open shows the original instruction text). */}
        {contracts === null ? (
          <p className="sim-desc">Loading…</p>
        ) : contracts.length === 0 ? (
          <p className="sim-desc">No contracts yet.</p>
        ) : (
          <ul className="endpoint-list">
            {contracts.map((c) => {
              const open = openName === c.name
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
                    <span className="grpc-contract-meta">{c.methods.length} method{c.methods.length === 1 ? '' : 's'}</span>
                    <button className="link" disabled={busy} onClick={() => addMethodTo(c.name)}>+ method</button>
                  </div>
                  {open && (
                    <div className="grpc-contract-body">
                      {c.instruction && (
                        <p className="grpc-instruction"><span className="grpc-label">instruction</span> {c.instruction}</p>
                      )}
                      {c.methods.map((m) => (
                        <div key={m.name} className="grpc-method">
                          <code>{m.name}</code>
                          <span className="grpc-sig">{methodSig(m)}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </li>
              )
            })}
          </ul>
        )}

        {/* Author a contract: build a method via the form, or upload a whole .proto. */}
        <div className="form-section">
          <div className="grpc-tabs">
            <button
              type="button"
              className={`grpc-tab${tab === 'form' ? ' active' : ''}`}
              onClick={() => { setTab('form'); setError(null) }}
              disabled={busy}
            >Add method</button>
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
                <input value={form.contract} onChange={set('contract')} placeholder="ChunkTransfer" disabled={busy} />
              </label>
              <label className="form-row">
                <span>Method</span>
                <input value={form.method} onChange={set('method')} placeholder="GetChunk" disabled={busy} />
              </label>

              {['request', 'response'].map((key) => (
                <div className="form-section" key={key}>
                  <div className="form-section-head">
                    <span>{key} fields <em className="grpc-optional">(optional — inferred if blank)</em></span>
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

              <label className="form-row">
                <span>Describe</span>
                <textarea
                  className="desc-input"
                  value={form.instruction}
                  onChange={set('instruction')}
                  placeholder="What should this method do? (drives the shared servicer implementation)"
                  rows={3}
                  disabled={busy}
                />
              </label>
            </>
          ) : (
            <>
              <p className="sim-desc">
                Upload a complete proto3 <code>.proto</code> (one self-contained <code>service</code>).
                It's validated with <code>protoc</code> before anything is created — if it doesn't
                compile, the exact error is shown and nothing is registered.
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
              <label className="form-row">
                <span>Describe</span>
                <textarea
                  className="desc-input"
                  value={protoInstruction}
                  onChange={(e) => setProtoInstruction(e.target.value)}
                  placeholder="What should these methods do? (drives the shared servicer implementation)"
                  rows={3}
                  disabled={busy}
                />
              </label>
            </>
          )}

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
            {tab === 'form' ? (
              <button type="button" className="primary" onClick={submit} disabled={busy}>
                {busy ? 'Working…' : 'Author & open Claude'}
              </button>
            ) : (
              <button type="button" className="primary" onClick={submitUpload} disabled={busy}>
                {busy ? 'Validating…' : 'Upload & validate'}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
