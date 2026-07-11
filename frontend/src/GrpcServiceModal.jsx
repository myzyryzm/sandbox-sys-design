import { useCallback, useEffect, useState } from 'react'
import {
  buildGrpcAttachPrompt,
  buildGrpcDescriptionsPrompt,
  buildGrpcDetachPrompt,
  joinDescription,
  methodSig,
} from './grpcBank.js'

// A history entry's ISO timestamp -> a short, local, human label (best-effort).
function fmtAt(at) {
  if (!at) return ''
  const d = new Date(at)
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleString()
}

// Collapsible, read-only changelog of a served method's description updates.
// Entries are stored oldest-first; shown newest-first (the first-ever entry is
// tagged "created"). Reuses the endpoint/consumer changelog styling.
function MethodChangelog({ history }) {
  const [open, setOpen] = useState(false)
  if (!history?.length) return null
  return (
    <>
      <button
        type="button"
        className="skill-toggle"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        <span className={`skill-caret${open ? ' open' : ''}`}>▶</span>
        Changelog ({history.length})
      </button>
      {open && (
        <div className="endpoint-history">
          <ol className="endpoint-history-list">
            {history
              .map((h, i) => ({ h, i }))
              .reverse()
              .map(({ h, i }) => (
                <li key={i} className="endpoint-history-row">
                  <div className="endpoint-history-meta">
                    <span className="endpoint-history-num">#{i + 1}</span>
                    {i === 0 && <span className="endpoint-history-initial">created</span>}
                    {fmtAt(h.at) && <span className="endpoint-history-at">{fmtAt(h.at)}</span>}
                  </div>
                  {h.change && <div className="endpoint-history-desc">{h.change}</div>}
                </li>
              ))}
          </ol>
        </div>
      )}
    </>
  )
}

/**
 * Per-service gRPC tab (Part B) — SERVER-only, endpoint-style.
 *
 * A contract is served by exactly ONE owning service. This tab:
 *  - Attach: pick an unowned contract, describe each method's behavior, then
 *    POST /api/grpc-attach (manifest grpc.servers + registry descriptions) and
 *    launch a session that authors the servicer + wiring (sandbox-grpc-attach).
 *  - Describe: append to a served method's description (endpoint-style
 *    accumulation) and launch a session that edits that method body in place.
 *  - Detach: POST /api/grpc-detach (409 while other services still dial this
 *    one) and launch the unwire session.
 *
 * Client wiring is NOT edited here: a service becomes a caller through the
 * flows that make it call the contract (endpoints, consumers, custom types),
 * which write the manifest `grpc.clients` block themselves. The Calls list
 * below is read-only visibility of that block.
 */
export default function GrpcServiceModal({ systemId, node, onClose, onLaunch, embedded = false, onBusyChange }) {
  const service = node.id
  const [data, setData] = useState(null) // { grpc, contracts } | null
  const [attachName, setAttachName] = useState('')
  const [attachDescs, setAttachDescs] = useState({}) // method -> description (attach form)
  const [descEdits, setDescEdits] = useState({}) // `${contract}|${method}` -> new chunk
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const load = useCallback(() => {
    return fetch(`/api/grpc-service?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(service)}`)
      .then((r) => r.json())
      .then((d) => setData(d.ok ? d : { grpc: { servers: [], clients: [], overrides: [] }, contracts: [] }))
      .catch(() => setData({ grpc: { servers: [], clients: [], overrides: [] }, contracts: [] }))
  }, [systemId, service])

  useEffect(() => {
    load()
  }, [load])

  const g = data?.grpc || { servers: [], clients: [], overrides: [] }
  const contracts = data?.contracts || []
  const byName = new Map(contracts.map((c) => [c.name, c]))
  const served = (g.servers || []).map((name) => byName.get(name)).filter(Boolean)
  // Attachable = unowned by anyone (custom multi-server contracts stay hidden).
  const attachable = contracts.filter((c) => !c.servers.length)
  const attaching = byName.get(attachName)

  async function postJson(url, body) {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const d = await res.json().catch(() => ({}))
    if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
    return d
  }

  // Attach as the contract's single owning server, then launch the session that
  // authors the servicer from the method descriptions and wires this service.
  async function submitAttach() {
    setError(null)
    if (!attaching) return setError('Pick a contract')
    const conversationId = crypto.randomUUID()
    const descriptions = {}
    for (const m of attaching.methods) {
      const text = (attachDescs[m.name] || '').trim()
      if (text) descriptions[m.name] = text
    }
    setBusy(true)
    try {
      await postJson('/api/grpc-attach', { system: systemId, service, contract: attaching.name, descriptions, conversationId })
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildGrpcAttachPrompt({
          systemId,
          service,
          contract: attaching.name,
          methods: attaching.methods.map((m) => ({ ...m, description: descriptions[m.name] || '' })),
        }),
      }, { kind: 'grpc', target: service, title: `attach ${attaching.name}` })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  // Persist appended descriptions for one served contract's edited methods,
  // then launch ONE session that edits those method bodies in place.
  async function saveDescriptions(c) {
    setError(null)
    const edited = c.methods
      .map((m) => ({ m, change: (descEdits[`${c.name}|${m.name}`] || '').trim() }))
      .filter((e) => e.change)
    if (!edited.length) return
    const conversationId = crypto.randomUUID()
    setBusy(true)
    try {
      for (const { m, change } of edited) {
        await postJson('/api/grpc-descriptions', {
          system: systemId,
          contract: c.name,
          method: m.name,
          description: joinDescription(m.description, change),
          change, // the raw delta — recorded as one changelog entry
          conversationId,
        })
      }
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildGrpcDescriptionsPrompt({
          systemId,
          service,
          contract: c.name,
          methods: edited.map(({ m, change }) => ({ ...m, priorDescription: m.description, change })),
        }),
      }, { kind: 'grpc', target: service, title: `describe ${c.name}` })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  // Detach (guarded server-side while other services still dial this one),
  // then launch the unwire session.
  async function detach(contract) {
    setError(null)
    setBusy(true)
    try {
      await postJson('/api/grpc-detach', { system: systemId, service, contract })
      onLaunch({
        sessionId: crypto.randomUUID(),
        mode: 'new',
        prompt: buildGrpcDetachPrompt({ systemId, service, contract }),
      }, { kind: 'grpc', target: service, title: `detach ${contract}` })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  const body = (
    <>
      {data === null ? (
        <p className="sim-desc">Loading…</p>
      ) : (
        <>
          {/* Serves — the contracts this service owns, with per-method behavior */}
          <div className="form-section">
            <div className="form-section-head"><span>Serves</span></div>
            {served.length === 0 ? (
              <p className="sim-desc">none — attach a contract below to serve it.</p>
            ) : (
              served.map((c) => {
                const dirty = c.methods.some((m) => (descEdits[`${c.name}|${m.name}`] || '').trim())
                return (
                  <div key={c.name} className="grpc-contract">
                    <div className="grpc-contract-head">
                      <code>{c.name}</code>
                      <span className="grpc-contract-meta">{c.methods.length} method{c.methods.length === 1 ? '' : 's'}</span>
                      <button className="link-danger" disabled={busy} onClick={() => detach(c.name)}>detach</button>
                    </div>
                    <div className="grpc-contract-body">
                      {c.methods.map((m) => {
                        const key = `${c.name}|${m.name}`
                        return (
                          <div key={m.name} className="grpc-method-block">
                            <div className="grpc-method">
                              <code>{m.name}</code>
                              <span className="grpc-sig">{methodSig(m)}</span>
                            </div>
                            {m.description && <p className="grpc-instruction"><span className="grpc-label">does</span> {m.description}</p>}
                            {m.history?.length > 0 && <MethodChangelog history={m.history} />}
                            <textarea
                              className="desc-input"
                              value={descEdits[key] || ''}
                              onChange={(e) => setDescEdits((d) => ({ ...d, [key]: e.target.value }))}
                              placeholder={m.description ? 'Describe a change to this method’s behavior…' : 'Describe what this method should do…'}
                              rows={2}
                              disabled={busy}
                            />
                          </div>
                        )
                      })}
                      {dirty && (
                        <div className="modal-actions">
                          <button type="button" className="primary" disabled={busy} onClick={() => saveDescriptions(c)}>
                            {busy ? 'Working…' : 'Save & update methods'}
                          </button>
                        </div>
                      )}
                    </div>
                  </div>
                )
              })
            )}
          </div>

          {/* Calls — read-only: client wiring is written by the flows that call */}
          {(g.clients || []).length > 0 && (
            <div className="form-section">
              <div className="form-section-head"><span>Calls</span></div>
              <ul className="grpc-attach-list">
                {g.clients.map((c) => (
                  <li key={c.contract}>
                    <code>{c.contract}</code>
                    <span className="grpc-targets">→ {c.targets?.length ? c.targets.join(', ') : '(no targets)'}</span>
                  </li>
                ))}
              </ul>
              <small className="form-hint">
                Wired by the flows that make this service call the contract (endpoints, consumer
                functions, custom types) — not editable here.
              </small>
            </div>
          )}

          {/* Attach a contract as this service's SERVER */}
          <div className="form-section">
            <div className="form-section-head"><span>Attach contract (as server)</span></div>
            {contracts.length === 0 ? (
              <p className="sim-desc">No contracts defined yet — author one with “＋ gRPC contract”.</p>
            ) : attachable.length === 0 ? (
              <p className="sim-desc">Every contract already has an owning server (one server per contract — detach it there first).</p>
            ) : (
              <>
                <label className="form-row">
                  <span>Contract</span>
                  <select
                    value={attachName}
                    onChange={(e) => { setAttachName(e.target.value); setAttachDescs({}) }}
                    disabled={busy}
                  >
                    <option value="">— pick —</option>
                    {attachable.map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}
                  </select>
                </label>

                {attaching && (
                  <>
                    <small className="form-hint">
                      {service} becomes this contract’s single owning server. Describe each method —
                      the descriptions drive the servicer implementation (blank = UNIMPLEMENTED stub).
                    </small>
                    {attaching.methods.map((m) => (
                      <div key={m.name} className="grpc-method-block">
                        <div className="grpc-method">
                          <code>{m.name}</code>
                          <span className="grpc-sig">{methodSig(m)}</span>
                        </div>
                        <textarea
                          className="desc-input"
                          value={attachDescs[m.name] || ''}
                          onChange={(e) => setAttachDescs((d) => ({ ...d, [m.name]: e.target.value }))}
                          placeholder="What should this method do?"
                          rows={2}
                          disabled={busy}
                        />
                      </div>
                    ))}
                    <div className="modal-actions">
                      <button type="button" className="primary" onClick={submitAttach} disabled={busy}>
                        {busy ? 'Working…' : 'Attach & open Claude'}
                      </button>
                    </div>
                  </>
                )}
              </>
            )}
          </div>

          {error && <p className="modal-error">{error}</p>}
        </>
      )}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>gRPC · <code>{service}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
