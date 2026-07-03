import { useCallback, useEffect, useState } from 'react'

/**
 * Per-service gRPC overview + attach (Part B). Shows what contracts a service
 * serves, which it consumes (and to which targets), and any overrides. Lets the
 * user:
 *  - Attach a contract as server and/or client (optionally with an override
 *    implementation): POST /api/grpc-attach writes the manifest grpc block (so the
 *    diagram draws edges immediately), then launches a Claude session to wire the
 *    code (sandbox-grpc-attach skill).
 *  - Re-point a client's targets: a manifest-only write (no regen) — the running
 *    service reads its targets from the mounted manifest, so a restart applies it.
 */

function blankAttach() {
  return { contract: '', asServer: false, asClient: false, targets: [], override: '' }
}

function buildAttachPrompt({ systemId, service, contract, asServer, asClient, targets, override }) {
  const roles = [asServer && 'server', asClient && 'client'].filter(Boolean).join(' + ') || 'none'
  const lines = [
    `Use the sandbox-grpc-attach skill to attach the gRPC contract "${contract}" to service "${service}" in the "${systemId}" system.`,
    ``,
    `Roles: ${roles}`,
  ]
  if (asServer) {
    lines.push(
      override.trim()
        ? `Server: use a SERVICE-SPECIFIC override servicer (the user supplied override text) at systems/${systemId}/${service}/grpc/${contract}_servicer_override.py — do NOT change the shared servicer.`
        : `Server: import the system's shared servicer systems/${systemId}/grpc/${contract}_servicer.py (do not regenerate it) and register it on ${service}'s gRPC server.`,
    )
  }
  if (asClient) {
    lines.push(`Client targets (read from the manifest grpc block, editable later): ${targets.length ? targets.join(', ') : '(none yet)'}`)
  }
  if (override.trim()) {
    lines.push(``, `Override implementation request:`, override.trim())
  }
  lines.push(
    ``,
    `The manifest grpc block for ${service} has already been written; wire app.py / Dockerfile /`,
    `requirements (gRPC server on port 50051 in the FastAPI lifespan; client stubs from the manifest`,
    `targets), then rebuild just this service.`,
  )
  return lines.join('\n')
}

export default function GrpcServiceModal({ systemId, node, onClose, onLaunch, embedded = false, onBusyChange }) {
  const service = node.id
  const [data, setData] = useState(null) // { grpc, contracts, services } | null
  const [attach, setAttach] = useState(blankAttach)
  const [editTargets, setEditTargets] = useState(null) // { contract, targets } being edited
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const load = useCallback(() => {
    return fetch(`/api/grpc-service?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(service)}`)
      .then((r) => r.json())
      .then((d) => setData(d.ok ? d : { grpc: { servers: [], clients: [], overrides: [] }, contracts: [], services: [] }))
      .catch(() => setData({ grpc: { servers: [], clients: [], overrides: [] }, contracts: [], services: [] }))
  }, [systemId, service])

  useEffect(() => {
    load()
  }, [load])

  // POST the attach state. `launch` true => also open a Claude session to wire code
  // (role/override change); false => manifest-only (a targets re-point, no regen).
  async function post({ contract, asServer, asClient, targets, override }, launch) {
    setError(null)
    setBusy(true)
    try {
      const res = await fetch('/api/grpc-attach', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId, service, contract,
          asServer, asClient, targets, override: !!override.trim(),
        }),
      })
      const d = await res.json().catch(() => ({}))
      if (!res.ok || !d.ok) throw new Error(d.error || `HTTP ${res.status}`)
      if (launch) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: buildAttachPrompt({ systemId, service, contract, asServer, asClient, targets, override }),
        }, { kind: 'grpc', target: service, title: `attach ${contract}` })
        onClose()
        return
      }
      setEditTargets(null)
      await load()
      setBusy(false)
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  function submitAttach() {
    if (!attach.contract) return setError('Pick a contract')
    if (!attach.asServer && !attach.asClient) return setError('Choose server and/or client')
    post(attach, true)
  }

  // Re-point a client's targets without code regen (manifest-only).
  function saveTargets() {
    const g = data.grpc
    post(
      {
        contract: editTargets.contract,
        asServer: (g.servers || []).includes(editTargets.contract),
        asClient: true,
        targets: editTargets.targets,
        override: (g.overrides || []).includes(editTargets.contract) ? 'keep' : '',
      },
      false,
    )
  }

  function detach(contract) {
    post({ contract, asServer: false, asClient: false, targets: [], override: '' }, false)
  }

  const toggleTarget = (list, t) =>
    list.includes(t) ? list.filter((x) => x !== t) : [...list, t]

  const g = data?.grpc || { servers: [], clients: [], overrides: [] }

  const body = (
    <>
      {data === null ? (
          <p className="sim-desc">Loading…</p>
        ) : (
          <>
            {/* Serves */}
            <div className="form-section">
              <div className="form-section-head"><span>Serves</span></div>
              {g.servers.length === 0 ? (
                <p className="sim-desc">none</p>
              ) : (
                <ul className="grpc-attach-list">
                  {g.servers.map((c) => (
                    <li key={c}>
                      <code>{c}</code>
                      {g.overrides.includes(c) && <span className="grpc-override-tag">override</span>}
                      <button className="link-danger" disabled={busy} onClick={() => detach(c)}>detach</button>
                    </li>
                  ))}
                </ul>
              )}
            </div>

            {/* Calls (clients + editable targets) */}
            <div className="form-section">
              <div className="form-section-head"><span>Calls</span></div>
              {g.clients.length === 0 ? (
                <p className="sim-desc">none</p>
              ) : (
                <ul className="grpc-attach-list">
                  {g.clients.map((c) => (
                    <li key={c.contract} className="grpc-client-row">
                      <code>{c.contract}</code>
                      {editTargets?.contract === c.contract ? (
                        <span className="grpc-target-editor">
                          {data.services.map((s) => (
                            <label key={s} className="grpc-check">
                              <input
                                type="checkbox"
                                checked={editTargets.targets.includes(s)}
                                onChange={() => setEditTargets((e) => ({ ...e, targets: toggleTarget(e.targets, s) }))}
                                disabled={busy}
                              />
                              <span>{s}</span>
                            </label>
                          ))}
                          <button className="link" disabled={busy} onClick={saveTargets}>save</button>
                          <button className="link" disabled={busy} onClick={() => setEditTargets(null)}>cancel</button>
                        </span>
                      ) : (
                        <>
                          <span className="grpc-targets">→ {c.targets.length ? c.targets.join(', ') : '(no targets)'}</span>
                          <button className="link" disabled={busy} onClick={() => setEditTargets({ contract: c.contract, targets: c.targets })}>edit targets</button>
                          <button className="link-danger" disabled={busy} onClick={() => detach(c.contract)}>detach</button>
                        </>
                      )}
                    </li>
                  ))}
                </ul>
              )}
            </div>

            {/* Attach a contract */}
            <div className="form-section">
              <div className="form-section-head"><span>Attach contract</span></div>
              {data.contracts.length === 0 ? (
                <p className="sim-desc">No contracts defined yet — author one with “＋ gRPC contract”.</p>
              ) : (
                <>
                  <label className="form-row">
                    <span>Contract</span>
                    <select
                      value={attach.contract}
                      onChange={(e) => setAttach((a) => ({ ...a, contract: e.target.value }))}
                      disabled={busy}
                    >
                      <option value="">— pick —</option>
                      {data.contracts.map((c) => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </label>

                  <label className="grpc-check">
                    <input
                      type="checkbox"
                      checked={attach.asServer}
                      onChange={(e) => setAttach((a) => ({ ...a, asServer: e.target.checked }))}
                      disabled={busy}
                    />
                    <span>Act as server</span>
                  </label>
                  <label className="grpc-check">
                    <input
                      type="checkbox"
                      checked={attach.asClient}
                      onChange={(e) => setAttach((a) => ({ ...a, asClient: e.target.checked }))}
                      disabled={busy}
                    />
                    <span>Act as client</span>
                  </label>

                  {attach.asClient && (
                    <div className="form-row">
                      <span>Targets</span>
                      <div className="grpc-target-editor">
                        {data.services.length === 0 ? (
                          <em className="grpc-optional">no other services to target</em>
                        ) : data.services.map((s) => (
                          <label key={s} className="grpc-check">
                            <input
                              type="checkbox"
                              checked={attach.targets.includes(s)}
                              onChange={() => setAttach((a) => ({ ...a, targets: toggleTarget(a.targets, s) }))}
                              disabled={busy}
                            />
                            <span>{s}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  )}

                  {attach.asServer && (
                    <label className="form-row">
                      <span>Override</span>
                      <textarea
                        className="desc-input"
                        value={attach.override}
                        onChange={(e) => setAttach((a) => ({ ...a, override: e.target.value }))}
                        placeholder="Optional — describe a service-specific servicer that diverges from the shared one. Leave blank to import the shared servicer."
                        rows={2}
                        disabled={busy}
                      />
                    </label>
                  )}

                  <div className="modal-actions">
                    <button type="button" className="primary" onClick={submitAttach} disabled={busy}>
                      {busy ? 'Working…' : 'Attach & open Claude'}
                    </button>
                  </div>
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
