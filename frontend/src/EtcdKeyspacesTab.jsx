import { useCallback, useEffect, useState } from 'react'

/**
 * The etcd node's "Keyspaces" tab (embedded in NodeEditModal). Manages SERVICE
 * DISCOVERY around this cluster:
 *
 *   - Register a service → creates the keyspace /services/<service>/ (identity =
 *     service, one keyspace each). The registry entry + compose env/mount are written
 *     mechanically by POST /api/etcd/keyspace; a launched Claude session
 *     (sandbox-etcd skill) then authors the real lease+put+keepalive loop in that
 *     service's app.py and flips implemented:true.
 *   - Per keyspace, add LISTENERS: services whose session-authored watch_prefix loop
 *     keeps a live in-memory view of the keyspace's workers (etcd pushes updates —
 *     no polling).
 *   - Live worker rows (<service>-N → host:port) read from the REAL cluster via
 *     etcdctl, so an expired lease (dead worker) visibly drops off.
 *
 * Mirrors ConsumerTab: pending badge while !implemented, Resume-able sessions,
 * delete drives a strip-the-code session when the loop was implemented.
 */

// Prompt seeding the launched session that implements (or updates) a service's
// REGISTRATION loop. The repeatable procedure lives in the sandbox-etcd skill.
function buildRegistrationPrompt({ systemId, etcdId, service, prefix, leaseTtlSeconds, description, editing, priorDescription }) {
  const lines = [
    `Use the sandbox-etcd skill to ${editing ? 'UPDATE' : 'IMPLEMENT'} etcd REGISTRATION for service "${service}" in the "${systemId}" system.`,
    '',
    `Cluster: "${etcdId}" · keyspace prefix: ${prefix} · current lease TTL: ${leaseTtlSeconds}s.`,
    '',
  ]
  if (editing) {
    lines.push(
      `This registration ALREADY EXISTS in systems/${systemId}/${service}/app.py. FIRST read it, then`,
      `modify it in place. Keep the metrics middleware and every other route/loop untouched.`,
      '',
      `Current behavior (existing description):`,
      (priorDescription || '').trim() || '(none recorded)',
      '',
    )
  }
  lines.push(
    `What it should do:`,
    (description || '').trim() || '(no description — standard worker registration per the skill)',
    '',
    `Per the skill's "Registration loop" contract:`,
    `- Add a daemon-thread registration loop to systems/${systemId}/${service}/app.py: grant a lease`,
    `  (TTL from the mounted /etcd/etcd.json, re-read by mtime), put ${prefix}<worker-id> = "<host>:8000"`,
    `  with the lease, refresh at TTL/3, and on ANY error reconnect + re-grant + re-put (it must`,
    `  survive cluster recreation and quorum loss). Worker id comes from ETCD_WORKER_ID, endpoints`,
    `  from ETCD_ENDPOINTS — both are ALREADY in the service's compose def, as is the etcd.json mount.`,
    `- The etcd.json keyspace entry and the compose edits are ALREADY written by the app — do NOT redo them.`,
    `- Add the pinned etcd client deps to systems/${systemId}/${service}/requirements.txt per the skill.`,
    `- Rebuild ONLY that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${service}`,
    `- Verify per the skill (etcdctl get --prefix ${prefix} shows the worker key(s)), then set`,
    `  "implemented": true on the keyspace entry for service "${service}" in systems/${systemId}/etcd.json.`,
  )
  return lines.join('\n')
}

// Prompt for a LISTENER: a watch_prefix loop keeping a live in-memory worker map.
function buildListenerPrompt({ systemId, etcdId, keyspaceService, listener, prefix, description, editing, priorDescription }) {
  const lines = [
    `Use the sandbox-etcd skill to ${editing ? 'UPDATE' : 'IMPLEMENT'} an etcd LISTENER in the "${systemId}" system:`,
    `service "${listener}" watching keyspace ${prefix} (the "${keyspaceService}" workers) on cluster "${etcdId}".`,
    '',
  ]
  if (editing) {
    lines.push(
      `This listener ALREADY EXISTS in systems/${systemId}/${listener}/app.py. FIRST read it, then`,
      `modify it in place. Keep the metrics middleware and every other route/loop untouched.`,
      '',
      `Current behavior (existing description):`,
      (priorDescription || '').trim() || '(none recorded)',
      '',
    )
  }
  lines.push(
    `What it should do:`,
    (description || '').trim() || `(no description — maintain a live worker map of ${keyspaceService} and use it for discovery)`,
    '',
    `Per the skill's "Watcher loop" contract:`,
    `- Add a daemon-thread watcher to systems/${systemId}/${listener}/app.py: on (re)connect do an`,
    `  initial get_prefix("${prefix}") into a module-level worker map, then watch_prefix — etcd`,
    `  PUSHES every change (a PUT adds/updates a worker; a DELETE or lease expiry removes it).`,
    `  Never poll. Resync from scratch on any watch error. Endpoints come from ETCD_ENDPOINTS`,
    `  (already in the compose def).`,
    `- Expose the debug route GET /discovery/${keyspaceService} returning the current map, per the skill.`,
    `- Add the pinned etcd client deps to systems/${systemId}/${listener}/requirements.txt.`,
    `- Rebuild ONLY that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${listener}`,
    `- Verify per the skill (kill a ${keyspaceService} worker → it drops from the map within the TTL),`,
    `  then set "implemented": true on this listener entry (keyspace "${keyspaceService}", service`,
    `  "${listener}") in systems/${systemId}/etcd.json.`,
  )
  return lines.join('\n')
}

// Deletes: registry + compose scrub already done by the DELETE; the session strips the code.
function buildRegistrationDeletePrompt({ systemId, etcdId, service, prefix }) {
  return [
    `Use the sandbox-etcd skill to DELETE the etcd registration of service "${service}" in the`,
    `"${systemId}" system (it registered workers under ${prefix} on cluster "${etcdId}").`,
    '',
    `Its etcd.json keyspace entry and compose env/mount are already removed. Strip the registration`,
    `loop (and its now-unused etcd client imports, if nothing else uses them) from`,
    `systems/${systemId}/${service}/app.py, leaving the metrics middleware and every other route/loop`,
    `intact, then rebuild only that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${service}`,
  ].join('\n')
}

function buildListenerDeletePrompt({ systemId, etcdId, keyspaceService, listener, prefix }) {
  return [
    `Use the sandbox-etcd skill to DELETE an etcd listener in the "${systemId}" system: service`,
    `"${listener}" no longer watches ${prefix} (the "${keyspaceService}" workers) on cluster "${etcdId}".`,
    '',
    `Its listener entry in etcd.json is already removed. Strip the watch loop (and the`,
    `GET /discovery/${keyspaceService} route) from systems/${systemId}/${listener}/app.py, leaving the`,
    `metrics middleware and every other route/loop intact, then rebuild only that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${listener}`,
  ].join('\n')
}

export default function EtcdKeyspacesTab({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }) {
  const etcdId = node.id
  const [info, setInfo] = useState(null) // GET /api/etcd response; keyspaces carry workers when live
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  // Register form.
  const [adding, setAdding] = useState(false)
  const [regService, setRegService] = useState('')
  const [regDescription, setRegDescription] = useState('')
  // Per-keyspace "add listener" picker: keyspace service -> selected listener service.
  const [listenerPick, setListenerPick] = useState({})
  const [confirmKey, setConfirmKey] = useState(null) // 'ks:<svc>' | 'ln:<ks>:<svc>' pending delete

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // Services that can register / listen: internal plain services or service-lb entries.
  const eligible = (manifest?.nodes || [])
    .filter((n) => (n.type === 'service' && !n.instanceOf) || n.type === 'service-lb')
    .map((n) => n.id)

  const load = useCallback(async (live) => {
    try {
      const res = await fetch(
        `/api/etcd?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(etcdId)}&live=${live ? 1 : 0}`,
      )
      const data = await res.json()
      if (!data.ok) throw new Error(data.error || 'failed to load')
      // Keep the last live worker listing while a registry-only refresh paints.
      setInfo((prev) => {
        if (live || !prev) return data
        const prevWorkers = Object.fromEntries((prev.keyspaces || []).map((k) => [k.service, k.workers]))
        return {
          ...data,
          keyspaces: (data.keyspaces || []).map((k) => ({ ...k, workers: k.workers ?? prevWorkers[k.service] ?? null })),
        }
      })
    } catch (err) {
      setError(err.message)
    }
  }, [systemId, etcdId])

  useEffect(() => {
    let cancelled = false
    load(false).then(() => { if (!cancelled) load(true) })
    const t = setInterval(() => { if (!document.hidden) load(true) }, 5000)
    return () => {
      cancelled = true
      clearInterval(t)
    }
  }, [load])

  if (!info) {
    return <p className="sim-desc">{error ? `Error: ${error}` : 'Loading…'}</p>
  }

  const keyspaces = info.keyspaces || []
  const registered = new Set(keyspaces.map((k) => k.service))
  const registrable = eligible.filter((s) => !registered.has(s))
  const leaseTtlSeconds = info.cluster?.leaseTtlSeconds

  function startAdd() {
    setRegService(registrable[0] || '')
    setRegDescription('')
    setError(null)
    setAdding(true)
  }

  // Register a service (create its keyspace) and launch the session that authors the
  // lease+keepalive loop. Mirrors ConsumerTab.submit.
  async function submitRegister() {
    if (!regService) return setError('Pick a service to register')
    const conversationId = crypto.randomUUID()
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/keyspace', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, service: regService, description: regDescription, conversationId }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildRegistrationPrompt({
          systemId, etcdId,
          service: regService,
          prefix: data.keyspace.prefix,
          leaseTtlSeconds,
          description: data.keyspace.description,
          editing: false,
        }),
      }, { kind: 'etcd', target: regService, title: `register ${regService}` })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  // Add a listener to a keyspace and launch the watch-loop session.
  async function addListener(ks) {
    const listener = listenerPick[ks.service]
    if (!listener) return
    const conversationId = crypto.randomUUID()
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/listener', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ks.service, service: listener, conversationId }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildListenerPrompt({
          systemId, etcdId,
          keyspaceService: ks.service,
          listener,
          prefix: ks.prefix,
          description: data.listener.description,
          editing: false,
        }),
      }, { kind: 'etcd', target: listener, title: `listen ${ks.prefix}` })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  async function removeKeyspace(ks) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/keyspace', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, service: ks.service }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmKey(null)
      if (data.wasImplemented) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: buildRegistrationDeletePrompt({ systemId, etcdId, service: ks.service, prefix: ks.prefix }),
        }, { kind: 'etcd', target: ks.service, title: `unregister ${ks.service}` })
        onClose()
        return
      }
      await load(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function removeListener(ks, l) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/listener', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ks.service, service: l.service }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmKey(null)
      if (data.wasImplemented) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: buildListenerDeletePrompt({ systemId, etcdId, keyspaceService: ks.service, listener: l.service, prefix: ks.prefix }),
        }, { kind: 'etcd', target: l.service, title: `unlisten ${ks.prefix}` })
        onClose()
        return
      }
      await load(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  function resume(conversationId) {
    if (!conversationId) return
    onLaunch({ sessionId: conversationId, mode: 'resume', prompt: '' })
    onClose()
  }

  const body = (
    <>
      <p className="sim-desc">
        <strong>Keyspaces</strong> on <code>{etcdId}</code> — one per registered service. Each of a
        service's workers keeps a leased key alive under <code>/services/&lt;service&gt;/</code>{' '}
        (value <code>host:port</code>, TTL {leaseTtlSeconds ?? '…'}s); listeners watch the prefix and get
        every change pushed. Click a KEY row on the diagram node to trace the flow.
      </p>

      {/* ---- Keyspaces ---- */}
      {keyspaces.length === 0 ? (
        <p className="sim-desc">No keyspaces yet — register a service below.</p>
      ) : (
        keyspaces.map((ks) => {
          const ksKey = `ks:${ks.service}`
          const listeners = ks.listeners || []
          const listenable = eligible.filter(
            (s) => s !== ks.service && !listeners.some((l) => l.service === s),
          )
          return (
            <div className="form-section" key={ks.service}>
              <div className="form-section-head">
                <span>
                  <code>{ks.prefix}</code>
                  {!ks.implemented && (
                    <span className="scenario-pending" title="Registration loop not implemented yet — resume the Claude session"> pending</span>
                  )}
                </span>
                {confirmKey === ksKey ? (
                  <span className="endpoint-list-actions">
                    <span className="endpoint-confirm">{ks.implemented ? 'Delete & rebuild?' : 'Delete?'}</span>
                    <button className="link" disabled={busy} onClick={() => removeKeyspace(ks)}>Yes</button>
                    <button className="link" disabled={busy} onClick={() => setConfirmKey(null)}>No</button>
                  </span>
                ) : (
                  <span className="endpoint-list-actions">
                    {ks.conversationId && (
                      <button className="link" disabled={busy} title="Resume this registration's Claude session"
                        onClick={() => resume(ks.conversationId)}>Resume</button>
                    )}
                    <button className="link-danger" disabled={busy} onClick={() => setConfirmKey(ksKey)}>Delete</button>
                  </span>
                )}
              </div>

              {/* Live workers (host:port), straight from the real cluster. */}
              {ks.workers === null ? (
                <small className="form-hint">Probing live workers…</small>
              ) : ks.workers.length === 0 ? (
                <small className="form-hint">
                  No live workers under this prefix{ks.implemented ? ' — are the containers up?' : ' yet (loop pending)'}.
                </small>
              ) : (
                <ul className="endpoint-list">
                  {ks.workers.map((w) => (
                    <li key={w.id} className="endpoint-list-row">
                      <span className="endpoint-list-method">KEY</span>
                      <code className="endpoint-alias">{w.id}</code>
                      <span className="endpoint-list-path">→ {w.value}</span>
                    </li>
                  ))}
                </ul>
              )}

              {/* Listeners. */}
              <ul className="endpoint-list">
                {listeners.map((l) => {
                  const lnKey = `ln:${ks.service}:${l.service}`
                  return (
                    <li key={l.service} className="endpoint-list-row">
                      <span className="endpoint-list-method">WATCH</span>
                      <code className="endpoint-alias">{l.service}</code>
                      {!l.implemented && (
                        <span className="scenario-pending" title="Watch loop not implemented yet — resume the Claude session">pending</span>
                      )}
                      {confirmKey === lnKey ? (
                        <span className="endpoint-list-actions">
                          <span className="endpoint-confirm">{l.implemented ? 'Delete & rebuild?' : 'Delete?'}</span>
                          <button className="link" disabled={busy} onClick={() => removeListener(ks, l)}>Yes</button>
                          <button className="link" disabled={busy} onClick={() => setConfirmKey(null)}>No</button>
                        </span>
                      ) : (
                        <span className="endpoint-list-actions">
                          {l.conversationId && (
                            <button className="link" disabled={busy} title="Resume this listener's Claude session"
                              onClick={() => resume(l.conversationId)}>Resume</button>
                          )}
                          <button className="link-danger" disabled={busy} onClick={() => setConfirmKey(lnKey)}>Remove</button>
                        </span>
                      )}
                    </li>
                  )
                })}
              </ul>
              {listenable.length > 0 && (
                <div className="entity-row">
                  <select
                    value={listenerPick[ks.service] || ''}
                    onChange={(e) => setListenerPick((p) => ({ ...p, [ks.service]: e.target.value }))}
                    disabled={busy}
                  >
                    <option value="">— add a listener —</option>
                    {listenable.map((s) => <option key={s} value={s}>{s}</option>)}
                  </select>
                  <button type="button" className="link" disabled={busy || !listenerPick[ks.service]}
                    onClick={() => addListener(ks)}>
                    Add listener
                  </button>
                </div>
              )}
            </div>
          )
        })
      )}

      {/* ---- Register a service ---- */}
      {!adding ? (
        <div className="form-section">
          <button className="link" onClick={startAdd} disabled={busy || registrable.length === 0}>
            ＋ Register a service
          </button>
          {registrable.length === 0 && (
            <small className="form-hint">
              {eligible.length === 0
                ? 'Add an internal service first — only services can register.'
                : 'Every eligible service is already registered.'}
            </small>
          )}
        </div>
      ) : (
        <div className="form-section">
          <div className="form-section-head"><span>Register a service</span></div>
          <label className="form-row">
            <span>Service</span>
            <select value={regService} onChange={(e) => setRegService(e.target.value)} disabled={busy}>
              {!regService && <option value="">— pick a service —</option>}
              {registrable.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          </label>
          <label className="form-row form-row-stack">
            <span>Describe</span>
            <textarea
              className="desc-input"
              value={regDescription}
              onChange={(e) => setRegDescription(e.target.value)}
              placeholder="Anything special about how this service should register? Leave blank for the standard leased-key registration."
              rows={3}
              disabled={busy}
              autoFocus
            />
          </label>
          <p className="sim-desc">
            Registering opens a Claude session that writes the real lease + keepalive loop into the
            service (each worker registers itself as <code>/services/{regService || '<service>'}/{regService || '<service>'}-N</code>)
            and rebuilds it.
          </p>
          <div className="modal-actions">
            <button type="button" onClick={() => { setAdding(false); setError(null) }} disabled={busy}>Cancel</button>
            <button type="button" className="primary" onClick={submitRegister} disabled={busy || !regService}>
              {busy ? 'Working…' : 'Register & open Claude'}
            </button>
          </div>
        </div>
      )}

      {error && <p className="modal-error">{error}</p>}

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
          <h2>Keyspaces · <code>{etcdId}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
