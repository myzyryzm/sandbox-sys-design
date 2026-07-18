import { useCallback, useEffect, useState } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName'
import {
  buildListenerPrompt,
  buildListenerDeletePrompt,
  buildConfigListenerPrompt,
  buildConfigListenerDeletePrompt,
} from './etcdListenerPrompts'

/**
 * The etcd node's "Keyspaces" tab (embedded in NodeEditModal). Manages the two
 * keyspace types around this cluster:
 *
 *   - SERVICE DISCOVERY — register a service → creates /services/<service>/
 *     (identity = service, one keyspace each). The registry entry + compose
 *     env/mount are written mechanically by POST /api/etcd/keyspace; a launched
 *     Claude session (sandbox-etcd skill) then authors the real lease+put+keepalive
 *     loop in that service's app.py and flips implemented:true. Live worker rows
 *     (<service>-N → host:port) read from the REAL cluster via etcdctl, so an
 *     expired lease (dead worker) visibly drops off.
 *   - CONFIG — a generic named key/value keyspace /config/<name>/ (env vars,
 *     configs). Pure data: the app writes the values itself via etcdctl (persistent
 *     keys, no lease, no session); this tab is the editor. A value save goes to the
 *     cluster FIRST, so watchers get the change pushed before the list repaints.
 *   - Per keyspace (either type), add LISTENERS: services whose session-authored
 *     watch_prefix loop keeps a live in-memory view of the keyspace (etcd pushes
 *     updates — no polling).
 *
 * Mirrors ConsumerTab: pending badge while !implemented, Resume-able sessions,
 * delete drives a strip-the-code session when the loop was implemented.
 */

// Mirrors the server's identity rule (frontend/server/etcd.js ksIdentity):
// discovery keyspaces are keyed by their service, config keyspaces by their name.
const ksId = (k) => (k.type === 'config' ? k.name : k.service)
// Mirrors KEY_RE in frontend/server/etcd.js — friendly pre-check only.
const KEY_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/

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

export default function EtcdKeyspacesTab({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }) {
  const etcdId = node.id
  const [info, setInfo] = useState(null) // GET /api/etcd response; keyspaces carry workers when live
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  // Add-keyspace form.
  const [adding, setAdding] = useState(false)
  const [regType, setRegType] = useState('discovery')
  const [regService, setRegService] = useState('')
  const [regName, setRegName] = useState('')
  const [regDescription, setRegDescription] = useState('')
  const [seedRows, setSeedRows] = useState([]) // config seed values: [{ key, value }]
  // Per-keyspace "add listener" picker: keyspace identity -> selected listener service.
  const [listenerPick, setListenerPick] = useState({})
  const [confirmKey, setConfirmKey] = useState(null) // 'ks:<identity>' | 'ln:<identity>:<svc>' pending delete
  // Config value editor: '<keyspace>:<key>' -> draft value; per-keyspace add-row.
  const [valDrafts, setValDrafts] = useState({})
  const [newKV, setNewKV] = useState({})

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
        const prevWorkers = Object.fromEntries((prev.keyspaces || []).map((k) => [ksId(k), k.workers]))
        return {
          ...data,
          keyspaces: (data.keyspaces || []).map((k) => ({ ...k, workers: k.workers ?? prevWorkers[ksId(k)] ?? null })),
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
  // Keyspace identities are one shared namespace (discovery services + config names).
  const identities = new Set(keyspaces.map(ksId))
  const registrable = eligible.filter((s) => !identities.has(s))
  const leaseTtlSeconds = info.cluster?.leaseTtlSeconds

  function startAdd() {
    setRegType(registrable.length ? 'discovery' : 'config')
    setRegService(registrable[0] || '')
    setRegName('')
    setRegDescription('')
    setSeedRows([])
    setError(null)
    setAdding(true)
  }

  function changeType(next) {
    setRegType(next)
    setRegService(next === 'discovery' ? registrable[0] || '' : '')
    setRegName('')
    setRegDescription('')
    setSeedRows([])
    setError(null)
  }

  // Create a keyspace. Discovery: registry + compose edits, then launch the session
  // that authors the lease+keepalive loop (mirrors ConsumerTab.submit). Config: pure
  // data — registry entry + etcdctl puts of the seed values, no session.
  async function submitRegister() {
    if (regType === 'config') {
      const name = regName.trim()
      const nameErr = nodeNameError(name)
      if (nameErr) return setError(nameErr)
      if (identities.has(name)) return setError(`"${name}" is already a keyspace identity on this cluster`)
      const values = seedRows.filter((r) => r.key.trim() !== '' || r.value !== '')
      for (const r of values) {
        if (!KEY_RE.test(r.key.trim())) {
          return setError(`Invalid key "${r.key}" — a letter/digit then letters, digits, _ . - (no /)`)
        }
      }
      setBusy(true)
      setError(null)
      try {
        const res = await fetch('/api/etcd/keyspace', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            system: systemId, id: etcdId, type: 'config', name,
            description: regDescription,
            values: values.map((r) => ({ key: r.key.trim(), value: r.value })),
          }),
        })
        const data = await res.json().catch(() => ({}))
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
        setAdding(false)
        await load(true)
      } catch (err) {
        setError(err.message)
      } finally {
        setBusy(false)
      }
      return
    }

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
    const listener = listenerPick[ksId(ks)]
    if (!listener) return
    const conversationId = crypto.randomUUID()
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/listener', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ksId(ks), service: listener, conversationId }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onLaunch({
        sessionId: conversationId,
        mode: 'new',
        prompt: ks.type === 'config'
          ? buildConfigListenerPrompt({
              systemId, etcdId,
              keyspaceName: ks.name,
              listener,
              prefix: ks.prefix,
              description: data.listener.description,
              editing: false,
            })
          : buildListenerPrompt({
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
        body: JSON.stringify(ks.type === 'config'
          ? { system: systemId, id: etcdId, name: ks.name }
          : { system: systemId, id: etcdId, service: ks.service }),
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
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ksId(ks), service: l.service }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmKey(null)
      if (data.wasImplemented) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: ks.type === 'config'
            ? buildConfigListenerDeletePrompt({ systemId, etcdId, keyspaceName: ks.name, listener: l.service, prefix: ks.prefix })
            : buildListenerDeletePrompt({ systemId, etcdId, keyspaceService: ks.service, listener: l.service, prefix: ks.prefix }),
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

  // Config keyspace key/values. The backend writes etcd FIRST (watchers get the
  // change pushed), then the registry copy — so a thrown error means nothing moved.
  async function saveKeyValue(ks, key, value) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/keyvalue', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ks.name, key, value }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setValDrafts((d) => {
        const next = { ...d }
        delete next[`${ks.name}:${key}`]
        return next
      })
      setNewKV((p) => ({ ...p, [ks.name]: { key: '', value: '' } }))
      await load(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function removeKeyValue(ks, key) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/keyvalue', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ks.name, key }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load(true)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  const body = (
    <>
      <p className="sim-desc">
        <strong>Keyspaces</strong> on <code>{etcdId}</code>. <em>Discovery</em> keyspaces
        (<code>/services/&lt;service&gt;/</code>): each of the service's workers keeps a leased key alive
        (value <code>host:port</code>, TTL {leaseTtlSeconds ?? '…'}s). <em>Config</em> keyspaces
        (<code>/config/&lt;name&gt;/</code>): persistent key/values you edit right here — for env vars,
        settings, flags. Either way listeners watch the prefix and get every change pushed. Click a KEY
        row on the diagram node to trace the flow.
      </p>

      {/* ---- Keyspaces ---- */}
      {keyspaces.length === 0 ? (
        <p className="sim-desc">No keyspaces yet — add one below.</p>
      ) : (
        keyspaces.map((ks) => {
          const identity = ksId(ks)
          const isConfig = ks.type === 'config'
          const ksKey = `ks:${identity}`
          const listeners = ks.listeners || []
          const listenable = eligible.filter(
            (s) => s !== ks.service && !listeners.some((l) => l.service === s),
          )
          return (
            <div className="form-section" key={identity}>
              <div className="form-section-head">
                <span>
                  <code>{ks.prefix}</code>
                  {isConfig ? (
                    <span className="schema-badge" title="Generic key/value keyspace — values are edited here and pushed to watchers">CONFIG</span>
                  ) : !ks.implemented && (
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

              {isConfig ? (
                <>
                  {/* Key/value editor — the registry copy. Each save/delete goes through
                      etcd first, so watchers see the change before this list repaints. */}
                  {(ks.values || []).length === 0 ? (
                    <small className="form-hint">No keys yet — add one below.</small>
                  ) : (
                    (ks.values || []).map((v) => {
                      const dk = `${ks.name}:${v.key}`
                      const draft = valDrafts[dk]
                      const dirty = draft !== undefined && draft !== v.value
                      return (
                        <div className="entity-row" key={v.key}>
                          <span className="endpoint-list-method endpoint-list-method-etcd">KEY</span>
                          <code className="endpoint-alias">{v.key}</code>
                          <input
                            value={draft ?? v.value}
                            onChange={(e) => setValDrafts((d) => ({ ...d, [dk]: e.target.value }))}
                            disabled={busy}
                          />
                          {dirty && (
                            <button className="link" disabled={busy} onClick={() => saveKeyValue(ks, v.key, draft)}>Save</button>
                          )}
                          <button className="link-danger" disabled={busy} onClick={() => removeKeyValue(ks, v.key)}>Delete</button>
                        </div>
                      )
                    })
                  )}
                  {Array.isArray(ks.workers) && ks.workers.length < (ks.values || []).length && (
                    <small className="form-hint">
                      Live cluster shows {ks.workers.length}/{(ks.values || []).length} keys — re-save a value to re-put it.
                    </small>
                  )}
                  <div className="entity-row">
                    <input
                      placeholder="KEY"
                      value={newKV[ks.name]?.key || ''}
                      onChange={(e) => setNewKV((p) => ({ ...p, [ks.name]: { ...(p[ks.name] || { value: '' }), key: e.target.value } }))}
                      disabled={busy}
                    />
                    <input
                      placeholder="value"
                      value={newKV[ks.name]?.value || ''}
                      onChange={(e) => setNewKV((p) => ({ ...p, [ks.name]: { ...(p[ks.name] || { key: '' }), value: e.target.value } }))}
                      disabled={busy}
                    />
                    <button
                      type="button" className="link"
                      disabled={busy || !KEY_RE.test((newKV[ks.name]?.key || '').trim())}
                      onClick={() => saveKeyValue(ks, (newKV[ks.name]?.key || '').trim(), newKV[ks.name]?.value || '')}
                    >
                      Add
                    </button>
                  </div>
                </>
              ) : ks.workers === null ? (
                /* Live workers (host:port), straight from the real cluster. */
                <small className="form-hint">Probing live workers…</small>
              ) : ks.workers.length === 0 ? (
                <small className="form-hint">
                  No live workers under this prefix{ks.implemented ? ' — are the containers up?' : ' yet (loop pending)'}.
                </small>
              ) : (
                <ul className="endpoint-list">
                  {ks.workers.map((w) => (
                    <li key={w.id} className="endpoint-list-row">
                      <span className="endpoint-list-method endpoint-list-method-etcd">KEY</span>
                      <code className="endpoint-alias">{w.id}</code>
                      <span className="endpoint-list-path">→ {w.value}</span>
                    </li>
                  ))}
                </ul>
              )}

              {/* Listeners. */}
              <ul className="endpoint-list">
                {listeners.map((l) => {
                  const lnKey = `ln:${identity}:${l.service}`
                  return (
                    <li key={l.service} className="endpoint-list-row">
                      <span className="endpoint-list-method endpoint-list-method-watch">WATCH</span>
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
                    value={listenerPick[identity] || ''}
                    onChange={(e) => setListenerPick((p) => ({ ...p, [identity]: e.target.value }))}
                    disabled={busy}
                  >
                    <option value="">— add a listener —</option>
                    {listenable.map((s) => <option key={s} value={s}>{s}</option>)}
                  </select>
                  <button type="button" className="link" disabled={busy || !listenerPick[identity]}
                    onClick={() => addListener(ks)}>
                    Add listener
                  </button>
                </div>
              )}
            </div>
          )
        })
      )}

      {/* ---- Add a keyspace ---- */}
      {!adding ? (
        <div className="form-section">
          <button className="link" onClick={startAdd} disabled={busy}>
            ＋ Add keyspace
          </button>
        </div>
      ) : (
        <div className="form-section">
          <div className="form-section-head"><span>Add a keyspace</span></div>
          <label className="form-row">
            <span>Type</span>
            <select value={regType} onChange={(e) => changeType(e.target.value)} disabled={busy}>
              <option value="discovery">Service discovery</option>
              <option value="config">Config (key/values)</option>
            </select>
          </label>

          {regType === 'discovery' ? (
            <>
              <label className="form-row">
                <span>Service</span>
                <select value={regService} onChange={(e) => setRegService(e.target.value)} disabled={busy}>
                  {!regService && <option value="">— pick a service —</option>}
                  {registrable.map((s) => <option key={s} value={s}>{s}</option>)}
                </select>
              </label>
              {registrable.length === 0 && (
                <small className="form-hint">
                  {eligible.length === 0
                    ? 'Add an internal service first — only services can register for discovery.'
                    : 'Every eligible service is already registered.'}
                </small>
              )}
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
            </>
          ) : (
            <>
              <label className="form-row">
                <span>Name</span>
                <input
                  value={regName}
                  onChange={(e) => setRegName(e.target.value)}
                  placeholder="app-settings"
                  disabled={busy}
                  autoFocus
                />
              </label>
              <small className="form-hint">{(regName && nodeNameError(regName)) || NODE_NAME_HINT}</small>
              <label className="form-row form-row-stack">
                <span>Describe</span>
                <textarea
                  className="desc-input"
                  value={regDescription}
                  onChange={(e) => setRegDescription(e.target.value)}
                  placeholder="What do these key/values configure? (optional)"
                  rows={2}
                  disabled={busy}
                />
              </label>
              <div className="form-row form-row-stack">
                <span>Seed values (optional)</span>
                {seedRows.map((r, i) => (
                  <div className="entity-row" key={i}>
                    <input
                      placeholder="KEY"
                      value={r.key}
                      onChange={(e) => setSeedRows((rows) => rows.map((x, j) => (j === i ? { ...x, key: e.target.value } : x)))}
                      disabled={busy}
                    />
                    <input
                      placeholder="value"
                      value={r.value}
                      onChange={(e) => setSeedRows((rows) => rows.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))}
                      disabled={busy}
                    />
                    <button type="button" className="link-danger" disabled={busy}
                      onClick={() => setSeedRows((rows) => rows.filter((_, j) => j !== i))}>
                      ✕
                    </button>
                  </div>
                ))}
                <div>
                  <button type="button" className="link" disabled={busy}
                    onClick={() => setSeedRows((rows) => [...rows, { key: '', value: '' }])}>
                    ＋ value
                  </button>
                </div>
              </div>
              <p className="sim-desc">
                Creates <code>/config/{regName.trim() || '<name>'}/</code> — persistent key/values written
                straight to the cluster (no lease, no service code, no Claude session). Edit values here
                any time; listeners get every change pushed, and the app replays the values if the
                cluster is recreated.
              </p>
            </>
          )}
          <div className="modal-actions">
            <button type="button" onClick={() => { setAdding(false); setError(null) }} disabled={busy}>Cancel</button>
            <button
              type="button" className="primary" onClick={submitRegister}
              disabled={busy || (regType === 'discovery' ? !regService : !!nodeNameError(regName))}
            >
              {busy ? 'Working…' : regType === 'discovery' ? 'Register & open Claude' : 'Create keyspace'}
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
