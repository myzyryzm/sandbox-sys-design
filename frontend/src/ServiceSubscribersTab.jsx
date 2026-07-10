import { useCallback, useEffect, useState } from 'react'
import {
  buildListenerPrompt,
  buildListenerDeletePrompt,
  buildConfigListenerPrompt,
  buildConfigListenerDeletePrompt,
} from './etcdListenerPrompts.js'

/**
 * A SERVICE's "Subscribers" tab (embedded in NodeEditModal). The service-first mirror of
 * the etcd node's Keyspaces tab (EtcdKeyspacesTab, which is keyspace-first): it lists the
 * etcd keyspaces THIS service watches — its SUB rows on the diagram — and lets the user
 * subscribe it to another keyspace from a free-form description, which launches a Claude
 * session (sandbox-etcd skill) that authors the watch_prefix loop and flips implemented:true.
 *
 * Mechanically identical to EtcdKeyspacesTab.addListener/removeListener/resume (same
 * POST/DELETE /api/etcd/listener + shared prompt builders), pre-scoped to service = node.id.
 */

// Mirrors the server's identity rule (frontend/server/etcd.js ksIdentity):
// discovery keyspaces are keyed by their service, config keyspaces by their name.
const ksId = (k) => (k.type === 'config' ? k.name : k.service)
// Matches SystemDiagram's SUB-row label: the KEY it watches, camelCased + `on`-prefixed
// (`llm-worker` → `onLlmWorker`, `app-settings` → `onAppSettings`).
const camelName = (id) => id.replace(/-+([a-z0-9])/g, (_, c) => c.toUpperCase())
const onName = (id) => {
  const c = camelName(id)
  return 'on' + c.charAt(0).toUpperCase() + c.slice(1)
}

export default function ServiceSubscribersTab({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }) {
  const service = node.id
  const etcdNode = (manifest?.nodes || []).find((n) => n.type === 'etcd')
  const etcdId = etcdNode?.id || null

  const [keyspaces, setKeyspaces] = useState(null) // null = loading
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [confirmKey, setConfirmKey] = useState(null) // 'sub:<identity>' pending delete
  const [pick, setPick] = useState('') // identity of the keyspace to subscribe to
  const [desc, setDesc] = useState('')
  const [editKey, setEditKey] = useState(null) // identity of the subscriber row being edited
  const [editDesc, setEditDesc] = useState('')

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const load = useCallback(async () => {
    if (!etcdId) return
    try {
      const res = await fetch(
        `/api/etcd?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(etcdId)}&live=0`,
      )
      const data = await res.json()
      if (!data.ok) throw new Error(data.error || 'failed to load')
      setKeyspaces(data.keyspaces || [])
    } catch (err) {
      setError(err.message)
    }
  }, [systemId, etcdId])

  useEffect(() => {
    if (!etcdId) return undefined
    let cancelled = false
    load()
    const t = setInterval(() => { if (!document.hidden) load() }, 5000)
    return () => {
      cancelled = true
      clearInterval(t)
    }
  }, [load, etcdId])

  // Subscribe this service to a keyspace and launch the watch-loop session.
  async function addSubscription() {
    const ks = (keyspaces || []).find((k) => ksId(k) === pick)
    if (!ks) return
    const conversationId = crypto.randomUUID()
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/listener', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ksId(ks), service, description: desc, conversationId }),
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
              listener: service,
              prefix: ks.prefix,
              description: data.listener.description,
              editing: false,
            })
          : buildListenerPrompt({
              systemId, etcdId,
              keyspaceService: ks.service,
              listener: service,
              prefix: ks.prefix,
              description: data.listener.description,
              editing: false,
            }),
      }, { kind: 'etcd', target: service, title: `listen ${ks.prefix}` })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  function startEdit(ks, l) {
    setConfirmKey(null)
    setEditKey(ksId(ks))
    setEditDesc(l.description || '')
  }

  // Re-author an existing subscriber: POST the new description with a FRESH conversationId,
  // then launch a mode:'new' editing session (resume can't inject a new prompt — terminal.js
  // runs --resume with no prompt). The upsert stores the fresh id, so the row's Resume then
  // resumes THIS edit session on the next load.
  async function saveEdit(ks, l) {
    const conversationId = crypto.randomUUID()
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/listener', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ksId(ks), service, description: editDesc, conversationId }),
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
              listener: service,
              prefix: ks.prefix,
              description: data.listener.description,
              editing: true,
              priorDescription: l.description,
            })
          : buildListenerPrompt({
              systemId, etcdId,
              keyspaceService: ks.service,
              listener: service,
              prefix: ks.prefix,
              description: data.listener.description,
              editing: true,
              priorDescription: l.description,
            }),
      }, { kind: 'etcd', target: service, title: `edit ${ks.prefix}` })
      onClose()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  async function removeSubscription(ks, l) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/etcd/listener', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: etcdId, keyspace: ksId(ks), service }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmKey(null)
      if (data.wasImplemented) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: ks.type === 'config'
            ? buildConfigListenerDeletePrompt({ systemId, etcdId, keyspaceName: ks.name, listener: service, prefix: ks.prefix })
            : buildListenerDeletePrompt({ systemId, etcdId, keyspaceService: ks.service, listener: service, prefix: ks.prefix }),
        }, { kind: 'etcd', target: service, title: `unlisten ${ks.prefix}` })
        onClose()
        return
      }
      await load()
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

  // The keyspaces this service already watches, paired with its own listener entry.
  const subscribed = (keyspaces || [])
    .map((ks) => ({ ks, l: (ks.listeners || []).find((l) => l.service === service) }))
    .filter((s) => s.l)
  // Keyspaces it could subscribe to: not already watched, and not its OWN discovery
  // keyspace (the backend forbids a service watching the prefix it registers under).
  const available = (keyspaces || []).filter(
    (ks) => ks.service !== service && !(ks.listeners || []).some((l) => l.service === service),
  )

  let body
  if (!etcdId) {
    body = <p className="sim-desc">This system has no etcd cluster — add one to subscribe services to keyspaces.</p>
  } else if (keyspaces === null) {
    body = <p className="sim-desc">{error ? `Error: ${error}` : 'Loading…'}</p>
  } else {
    body = (
      <>
        <p className="sim-desc">
          <strong>Subscriptions</strong> of <code>{service}</code> on <code>{etcdId}</code> — the etcd
          keyspaces this service watches. Each runs a <code>watch_prefix</code> loop that keeps a live
          in-memory view (etcd pushes every change — no polling) and runs a per-event handler you
          describe on every change, shown as a <strong>SUB</strong> row on the service node. Click one
          on the diagram to trace source&nbsp;→&nbsp;etcd&nbsp;→&nbsp;{service}.
        </p>

        {/* ---- Current subscriptions ---- */}
        {subscribed.length === 0 ? (
          <p className="sim-desc">Not subscribed to any keyspace yet — add one below.</p>
        ) : (
          <ul className="endpoint-list">
            {subscribed.map(({ ks, l }) => {
              const identity = ksId(ks)
              const subKey = `sub:${identity}`
              return (
                <li key={identity} className="endpoint-list-row"
                  style={editKey === identity ? { flexWrap: 'wrap' } : undefined}>
                  <span className="endpoint-list-method endpoint-list-method-etcd">SUB</span>
                  <code className="endpoint-alias">{onName(identity)}</code>
                  <span className="endpoint-list-path" title={ks.type === 'config' ? 'config keyspace' : 'discovery keyspace'}>
                    {ks.prefix}
                  </span>
                  {!l.implemented && (
                    <span className="scenario-pending" title="Watch loop not implemented yet — resume the Claude session">pending</span>
                  )}
                  {confirmKey === subKey ? (
                    <span className="endpoint-list-actions">
                      <span className="endpoint-confirm">{l.implemented ? 'Delete & rebuild?' : 'Delete?'}</span>
                      <button className="link" disabled={busy} onClick={() => removeSubscription(ks, l)}>Yes</button>
                      <button className="link" disabled={busy} onClick={() => setConfirmKey(null)}>No</button>
                    </span>
                  ) : (
                    <span className="endpoint-list-actions">
                      <button className="link" disabled={busy} title="Edit this subscriber's description and re-author its handler"
                        onClick={() => startEdit(ks, l)}>Edit</button>
                      {l.conversationId && (
                        <button className="link" disabled={busy} title="Resume this subscription's Claude session"
                          onClick={() => resume(l.conversationId)}>Resume</button>
                      )}
                      <button className="link-danger" disabled={busy} onClick={() => setConfirmKey(subKey)}>Remove</button>
                    </span>
                  )}
                  {editKey === identity && (
                    <div style={{ flexBasis: '100%', width: '100%', marginTop: 6 }}>
                      <textarea
                        className="desc-input"
                        style={{ width: '100%', boxSizing: 'border-box' }}
                        value={editDesc}
                        onChange={(e) => setEditDesc(e.target.value)}
                        placeholder="What should this subscriber's handler do on each etcd event (put/delete)? The live map is always kept — leave blank for map-only."
                        rows={3}
                        disabled={busy}
                        autoFocus
                      />
                      <div className="modal-actions">
                        <button type="button" onClick={() => setEditKey(null)} disabled={busy}>Cancel</button>
                        <button type="button" className="primary" onClick={() => saveEdit(ks, l)} disabled={busy}>
                          {busy ? 'Working…' : 'Save & open Claude'}
                        </button>
                      </div>
                    </div>
                  )}
                </li>
              )
            })}
          </ul>
        )}

        {/* ---- Add a subscription ---- */}
        <div className="form-section">
          <div className="form-section-head">
            <span>Subscribe to a keyspace</span>
          </div>
          {available.length === 0 ? (
            <small className="form-hint">No other keyspaces to subscribe to — add one in the etcd node's Keyspaces tab first.</small>
          ) : (
            <>
              <label className="form-row form-row-stack">
                <span>Keyspace</span>
                <select value={pick} onChange={(e) => setPick(e.target.value)} disabled={busy}>
                  <option value="">— pick a keyspace —</option>
                  {available.map((ks) => {
                    const identity = ksId(ks)
                    return (
                      <option key={identity} value={identity}>
                        {onName(identity)} · {ks.prefix} {ks.type === 'config' ? '(config)' : '(discovery)'}
                      </option>
                    )
                  })}
                </select>
              </label>
              <label className="form-row form-row-stack">
                <span>Describe</span>
                <textarea
                  className="desc-input"
                  value={desc}
                  onChange={(e) => setDesc(e.target.value)}
                  placeholder="What should this subscriber's handler do on each etcd event (put/delete)? The live map is always kept — leave blank for map-only."
                  rows={3}
                  disabled={busy}
                />
              </label>
              <div className="modal-actions">
                <button type="button" className="primary" onClick={addSubscription} disabled={busy || !pick}>
                  {busy ? 'Working…' : 'Subscribe & open Claude'}
                </button>
              </div>
            </>
          )}
        </div>

        {error && <p className="modal-error">{error}</p>}

        {!embedded && (
          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={busy}>Close</button>
          </div>
        )}
      </>
    )
  }

  if (embedded) return body
  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Subscribers · <code>{service}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
