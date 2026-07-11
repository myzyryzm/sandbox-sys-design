import { useCallback, useEffect, useState } from 'react'
import {
  REDIS_BADGE, REDIS_KS_TYPES, REDIS_KS_RE, REDIS_SHORTHAND_RE, redisTypesCompatible,
} from './redisKeyspaceMeta.js'

/**
 * A redis node's "Keyspaces" tab (embedded in NodeEditModal, for EVERY type:"redis"
 * node — create-database caches, an LLM worker's token stream, a websocket tier's
 * bus/presence). Manages the node's manifest `keyspaces` block via /api/redis:
 *
 *   - Each keyspace: a key name or prefix, its expected redis TYPE (the diagram
 *     badge: STR/LIST/SET/HSET/ZSET/STRM/GEO), an optional shorthand (what the
 *     diagram row displays and services reference), and the declared WRITERS /
 *     READERS the row's click-trace draws arrows for.
 *   - "Verify (scan live keys)" reads the RUNNING container (SCAN + TYPE): live
 *     namespaces nobody declared are added as `unverified` entries, declared-vs-live
 *     type drift is flagged, and a source grep suggests writers/readers to accept
 *     or dismiss. Needs the container up — and writers must have run for their keys
 *     to exist (a pub/sub bus legitimately scans empty: channels aren't keys).
 *   - An unverified entry's Verify button just flips the flag (no Claude session,
 *     no rebuild — every action here is a live registry edit).
 *   - Each declared WRITER carries a write mode: async (default) or WAIT — a
 *     pseudo-synchronous write that blocks until `numreplicas` replicas acknowledge
 *     or `timeoutMs` elapses. Only selectable when the topology has replicas (the
 *     Topology tab manages those); the scan greps the writer's source for an actual
 *     WAIT call and badges each wait writer implemented / not implemented.
 */

export default function RedisKeyspacesTab({ systemId, node, manifest, onClose, embedded = false, onBusyChange }) {
  const redisId = node.id
  const [keyspaces, setKeyspaces] = useState(null)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const [scanning, setScanning] = useState(false)
  const [report, setReport] = useState(null)

  // Add/edit form. `editing` is the entry's current name (the upsert's prevName).
  const [formOpen, setFormOpen] = useState(false)
  const [editing, setEditing] = useState(null)
  const [fName, setFName] = useState('')
  const [fMatch, setFMatch] = useState('prefix')
  const [fType, setFType] = useState('string')
  const [fShorthand, setFShorthand] = useState('')

  // Per-keyspace "add a writer/reader" picks: `${name}:writer` -> service id.
  const [rolePick, setRolePick] = useState({})
  const [confirmKey, setConfirmKey] = useState(null) // keyspace name pending delete
  // In-progress WAIT param edits, keyed `${ks.name}:${writer}` (committed on blur).
  const [waitEdit, setWaitEdit] = useState({})

  useEffect(() => onBusyChange?.(busy || scanning), [busy, scanning, onBusyChange])

  // Mirrors the backend's isCodeBearing: nodes that carry real code and can be
  // declared as a keyspace's writers/readers.
  const eligible = (manifest?.nodes || [])
    .filter((n) => (n.type === 'service' && !n.instanceOf) || n.type === 'service-lb' || n.type === 'websocket-server')
    .map((n) => n.id)
  const nodeExists = (id) => (manifest?.nodes || []).some((n) => n.id === id)

  // How many replicas could acknowledge a WAIT: the replicaOf secondaries in
  // replicated mode, or replicas-per-shard in cluster mode (WAIT on a shard master
  // acks its local replicas). 0 = WAIT would always time out, so it's not offered.
  const replicaCount =
    (manifest?.nodes || []).filter((n) => n.replicaOf === node.id).length ||
    (node.redisCluster?.replicasPerShard ?? 0)

  const load = useCallback(async () => {
    try {
      const res = await fetch(
        `/api/redis/keyspaces?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(redisId)}`,
      )
      const data = await res.json()
      if (!data.ok) throw new Error(data.error || 'failed to load')
      setKeyspaces(data.keyspaces || [])
    } catch (err) {
      setError(err.message)
    }
  }, [systemId, redisId])

  useEffect(() => {
    load()
    const t = setInterval(() => { if (!document.hidden) load() }, 5000)
    return () => clearInterval(t)
  }, [load])

  if (!keyspaces) {
    return <p className="sim-desc">{error ? `Error: ${error}` : 'Loading…'}</p>
  }

  async function call(method, path, body) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch(path, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      await load()
      return data
    } catch (err) {
      setError(err.message)
      return null
    } finally {
      setBusy(false)
    }
  }

  function startAdd() {
    setEditing(null)
    setFName('')
    setFMatch('prefix')
    setFType('string')
    setFShorthand('')
    setError(null)
    setFormOpen(true)
  }

  function startEdit(ks) {
    setEditing(ks.name)
    setFName(ks.name)
    setFMatch(ks.match)
    setFType(ks.type)
    setFShorthand(ks.shorthand || '')
    setError(null)
    setFormOpen(true)
  }

  function formError() {
    const name = fName.trim()
    if (!REDIS_KS_RE.test(name)) {
      return 'Name: 1-128 chars of letters, digits, _ . : - (start with a letter or digit)'
    }
    if (keyspaces.some((k) => k.name === name && k.name !== editing)) {
      return `A keyspace named "${name}" already exists`
    }
    const sh = fShorthand.trim()
    if (sh && !REDIS_SHORTHAND_RE.test(sh)) {
      return 'Shorthand: 1-32 chars of letters, digits, _ - (start with a letter)'
    }
    if (sh && keyspaces.some((k) => k.shorthand === sh && k.name !== editing)) {
      return `Shorthand "${sh}" is already used`
    }
    return null
  }

  async function submitForm() {
    const err = formError()
    if (err) return setError(err)
    const prev = editing ? keyspaces.find((k) => k.name === editing) : null
    const data = await call('POST', '/api/redis/keyspace', {
      system: systemId,
      id: redisId,
      ...(editing ? { prevName: editing } : {}),
      keyspace: {
        name: fName.trim(),
        match: fMatch,
        type: fType,
        shorthand: fShorthand.trim(),
        writers: prev?.writers || [],
        readers: prev?.readers || [],
        writeModes: prev?.writeModes || {},
      },
    })
    if (data) setFormOpen(false)
  }

  // Add/remove a declared writer/reader: re-upsert the entry with the role list
  // changed (the backend preserves verified/origin/suggestions on edit, and drops
  // writeModes keys whose writer is no longer declared).
  async function setRole(ks, role, ids) {
    await call('POST', '/api/redis/keyspace', {
      system: systemId,
      id: redisId,
      prevName: ks.name,
      keyspace: {
        name: ks.name,
        match: ks.match,
        type: ks.type,
        shorthand: ks.shorthand || '',
        writers: role === 'writers' ? ids : ks.writers || [],
        readers: role === 'readers' ? ids : ks.readers || [],
        writeModes: ks.writeModes || {},
      },
    })
  }

  // Set one writer's write mode: `wm` is { mode:'wait', numreplicas, timeoutMs } or
  // null for async (the unstored default) — a full-map re-upsert like setRole.
  async function setWriteMode(ks, svc, wm) {
    const writeModes = { ...(ks.writeModes || {}) }
    if (wm) writeModes[svc] = wm
    else delete writeModes[svc]
    await call('POST', '/api/redis/keyspace', {
      system: systemId,
      id: redisId,
      prevName: ks.name,
      keyspace: {
        name: ks.name,
        match: ks.match,
        type: ks.type,
        shorthand: ks.shorthand || '',
        writers: ks.writers || [],
        readers: ks.readers || [],
        writeModes,
      },
    })
  }

  // Commit an in-progress WAIT param edit (on blur): only POSTs a valid change.
  function commitWaitEdit(ks, svc, wm) {
    const editKey = `${ks.name}:${svc}`
    const draft = waitEdit[editKey]
    if (!draft) return
    setWaitEdit((p) => {
      const next = { ...p }
      delete next[editKey]
      return next
    })
    const numreplicas = Number(draft.numreplicas ?? wm.numreplicas)
    const timeoutMs = Number(draft.timeoutMs ?? wm.timeoutMs)
    if (!Number.isInteger(numreplicas) || numreplicas < 1 || numreplicas > 9) return setError('WAIT numreplicas must be 1-9')
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 60000) return setError('WAIT timeout must be 0-60000 ms')
    if (numreplicas === wm.numreplicas && timeoutMs === wm.timeoutMs) return
    setWriteMode(ks, svc, { mode: 'wait', numreplicas, timeoutMs })
  }

  async function runScan() {
    setScanning(true)
    setError(null)
    setReport(null)
    try {
      const res = await fetch('/api/redis/scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: redisId }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setReport(data.report)
      setKeyspaces(data.keyspaces || [])
    } catch (err) {
      setError(err.message)
    } finally {
      setScanning(false)
    }
  }

  // One declared writer/reader list + its suggestions + the add-dropdown.
  function roleRows(ks, role) {
    const singular = role === 'writers' ? 'writer' : 'reader'
    const suggestedKey = role === 'writers' ? 'suggestedWriters' : 'suggestedReaders'
    const declared = ks[role] || []
    const suggested = ks[suggestedKey] || []
    const pickKey = `${ks.name}:${singular}`
    const addable = eligible.filter((s) => !declared.includes(s) && !suggested.includes(s))
    const badgeClass = role === 'writers'
      ? 'endpoint-list-method endpoint-list-method-redis'
      : 'endpoint-list-method endpoint-list-method-watch'
    return (
      <>
        <ul className="endpoint-list">
          {declared.map((svc) => {
            const wm = role === 'writers' ? (ks.writeModes || {})[svc] : null
            const editKey = `${ks.name}:${svc}`
            const draft = waitEdit[editKey]
            return (
            <li key={svc} className="endpoint-list-row">
              <span className={badgeClass}>{singular.toUpperCase()}</span>
              <code className="endpoint-alias">
                {nodeExists(svc) ? svc : <s title="This node no longer exists">{svc}</s>}
              </code>
              {role === 'writers' && (
                <span className="endpoint-list-actions">
                  <select
                    value={wm ? 'wait' : 'async'}
                    disabled={busy || (!wm && replicaCount === 0)}
                    title={
                      replicaCount === 0 && !wm
                        ? 'WAIT is meaningless with 0 replicas — add replicas in the Topology tab first'
                        : 'How this writer’s writes are acknowledged: async (fire and forget) or WAIT (block until N replicas ack)'
                    }
                    onChange={(e) =>
                      setWriteMode(
                        ks, svc,
                        e.target.value === 'wait'
                          ? { mode: 'wait', numreplicas: Math.max(1, Math.min(replicaCount, 9)) || 1, timeoutMs: 1000 }
                          : null,
                      )
                    }
                  >
                    <option value="async">async</option>
                    <option value="wait">WAIT</option>
                  </select>
                  {wm && (
                    <>
                      <label title="numreplicas — how many replicas must acknowledge the write">
                        n=
                        <input
                          type="number" min={1} max={9} style={{ width: 40 }} disabled={busy}
                          value={draft?.numreplicas ?? wm.numreplicas}
                          onChange={(e) => setWaitEdit((p) => ({ ...p, [editKey]: { ...(p[editKey] || {}), numreplicas: e.target.value } }))}
                          onBlur={() => commitWaitEdit(ks, svc, wm)}
                        />
                      </label>
                      <label title="timeout (ms) — how long the write blocks waiting for acks; 0 = forever">
                        t=
                        <input
                          type="number" min={0} max={60000} step={100} style={{ width: 62 }} disabled={busy}
                          value={draft?.timeoutMs ?? wm.timeoutMs}
                          onChange={(e) => setWaitEdit((p) => ({ ...p, [editKey]: { ...(p[editKey] || {}), timeoutMs: e.target.value } }))}
                          onBlur={() => commitWaitEdit(ks, svc, wm)}
                        />
                      </label>
                      <span
                        className={wm.implemented ? 'llm-implemented' : 'scenario-pending'}
                        title={wm.implemented
                          ? 'The last scan found a WAIT call in this writer’s source'
                          : 'No WAIT call found in this writer’s source yet — the retrofit session (or the next endpoint edit) wires it; re-scan to re-check'}
                      >
                        {wm.implemented ? 'wait ✓' : 'wait ✗'}
                      </span>
                    </>
                  )}
                </span>
              )}
              <span className="endpoint-list-actions">
                <button
                  className="link-danger" disabled={busy}
                  onClick={() => setRole(ks, role, declared.filter((s) => s !== svc))}
                >
                  Remove
                </button>
              </span>
            </li>
            )
          })}
          {suggested.map((svc) => (
            <li key={`sug-${svc}`} className="endpoint-list-row">
              <span className={badgeClass}>{singular.toUpperCase()}</span>
              <code className="endpoint-alias">{svc}</code>
              <span className="scenario-pending" title={`The scan found ${svc} ${role === 'writers' ? 'writing' : 'reading'} this keyspace in its source`}>
                suggested
              </span>
              <span className="endpoint-list-actions">
                <button
                  className="link" disabled={busy} title={`Accept ${svc} as a ${singular}`}
                  onClick={() => call('POST', '/api/redis/keyspace/suggestion', {
                    system: systemId, id: redisId, name: ks.name, service: svc, role: singular, action: 'accept',
                  })}
                >
                  ✓
                </button>
                <button
                  className="link-danger" disabled={busy} title="Dismiss this suggestion"
                  onClick={() => call('POST', '/api/redis/keyspace/suggestion', {
                    system: systemId, id: redisId, name: ks.name, service: svc, role: singular, action: 'dismiss',
                  })}
                >
                  ✕
                </button>
              </span>
            </li>
          ))}
        </ul>
        {addable.length > 0 && (
          <div className="entity-row">
            <select
              value={rolePick[pickKey] || ''}
              onChange={(e) => setRolePick((p) => ({ ...p, [pickKey]: e.target.value }))}
              disabled={busy}
            >
              <option value="">— add a {singular} —</option>
              {addable.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
            <button
              type="button" className="link" disabled={busy || !rolePick[pickKey]}
              onClick={() => {
                const svc = rolePick[pickKey]
                setRolePick((p) => ({ ...p, [pickKey]: '' }))
                setRole(ks, role, [...declared, svc])
              }}
            >
              Add {singular}
            </button>
          </div>
        )}
      </>
    )
  }

  const body = (
    <>
      <p className="sim-desc">
        <strong>Keyspaces</strong> on <code>{redisId}</code>: each names a key
        (<em>exact</em>, e.g. <code>matchmaking_pool</code>) or a key prefix (<em>prefix</em>,
        e.g. <code>match:</code>) with its expected redis type. The diagram shows one typed row per
        keyspace (the shorthand when set); clicking it traces each declared writer → redis and
        redis → each reader. <em>Verify</em> scans the live container — undeclared namespaces are
        added as <em>unverified</em> entries (verify each to confirm it), type drift is flagged,
        and a source grep suggests writers/readers. Pub/sub channels never appear in a scan.
      </p>

      {/* ---- Verify (live scan) ---- */}
      <div className="form-section">
        <div className="form-section-head">
          <span>Verify against the live container</span>
          <button className="link" onClick={runScan} disabled={busy || scanning}>
            {scanning ? 'Scanning…' : 'Verify (scan live keys)'}
          </button>
        </div>
        {report && (
          <>
            <small className="form-hint">Scanned {report.scannedKeys} live key{report.scannedKeys === 1 ? '' : 's'}.</small>
            {report.matched.map((m) => (
              <small className="form-hint" key={`m-${m.name}`}>
                <code>{m.name}</code> — {m.keyCount} key{m.keyCount === 1 ? '' : 's'}, live {m.observedType}
                {m.mismatch ? ' — TYPE MISMATCH with the declared type' : ''}
              </small>
            ))}
            {report.unseen.length > 0 && (
              <small className="form-hint">Declared but not seen live: {report.unseen.join(', ')}</small>
            )}
            {report.added.length > 0 && (
              <small className="form-hint">
                Added {report.added.length} unverified keyspace{report.added.length === 1 ? '' : 's'}:{' '}
                {report.added.map((a) => `${a.name} (${a.type}, ${a.keyCount} keys)`).join(', ')}
              </small>
            )}
            {report.suggestions.map((s) => (
              <small className="form-hint" key={`s-${s.name}`}>
                <code>{s.name}</code> suggestions —
                {s.suggestedWriters.length ? ` writers: ${s.suggestedWriters.join(', ')}` : ''}
                {s.suggestedReaders.length ? ` readers: ${s.suggestedReaders.join(', ')}` : ''}
              </small>
            ))}
            {(report.waitChecks || []).map((w) => (
              <small className="form-hint" key={`w-${w.name}-${w.writer}`}>
                <code>{w.name}</code> writer {w.writer}: WAIT {w.implemented ? 'call detected in source ✓' : 'call NOT found in source ✗'}
              </small>
            ))}
            {report.notes.map((n, i) => (
              <small className="form-hint" key={`n-${i}`}>{n}</small>
            ))}
          </>
        )}
      </div>

      {/* ---- Keyspaces ---- */}
      {keyspaces.length === 0 ? (
        <p className="sim-desc">No keyspaces yet — add one below, or Verify to discover live ones.</p>
      ) : (
        keyspaces.map((ks) => {
          const mismatch = ks.observedType && !redisTypesCompatible(ks.type, ks.observedType)
          return (
            <div className="form-section" key={ks.name}>
              <div className="form-section-head">
                <span>
                  <span className="endpoint-list-method endpoint-list-method-redis">{REDIS_BADGE[ks.type]}</span>{' '}
                  <code>{ks.name}{ks.match === 'prefix' ? '*' : ''}</code>
                  {ks.shorthand && <code className="endpoint-alias" title="Shorthand — what the diagram row displays and services reference"> {ks.shorthand}</code>}
                  {ks.verified === false && (
                    <span className="scenario-pending" title="Discovered by the scan — click Verify to confirm it"> unverified</span>
                  )}
                  {mismatch && (
                    <span className="scenario-pending" title={`Last scan saw ${ks.observedType} keys here, but the declared type is ${ks.type}`}>
                      {' '}live {ks.observedType}
                    </span>
                  )}
                </span>
                {confirmKey === ks.name ? (
                  <span className="endpoint-list-actions">
                    <span className="endpoint-confirm">Delete?</span>
                    <button
                      className="link" disabled={busy}
                      onClick={async () => {
                        setConfirmKey(null)
                        await call('DELETE', '/api/redis/keyspace', { system: systemId, id: redisId, name: ks.name })
                      }}
                    >
                      Yes
                    </button>
                    <button className="link" disabled={busy} onClick={() => setConfirmKey(null)}>No</button>
                  </span>
                ) : (
                  <span className="endpoint-list-actions">
                    {ks.verified === false && (
                      <button
                        className="link" disabled={busy}
                        title="Confirm this keyspace (just clears the unverified flag)"
                        onClick={() => call('POST', '/api/redis/keyspace/verify', { system: systemId, id: redisId, name: ks.name })}
                      >
                        Verify
                      </button>
                    )}
                    <button className="link" disabled={busy} onClick={() => startEdit(ks)}>Edit</button>
                    <button className="link-danger" disabled={busy} onClick={() => setConfirmKey(ks.name)}>Delete</button>
                  </span>
                )}
              </div>
              {roleRows(ks, 'writers')}
              {roleRows(ks, 'readers')}
              {replicaCount === 0 && Object.values(ks.writeModes || {}).some((wm) => wm.mode === 'wait') && (
                <small className="form-hint" style={{ color: '#d8a657' }}>
                  ⚠ WAIT writer(s) declared but the topology has no replicas — every WAIT will time out.
                  Add replicas in the Topology tab (the settings are kept meanwhile).
                </small>
              )}
            </div>
          )
        })
      )}

      {/* ---- Add / edit a keyspace ---- */}
      {!formOpen ? (
        <div className="form-section">
          <button className="link" onClick={startAdd} disabled={busy}>
            ＋ Add keyspace
          </button>
        </div>
      ) : (
        <div className="form-section">
          <div className="form-section-head">
            <span>{editing ? <>Edit <code>{editing}</code></> : 'Add a keyspace'}</span>
          </div>
          <label className="form-row">
            <span>Name</span>
            <input
              value={fName}
              onChange={(e) => setFName(e.target.value)}
              placeholder="match:  ·  matchmaking_pool"
              disabled={busy}
              autoFocus
            />
          </label>
          <label className="form-row">
            <span>Match</span>
            <select value={fMatch} onChange={(e) => setFMatch(e.target.value)} disabled={busy}>
              <option value="prefix">prefix — every key starting with the name</option>
              <option value="exact">exact — one key, the name itself</option>
            </select>
          </label>
          <label className="form-row">
            <span>Type</span>
            <select value={fType} onChange={(e) => setFType(e.target.value)} disabled={busy}>
              {REDIS_KS_TYPES.map((t) => (
                <option key={t} value={t}>{t} ({REDIS_BADGE[t]})</option>
              ))}
            </select>
          </label>
          <label className="form-row">
            <span>Shorthand</span>
            <input
              value={fShorthand}
              onChange={(e) => setFShorthand(e.target.value)}
              placeholder="optional — shown on the diagram instead of the key name"
              disabled={busy}
            />
          </label>
          <small className="form-hint">
            End a prefix name with <code>:</code> by convention (e.g. <code>tokens:</code>).
          </small>
          <div className="modal-actions">
            <button type="button" onClick={() => { setFormOpen(false); setError(null) }} disabled={busy}>Cancel</button>
            <button type="button" className="primary" onClick={submitForm} disabled={busy || !fName.trim()}>
              {busy ? 'Working…' : editing ? 'Save' : 'Add keyspace'}
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
          <h2>Keyspaces · <code>{redisId}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
