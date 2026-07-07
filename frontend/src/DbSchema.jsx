import { useEffect, useState } from 'react'
import { referencedModels, buildDbSchemaPrompt } from './modelBank.js'

/**
 * Per-database popup. Shows the node's CURRENT schema (fetched live from the
 * running container via GET /api/db-schema) and — for an engine that supports it
 * (postgres / mongodb / redis) — lets you manage read replicas: add a real
 * read-only streaming secondary or remove one. The "entity"/"field" wording
 * adapts to the engine (table/column, collection/field, key namespace, bucket).
 *
 * Replicas are provisioned directly by the backend (POST /api/db-replicas),
 * mirroring "Add database"; the diagram's primary↔secondary arrow and dotted
 * cluster box are derived from each secondary's manifest `replicaOf`.
 */

const WORDS = {
  postgres: { entity: 'Table', empty: 'No tables yet.' },
  mongodb: { entity: 'Collection', empty: 'No collections yet.' },
  redis: { entity: 'Namespace', empty: 'No keys yet.' },
  'object-store': { entity: 'Bucket', empty: 'No buckets yet.' },
  dynamodb: { entity: 'Table', empty: 'No tables yet.' },
  cassandra: { entity: 'Table', empty: 'No tables yet.' },
}

// Engines that can stream to read replicas (object-store has no such concept;
// Cassandra "replicas" join the ring as a second node — see sandbox-database skill).
const REPLICA_ENGINES = ['postgres', 'mongodb', 'redis', 'cassandra']

// Engines whose schema can be (re)authored from the model bank.
const MODEL_ENGINES = ['postgres', 'mongodb', 'dynamodb', 'cassandra']

export default function DbSchema({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }) {
  const [state, setState] = useState({ status: 'loading' })
  const [mode, setMode] = useState('async')
  const [busy, setBusy] = useState(null) // 'add' | `del:<id>` | 'models' | null
  const [opError, setOpError] = useState(null)
  // "Schema from models" picker (postgres/mongodb only).
  const [models, setModels] = useState([]) // the system's model bank
  const [modelSel, setModelSel] = useState([]) // model names to apply this round

  const engine = node.type
  const isSecondary = !!node.replicaOf
  const modelCapable = MODEL_ENGINES.includes(engine) && !isSecondary
  // Models already applied to this database (additively merged on the node).
  const appliedModels = node.schemaModels || []

  useEffect(() => onBusyChange?.(!!busy), [busy, onBusyChange])

  useEffect(() => {
    let cancelled = false
    fetch(`/api/db-schema?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(node.id)}`)
      .then((r) => r.json())
      .then((d) => {
        if (cancelled) return
        if (!d.ok) setState({ status: 'error', error: d.error })
        else setState({ status: 'ok', type: d.type, entities: d.entities })
      })
      .catch((err) => !cancelled && setState({ status: 'error', error: err.message }))
    return () => {
      cancelled = true
    }
  }, [systemId, node.id])

  // Load the model bank for the "Schema from models" picker.
  useEffect(() => {
    if (!modelCapable) return
    let cancelled = false
    fetch(`/api/models?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json())
      .then((d) => !cancelled && setModels(Array.isArray(d.models) ? d.models : []))
      .catch(() => !cancelled && setModels([]))
    return () => {
      cancelled = true
    }
  }, [systemId, modelCapable])

  const words = WORDS[engine] || { entity: 'Entity', empty: 'Empty.' }
  const replicaCapable = REPLICA_ENGINES.includes(engine) && !isSecondary
  const supportsSync = engine === 'postgres'
  // The replica nodes that stream from this primary (re-read every poll).
  const secondaries = (manifest?.nodes || []).filter((n) => n.replicaOf === node.id)

  async function post(url, body, busyKey) {
    setBusy(busyKey)
    setOpError(null)
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
    } catch (err) {
      setOpError(err.message)
    } finally {
      setBusy(null)
    }
  }

  const addReplica = () => post('/api/db-replicas', { system: systemId, primary: node.id, mode }, 'add')
  const removeReplica = (id) => post('/api/delete', { system: systemId, id }, `del:${id}`)

  const toggleModel = (n) =>
    setModelSel((s) => (s.includes(n) ? s.filter((x) => x !== n) : [...s, n]))

  // Record the selected models on the db node, then launch a Claude session to apply them
  // additively (new tables/collections + FKs) to the live container via the skill.
  async function applyModels() {
    if (modelSel.length === 0) return
    setBusy('models')
    setOpError(null)
    try {
      const res = await fetch('/api/db-models', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: node.id, models: modelSel }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onLaunch?.({
        sessionId: crypto.randomUUID(),
        mode: 'new',
        prompt: buildDbSchemaPrompt({
          systemId,
          dbId: node.id,
          engine,
          models: modelSel,
          allModels: models,
          update: true,
        }),
      }, { kind: 'database', target: node.id, title: 'schema' })
      onClose()
    } catch (err) {
      setOpError(err.message)
      setBusy(null)
    }
  }

  const body = (
    <>
      {isSecondary && (
        <p className="replica-badge">
          {node.readonly === false ? (
            <>🔗 Cluster node of <code>{node.replicaOf}</code> · {node.replication || 'peer'} (accepts writes)</>
          ) : (
            <>🔒 Read-only replica of <code>{node.replicaOf}</code> · {node.replication || 'async'} streaming</>
          )}
        </p>
      )}

      {state.status === 'loading' && <p className="sim-desc">Reading live schema…</p>}
      {state.status === 'error' && <p className="modal-error">{state.error}</p>}

      {state.status === 'ok' &&
          (state.entities.length === 0 ? (
            <p className="sim-desc">{words.empty}</p>
          ) : (
            <div className="schema-list">
              {state.entities.map((ent) => (
                <div key={ent.name} className="schema-entity">
                  <div className="schema-entity-head">
                    <span className="schema-entity-kind">{words.entity}</span>
                    <span className="schema-entity-name">{ent.name}</span>
                  </div>
                  {ent.fields.length > 0 && (
                    <ul className="schema-fields">
                      {ent.fields.map((f) => (
                        <li key={f.name} className="schema-field">
                          <span className="schema-field-name">{f.name}</span>
                          {f.type && <span className="schema-field-type">{f.type}</span>}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              ))}
            </div>
          ))}

        {modelCapable && (
          <div className="form-section schema-models-panel">
            <div className="form-section-head">
              <span>Schema from models</span>
            </div>

            {node.schemaModels?.length > 0 && (
              <p className="sim-desc">Built from models: {node.schemaModels.join(', ')}</p>
            )}

            {models.length === 0 ? (
              <p className="sim-desc">No models in the bank yet — add some with ＋ Models.</p>
            ) : (
              <>
                <ul className="model-pick-list">
                  {models.map((m) => {
                    const isApplied = appliedModels.includes(m.name)
                    // Applied models show checked + locked (apply is additive — you can't
                    // un-apply here); modelSel tracks only NEW models to apply this round.
                    const on = isApplied || modelSel.includes(m.name)
                    const refs = referencedModels(m.name, models)
                    return (
                      <li key={m.name} className="model-pick-row">
                        <label className="model-pick-label">
                          <input
                            type="checkbox"
                            checked={on}
                            onChange={() => toggleModel(m.name)}
                            disabled={!!busy || isApplied}
                          />
                          <span className="model-pick-name">{m.name}</span>
                        </label>
                        {isApplied && <span className="model-pick-applied">applied</span>}
                        {refs.length > 0 && <span className="model-pick-refs">→ {refs.join(', ')}</span>}
                      </li>
                    )
                  })}
                </ul>
                <p className="form-hint">
                  Each becomes a new {words.entity.toLowerCase()} (foreign keys from references), applied
                  additively — existing {words.entity.toLowerCase()}s are kept. Claude does the apply.
                </p>
                <div className="replica-add">
                  <button
                    type="button"
                    className="primary"
                    onClick={applyModels}
                    disabled={!!busy || modelSel.length === 0}
                  >
                    {busy === 'models' ? 'Applying…' : 'Apply models'}
                  </button>
                </div>
              </>
            )}
          </div>
        )}

        {replicaCapable && (
          <div className="form-section replica-panel">
            <div className="form-section-head">
              <span>Read replicas</span>
            </div>

            {secondaries.length === 0 ? (
              <p className="sim-desc">No replicas. Add a read-only standby that streams from this primary.</p>
            ) : (
              <ul className="replica-list">
                {secondaries.map((s) => (
                  <li key={s.id} className="replica-row">
                    <code>{s.id}</code>
                    <span className="replica-mode">{s.replication || 'async'}</span>
                    <button
                      type="button"
                      className="link-danger"
                      onClick={() => removeReplica(s.id)}
                      disabled={!!busy}
                    >
                      {busy === `del:${s.id}` ? 'removing…' : 'remove'}
                    </button>
                  </li>
                ))}
              </ul>
            )}

            <div className="replica-add">
              {supportsSync ? (
                <select value={mode} onChange={(e) => setMode(e.target.value)} disabled={!!busy}>
                  <option value="async">async</option>
                  <option value="sync">sync</option>
                </select>
              ) : (
                <span className="replica-mode" title="mongo/redis replicas stream asynchronously">async</span>
              )}
              <button type="button" className="primary" onClick={addReplica} disabled={!!busy}>
                {busy === 'add' ? 'Provisioning… (can take a minute)' : '+ Add read replica'}
              </button>
            </div>
          </div>
        )}

      {opError && <p className="modal-error">{opError}</p>}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>
            {isSecondary ? 'Replica' : 'Database'} · <code>{node.id}</code>
          </h2>
          <button className="modal-close" onClick={onClose} disabled={!!busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
