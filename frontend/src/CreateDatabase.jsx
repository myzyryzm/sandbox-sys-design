import { useEffect, useState } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName.js'
import { referencedModels, buildDbSchemaPrompt } from './modelBank.js'
import { REDIS_KS_TYPES, REDIS_BADGE } from './redisKeyspaceMeta.js'

/**
 * Modal form for "Add database". Lets the user pick a database engine and
 * declare its entities (with fields for SQL/NoSQL), then POSTs to
 * /api/databases (see frontend/server/databases.js), which provisions a real
 * container + exporter, scrapes it, and adds a node to the live diagram.
 */

const TYPE_META = {
  postgres: {
    label: 'PostgreSQL (SQL)',
    entityWord: 'Table',
    hasFields: true,
    fieldTypes: ['text', 'varchar', 'integer', 'bigint', 'numeric', 'boolean', 'timestamp', 'timestamptz', 'date', 'uuid', 'jsonb', 'serial', 'bigserial'],
    defaultName: 'app-db',
  },
  mongodb: {
    label: 'MongoDB (NoSQL)',
    entityWord: 'Collection',
    hasFields: true,
    fieldTypes: ['string', 'number', 'boolean', 'date', 'objectId', 'object', 'array'],
    defaultName: 'app-db',
  },
  redis: {
    label: 'Redis (key-value)',
    entityWord: 'Key namespace',
    hasFields: false,
    // Redis entities are KEYSPACES: name + prefix/exact match + expected redis type +
    // optional shorthand. They persist onto the manifest node as its `keyspaces` block.
    isRedis: true,
    defaultName: 'app-cache',
  },
  blob: {
    label: 'Blob storage (simulated S3)',
    entityWord: 'Bucket',
    hasFields: false,
    defaultName: 'app-blob',
  },
  dynamodb: {
    label: 'DynamoDB (NoSQL key-value)',
    entityWord: 'Table',
    hasFields: false,
    defaultName: 'app-ddb',
  },
  cassandra: {
    label: 'Cassandra (wide-column)',
    entityWord: 'Table',
    hasFields: false,
    defaultName: 'app-cass',
  },
}

// Engines whose schema can be authored from the model bank (a launched Claude session
// turns selected models into tables/collections). Others use manual entities only.
const MODEL_ENGINES = ['postgres', 'mongodb', 'dynamodb', 'cassandra']

function blankEntity(meta) {
  if (meta.isRedis) return { name: '', match: 'prefix', ksType: 'string', shorthand: '', fields: [] }
  return { name: '', fields: meta.hasFields ? [{ name: '', type: meta.fieldTypes[0] }] : [] }
}

export default function CreateDatabase({ systemId, onClose, onLaunch }) {
  const [type, setType] = useState('postgres')
  const [name, setName] = useState(TYPE_META.postgres.defaultName)
  const [entities, setEntities] = useState([blankEntity(TYPE_META.postgres)])
  const [status, setStatus] = useState('idle') // idle | submitting | error
  const [error, setError] = useState(null)
  // Schema-from-models (postgres/mongodb only): pick bank models instead of typing
  // entities; a Claude session then authors the tables/collections + foreign keys.
  const [schemaSource, setSchemaSource] = useState('manual') // 'manual' | 'models'
  const [models, setModels] = useState([]) // the system's model bank
  const [selected, setSelected] = useState([]) // selected model names

  const meta = TYPE_META[type]
  const busy = status === 'submitting'
  const nameErr = nodeNameError(name)
  const supportsModels = MODEL_ENGINES.includes(type)
  const modelMode = supportsModels && schemaSource === 'models'

  // The model bank powers the "From model bank" picker.
  useEffect(() => {
    fetch(`/api/models?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json())
      .then((d) => setModels(Array.isArray(d.models) ? d.models : []))
      .catch(() => setModels([]))
  }, [systemId])

  function changeType(next) {
    setType(next)
    setName(TYPE_META[next].defaultName)
    setEntities([blankEntity(TYPE_META[next])])
    setError(null)
    // Model-bank schemas aren't offered for every engine (e.g. redis/blob) — fall back to manual.
    if (!MODEL_ENGINES.includes(next)) setSchemaSource('manual')
  }

  const toggleModel = (n) =>
    setSelected((s) => (s.includes(n) ? s.filter((x) => x !== n) : [...s, n]))

  // Entity / field editing helpers operate on copies so React sees new refs.
  const updateEntity = (i, patch) =>
    setEntities((es) => es.map((e, j) => (j === i ? { ...e, ...patch } : e)))
  const addEntity = () => setEntities((es) => [...es, blankEntity(meta)])
  const removeEntity = (i) => setEntities((es) => es.filter((_, j) => j !== i))

  const updateField = (ei, fi, patch) =>
    setEntities((es) => es.map((e, j) => (j === ei
      ? { ...e, fields: e.fields.map((f, k) => (k === fi ? { ...f, ...patch } : f)) }
      : e)))
  const addField = (ei) =>
    setEntities((es) => es.map((e, j) => (j === ei
      ? { ...e, fields: [...e.fields, { name: '', type: meta.fieldTypes[0] }] }
      : e)))
  const removeField = (ei, fi) =>
    setEntities((es) => es.map((e, j) => (j === ei
      ? { ...e, fields: e.fields.filter((_, k) => k !== fi) }
      : e)))

  async function submit(e) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      if (modelMode) {
        if (selected.length === 0) throw new Error('select at least one model')
        const res = await fetch('/api/databases', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system: systemId, type, name: name.trim(), models: selected }),
        })
        const data = await res.json().catch(() => ({}))
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
        // The container was provisioned empty — launch Claude to author the schema
        // (tables/collections + FKs) from the selected models.
        onLaunch?.({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: buildDbSchemaPrompt({
            systemId,
            dbId: name.trim(),
            engine: type,
            models: selected,
            allModels: models,
            update: false,
          }),
        }, { kind: 'database', target: name.trim(), title: 'schema' })
        onClose()
        return
      }

      // Manual entities: drop blank rows; only send fields for engines that use them.
      // Redis rows carry the keyspace shape instead: { name, match, type, shorthand }.
      const payloadEntities = entities
        .filter((en) => en.name.trim())
        .map((en) => meta.isRedis
          ? { name: en.name.trim(), match: en.match, type: en.ksType, shorthand: (en.shorthand || '').trim() }
          : meta.hasFields
            ? { name: en.name.trim(), fields: en.fields.filter((f) => f.name.trim()).map((f) => ({ name: f.name.trim(), type: f.type })) }
            : { name: en.name.trim() })
      const res = await fetch('/api/databases', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, type, name: name.trim(), entities: payloadEntities }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setStatus('error')
      setError(err.message)
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Add a database</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <form onSubmit={submit}>
          <label className="form-row">
            <span>Type</span>
            <select value={type} onChange={(e) => changeType(e.target.value)} disabled={busy}>
              {Object.entries(TYPE_META).map(([k, m]) => (
                <option key={k} value={k}>{m.label}</option>
              ))}
            </select>
          </label>

          <label className="form-row">
            <span>Name</span>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="app-db"
              disabled={busy}
            />
          </label>
          {name.trim() && nameErr
            ? <small className="field-error">{nameErr}</small>
            : <small className="form-hint">{NODE_NAME_HINT}</small>}

          {supportsModels && (
            <label className="form-row">
              <span>Schema</span>
              <select value={schemaSource} onChange={(e) => setSchemaSource(e.target.value)} disabled={busy}>
                <option value="manual">Manual entities</option>
                <option value="models">From model bank</option>
              </select>
            </label>
          )}

          {modelMode ? (
            <div className="form-section">
              <div className="form-section-head">
                <span>Models → {meta.entityWord.toLowerCase()}s</span>
              </div>
              {models.length === 0 ? (
                <p className="sim-desc">No models in the bank yet — add some with ＋ Models, then come back.</p>
              ) : (
                <ul className="model-pick-list">
                  {models.map((m) => {
                    const on = selected.includes(m.name)
                    const refs = referencedModels(m.name, models)
                    return (
                      <li key={m.name} className="model-pick-row">
                        <label className="model-pick-label">
                          <input type="checkbox" checked={on} onChange={() => toggleModel(m.name)} disabled={busy} />
                          <span className="model-pick-name">{m.name}</span>
                        </label>
                        {refs.length > 0 && <span className="model-pick-refs">→ {refs.join(', ')}</span>}
                      </li>
                    )
                  })}
                </ul>
              )}
              <p className="form-hint">
                Each model becomes a {meta.entityWord.toLowerCase()}; a field whose type is another selected
                model becomes a foreign key. Claude authors the schema once the database is created.
              </p>
            </div>
          ) : (
          <div className="form-section">
            <div className="form-section-head">
              <span>{meta.entityWord}s</span>
              <button type="button" onClick={addEntity} disabled={busy}>+ {meta.entityWord}</button>
            </div>

            {entities.map((en, ei) => (
              <div className="entity" key={ei}>
                <div className="entity-row">
                  <input
                    value={en.name}
                    onChange={(e) => updateEntity(ei, { name: e.target.value })}
                    placeholder={meta.isRedis ? 'match:  ·  matchmaking_pool' : `${meta.entityWord.toLowerCase()} name`}
                    disabled={busy}
                  />
                  {meta.isRedis && (
                    <>
                      <select
                        value={en.match}
                        title="prefix: every key starting with the name · exact: the one key itself"
                        onChange={(e) => updateEntity(ei, { match: e.target.value })}
                        disabled={busy}
                      >
                        <option value="prefix">prefix</option>
                        <option value="exact">exact</option>
                      </select>
                      <select
                        value={en.ksType}
                        title="Expected redis type — the badge shown on the diagram row"
                        onChange={(e) => updateEntity(ei, { ksType: e.target.value })}
                        disabled={busy}
                      >
                        {REDIS_KS_TYPES.map((t) => (
                          <option key={t} value={t}>{t} ({REDIS_BADGE[t]})</option>
                        ))}
                      </select>
                      <input
                        value={en.shorthand}
                        onChange={(e) => updateEntity(ei, { shorthand: e.target.value })}
                        placeholder="shorthand (optional)"
                        title="Displayed on the diagram instead of the key name; what services reference"
                        disabled={busy}
                      />
                    </>
                  )}
                  {entities.length > 1 && (
                    <button type="button" className="link-danger" onClick={() => removeEntity(ei)} disabled={busy}>remove</button>
                  )}
                </div>

                {meta.hasFields && (
                  <div className="fields">
                    {en.fields.map((f, fi) => (
                      <div className="field-row" key={fi}>
                        <input
                          value={f.name}
                          onChange={(e) => updateField(ei, fi, { name: e.target.value })}
                          placeholder="field"
                          disabled={busy}
                        />
                        <select value={f.type} onChange={(e) => updateField(ei, fi, { type: e.target.value })} disabled={busy}>
                          {meta.fieldTypes.map((t) => <option key={t} value={t}>{t}</option>)}
                        </select>
                        <button type="button" className="link-danger" onClick={() => removeField(ei, fi)} disabled={busy}>×</button>
                      </div>
                    ))}
                    <button type="button" className="link" onClick={() => addField(ei)} disabled={busy}>+ field</button>
                  </div>
                )}
              </div>
            ))}
          </div>
          )}

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
            <button
              type="submit"
              className="primary"
              disabled={busy || !!nameErr || (modelMode && selected.length === 0)}
            >
              {busy
                ? 'Provisioning… (pulling images can take a minute)'
                : modelMode
                  ? 'Create & author schema'
                  : 'Create database'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
