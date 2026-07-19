// LLM Worker — "Persistence" tab (embedded in NodeEditModal for the worker's base
// node). Manages the worker's PERSISTENCE READERS: a consumer group of containers
// that XREADGROUP the worker's runs:started announcements, accumulate each claimed
// run's token stream, and write the finished output to a database.
//
// ADDING readers CREATES a brand-new reading service (the persistence_reader custom
// type): POST /api/custom-services scaffolds the service (plain FastAPI template with
// REDIS_HOST / ANNOUNCE_STREAM / READER_GROUP / DB_NODE pre-wired in env) plus the
// persistence.json entry and the reader→stream / reader→db edges — then a launched
// sandbox-llm-persistence session authors the real claim/accumulate/persist loop and
// flips implemented:true. The persist target is either structured — a DB → table →
// field pick, driven by GET /api/db-schema off the live container — or a freeform
// "specialized implementation" spec. Scaling and later edits live on the reader
// node's own Readers tab.
import { useEffect, useState } from 'react'
import type { EditTabProps } from '../../types/customTypes'
import { nodeNameError, NODE_NAME_HINT } from '../../nodeName'
import { buildPersistencePrompt } from '../persistenceReader/prompt'
import type { PersistenceReaderState } from '../persistenceReader/ReadersTab'

const STATE_URL = (sys: string) => `/api/custom/persistence-reader/state?system=${encodeURIComponent(sys)}`
const DB_TYPES = new Set(['postgres', 'mongodb'])

// The "add readers" form.
interface AddForm {
  name: string
  db: string
  table: string
  field: string
  freeform: string
  description: string
}

// GET /api/db-schema introspection of the picked db's live container.
interface SchemaField {
  name: string
  type?: string
}
interface SchemaEntity {
  name: string
  fields?: SchemaField[]
}
type SchemaState =
  | { status: 'loading' }
  | { status: 'error'; error: string }
  | { status: 'ok'; entities: SchemaEntity[] }

export default function PersistenceTab({ systemId, node, manifest, onClose, onLaunch, onBusyChange }: EditTabProps) {
  const [nodes, setNodes] = useState<Record<string, PersistenceReaderState> | null>(null) // the state route's node map
  const [adding, setAdding] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [form, setForm] = useState<AddForm>({ name: '', db: '', table: '', field: '', freeform: '', description: '' })
  const [specialized, setSpecialized] = useState(false)
  const [schema, setSchema] = useState<SchemaState | null>(null) // { status, entities } for the picked db

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(STATE_URL(systemId))
        const data = (await res.json()) as { ok: boolean; nodes: Record<string, PersistenceReaderState> }
        if (!cancelled && data.ok) setNodes(data.nodes)
      } catch {
        /* keep last good */
      }
    }
    tick()
    const t = setInterval(tick, 3000)
    return () => { cancelled = true; clearInterval(t) }
  }, [systemId, node.id])

  // Table/field pickers: introspect the picked db's live container.
  useEffect(() => {
    if (!form.db) return setSchema(null)
    let cancelled = false
    setSchema({ status: 'loading' })
    fetch(`/api/db-schema?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(form.db)}`)
      .then((r) => r.json())
      .then((d: { ok?: boolean; error?: string; entities?: SchemaEntity[] }) => {
        if (cancelled) return
        if (!d.ok) setSchema({ status: 'error', error: d.error || 'schema introspection failed' })
        else setSchema({ status: 'ok', entities: d.entities || [] })
      })
      .catch((err) => !cancelled && setSchema({ status: 'error', error: err.message }))
    return () => { cancelled = true }
  }, [systemId, form.db])

  // This worker's reader groups (base nodes carrying persistence.worker === this id).
  const readers = (manifest?.nodes || []).filter(
    (n) => n.service_type === 'persistence_reader' && !n.instanceOf && n.persistence?.worker === node.id,
  )
  const dbNodes = (manifest?.nodes || []).filter(
    (n) => n.origin === 'create-database' && DB_TYPES.has(n.type),
  )
  const entities = schema?.status === 'ok' ? schema.entities : []
  const fields = entities.find((e) => e.name === form.table)?.fields || []
  const stream = node.llm?.stream

  function set(patch: Partial<AddForm>) {
    setForm((f) => ({ ...f, ...patch }))
  }

  async function submit() {
    setError(null)
    const name = form.name.trim()
    const nameErr = !name ? 'Service name is required' : nodeNameError(name)
    if (nameErr) return setError(nameErr)
    if ((manifest?.nodes || []).some((n) => n.id === name)) {
      return setError(`a node named "${name}" already exists in this system`)
    }
    if (!specialized && (!form.db || !form.table || !form.field)) {
      return setError('Pick the database, table and field to write the output to (or switch to a specialized implementation)')
    }
    if (specialized && !form.freeform.trim()) {
      return setError('Describe the specialized implementation')
    }
    if (!form.description.trim()) return setError('Describe what gets written')
    const conversationId = crypto.randomUUID()

    setBusy(true)
    try {
      const res = await fetch('/api/custom-services', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          serviceType: 'persistence_reader',
          name,
          options: {
            worker: node.id,
            db: specialized ? '' : form.db,
            table: specialized ? '' : form.table,
            field: specialized ? '' : form.field,
            freeform: specialized ? form.freeform : '',
            description: form.description,
            conversationId,
          },
        }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)

      // NodeEditModal always passes onLaunch (App wires it to enqueueSession).
      onLaunch!({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildPersistencePrompt({
          systemId,
          service: name,
          worker: node.id,
          stream,
          group: name,
          db: specialized ? '' : form.db,
          table: specialized ? '' : form.table,
          field: specialized ? '' : form.field,
          freeform: specialized ? form.freeform : '',
          description: form.description,
        }),
      }, { kind: 'persistence', target: name, title: 'readers' })
      onClose()
    } catch (err) {
      setError((err as Error).message)
      setBusy(false)
    }
  }

  return (
    <div>
      <p className="sim-desc">
        Persistence readers consume this worker's <code>runs:started</code> announcements on{' '}
        <code>{stream}</code> as a redis consumer group — each member claims a run, accumulates its
        token stream (<code>tokens:&lt;run_id&gt;</code>), and writes the finished output to a
        database. Generations are otherwise ephemeral (the token stream expires).
      </p>

      {/* Existing reader groups */}
      <div className="form-section">
        <div className="form-section-head"><span>Reader groups</span></div>
        {readers.length === 0 && <p className="sim-desc">none yet — the assistant's replies are not persisted.</p>}
        {readers.map((r) => {
          const entry = nodes?.[r.id]?.registry
          const members = 1 + (r.replicas?.instances?.length || 0)
          return (
            <div className="form-row" key={r.id}>
              <span>
                {r.id}
                {entry && !entry.implemented && <> <span className="scenario-pending">pending</span></>}
              </span>
              <code>
                {r.persistence?.db
                  ? `${r.persistence.db} · ${r.persistence.table}.${r.persistence.field}`
                  : 'specialized'}
                {` · ${members} member${members === 1 ? '' : 's'}`}
              </code>
              {entry?.conversationId && (
                <button
                  type="button"
                  onClick={() => { onLaunch!({ sessionId: entry.conversationId!, mode: 'resume' }, { kind: 'persistence', target: r.id, title: 'readers' }); onClose() }}
                  disabled={busy}
                >
                  Resume
                </button>
              )}
            </div>
          )
        })}
        <small className="form-hint">
          Scaling and spec edits live on each reader node's own Readers tab; delete via its Delete tab.
        </small>
      </div>

      {/* Add readers */}
      <div className="form-section">
        <div className="form-section-head">
          <span>Add persistence readers</span>
          {!adding && (
            <button type="button" onClick={() => setAdding(true)} disabled={busy}>Add</button>
          )}
        </div>
        {adding && (
          <>
            <label className="form-row">
              <span>Service name</span>
              <input
                placeholder="db-writers"
                title={NODE_NAME_HINT}
                value={form.name}
                onChange={(e) => set({ name: e.target.value })}
                disabled={busy}
              />
            </label>
            <label className="form-check">
              <input
                type="checkbox"
                checked={specialized}
                onChange={(e) => setSpecialized(e.target.checked)}
                disabled={busy}
              />
              <span>Specialized implementation (freeform spec instead of a table/field target)</span>
            </label>
            {!specialized ? (
              <>
                <label className="form-row">
                  <span>Database</span>
                  <select
                    value={form.db}
                    onChange={(e) => set({ db: e.target.value, table: '', field: '' })}
                    disabled={busy}
                  >
                    <option value="">— pick a database —</option>
                    {dbNodes.map((d) => (
                      <option key={d.id} value={d.id}>{d.label || d.id} ({d.type})</option>
                    ))}
                  </select>
                </label>
                {form.db && schema?.status === 'loading' && <p className="sim-desc">reading schema…</p>}
                {form.db && schema?.status === 'error' && <small className="field-error">{schema.error}</small>}
                {schema?.status === 'ok' && (
                  <>
                    <label className="form-row">
                      <span>Table</span>
                      <select
                        value={form.table}
                        onChange={(e) => set({ table: e.target.value, field: '' })}
                        disabled={busy}
                      >
                        <option value="">— pick a table —</option>
                        {entities.map((e) => (
                          <option key={e.name} value={e.name}>{e.name}</option>
                        ))}
                      </select>
                    </label>
                    {form.table && (
                      <label className="form-row">
                        <span>Field</span>
                        <select
                          value={form.field}
                          onChange={(e) => set({ field: e.target.value })}
                          disabled={busy}
                        >
                          <option value="">— pick the output field —</option>
                          {fields.map((f) => (
                            <option key={f.name} value={f.name}>{f.name}{f.type ? ` (${f.type})` : ''}</option>
                          ))}
                        </select>
                      </label>
                    )}
                  </>
                )}
              </>
            ) : (
              <textarea
                rows={4}
                placeholder="Describe exactly how each finished run should be persisted (where, what shape, extra logic)…"
                value={form.freeform}
                onChange={(e) => set({ freeform: e.target.value })}
                disabled={busy}
              />
            )}
            <textarea
              rows={3}
              placeholder="What gets written? e.g. persist each finished generation as an assistant message row for its chat…"
              value={form.description}
              onChange={(e) => set({ description: e.target.value })}
              disabled={busy}
            />
            <small className="form-hint">
              Creates the service + registry entry now, then a Claude session (queued) authors the
              claim/accumulate/persist loop from this spec and rebuilds the container.
            </small>
            <div className="modal-actions">
              <button type="button" onClick={() => { setAdding(false); setError(null) }} disabled={busy}>
                Cancel
              </button>
              <button type="button" className="primary" onClick={submit} disabled={busy}>
                {busy ? 'Creating… (building the container can take a minute)' : 'Create readers'}
              </button>
            </div>
          </>
        )}
      </div>

      {error && <p className="modal-error">{error}</p>}
    </div>
  )
}
