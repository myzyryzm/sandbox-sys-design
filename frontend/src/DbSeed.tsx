import { useEffect, useState } from 'react'
import type { ManifestNode } from './types/manifest'

/**
 * Per-database "Seed" tab. Lets you fill a postgres/mongodb db with fixture rows that
 * PERSIST and AUTO-REPLAY: each entry is saved to systems/<id>/<db>/seeds.json and an
 * idempotent seed script (seed.sql / seed.js) that the backend applies live, re-runs
 * after a schema migration, and mounts so a from-scratch rebuild reproduces the data.
 *
 * You pick a table/collection (the "type of entry"), fill its fields (driven by the
 * live introspected schema), and Add. Blank fields are omitted so DB defaults apply
 * (auto IDs, timestamps). "Re-seed now" re-applies everything after a test wipe.
 */

const WORDS: Record<string, { entity: string; empty: string }> = {
  postgres: { entity: 'Table', empty: 'No tables yet — author a schema first.' },
  mongodb: { entity: 'Collection', empty: 'No collections yet — author a schema first.' },
  cassandra: { entity: 'Table', empty: 'No tables yet — author a schema first.' },
  dynamodb: { entity: 'Table', empty: 'No tables yet — author a schema first.' },
}

// Live-introspected entity shapes (GET /api/db-seed) + the seeds.json registry copy.
interface SeedField {
  name: string
  type?: string
}

interface SeedEntity {
  name: string
  fields?: SeedField[]
}

interface SeedTable {
  table: string
  rows: Array<Record<string, unknown>>
}

interface SeedsFile {
  tables: SeedTable[]
}

type SeedState =
  | { status: 'loading' }
  | { status: 'error'; error?: string }
  | { status: 'ok'; entities: SeedEntity[] }

interface SeedResponse {
  ok?: boolean
  error?: string
  entities?: SeedEntity[]
  seeds?: SeedsFile
}

interface DbSeedProps {
  systemId: string
  node: ManifestNode
  onClose: () => void
  embedded?: boolean
  onBusyChange?: (busy: boolean) => void
}

export default function DbSeed({ systemId, node, onClose, embedded = false, onBusyChange }: DbSeedProps) {
  const [state, setState] = useState<SeedState>({ status: 'loading' })
  const [seeds, setSeeds] = useState<SeedsFile>({ tables: [] })
  const [table, setTable] = useState('')
  const [values, setValues] = useState<Record<string, string>>({}) // field -> string (for tables with known fields)
  const [extra, setExtra] = useState<Array<{ key: string; val: string }>>([]) // mongo empty-collection fallback: [{ key, val }]
  const [busy, setBusy] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const engine = node.type
  const words = WORDS[engine] || { entity: 'Entity', empty: 'Empty.' }

  useEffect(() => onBusyChange?.(!!busy), [busy, onBusyChange])

  useEffect(() => {
    let cancelled = false
    setState({ status: 'loading' })
    fetch(`/api/db-seed?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(node.id)}`)
      .then((r) => r.json() as Promise<SeedResponse>)
      .then((d) => {
        if (cancelled) return
        if (!d.ok) return setState({ status: 'error', error: d.error })
        const ents = Array.isArray(d.entities) ? d.entities : []
        setState({ status: 'ok', entities: ents })
        setSeeds(d.seeds && Array.isArray(d.seeds.tables) ? d.seeds : { tables: [] })
        setTable((t) => t || ents[0]?.name || '')
      })
      .catch((err: Error) => !cancelled && setState({ status: 'error', error: err.message }))
    return () => {
      cancelled = true
    }
  }, [systemId, node.id])

  const entities = state.status === 'ok' ? state.entities : []
  const current = entities.find((e) => e.name === table)
  const fields = current?.fields || []

  // Clear the form when switching tables.
  useEffect(() => {
    setValues({})
    setExtra([])
  }, [table])

  async function post(url: string, body: unknown, key: string, after?: (data: SeedResponse) => void) {
    setBusy(key)
    setError(null)
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = (await res.json().catch(() => ({}))) as SeedResponse
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      if (data.seeds) setSeeds(data.seeds)
      after?.(data)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(null)
    }
  }

  function addEntry() {
    const row: Record<string, string> = {}
    for (const f of fields) {
      const v = values[f.name]
      if (v !== undefined && v !== '') row[f.name] = v
    }
    for (const { key, val } of extra) if (key.trim() && val !== '') row[key.trim()] = val
    if (!Object.keys(row).length) return setError('Enter at least one field value.')
    post('/api/db-seed', { system: systemId, id: node.id, table, row }, 'add', () => {
      setValues({})
      setExtra([])
    })
  }

  const removeEntry = (t: string, i: number) =>
    post('/api/db-seed-remove', { system: systemId, id: node.id, table: t, index: i }, `del:${t}:${i}`)
  const reseed = () => post('/api/db-seed-apply', { system: systemId, id: node.id }, 'apply')

  const count = seeds.tables.reduce((n, t) => n + t.rows.length, 0)

  const body = (
    <>
      {state.status === 'loading' && <p className="sim-desc">Reading live schema…</p>}
      {state.status === 'error' && <p className="modal-error">{state.error}</p>}

      {state.status === 'ok' &&
        (entities.length === 0 ? (
          <p className="sim-desc">{words.empty}</p>
        ) : (
          <>
            <div className="form-section">
              <div className="form-section-head">
                <span>Add entry</span>
              </div>

              <div className="form-row">
                <span>{words.entity}</span>
                <select value={table} onChange={(e) => setTable(e.target.value)} disabled={!!busy}>
                  {entities.map((e) => (
                    <option key={e.name} value={e.name}>
                      {e.name}
                    </option>
                  ))}
                </select>
              </div>

              {fields.length > 0 ? (
                <div className="seed-fields">
                  {fields.map((f) => (
                    <label key={f.name} className="seed-field">
                      <span className="seed-field-label">
                        <span className="seed-field-name">{f.name}</span>
                        {f.type && <span className="seed-field-type">{f.type}</span>}
                      </span>
                      <input
                        type="text"
                        value={values[f.name] ?? ''}
                        placeholder={f.type || ''}
                        onChange={(e) => setValues((v) => ({ ...v, [f.name]: e.target.value }))}
                        disabled={!!busy}
                      />
                    </label>
                  ))}
                </div>
              ) : (
                <div className="seed-fields">
                  <p className="form-hint">
                    This collection has no sampled fields yet — add field / value pairs.
                  </p>
                  {extra.map((row, i) => (
                    <div key={i} className="seed-kv">
                      <input
                        type="text"
                        placeholder="field"
                        value={row.key}
                        onChange={(e) =>
                          setExtra((x) => x.map((r, j) => (j === i ? { ...r, key: e.target.value } : r)))
                        }
                        disabled={!!busy}
                      />
                      <input
                        type="text"
                        placeholder="value"
                        value={row.val}
                        onChange={(e) =>
                          setExtra((x) => x.map((r, j) => (j === i ? { ...r, val: e.target.value } : r)))
                        }
                        disabled={!!busy}
                      />
                    </div>
                  ))}
                  <button
                    type="button"
                    className="link"
                    onClick={() => setExtra((x) => [...x, { key: '', val: '' }])}
                    disabled={!!busy}
                  >
                    + field
                  </button>
                </div>
              )}

              <p className="form-hint">
                Leave a field blank to use the database default (auto IDs, timestamps, …). Add parent
                rows before the rows that reference them.
              </p>
              <div className="replica-add">
                <button type="button" className="primary" onClick={addEntry} disabled={!!busy || !table}>
                  {busy === 'add' ? 'Adding…' : 'Add entry'}
                </button>
              </div>
            </div>

            <div className="form-section">
              <div className="form-section-head">
                <span>Seed entries</span>
                {count > 0 && (
                  <button type="button" className="link" onClick={reseed} disabled={!!busy}>
                    {busy === 'apply' ? 'Re-seeding…' : 'Re-seed now'}
                  </button>
                )}
              </div>

              {count === 0 ? (
                <p className="sim-desc">
                  No seed entries yet. Entries you add persist and auto-refill after migrations, test
                  resets, and rebuilds.
                </p>
              ) : (
                seeds.tables.map((t) => (
                  <div key={t.table} className="seed-group">
                    <div className="seed-group-head">
                      <code>{t.table}</code>
                      <span className="seed-count">{t.rows.length}</span>
                    </div>
                    <ul className="seed-list">
                      {t.rows.map((r, i) => (
                        <li key={i} className="seed-entry">
                          <span className="seed-entry-vals">
                            {Object.entries(r)
                              .map(([k, v]) => `${k}=${v}`)
                              .join(', ')}
                          </span>
                          <button
                            type="button"
                            className="link-danger"
                            onClick={() => removeEntry(t.table, i)}
                            disabled={!!busy}
                          >
                            {busy === `del:${t.table}:${i}` ? 'removing…' : 'remove'}
                          </button>
                        </li>
                      ))}
                    </ul>
                  </div>
                ))
              )}
            </div>
          </>
        ))}

      {error && <p className="modal-error">{error}</p>}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>
            Seed · <code>{node.id}</code>
          </h2>
          <button className="modal-close" onClick={onClose} disabled={!!busy}>
            ✕
          </button>
        </header>
        {body}
      </div>
    </div>
  )
}
