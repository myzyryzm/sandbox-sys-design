import { useEffect, useState } from 'react'
import { CDC_OPS as OPS } from './cdcMeta'
import type { Manifest, ManifestNode } from './types/manifest'
import type { CdcRule } from './types/registries'
import type { LaunchSession } from './types/customTypes'

/**
 * Per-database "CDC" (Change Data Capture) tab. Manages a flat list of rules — each
 * { table, operations:[INSERT|UPDATE|DELETE], stream, topic } — that route a table's
 * filtered change events to a Kafka topic. Capture is REAL: a per-database worker
 * container (`<db>-cdc`) streams from postgres logical replication / mongo change
 * streams and produces to the broker.
 *
 * Rules are saved to systems/<id>/<db>/cdc.json and mounted into the worker. The FIRST
 * rule restarts the database (postgres wal_level=logical / mongo replica set) and
 * launches a Claude session that authors + builds the worker via the sandbox-database-cdc
 * skill; later edits are pure registry changes + a worker restart (no session).
 */

const NEW_TOPIC = '__new__'

// Prompt for the spawned session that authors + builds the CDC worker. The mechanical
// scaffold (cdc.json, engine enablement, compose service, scrape job, manifest, producer
// registration) is already done by the backend — this only asks for the worker code.
function buildCdcPrompt({ systemId, dbId, engine, dbNameStr, rules }: {
  systemId: string
  dbId: string
  engine: string
  dbNameStr: string
  rules: CdcRule[]
}): string {
  const wid = `${dbId}-cdc`
  const ruleLines = rules
    .map((r) => `  - table "${r.table}" — ${r.operations!.join(', ')} -> stream "${r.stream}", topic "${r.topic}"`)
    .join('\n')
  return [
    `Use the sandbox-database-cdc skill to build the CDC worker for database "${dbId}" in system "${systemId}".`,
    '',
    `Engine: ${engine}`,
    `Database service: ${dbId} (db name: ${dbNameStr})`,
    `Worker container to author + build: ${wid}`,
    '',
    'The backend has ALREADY done the mechanical scaffold — DO NOT redo any of it:',
    `  • wrote systems/${systemId}/${dbId}/cdc.json (the rules below, mounted into the worker at /cdc.json:ro)`,
    ({
      postgres: `  • set wal_level=logical on ${dbId} and recreated it`,
      mongodb: `  • converted ${dbId} to single-node replica set rs0 and initiated it`,
      dynamodb: `  • ${dbId}'s tables already have DynamoDB Streams enabled (NEW_AND_OLD_IMAGES) — tail them via boto3 dynamodbstreams`,
      cassandra: `  • left ${dbId} unchanged — use POLLING capture (no commitlog CDC): periodically query each table and emit new/changed rows`,
    } as Record<string, string>)[engine],
    `  • added the ${wid} compose service (build ./${wid}), its prometheus scrape job (job ${wid}, target ${wid}:8000),`,
    `    the manifest node + edges (${dbId} → ${wid} → each stream), and registered ${wid} as a producer in streams.json`,
    '',
    'CDC rules (read them live from /cdc.json at runtime — do not hardcode the list):',
    ruleLines,
    '',
    `Your job: author systems/${systemId}/${wid}/{Dockerfile,requirements.txt,app.py} so the worker does REAL`,
    'change capture and produces each captured change event to that rule\'s Kafka topic, then build + verify:',
    `  docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${wid}`,
    '',
    engine === 'dynamodb'
      ? 'Env the worker receives: CDC_ENGINE, CDC_DB_HOST, CDC_DB_PORT, DDB_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION.'
      : 'Env the worker receives: CDC_ENGINE, CDC_DB_HOST, CDC_DB_PORT, CDC_DB_NAME, CDC_DB_USER, CDC_DB_PASSWORD.',
    'Each rule\'s Kafka bootstrap is <stream>:9092. Export prometheus metrics on :8000 — cdc_events_captured_total{table,op},',
    'cdc_events_produced_total{topic}, cdc_errors_total. Follow the skill for the per-engine capture details.',
  ].join('\n')
}

// Live-introspected shapes of GET /api/db-cdc.
interface CdcEntity {
  name: string
}

interface CdcStreamOption {
  id: string
  topics?: string[]
}

type CdcState =
  | { status: 'loading' }
  | { status: 'error'; error?: string }
  | { status: 'ok' }

interface CdcPostResponse {
  ok?: boolean
  error?: string
  rules?: CdcRule[]
  needsWorker?: boolean
}

interface DbCdcProps {
  systemId: string
  node: ManifestNode
  manifest: Manifest
  onClose: () => void
  onLaunch?: LaunchSession
  embedded?: boolean
  onBusyChange?: (busy: boolean) => void
}

export default function DbCdc({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }: DbCdcProps) {
  const [state, setState] = useState<CdcState>({ status: 'loading' })
  const [rules, setRules] = useState<CdcRule[]>([])
  const [entities, setEntities] = useState<CdcEntity[]>([])
  const [streams, setStreams] = useState<CdcStreamOption[]>([])
  // New-rule form.
  const [table, setTable] = useState('')
  const [ops, setOps] = useState<Set<string>>(() => new Set(OPS))
  const [stream, setStream] = useState('')
  const [topicSel, setTopicSel] = useState(NEW_TOPIC)
  const [newTopic, setNewTopic] = useState('')
  const [busy, setBusy] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const engine = node.type

  useEffect(() => onBusyChange?.(!!busy), [busy, onBusyChange])

  useEffect(() => {
    let cancelled = false
    setState({ status: 'loading' })
    fetch(`/api/db-cdc?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(node.id)}`)
      .then((r) => r.json() as Promise<{ ok?: boolean; error?: string; entities?: CdcEntity[]; streams?: CdcStreamOption[]; rules?: CdcRule[] }>)
      .then((d) => {
        if (cancelled) return
        if (!d.ok) return setState({ status: 'error', error: d.error })
        const ents = Array.isArray(d.entities) ? d.entities : []
        const strs = Array.isArray(d.streams) ? d.streams : []
        setEntities(ents)
        setStreams(strs)
        setRules(Array.isArray(d.rules) ? d.rules : [])
        setTable((t) => t || ents[0]?.name || '')
        setStream((s) => s || strs[0]?.id || '')
        setState({ status: 'ok' })
      })
      .catch((err: Error) => !cancelled && setState({ status: 'error', error: err.message }))
    return () => {
      cancelled = true
    }
  }, [systemId, node.id])

  const currentStream = streams.find((s) => s.id === stream)
  const topicOptions = currentStream?.topics || []

  // Pick a sensible topic when the selected stream changes.
  useEffect(() => {
    setTopicSel(topicOptions[0] || NEW_TOPIC)
    setNewTopic('')
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stream])

  const toggleOp = (op: string) =>
    setOps((s) => {
      const next = new Set(s)
      next.has(op) ? next.delete(op) : next.add(op)
      return next
    })

  async function post(url: string, body: unknown, key: string, after?: (data: CdcPostResponse) => void) {
    setBusy(key)
    setError(null)
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = (await res.json().catch(() => ({}))) as CdcPostResponse
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      if (Array.isArray(data.rules)) setRules(data.rules)
      after?.(data)
      return data
    } catch (err) {
      setError((err as Error).message)
      return null
    } finally {
      setBusy(null)
    }
  }

  // Launch the worker-authoring session (first rule) or just refresh the list.
  function applyAddResult(data: CdcPostResponse) {
    if (data?.needsWorker) {
      onLaunch?.({
        sessionId: crypto.randomUUID(),
        mode: 'new',
        prompt: buildCdcPrompt({
          systemId,
          dbId: node.id,
          engine,
          dbNameStr: node.id.replace(/-/g, '_'),
          rules: data.rules || rules,
        }),
      }, { kind: 'cdc', target: node.id, title: 'CDC worker' })
      onClose()
    }
  }

  function addRule() {
    const topic = (topicSel === NEW_TOPIC ? newTopic : topicSel).trim()
    const operations = OPS.filter((o) => ops.has(o))
    if (!table) return setError('Pick a table.')
    if (!operations.length) return setError('Select at least one operation.')
    if (!stream) return setError('Pick an event stream.')
    if (!topic) return setError('Choose or name a topic.')
    post(
      '/api/db-cdc',
      { system: systemId, id: node.id, table, operations, stream, topic },
      'add',
      applyAddResult,
    )
  }

  // Toggle an operation on an existing rule (pure registry edit + worker restart).
  function toggleRuleOp(rule: CdcRule, op: string) {
    const has = rule.operations!.includes(op)
    const operations = OPS.filter((o) => (o === op ? !has : rule.operations!.includes(o)))
    if (!operations.length) return setError('A rule needs at least one operation — remove it instead.')
    post(
      '/api/db-cdc',
      { system: systemId, id: node.id, table: rule.table, operations, stream: rule.stream, topic: rule.topic },
      `op:${rule.table}:${rule.stream}:${rule.topic}`,
    )
  }

  const removeRule = (rule: CdcRule) =>
    post(
      '/api/db-cdc-remove',
      { system: systemId, id: node.id, table: rule.table, stream: rule.stream, topic: rule.topic },
      `del:${rule.table}:${rule.stream}:${rule.topic}`,
    )

  const noStreams = state.status === 'ok' && streams.length === 0
  const noTables = state.status === 'ok' && entities.length === 0

  const body = (
    <>
      {state.status === 'loading' && <p className="sim-desc">Reading live schema…</p>}
      {state.status === 'error' && <p className="modal-error">{state.error}</p>}

      {state.status === 'ok' && (
        <>
          <div className="form-section">
            <div className="form-section-head">
              <span>Add CDC rule</span>
            </div>

            {noStreams ? (
              <p className="sim-desc">
                No event streams yet — add a Kafka event stream first, then route changes to one of its topics.
              </p>
            ) : noTables ? (
              <p className="sim-desc">No tables yet — author a schema first.</p>
            ) : (
              <>
                <div className="form-row">
                  <span>{engine === 'mongodb' ? 'Collection' : 'Table'}</span>
                  <select value={table} onChange={(e) => setTable(e.target.value)} disabled={!!busy}>
                    {entities.map((e) => (
                      <option key={e.name} value={e.name}>
                        {e.name}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="form-row">
                  <span>Operations</span>
                  <span className="cdc-ops">
                    {OPS.map((op) => (
                      <label key={op} className="cdc-op">
                        <input
                          type="checkbox"
                          checked={ops.has(op)}
                          onChange={() => toggleOp(op)}
                          disabled={!!busy}
                        />
                        {op}
                      </label>
                    ))}
                  </span>
                </div>

                <div className="form-row">
                  <span>Event stream</span>
                  <select value={stream} onChange={(e) => setStream(e.target.value)} disabled={!!busy}>
                    {streams.map((s) => (
                      <option key={s.id} value={s.id}>
                        {s.id}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="form-row">
                  <span>Topic</span>
                  <select value={topicSel} onChange={(e) => setTopicSel(e.target.value)} disabled={!!busy}>
                    {topicOptions.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                    <option value={NEW_TOPIC}>＋ new topic…</option>
                  </select>
                </div>

                {topicSel === NEW_TOPIC && (
                  <div className="form-row">
                    <span></span>
                    <input
                      type="text"
                      placeholder="new topic name"
                      value={newTopic}
                      onChange={(e) => setNewTopic(e.target.value)}
                      disabled={!!busy}
                    />
                  </div>
                )}

                {rules.length === 0 && (
                  <p className="form-hint">
                    {engine === 'postgres' || engine === 'mongodb' ? (
                      <>
                        Adding the first rule restarts <code>{node.id}</code>{' '}
                        {engine === 'postgres' ? '(enables logical replication)' : '(converts it to a replica set)'}{' '}
                        and opens Claude to build the <code>{node.id}-cdc</code> worker.
                      </>
                    ) : (
                      <>
                        Adding the first rule opens Claude to build the <code>{node.id}-cdc</code> worker{' '}
                        ({engine === 'dynamodb' ? 'tails DynamoDB Streams' : 'polls for changes'}) — <code>{node.id}</code> is not restarted.
                      </>
                    )}
                  </p>
                )}

                <div className="replica-add">
                  <button type="button" className="primary" onClick={addRule} disabled={!!busy}>
                    {busy === 'add' ? 'Saving…' : rules.length === 0 ? 'Add rule & build worker' : 'Add rule'}
                  </button>
                </div>
              </>
            )}
          </div>

          <div className="form-section">
            <div className="form-section-head">
              <span>CDC rules</span>
            </div>

            {rules.length === 0 ? (
              <p className="sim-desc">
                No CDC rules yet. Each rule streams a table's INSERT/UPDATE/DELETE changes to a Kafka topic.
              </p>
            ) : (
              <ul className="seed-list">
                {rules.map((r) => {
                  const key = `${r.table}:${r.stream}:${r.topic}`
                  return (
                    <li key={key} className="cdc-rule">
                      <div className="cdc-rule-main">
                        <code>{r.table}</code>
                        <span className="cdc-rule-arrow">→</span>
                        <code>
                          {r.stream} / {r.topic}
                        </code>
                        <button
                          type="button"
                          className="link-danger"
                          onClick={() => removeRule(r)}
                          disabled={!!busy}
                        >
                          {busy === `del:${key}` ? 'removing…' : 'remove'}
                        </button>
                      </div>
                      <span className="cdc-ops">
                        {OPS.map((op) => (
                          <label key={op} className="cdc-op">
                            <input
                              type="checkbox"
                              checked={r.operations!.includes(op)}
                              onChange={() => toggleRuleOp(r, op)}
                              disabled={!!busy}
                            />
                            {op}
                          </label>
                        ))}
                      </span>
                    </li>
                  )
                })}
              </ul>
            )}
          </div>
        </>
      )}

      {error && <p className="modal-error">{error}</p>}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>
            CDC · <code>{node.id}</code>
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
