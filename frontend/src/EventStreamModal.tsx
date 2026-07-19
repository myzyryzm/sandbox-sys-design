import { useEffect, useState } from 'react'
import type { ChangeEvent, FormEvent } from 'react'
import { resolveModelTs } from './modelBank'
import type { ManifestNode } from './types/manifest'
import type { DiscoveredTopic, ModelRecord } from './types/registries'
import type { LaunchSession } from './types/customTypes'

/**
 * View of an event stream's topics, fetched live from GET /api/event-stream
 * (see frontend/server/eventstreams.js). Opens (as the Topics tab) when an
 * event-stream node is clicked. Each row is a topic; expanding it reveals that
 * topic's message schema, its producers and its consumers grouped by consumer-group id.
 *
 * Producers/consumers are declared in the cluster's streams.json registry (a
 * broker can't report producers and only sees consumer-group membership while
 * clients are connected); `live` reflects whether the topic exists on the broker
 * (probing it is a slow `docker compose exec`, so we fetch in two phases — a fast
 * registry-only paint first, then the authoritative live flags).
 *
 * A topic can carry a message-schema contract — a model-bank reference (schemaModel)
 * so consumers know what to expect when reading it — plus an `enforceSchema` flag for
 * whether producer/consumer code validates against it at runtime. Setting the schema is
 * a pure registry write (POST /api/event-stream, no rebuild); when enforcement is turned
 * on for a topic that already has producers/consumers, a sandbox-event-stream session is
 * launched to wire the validation + rebuild those services.
 *
 * Adding a topic is a judgement task (create it on the broker + register it), so
 * the "＋ Add topic" form launches a `sandbox-event-stream` Claude session via
 * `onLaunch`, mirroring how the Endpoints / Schema / CDC tabs delegate mutations.
 */

const TOPIC_RE = /^[a-zA-Z0-9._-]+$/ // mirrors TOPIC_RE in server/eventstreams.js

// GET /api/event-stream's payload (registry topics ⊕ live broker probe).
interface EventStreamResponse {
  ok?: boolean
  error?: string
  topics?: DiscoveredTopic[]
  consumersPaused?: boolean
}

// The modal's three-phase view of that payload.
type StreamState =
  | { status: 'loading' }
  | { status: 'ok'; topics: DiscoveredTopic[]; consumersPaused?: boolean }
  | { status: 'error'; error?: string }

// Prompt seeding the launched session. The procedure (broker create + streams.json
// entry, no rebuild) lives in the sandbox-event-stream skill, so we point Claude at it.
function buildAddTopicPrompt({ systemId, cluster, topic, partitions, schemaModel, enforceSchema }: {
  systemId: string
  cluster: string
  topic: string
  partitions: number | string
  schemaModel: string
  enforceSchema: boolean
}): string {
  const parts = Math.max(1, Number(partitions) || 1)
  const compose = `systems/${systemId}/docker-compose.yml`
  const entry = schemaModel
    ? `{ "id": "${topic}", "partitions": ${parts}, "producers": [], "consumers": [], "schemaModel": "${schemaModel}"${enforceSchema ? ', "enforceSchema": true' : ''} }`
    : `{ "id": "${topic}", "partitions": ${parts}, "producers": [], "consumers": [] }`
  const lines = [
    `Use the sandbox-event-stream skill to ADD a topic to the Kafka cluster "${cluster}" in the "${systemId}" system.`,
    '',
    `Topic id: ${topic}`,
    `Partitions: ${parts}  (replication-factor 1)`,
  ]
  if (schemaModel) {
    lines.push(
      `Message schema: model "${schemaModel}" (defined in systems/${systemId}/models.json) — ` +
        (enforceSchema ? 'ENFORCED in producer/consumer code.' : 'documented only (no runtime validation).'),
    )
  }
  lines.push(
    '',
    'Per the skill’s "Add/remove a topic" step — no docker rebuild is needed:',
    `1. Create it on the live broker:`,
    `   docker compose -f ${compose} exec -T ${cluster} \\`,
    `     /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${cluster}:9092 \\`,
    `     --create --if-not-exists --topic ${topic} --partitions ${parts} --replication-factor 1`,
    `2. Append ${entry} to the "topics" array in`,
    `   systems/${systemId}/${cluster}/streams.json (skip if an entry with that id already exists).`,
    '',
    `Then verify with kafka-topics.sh --list that "${topic}" appears. The Topics modal re-reads the`,
    `registry and re-checks the broker live, so the new topic surfaces without a rebuild.`,
  )
  return lines.join('\n')
}

// Prompt to wire runtime validation for an already-enforced topic that has producers/
// consumers. The schema reference + enforceSchema flag are already saved in streams.json
// by the POST; this session only edits the producing/consuming app.py and rebuilds them.
function buildEnforceSchemaPrompt({ systemId, cluster, topic, model }: {
  systemId: string
  cluster: string
  topic: string
  model: string
}): string {
  return [
    `Use the sandbox-event-stream skill to ENFORCE the message schema on topic "${topic}" of the Kafka cluster "${cluster}" in the "${systemId}" system.`,
    '',
    `systems/${systemId}/${cluster}/streams.json now declares this topic with`,
    `schemaModel "${model}" and enforceSchema:true. The model's TypeScript is in`,
    `systems/${systemId}/models.json (resolve any models it references).`,
    '',
    `Per the skill's "Enforce a topic's message schema" step:`,
    `- For every producer of "${topic}": validate each outgoing payload against "${model}" before send() (raise on mismatch).`,
    `- For every consumer of "${topic}": parse/validate each message against "${model}" after read.`,
    `- Rebuild ONLY those producing/consuming services and verify.`,
    '',
    `The producers and consumers are listed under that topic in streams.json.`,
  ].join('\n')
}

interface TopicSchemaProps {
  systemId: string
  clusterId: string
  topic: DiscoveredTopic
  models: ModelRecord[]
  onLaunch?: LaunchSession
  onClose?: () => void
  onSaved?: () => void
}

/** The per-topic message-schema contract: a model-bank picker + enforce checkbox. */
function TopicSchema({ systemId, clusterId, topic, models, onLaunch, onClose, onSaved }: TopicSchemaProps) {
  const savedModel = topic.schemaModel || ''
  const savedEnforce = !!topic.enforceSchema
  const [sel, setSel] = useState(savedModel)
  const [enforce, setEnforce] = useState(savedEnforce)
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')

  // Re-sync to the registry's saved values when they actually change (e.g. after a save
  // refreshes the list). Deps are the saved values, so the background live=1 re-fetch —
  // which carries the same schema fields — won't clobber an in-progress selection.
  useEffect(() => {
    setSel(savedModel)
    setEnforce(savedEnforce)
    setErr('')
  }, [savedModel, savedEnforce])

  const dirty = sel !== savedModel || enforce !== savedEnforce

  function pick(e: ChangeEvent<HTMLSelectElement>) {
    const v = e.target.value
    setSel(v)
    if (!v) setEnforce(false) // no model → nothing to enforce
  }

  function reset() {
    setSel(savedModel)
    setEnforce(savedEnforce)
    setErr('')
  }

  async function save() {
    setSaving(true)
    setErr('')
    try {
      const res = await fetch('/api/event-stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: clusterId, topic: topic.id, schemaModel: sel, enforceSchema: enforce }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      // The registry write is done. Enforcing on a topic that already has producers/
      // consumers needs code changes + a rebuild — hand that to a launched session.
      const hasMembers = topic.producers.length > 0 || topic.consumers.some((c) => c.members.length > 0)
      if (enforce && sel && hasMembers && onLaunch) {
        onLaunch({ sessionId: crypto.randomUUID(), mode: 'new', prompt: buildEnforceSchemaPrompt({ systemId, cluster: clusterId, topic: topic.id, model: sel }) }, { kind: 'event-stream', target: clusterId, title: `${topic.id} schema` })
        onClose?.()
        return
      }
      onSaved?.()
    } catch (e) {
      setErr((e as Error).message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="topic-section">
      <div className="topic-section-head">
        Message schema
        {savedModel && (
          <span className={`schema-badge${savedEnforce ? ' enforced' : ''}`}>
            {savedEnforce ? 'enforced' : 'documented'}
          </span>
        )}
      </div>
      {models.length === 0 ? (
        <div className="topic-none">no models in the bank — define one in the Models panel first</div>
      ) : (
        <div className="schema-field">
          <select className="model-select" value={sel} onChange={pick} disabled={saving}>
            <option value="">— none —</option>
            {models.map((m) => (
              <option key={m.name} value={m.name}>{m.name}</option>
            ))}
          </select>
          {sel && <pre className="model-preview">{resolveModelTs(sel, models)}</pre>}
          <label className="form-check">
            <input
              type="checkbox"
              checked={enforce}
              disabled={saving || !sel}
              onChange={(e) => setEnforce(e.target.checked)}
            />
            <span>Enforce in producer/consumer code (validates messages at runtime; rebuilds those services)</span>
          </label>
          {err && <small className="field-error">{err}</small>}
          {dirty && (
            <div className="modal-actions">
              <button type="button" onClick={reset} disabled={saving}>Cancel</button>
              <button type="button" className="primary" onClick={save} disabled={saving}>
                {saving ? 'Saving…' : 'Save schema'}
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

/**
 * The per-topic partition count: shown always, growable in place. Kafka can only
 * ADD partitions, so the editor is increase-only (the backend enforces it too). A
 * grow is a mechanical broker `--alter` + registry write — no rebuild, no session;
 * consumer groups on the topic rebalance onto the new partitions automatically.
 */
interface TopicPartitionsProps {
  systemId: string
  clusterId: string
  topic: DiscoveredTopic
  onSaved?: () => void
}

function TopicPartitions({ systemId, clusterId, topic, onSaved }: TopicPartitionsProps) {
  const current = Number.isInteger(topic.partitions) ? (topic.partitions as number) : null
  const [value, setValue] = useState<number | string>(current ?? 1)
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')

  useEffect(() => {
    setValue(current ?? 1)
    setErr('')
  }, [current])

  // Registry-less broker-only topics have an unknown count — nothing to edit.
  if (current === null) return null

  const n = Math.round(Number(value))
  const grow = Number.isInteger(n) && n > current

  async function save() {
    setSaving(true)
    setErr('')
    try {
      const res = await fetch('/api/event-stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: clusterId, topic: topic.id, partitions: n }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onSaved?.()
    } catch (e) {
      setErr((e as Error).message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="topic-section">
      <div className="topic-section-head">Partitions</div>
      <div className="schema-field">
        <div className="entity-row">
          <input
            type="number"
            min={current}
            max={64}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            disabled={saving}
            style={{ width: 90, flex: '0 0 auto' }}
          />
          {grow && (
            <button type="button" className="primary" onClick={save} disabled={saving}>
              {saving ? 'Growing…' : `Grow ${current} → ${n}`}
            </button>
          )}
        </div>
        <small className="form-hint">
          Increase-only (Kafka can’t shrink a topic). Applies to the live broker — consumer
          groups rebalance onto the new partitions automatically.
        </small>
        {err && <small className="field-error">{err}</small>}
      </div>
    </div>
  )
}

interface TopicRowProps {
  systemId: string
  clusterId: string
  topic: DiscoveredTopic
  models: ModelRecord[]
  onLaunch?: LaunchSession
  onClose?: () => void
  onSaved?: () => void
}

/** One topic row: id + partitions + live/pending tag, expandable to schema/producers/consumers. */
function TopicRow({ systemId, clusterId, topic, models, onLaunch, onClose, onSaved }: TopicRowProps) {
  const [open, setOpen] = useState(false)
  return (
    <div className="topic-item">
      <button
        type="button"
        className="topic-head"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        <span className={`skill-caret${open ? ' open' : ''}`}>▶</span>
        <span className="topic-id">{topic.id}</span>
        {Number.isInteger(topic.partitions) && (
          <span className="topic-partitions">{topic.partitions} partition{topic.partitions === 1 ? '' : 's'}</span>
        )}
        {/* live is null until the broker probe returns — show no badge while unknown. */}
        {topic.live === false && <span className="topic-pending">pending</span>}
      </button>

      {open && (
        <div className="topic-detail">
          <TopicPartitions
            systemId={systemId}
            clusterId={clusterId}
            topic={topic}
            onSaved={onSaved}
          />
          <TopicSchema
            systemId={systemId}
            clusterId={clusterId}
            topic={topic}
            models={models}
            onLaunch={onLaunch}
            onClose={onClose}
            onSaved={onSaved}
          />

          <div className="topic-section">
            <div className="topic-section-head">Producers</div>
            {topic.producers.length === 0 ? (
              <div className="topic-none">none declared</div>
            ) : (
              <ul className="topic-members">
                {topic.producers.map((p) => (
                  <li key={p} className="topic-member">{p}</li>
                ))}
              </ul>
            )}
          </div>

          <div className="topic-section">
            <div className="topic-section-head">Consumers</div>
            {topic.consumers.length === 0 ? (
              <div className="topic-none">none declared</div>
            ) : (
              topic.consumers.map((c) => (
                <div key={c.groupId} className="topic-group">
                  <div className="topic-group-id">
                    group <code>{c.groupId}</code>
                  </div>
                  {c.members.length === 0 ? (
                    <div className="topic-none">no members</div>
                  ) : (
                    <ul className="topic-members">
                      {c.members.map((m) => (
                        <li key={m} className="topic-member">{m}</li>
                      ))}
                    </ul>
                  )}
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  )
}

interface EventStreamModalProps {
  systemId: string
  node: ManifestNode
  onClose?: () => void
  onLaunch?: LaunchSession
  embedded?: boolean
}

export default function EventStreamModal({ systemId, node, onClose, onLaunch, embedded = false }: EventStreamModalProps) {
  const [state, setState] = useState<StreamState>({ status: 'loading' })
  const [models, setModels] = useState<ModelRecord[]>([])
  const [reloadKey, setReloadKey] = useState(0)
  const [adding, setAdding] = useState(false)
  const [topicId, setTopicId] = useState('')
  const [partitions, setPartitions] = useState<number | string>(1)
  const [schemaModel, setSchemaModel] = useState('')
  const [enforceSchema, setEnforceSchema] = useState(false)

  // The model bank — populates the per-topic schema picker (same source endpoints use).
  useEffect(() => {
    fetch(`/api/models?system=${encodeURIComponent(systemId)}`)
      .then((r) => r.json() as Promise<{ models?: ModelRecord[] }>)
      .then((d) => setModels(Array.isArray(d.models) ? d.models : []))
      .catch(() => setModels([]))
  }, [systemId])

  useEffect(() => {
    let cancelled = false
    // On a refresh (reloadKey bump after a schema save) keep the current topics visible
    // instead of flashing the loader, so an expanded topic stays open while it re-reads.
    setState((s) => (s.status === 'ok' ? s : { status: 'loading' }))
    const base = `/api/event-stream?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(node.id)}`

    // Phase 1 — instant registry-only paint (no broker probe). topics carry live:null.
    fetch(`${base}&live=0`)
      .then((r) => r.json())
      .then((d) => {
        if (cancelled || !d.ok) return
        // Don't clobber phase 2 if it already won the race.
        setState((s) => (s.status === 'ok' ? s : { status: 'ok', topics: d.topics, consumersPaused: d.consumersPaused }))
      })
      .catch(() => {})

    // Phase 2 — authoritative liveness (slower): real live flags + broker-only topics.
    fetch(`${base}&live=1`)
      .then((r) => r.json())
      .then((d) => {
        if (cancelled) return
        if (d.ok) setState({ status: 'ok', topics: d.topics, consumersPaused: d.consumersPaused })
        else setState((s) => (s.status === 'ok' ? s : { status: 'error', error: d.error }))
      })
      .catch((err) => {
        if (!cancelled) setState((s) => (s.status === 'ok' ? s : { status: 'error', error: err.message }))
      })

    return () => {
      cancelled = true
    }
  }, [systemId, node.id, reloadKey])

  const refresh = () => setReloadKey((k) => k + 1)

  // Cluster-level "pause consumers" toggle. Pure registry write (POST with no `topic`)
  // that pause-aware consumer loops honor live — so no rebuild, no launched session.
  // Optimistic so the checkbox flips instantly; revert + surface the error on failure.
  const [pauseBusy, setPauseBusy] = useState(false)
  const [pauseErr, setPauseErr] = useState('')

  async function togglePause(next: boolean) {
    setPauseBusy(true)
    setPauseErr('')
    setState((s) => (s.status === 'ok' ? { ...s, consumersPaused: next } : s))
    try {
      const res = await fetch('/api/event-stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, id: node.id, consumersPaused: next }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
    } catch (e) {
      setPauseErr(e instanceof Error ? e.message : String(e))
      setState((s) => (s.status === 'ok' ? { ...s, consumersPaused: !next } : s))
    } finally {
      setPauseBusy(false)
    }
  }

  const trimmed = topicId.trim()
  const existing = state.status === 'ok' ? state.topics.map((t) => t.id) : []
  let topicErr = ''
  if (trimmed) {
    if (!TOPIC_RE.test(trimmed) || trimmed.length > 100) {
      topicErr = 'letters, digits, dot, underscore and hyphen only (max 100)'
    } else if (existing.includes(trimmed)) {
      topicErr = 'a topic with this id already exists'
    }
  }

  function cancelAdd() {
    setAdding(false)
    setTopicId('')
    setSchemaModel('')
    setEnforceSchema(false)
  }

  function submitTopic(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    if (!trimmed || topicErr || !onLaunch) return
    const prompt = buildAddTopicPrompt({ systemId, cluster: node.id, topic: trimmed, partitions, schemaModel, enforceSchema })
    onLaunch({ sessionId: crypto.randomUUID(), mode: 'new', prompt }, { kind: 'event-stream', target: node.id, title: trimmed })
    onClose?.()
  }

  const body = (
    <>
      <p className="sim-desc">
        Each row is a topic on this cluster. Expand it to see its message schema, its producers
        and its consumers, grouped by consumer-group id.
      </p>

      {state.status === 'ok' && (
        <div className="pause-consumers">
          <label className="form-check">
            <input
              type="checkbox"
              checked={!!state.consumersPaused}
              disabled={pauseBusy}
              onChange={(e) => togglePause(e.target.checked)}
            />
            <span>Pause consumers — stop all consumer polling on this cluster</span>
          </label>
          <small className="form-hint">
            Consumers stop fetching within one poll cycle but keep serving HTTP. Takes effect
            live — no rebuild. Lag builds while paused and drains when resumed.
          </small>
          {pauseErr && <small className="field-error">{pauseErr}</small>}
        </div>
      )}

      {state.status === 'loading' && <p className="sim-desc">Reading topics…</p>}
      {state.status === 'error' && <p className="modal-error">{state.error}</p>}

      {state.status === 'ok' &&
        (state.topics.length === 0 ? (
          <p className="sim-desc">No topics yet.</p>
        ) : (
          <div className="topic-list">
            {state.topics.map((t) => (
              <TopicRow
                key={t.id}
                systemId={systemId}
                clusterId={node.id}
                topic={t}
                models={models}
                onLaunch={onLaunch}
                onClose={onClose}
                onSaved={refresh}
              />
            ))}
          </div>
        ))}

      {/* Add a topic — launches a sandbox-event-stream session (broker create +
          streams.json entry, no rebuild). Only offered when a launcher is wired in. */}
      {onLaunch && state.status !== 'loading' && (
        !adding ? (
          <div className="form-section">
            <button type="button" className="link" onClick={() => setAdding(true)}>
              ＋ Add topic
            </button>
          </div>
        ) : (
          <form className="form-section" onSubmit={submitTopic}>
            <div className="form-section-head">
              <span>New topic</span>
            </div>
            <label className="form-row">
              <span>Topic id</span>
              <input
                autoFocus
                value={topicId}
                onChange={(e) => setTopicId(e.target.value)}
                placeholder="e.g. orders"
              />
            </label>
            <label className="form-row">
              <span>Partitions</span>
              <input
                type="number"
                min="1"
                value={partitions}
                onChange={(e) => setPartitions(e.target.value)}
              />
            </label>
            <div className="form-row form-row-stack">
              <span>Message schema</span>
              <div className="schema-field">
                <select
                  className="model-select"
                  value={schemaModel}
                  onChange={(e) => {
                    setSchemaModel(e.target.value)
                    if (!e.target.value) setEnforceSchema(false)
                  }}
                >
                  <option value="">— none —</option>
                  {models.map((m) => (
                    <option key={m.name} value={m.name}>{m.name}</option>
                  ))}
                </select>
                {schemaModel && <pre className="model-preview">{resolveModelTs(schemaModel, models)}</pre>}
                <label className="form-check">
                  <input
                    type="checkbox"
                    checked={enforceSchema}
                    disabled={!schemaModel}
                    onChange={(e) => setEnforceSchema(e.target.checked)}
                  />
                  <span>Enforce in producer/consumer code (validates messages at runtime)</span>
                </label>
              </div>
            </div>
            {topicErr ? (
              <small className="field-error">{topicErr}</small>
            ) : (
              <small className="form-hint">
                Creates the topic on the broker and registers it — no rebuild.
              </small>
            )}
            <div className="modal-actions">
              <button type="button" onClick={cancelAdd}>
                Cancel
              </button>
              <button type="submit" className="primary" disabled={!trimmed || !!topicErr}>
                Add topic ▸
              </button>
            </div>
          </form>
        )
      )}
    </>
  )

  if (embedded) return body

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>
            Topics · <code>{node.id}</code>
          </h2>
          <button className="modal-close" onClick={onClose}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
