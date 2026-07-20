import { useState, type FormEvent } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName'

/**
 * Modal for "Add event stream". Provisions a real single-broker Kafka (KRaft) +
 * a kafka-exporter via POST /api/event-streams (see frontend/server/eventstreams.js),
 * seeding the optional initial topics. On success the new node appears on the live
 * diagram (no edge yet — producers/consumers are wired later via the skill).
 *
 * The Type control is a dropdown with only "kafka" today, built to grow as more
 * event-stream engines are added.
 */

const TYPE_META: Record<string, { label: string; defaultName: string }> = {
  kafka: { label: 'Kafka', defaultName: 'events' },
}

// The partitions field holds the raw input text while editing (coerced on submit).
interface TopicRow {
  id: string
  partitions: number | string
}

interface CreateEventStreamProps {
  systemId: string
  onClose: () => void
}

export default function CreateEventStream({ systemId, onClose }: CreateEventStreamProps) {
  const [type, setType] = useState('kafka')
  const [name, setName] = useState(TYPE_META.kafka.defaultName)
  const [topics, setTopics] = useState<TopicRow[]>([{ id: '', partitions: 1 }])
  const [status, setStatus] = useState<'idle' | 'submitting' | 'error'>('idle')
  const [error, setError] = useState<string | null>(null)

  const busy = status === 'submitting'
  const nameErr = nodeNameError(name)

  function changeType(next: string) {
    setType(next)
    setName(TYPE_META[next].defaultName)
    setError(null)
  }

  function updateTopic(i: number, patch: Partial<TopicRow>) {
    setTopics((ts) => ts.map((t, j) => (j === i ? { ...t, ...patch } : t)))
  }
  function addTopic() {
    setTopics((ts) => [...ts, { id: '', partitions: 1 }])
  }
  function removeTopic(i: number) {
    setTopics((ts) => ts.filter((_, j) => j !== i))
  }

  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      const payloadTopics = topics
        .map((t) => ({ id: t.id.trim(), partitions: Math.max(1, Math.round(Number(t.partitions) || 1)) }))
        .filter((t) => t.id)
      const res = await fetch('/api/event-streams', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, type, name: name.trim(), topics: payloadTopics }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setStatus('error')
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Add an event stream</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <form onSubmit={submit}>
          <p className="sim-desc">
            Provisions a single-broker Kafka (KRaft) and a kafka-exporter scraped by
            Prometheus. Declare any initial topics here; producers and consumers are
            wired up afterward (with the <code>sandbox-event-stream</code> skill).
          </p>

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
              placeholder="events"
              disabled={busy}
            />
          </label>
          {name.trim() && nameErr
            ? <small className="field-error">{nameErr}</small>
            : <small className="form-hint">{NODE_NAME_HINT}</small>}

          <div className="form-section">
            <div className="form-section-head">
              <span>Topics</span>
              <button type="button" onClick={addTopic} disabled={busy}>+ Topic</button>
            </div>

            {topics.length === 0 && <p className="sim-desc">No topics — add some later.</p>}
            {topics.map((t, i) => (
              <div className="entity-row" key={i}>
                <input
                  value={t.id}
                  onChange={(e) => updateTopic(i, { id: e.target.value })}
                  placeholder="topic name (e.g. orders)"
                  disabled={busy}
                />
                <input
                  type="number"
                  min={1}
                  max={64}
                  value={t.partitions}
                  onChange={(e) => updateTopic(i, { partitions: e.target.value })}
                  title="partitions"
                  style={{ width: 72, flex: '0 0 auto' }}
                  disabled={busy}
                />
                <button type="button" className="link-danger" onClick={() => removeTopic(i)} disabled={busy}>remove</button>
              </div>
            ))}
            {topics.length > 0 && <small className="form-hint">Number = partitions per topic (fan-out for consumer-group scaling).</small>}
          </div>

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={busy}>Cancel</button>
            <button type="submit" className="primary" disabled={busy || !!nameErr}>
              {busy ? 'Provisioning… (pulling images can take a minute)' : 'Create event stream'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
