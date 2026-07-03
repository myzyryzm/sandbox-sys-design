import { useState } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName.js'

/**
 * Modal for "Add event stream". Provisions a real single-broker Kafka (KRaft) +
 * a kafka-exporter via POST /api/event-streams (see frontend/server/eventstreams.js),
 * seeding the optional initial topics. On success the new node appears on the live
 * diagram (no edge yet — producers/consumers are wired later via the skill).
 *
 * The Type control is a dropdown with only "kafka" today, built to grow as more
 * event-stream engines are added.
 */

const TYPE_META = {
  kafka: { label: 'Kafka', defaultName: 'events' },
}

export default function CreateEventStream({ systemId, onClose }) {
  const [type, setType] = useState('kafka')
  const [name, setName] = useState(TYPE_META.kafka.defaultName)
  const [topics, setTopics] = useState([''])
  const [status, setStatus] = useState('idle') // idle | submitting | error
  const [error, setError] = useState(null)

  const busy = status === 'submitting'
  const nameErr = nodeNameError(name)

  function changeType(next) {
    setType(next)
    setName(TYPE_META[next].defaultName)
    setError(null)
  }

  function updateTopic(i, value) {
    setTopics((ts) => ts.map((t, j) => (j === i ? value : t)))
  }
  function addTopic() {
    setTopics((ts) => [...ts, ''])
  }
  function removeTopic(i) {
    setTopics((ts) => ts.filter((_, j) => j !== i))
  }

  async function submit(e) {
    e.preventDefault()
    setStatus('submitting')
    setError(null)
    try {
      const payloadTopics = topics.map((t) => t.trim()).filter(Boolean)
      const res = await fetch('/api/event-streams', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, type, name: name.trim(), topics: payloadTopics }),
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
                  value={t}
                  onChange={(e) => updateTopic(i, e.target.value)}
                  placeholder="topic name (e.g. orders)"
                  disabled={busy}
                />
                <button type="button" className="link-danger" onClick={() => removeTopic(i)} disabled={busy}>remove</button>
              </div>
            ))}
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
