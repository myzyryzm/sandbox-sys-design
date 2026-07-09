import { useCallback, useEffect, useState } from 'react'
import { nodeNameError, NODE_NAME_HINT } from './nodeName.js'

/**
 * An event stream's "Consumers" tab (embedded in NodeEditModal for a Kafka cluster node). It
 * manages this cluster's CONSUMER FUNCTIONS: a named, per-service background poll loop by which an
 * internal service consumes one of the cluster's topics. Each function is OWNED by exactly one
 * service — identity is (service, name) — and lives in systems/<id>/consumers.json.
 *
 * DEFINING a consumer CREATES a brand-new consuming service (the consumer_group custom type):
 * POST /api/custom-services scaffolds the service (plain FastAPI template with the cluster's
 * streams.json pause-flag mount and SERVICE_ID pre-wired), its `<name>-scaler` autoscaler
 * container, the named Kafka consumer group, and the consume edge — then a launched
 * sandbox-event-stream session authors the real poll loop. The group scales to N member
 * containers from its Scaling tab (all joining the same group id, so Kafka rebalances the
 * topic's partitions across them) and the scaler drives that automatically from lag.
 *
 * EDITING an existing function (topic / poll rate / group / description) keeps the original
 * mechanical path — POST /api/consumers — plus a session when code must change. The shape
 * mirrors the Endpoints / client-Functions tabs: a permanent id (the name), an auto-generated +
 * editable (append-style) description, a read-only changelog, and a Resume-able Claude session.
 * On the diagram the service shows a "CONS <name>" row; clicking it traces cluster → service.
 */

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
const GROUP_RE = /^[a-zA-Z0-9._-]+$/ // mirrors GROUP_RE in server/consumers.js
const POLL_MIN = 100
const POLL_MAX = 600_000

function blankForm() {
  return { name: '', service: '', serviceName: '', topic: '', pollRate: 1000, groupId: '', description: '' }
}

// The default Kafka group id for a function — mirrors groupIdFor in server/consumers.js.
// Shown live in the group field until the user takes it over.
function defaultGroupId(service, name) {
  return service && name ? `${service}-${name}` : ''
}

// A history entry's ISO timestamp -> a short, local, human label (best-effort).
function fmtAt(at) {
  if (!at) return ''
  const d = new Date(at)
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleString()
}

// When editing, a new description entry is APPENDED to the existing one (an empty entry leaves it
// unchanged), so the description accumulates over successive edits. (Same rule as the other tabs.)
function joinDescription(base, addition) {
  const b = (base || '').trim()
  const a = (addition || '').trim()
  if (!b) return a
  if (!a) return b
  return `${b}\n\n${a}`
}

// Reduce a history snapshot to just what changed vs the previous one, so the trail reads like a
// changelog. First snapshot (prev == null) is the creation. Descriptions accumulate by append, so a
// later description is the previous one plus "\n\n<chunk>" — surface only that appended chunk.
function diffEntry(curr, prev) {
  if (!prev) {
    return { initial: true, description: (curr.description || '').trim(), topic: curr.topic, pollRate: curr.pollRate }
  }
  const diff = {}
  const cd = curr.description || ''
  const pd = prev.description || ''
  if (cd !== pd) {
    diff.description = (cd.startsWith(pd) ? cd.slice(pd.length) : cd).trim()
    diff.descriptionReplaced = !cd.startsWith(pd)
  }
  if (curr.topic !== prev.topic) diff.topic = { from: prev.topic, to: curr.topic }
  if (curr.pollRate !== prev.pollRate) diff.pollRate = { from: prev.pollRate, to: curr.pollRate }
  // Pre-named-groups snapshots lack groupId — only diff when both sides carry one.
  if (curr.groupId && prev.groupId && curr.groupId !== prev.groupId) {
    diff.group = { from: prev.groupId, to: curr.groupId }
  }
  // A rename snapshot carries `renamedFrom` (the spec is otherwise unchanged).
  if (curr.renamedFrom && curr.name && curr.renamedFrom !== curr.name) {
    diff.rename = { from: curr.renamedFrom, to: curr.name }
  }
  diff.empty = !diff.description && !diff.topic && !diff.pollRate && !diff.group && !diff.rename
  return diff
}

// Prompt seeding the launched Claude session that implements (or updates) the poll loop. The
// repeatable procedure lives in the sandbox-event-stream skill, so this stays short. The
// consumers.json entry, the streams.json consumer group, and the cluster→service edge already
// exist (written by POST /api/consumers); the session writes the code + rebuilds the service.
function buildConsumerPrompt({ systemId, service, cluster, name, topic, pollRate, groupId, description, editing, created, priorDescription, priorTopic, priorPollRate, priorGroupId }) {
  const lines = [
    `Use the sandbox-event-stream skill to ${editing ? 'UPDATE' : 'IMPLEMENT'} a Kafka CONSUMER FUNCTION in the "${systemId}" system.`,
    '',
    `Consumer function "${name}" — owned by service "${service}", consuming the "${cluster}" Kafka cluster.`,
    `Topic: ${topic}`,
    `Consumer group id: ${groupId}`,
    `Poll rate: ${pollRate}ms`,
    '',
  ]
  if (created) {
    lines.push(
      `The consuming service "${service}" was JUST created for this function (a consumer-group service:`,
      `fresh FastAPI code from the plain template, container already built and running, with a`,
      `"${service}-scaler" autoscaler alongside it). Its compose entry ALREADY mounts`,
      `./${cluster}/streams.json at /streams/${cluster}.json:ro (the pause flag) and sets`,
      `SERVICE_ID=${service} — do NOT edit docker-compose.yml. Set client_id=os.environ["SERVICE_ID"]`,
      `on the KafkaConsumer: the group scales to N member containers that all run this same loop, and`,
      `the diagram maps members to their assigned partitions by that client id.`,
      '',
    )
  }
  if (editing) {
    lines.push(
      `This consumer ALREADY EXISTS and is implemented in systems/${systemId}/${service}/app.py. FIRST read it,`,
      `then MODIFY it in place to match the values above` +
        (priorTopic && priorTopic !== topic ? ` (topic changed ${priorTopic} → ${topic})` : '') +
        (priorPollRate && priorPollRate !== pollRate ? ` (poll rate changed ${priorPollRate}ms → ${pollRate}ms)` : '') +
        (priorGroupId && priorGroupId !== groupId ? ` (consumer group changed ${priorGroupId} → ${groupId} — a fresh group, so it starts from auto_offset_reset)` : '') +
        `. Keep the metrics middleware and every other route/loop untouched.`,
      '',
      `Current behavior (existing description):`,
      (priorDescription || '').trim() || '(none recorded)',
      '',
    )
  }
  lines.push(
    `What it should do:`,
    (description || '').trim() || '(no description — consume each message and process it sensibly)',
    '',
    `Per the skill's "Consumer function" step:`,
    `- Add a background Kafka consumer in systems/${systemId}/${service}/app.py that subscribes to`,
    `  topic "${topic}" on bootstrap server ${cluster}:9092 using group id "${groupId}", polling every`,
    `  ${pollRate}ms, and processes each message per the description (add the kafka client to`,
    `  requirements.txt; honor the topic's schemaModel/enforceSchema if set).`,
    `- The streams.json consumer group and the ${cluster}→${service} manifest edge are ALREADY written`,
    `  by the app — do NOT edit those.`,
    `- Set this consumer's \`downstream\` (the node ids the loop calls/reads/writes) AND a brief`,
    `  \`downstreamDescriptions\` map (node id -> one short line on what the loop uses that connection`,
    `  for) on this entry in systems/${systemId}/consumers.json, per the skill. The diagram prints`,
    `  those lines on the trace when this consumer's CONS row is clicked.`,
    `- Rebuild ONLY that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${service}`,
    `- Then set "implemented": true on this entry in systems/${systemId}/consumers.json (the one with`,
    `  service "${service}" and name "${name}").`,
  )
  return lines.join('\n')
}

// Prompt for the "Update descriptions" button: (re)generate ONLY this consumer's per-downstream
// connection metadata — the `downstream` list and its `downstreamDescriptions` text map — a pure
// consumers.json edit, no code change and no rebuild. Also the backfill path for consumers created
// before downstreamDescriptions existed. The procedure lives in the sandbox-event-stream skill.
function buildConsumerDescriptionsPrompt({ systemId, service, cluster, name, topic, downstream }) {
  const list = (downstream || []).length ? downstream.join(', ') : '(none recorded yet)'
  return [
    `Use the sandbox-event-stream skill to UPDATE the connection metadata for the Kafka consumer`,
    `function "${name}" — owned by service "${service}", consuming topic "${topic}" on cluster "${cluster}" —`,
    `in the "${systemId}" system.`,
    ``,
    `Downstream nodes: ${list}`,
    ``,
    `Read the consumer's poll loop in systems/${systemId}/${service}/app.py, then edit ONLY this`,
    `consumer's \`downstream\` list and \`downstreamDescriptions\` map on its entry in`,
    `systems/${systemId}/consumers.json (the one with service "${service}" and name "${name}"). For`,
    `\`downstream\`, list every node id the loop calls/reads/writes; for \`downstreamDescriptions\`,`,
    `write one brief line per downstream id describing what the loop uses that connection for. Do`,
    `NOT modify app.py, and do NOT rebuild — this is a pure JSON edit.`,
  ].join('\n')
}

// Prompt for renaming an IMPLEMENTED consumer: the consumers.json entry + streams.json group are
// already renamed by the PUT; this session renames the poll loop + its group id in app.py and
// rebuilds. (A pending consumer is registry-only — no session.)
function buildConsumerRenamePrompt({ systemId, service, cluster, oldName, newName, topic, oldGroupId, newGroupId }) {
  const groupChanged = oldGroupId !== newGroupId
  return [
    `Use the sandbox-event-stream skill to RENAME a Kafka consumer function in the "${systemId}" system.`,
    '',
    `Service "${service}" had a consumer function "${oldName}" (consuming topic "${topic}" on cluster "${cluster}").`,
    `It is now "${newName}" in the registry` +
      (groupChanged
        ? `, and its streams.json consumer group has already been moved from "${oldGroupId}" to "${newGroupId}". Update the CODE to match.`
        : `. Its consumer group "${newGroupId}" is user-named and UNCHANGED — only the function name moves.`),
    '',
    `In systems/${systemId}/${service}/app.py:`,
    `- Rename the poll-loop function _consume_${oldName} → _consume_${newName} (and its thread start).`,
    groupChanged
      ? `- Change that consumer's Kafka group_id from "${oldGroupId}" to "${newGroupId}".`
      : `- Leave that consumer's Kafka group_id ("${newGroupId}") exactly as it is.`,
    `- Leave the topic, poll cadence, pause-awareness, metrics middleware and every other route/loop intact.`,
    '',
    `Then rebuild ONLY that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${service}`,
    ...(groupChanged
      ? ['', `(The new group id is a fresh Kafka consumer group, so it starts from auto_offset_reset = earliest.)`]
      : []),
  ].join('\n')
}

// Prompt for deleting an implemented consumer: its consumers.json entry + streams.json group + edge
// are already removed by the DELETE; this session strips the poll loop from app.py and rebuilds.
function buildConsumerDeletePrompt({ systemId, service, cluster, name, topic, groupId }) {
  return [
    `Use the sandbox-event-stream skill to DELETE the Kafka consumer function "${name}" from service`,
    `"${service}" in the "${systemId}" system (it consumed topic "${topic}" on cluster "${cluster}").`,
    '',
    `Its consumers.json entry, streams.json consumer group, and the ${cluster}→${service} manifest edge`,
    `have already been removed. Remove the matching background consumer loop (group id "${groupId}")`,
    `from systems/${systemId}/${service}/app.py, leaving the metrics middleware and every other route/loop`,
    `intact, then rebuild only that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${service}`,
  ].join('\n')
}

export default function ConsumerTab({ systemId, node, manifest, onClose, onLaunch, embedded = false, onBusyChange }) {
  const cluster = node.id
  const [consumers, setConsumers] = useState(null) // this cluster's consumer functions; null = loading
  const [topics, setTopics] = useState([]) // this cluster's topic ids (for the picker)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)

  // Define / edit form.
  const [adding, setAdding] = useState(false)
  const [editingName, setEditingName] = useState(null) // (service,name) of the function being edited
  const [editingService, setEditingService] = useState(null)
  const [editingDescription, setEditingDescription] = useState('') // current accumulated description (read-only)
  const [editingDownstream, setEditingDownstream] = useState([]) // Claude-managed node ids this loop calls/reads/writes
  const [editingDownstreamDescriptions, setEditingDownstreamDescriptions] = useState({}) // node id -> connection blurb
  const [editingHistory, setEditingHistory] = useState([])
  const [editingOriginal, setEditingOriginal] = useState(null) // { topic, pollRate, groupId } baseline for change detection
  const [groupTouched, setGroupTouched] = useState(false) // once the user edits the group field it stops tracking service+name
  const [form, setForm] = useState(blankForm)
  const [confirmKey, setConfirmKey] = useState(null) // function pending delete confirm
  const [renamingKey, setRenamingKey] = useState(null) // function whose name is being edited inline
  const [renameValue, setRenameValue] = useState('')

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  const load = useCallback(() => {
    return Promise.all([
      fetch(`/api/consumers?system=${encodeURIComponent(systemId)}`).then((r) => r.json()).catch(() => ({})),
      fetch(`/api/event-stream?system=${encodeURIComponent(systemId)}&id=${encodeURIComponent(cluster)}&live=0`)
        .then((r) => r.json()).catch(() => ({})),
    ]).then(([cons, streams]) => {
      const mine = cons.ok ? (cons.consumers || []).filter((c) => c.cluster === cluster) : []
      setConsumers(mine)
      setTopics(streams.ok ? (streams.topics || []).map((t) => t.id) : [])
    })
  }, [systemId, cluster])

  useEffect(() => { load() }, [load])

  const editing = editingName !== null
  const setField = (k) => (e) => setForm((f) => ({ ...f, [k]: e.target.value }))
  const key = (c) => `${c.service} ${c.name}`

  function startAdd() {
    setForm({ ...blankForm(), topic: topics[0] || '' })
    setEditingName(null)
    setEditingService(null)
    setEditingDescription('')
    setEditingDownstream([])
    setEditingDownstreamDescriptions({})
    setEditingHistory([])
    setEditingOriginal(null)
    setGroupTouched(false)
    setError(null)
    setAdding(true)
  }

  function startEdit(c) {
    setForm({ name: c.name, service: c.service, topic: c.topic, pollRate: c.pollRate, groupId: c.groupId || defaultGroupId(c.service, c.name), description: '' })
    setGroupTouched(true) // an existing group id never auto-tracks; it's edited deliberately
    setEditingName(c.name)
    setEditingService(c.service)
    setEditingDescription(c.description || '')
    // Connection metadata (Claude-managed), reconciled to nodes that still exist so a deleted
    // downstream doesn't linger in the Connections list. Mirrors the endpoint modal.
    const nodeIds = new Set((manifest?.nodes || []).map((n) => n.id))
    const ds = (Array.isArray(c.downstream) ? c.downstream : []).filter((d) => nodeIds.has(d))
    const dd = c.downstreamDescriptions && typeof c.downstreamDescriptions === 'object' ? c.downstreamDescriptions : {}
    setEditingDownstream(ds)
    setEditingDownstreamDescriptions(Object.fromEntries(ds.filter((d) => typeof dd[d] === 'string').map((d) => [d, dd[d]])))
    setEditingHistory(Array.isArray(c.history) ? c.history : [])
    setEditingOriginal({ topic: c.topic, pollRate: c.pollRate, groupId: c.groupId || defaultGroupId(c.service, c.name) })
    setConfirmKey(null)
    setError(null)
    setAdding(true)
  }

  function cancelForm() {
    setAdding(false)
    setEditingName(null)
    setEditingService(null)
    setEditingDescription('')
    setEditingDownstream([])
    setEditingDownstreamDescriptions({})
    setEditingHistory([])
    setEditingOriginal(null)
    setError(null)
  }

  async function submit() {
    setError(null)
    const name = editing ? editingName : form.name.trim()
    const service = editing ? editingService : form.serviceName.trim()
    if (!editing) {
      if (!name) return setError('Function name is required')
      if (!IDENT_RE.test(name)) {
        return setError('Function name must start with a letter or underscore and use only letters, digits and underscores')
      }
      const svcErr = !service ? 'New service name is required' : nodeNameError(service)
      if (svcErr) return setError(svcErr)
      if ((manifest?.nodes || []).some((n) => n.id === service || n.id === `${service}-scaler`)) {
        return setError(`a node named "${service}" (or its scaler) already exists in this system`)
      }
    }
    if (!form.topic) return setError('Pick a topic to consume')
    const pollRate = Math.min(POLL_MAX, Math.max(POLL_MIN, Math.round(Number(form.pollRate) || 0)))
    if (!Number.isFinite(pollRate) || pollRate <= 0) return setError('Poll rate must be a positive number (ms)')

    // The Kafka group id: the field's value once touched, else the live default.
    const groupId = (groupTouched ? form.groupId.trim() : '') || defaultGroupId(service, name)
    if (!GROUP_RE.test(groupId) || groupId.length > 100) {
      return setError('Consumer group must use only letters, digits, dot, underscore and hyphen (max 100)')
    }
    if ((consumers || []).some((c) => c.groupId === groupId && !(c.service === service && c.name === name))) {
      return setError(`consumer group "${groupId}" is already used by another consumer on this cluster`)
    }

    // On edit the Describe field holds only the NEW text — append it to the existing description.
    const description = editing ? joinDescription(editingDescription, form.description) : form.description
    // A code change (needs a Claude session + rebuild) = create, or an edit that moved the topic /
    // poll rate / group id. A description-only edit is registry-only (no session).
    const dirtyCode =
      !editing ||
      form.topic !== editingOriginal.topic ||
      pollRate !== editingOriginal.pollRate ||
      groupId !== editingOriginal.groupId
    const conversationId = crypto.randomUUID()

    setBusy(true)
    try {
      // 1. Persist. A NEW consumer creates its own consuming service + scaler (the
      //    consumer_group custom type — builds two containers, so this takes a minute);
      //    an EDIT is the original registry-only upsert.
      if (!editing) {
        const res = await fetch('/api/custom-services', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            system: systemId,
            serviceType: 'consumer_group',
            name: service,
            options: { cluster, topic: form.topic, fn: name, groupId, pollRate, description, conversationId },
          }),
        })
        const data = await res.json().catch(() => ({}))
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      } else {
        const res = await fetch('/api/consumers', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system: systemId, service, name, cluster, topic: form.topic, pollRate, groupId, description, conversationId }),
        })
        const data = await res.json().catch(() => ({}))
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      }

      // 2. A code change launches the implement/update session; a description-only edit just closes.
      if (dirtyCode) {
        onLaunch({
          sessionId: conversationId,
          mode: 'new',
          prompt: buildConsumerPrompt({
            systemId, service, cluster, name, topic: form.topic, pollRate, groupId, description,
            editing,
            created: !editing,
            priorDescription: editingDescription,
            priorTopic: editingOriginal?.topic,
            priorPollRate: editingOriginal?.pollRate,
            priorGroupId: editingOriginal?.groupId,
          }),
        }, { kind: 'consumer', target: service, title: name })
        onClose()
        return
      }
      cancelForm()
      await load()
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  function onDescriptionKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      submit()
    }
  }

  // "Update descriptions": launch a Claude session that (re)generates only this consumer's
  // downstream + downstreamDescriptions in consumers.json (pure JSON, no rebuild), then close.
  function updateDescriptions() {
    if (!editing) return
    onLaunch({
      sessionId: crypto.randomUUID(),
      mode: 'new',
      prompt: buildConsumerDescriptionsPrompt({
        systemId,
        service: editingService,
        cluster,
        name: editingName,
        topic: editingOriginal?.topic || form.topic,
        downstream: editingDownstream,
      }),
    }, { kind: 'consumer', target: editingService, title: 'descriptions' })
    onClose()
  }

  function startRename(c) {
    setRenamingKey(key(c))
    setRenameValue(c.name)
    setConfirmKey(null)
    setAdding(false)
    setError(null)
  }
  function cancelRename() {
    setRenamingKey(null)
    setRenameValue('')
    setError(null)
  }

  // Rename a consumer function (its name is the permanent id, so this is a dedicated PUT, not an
  // upsert). Registry + streams.json group move happen server-side; an implemented consumer also
  // needs its app.py loop + group id renamed, which a launched session does (then we close).
  async function renameConsumer(c) {
    const newName = renameValue.trim()
    if (newName === c.name) return cancelRename()
    if (!IDENT_RE.test(newName) || newName.length > 60) {
      return setError('Name must start with a letter or underscore and use only letters, digits and underscores')
    }
    if ((consumers || []).some((x) => x.service === c.service && x.name === newName)) {
      return setError(`a consumer function "${newName}" already exists on ${c.service}`)
    }
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/consumers', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, service: c.service, oldName: c.name, newName }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setRenamingKey(null)
      // An implemented consumer has a live loop + group id in app.py — rename them + rebuild via a session.
      if (data.wasImplemented) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: buildConsumerRenamePrompt({
            systemId, service: c.service, cluster, oldName: c.name, newName, topic: c.topic,
            oldGroupId: data.oldGroupId || c.groupId,
            newGroupId: data.consumer?.groupId || c.groupId,
          }),
        }, { kind: 'consumer', target: c.service, title: `rename ${newName}` })
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

  function onRenameKeyDown(e, c) {
    if (e.key === 'Enter') {
      e.preventDefault()
      renameConsumer(c)
    } else if (e.key === 'Escape') {
      e.preventDefault()
      cancelRename()
    }
  }

  async function removeConsumer(c) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/consumers', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, service: c.service, name: c.name }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setConfirmKey(null)
      // An implemented consumer has a live poll loop in the service — strip it + rebuild via a session.
      if (data.wasImplemented) {
        onLaunch({
          sessionId: crypto.randomUUID(),
          mode: 'new',
          prompt: buildConsumerDeletePrompt({ systemId, service: c.service, cluster, name: c.name, topic: c.topic, groupId: data.groupId || c.groupId }),
        }, { kind: 'consumer', target: c.service, title: `delete ${c.name}` })
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

  const body = (
    <>
      <p className="sim-desc">
        <strong>Consumers</strong> of <code>{cluster}</code> — each a named background poll loop by which an
        internal service consumes one of this cluster's topics. Click a consumer on its service node in the
        diagram to trace <code>{cluster}</code> → that service.
      </p>

      {/* ---- This cluster's consumer functions ---- */}
      {consumers === null ? (
        <p className="sim-desc">Loading…</p>
      ) : consumers.length === 0 ? (
        <p className="sim-desc">No consumer functions yet.</p>
      ) : (
        <ul className="endpoint-list">
          {consumers.map((c) => {
            const k = key(c)
            const confirming = confirmKey === k
            return (
              <li key={k} className="endpoint-list-row">
                <span className="endpoint-list-method">CONS</span>
                <code className="endpoint-alias">{c.name}</code>
                <span className="endpoint-list-path" title={`consumer group: ${c.groupId}`}>
                  {c.service} ← {c.topic} · {c.pollRate}ms · <code>{c.groupId}</code>
                </span>
                {!c.implemented && (
                  <span className="scenario-pending" title="Poll loop not implemented yet — open or resume the Claude session">pending</span>
                )}
                {renamingKey === k ? (
                  <span className="endpoint-list-actions consumer-rename">
                    <input
                      className="rename-input"
                      value={renameValue}
                      onChange={(e) => setRenameValue(e.target.value)}
                      onKeyDown={(e) => onRenameKeyDown(e, c)}
                      disabled={busy}
                      autoFocus
                      aria-label={`New name for ${c.name}`}
                    />
                    <button className="link" disabled={busy || !renameValue.trim()} onClick={() => renameConsumer(c)}>
                      {busy ? 'Working…' : 'Save'}
                    </button>
                    <button className="link" disabled={busy} onClick={cancelRename}>Cancel</button>
                  </span>
                ) : confirming ? (
                  <span className="endpoint-list-actions">
                    <span className="endpoint-confirm">{c.implemented ? 'Delete & rebuild?' : 'Delete?'}</span>
                    <button className="link" disabled={busy} onClick={() => removeConsumer(c)}>Yes</button>
                    <button className="link" disabled={busy} onClick={() => setConfirmKey(null)}>No</button>
                  </span>
                ) : (
                  <span className="endpoint-list-actions">
                    {c.conversationId && (
                      <button
                        className="link"
                        disabled={busy}
                        title="Resume this consumer's Claude session"
                        onClick={() => {
                          onLaunch({ sessionId: c.conversationId, mode: 'resume', prompt: '' })
                          onClose()
                        }}
                      >
                        Resume
                      </button>
                    )}
                    <button className="link" disabled={busy} onClick={() => startEdit(c)}>Edit</button>
                    <button
                      className="link"
                      disabled={busy}
                      title={c.implemented ? 'Rename this consumer (renames its loop + group id and rebuilds the service)' : 'Rename this consumer function'}
                      onClick={() => startRename(c)}
                    >
                      Rename
                    </button>
                    <button className="link-danger" disabled={busy} onClick={() => setConfirmKey(k)}>Delete</button>
                  </span>
                )}
              </li>
            )
          })}
        </ul>
      )}

      {/* ---- Define / edit a consumer function ---- */}
      {!adding ? (
        <div className="form-section">
          <button className="link" onClick={startAdd} disabled={busy || consumers === null || topics.length === 0}>
            ＋ Define a consumer
          </button>
          {consumers !== null && topics.length === 0 && (
            <small className="form-hint">This cluster has no topics yet — add one in the Topics tab first.</small>
          )}
        </div>
      ) : (
        <div className="form-section">
          <div className="form-section-head">
            <span>{editing ? `Edit ${editingName}` : 'New consumer'}</span>
          </div>

          {/* Read-only changelog of every spec this consumer was created/updated with. */}
          {editing && editingHistory.length > 0 && (
            <div className="endpoint-history">
              <div className="endpoint-history-head">Changelog</div>
              <ol className="endpoint-history-list">
                {editingHistory
                  .map((h, i) => ({ i, h, diff: diffEntry(h, i > 0 ? editingHistory[i - 1] : null) }))
                  .reverse()
                  .map(({ i, h, diff }) => (
                    <li key={i} className="endpoint-history-row">
                      <div className="endpoint-history-meta">
                        <span className="endpoint-history-num">#{i + 1}</span>
                        {diff.initial && <span className="endpoint-history-initial">created</span>}
                        {fmtAt(h.at) && <span className="endpoint-history-at">{fmtAt(h.at)}</span>}
                      </div>
                      {diff.initial ? (
                        <>
                          {diff.description && <div className="endpoint-history-desc">{diff.description}</div>}
                          <div className="endpoint-history-schemas">
                            <code>topic: {diff.topic}</code>
                            <code>poll: {diff.pollRate}ms</code>
                          </div>
                        </>
                      ) : diff.empty ? (
                        <div className="endpoint-history-empty">no spec changes</div>
                      ) : (
                        <>
                          {diff.description && (
                            <div className="endpoint-history-desc">
                              <span className="endpoint-history-field">
                                {diff.descriptionReplaced ? 'description replaced:' : 'added:'}
                              </span>{' '}
                              {diff.description}
                            </div>
                          )}
                          {diff.rename && (
                            <div className="endpoint-history-change">
                              <span className="endpoint-history-field">renamed:</span>
                              <code>{diff.rename.from}</code>
                              <span className="endpoint-history-arrow">→</span>
                              <code>{diff.rename.to}</code>
                            </div>
                          )}
                          {diff.topic && (
                            <div className="endpoint-history-change">
                              <span className="endpoint-history-field">topic:</span>
                              <code>{diff.topic.from}</code>
                              <span className="endpoint-history-arrow">→</span>
                              <code>{diff.topic.to}</code>
                            </div>
                          )}
                          {diff.group && (
                            <div className="endpoint-history-change">
                              <span className="endpoint-history-field">group:</span>
                              <code>{diff.group.from}</code>
                              <span className="endpoint-history-arrow">→</span>
                              <code>{diff.group.to}</code>
                            </div>
                          )}
                          {diff.pollRate && (
                            <div className="endpoint-history-change">
                              <span className="endpoint-history-field">poll:</span>
                              <code>{diff.pollRate.from}ms</code>
                              <span className="endpoint-history-arrow">→</span>
                              <code>{diff.pollRate.to}ms</code>
                            </div>
                          )}
                        </>
                      )}
                    </li>
                  ))}
              </ol>
            </div>
          )}

          <label className="form-row">
            <span>Name</span>
            <input
              value={editing ? editingName : form.name}
              onChange={setField('name')}
              placeholder="processRefunds  (a function name — unique to this service)"
              disabled={busy || editing}
            />
          </label>

          {editing ? (
            <label className="form-row">
              <span>Service</span>
              <input value={editingService} disabled />
            </label>
          ) : (
            <>
              <label className="form-row">
                <span>New service name</span>
                <input
                  value={form.serviceName}
                  onChange={setField('serviceName')}
                  placeholder="order-consumer  (a brand-new consuming service)"
                  disabled={busy}
                />
              </label>
              <small className="form-hint">
                Creates a NEW service to own this consumer (plus a <code>-scaler</code> that autoscales
                its members from consumer lag). {NODE_NAME_HINT}
              </small>
            </>
          )}

          <label className="form-row">
            <span>Topic</span>
            <select value={form.topic} onChange={setField('topic')} disabled={busy}>
              {!form.topic && <option value="">— pick a topic —</option>}
              {topics.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </label>

          <label className="form-row">
            <span>Consumer group</span>
            <input
              value={groupTouched ? form.groupId : defaultGroupId(editing ? editingService : form.serviceName.trim(), editing ? editingName : form.name.trim())}
              onChange={(e) => {
                setGroupTouched(true)
                setForm((f) => ({ ...f, groupId: e.target.value }))
              }}
              placeholder="kafka consumer-group id"
              disabled={busy}
            />
          </label>
          <small className="form-hint">
            The Kafka consumer-group id — every instance of the consuming service joins it, and Kafka
            spreads the topic's partitions across them.
            {editing && ' Changing it makes a FRESH group (offsets restart from auto_offset_reset).'}
          </small>

          <label className="form-row">
            <span>Poll rate (ms)</span>
            <input type="number" min={POLL_MIN} max={POLL_MAX} value={form.pollRate} onChange={setField('pollRate')} disabled={busy} />
          </label>

          {editing && editingDescription.trim() && (
            <div className="form-row form-row-stack">
              <span>Current description</span>
              <p className="endpoint-current-desc">{editingDescription}</p>
            </div>
          )}

          <label className="form-row form-row-stack">
            <span>{editing ? 'Add to description' : 'Describe'}</span>
            <textarea
              className="desc-input"
              value={form.description}
              onChange={setField('description')}
              onKeyDown={onDescriptionKeyDown}
              placeholder={editing
                ? 'Add to this consumer’s description — appended to the current one (Enter to submit, Shift+Enter for a newline)'
                : 'What should this consumer do with each message? Leave blank to auto-generate a description. (Enter to submit, Shift+Enter for a newline)'}
              rows={3}
              disabled={busy}
              autoFocus
            />
          </label>

          {/* Per-downstream connection descriptions (Claude-authored, shown read-only).
              "Update descriptions" launches a session that rewrites just these — no rebuild. */}
          {editing && editingDownstream.length > 0 && (
            <div className="form-row form-row-stack">
              <span>Connections</span>
              <div className="endpoint-conn">
                <ul className="endpoint-conn-list">
                  {editingDownstream.map((id) => (
                    <li key={id} className="endpoint-conn-row">
                      <code className="endpoint-conn-node">{id}</code>
                      <span className="endpoint-conn-text">
                        {editingDownstreamDescriptions[id] || '—'}
                      </span>
                    </li>
                  ))}
                </ul>
                <button type="button" className="link" onClick={updateDescriptions} disabled={busy}>
                  Update descriptions
                </button>
              </div>
            </div>
          )}

          <p className="sim-desc">
            {editing
              ? 'Changing the topic, poll rate or group re-authors the loop in a fresh Claude session; a description-only edit just saves.'
              : 'Creating builds the new service + its scaler (about a minute), then opens a Claude session that writes the real Kafka poll loop.'}
          </p>

          {error && <p className="modal-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" onClick={cancelForm} disabled={busy}>Cancel</button>
            <button type="button" className="primary" onClick={submit} disabled={busy}>
              {busy
                ? editing ? 'Working…' : 'Creating service… (builds two containers)'
                : editing ? 'Save' : 'Create service & open Claude'}
            </button>
          </div>
        </div>
      )}

      {error && !adding && <p className="modal-error">{error}</p>}

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
          <h2>Consumers · <code>{cluster}</code></h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>
        {body}
      </div>
    </div>
  )
}
