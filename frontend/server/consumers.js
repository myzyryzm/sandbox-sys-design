// Vite dev-server plugin: per-SERVICE Kafka "consumer functions" — named, described
// background poll loops by which an in-system service CONSUMES a topic of an event stream.
//
//   GET    /api/consumers?system=<id>
//     -> { ok, consumers: [{ service, name, cluster, topic, pollRate, downstream,
//                            downstreamDescriptions, description, implemented, conversationId,
//                            createdAt, updatedAt, history }] }
//        Returns every service's consumer functions (each carries its owner `service`);
//        consumers filter by `service`/`cluster` as needed. `downstream` (the node ids this
//        consumer's loop CALLS/reads/writes — e.g. an API it POSTs to, a db it touches) is
//        Claude-managed connection metadata: the diagram draws a persistent service->downstream
//        line for each, exactly as it does for an endpoint's `downstream` (the consume edge
//        service->cluster is separate, from the manifest). `downstreamDescriptions` (node id ->
//        one short line) is the matching Claude-authored blurb the diagram prints on each of those
//        trace lines when the CONS row is clicked. Both are edited directly in consumers.json by a
//        launched session and preserved across modal upserts (see upsertConsumer's ...prev).
//   POST   /api/consumers  { system, service, name, cluster, topic, pollRate, groupId?, description?, conversationId? }
//     -> upsert the consumer function identified by (service, name). `groupId` is the Kafka
//        consumer-group id — user-named, defaulting to "<service>-<name>", unique per cluster
//        across functions (stored on the record; authoritative everywhere). The actual poll loop in
//        the service's app.py is authored by a launched Claude session (sandbox-event-stream
//        skill) — like the endpoint flow. This plugin only does the MECHANICAL scaffold (no
//        docker rebuild): register the consumer group in the cluster's streams.json and add the
//        manifest edge service->cluster so the diagram draws + can trace the relationship.
//   DELETE /api/consumers  { system, service, name }
//     -> remove the (service, name) function, its streams.json consumer group, and the
//        service->cluster edge (when nothing else needs it). Returns { ok, removed, wasImplemented }.
//
// Mirrors scenarios.js (pure-JSON registry, no rebuild) but the owner is a service and the
// relationship is "consumes topic X of cluster Y". The CONS rows on the service node and the
// service->cluster trace are derived from this registry + the manifest edge.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'
import { bad, readJsonBody, serviceMetrics, serviceCodeContainers } from './scaffold.js'

// A consumer function name is a code-style identifier; with its owner service the name is the
// function's permanent id (unique per service). Mirrors the endpoint alias / scenario name rule.
const FUNCTION_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
const MAX_NAME = 60
const MAX_DESC = 4000
const POLL_MIN = 100
const POLL_MAX = 600_000
const POLL_DEFAULT = 1000
// Kafka consumer-group id charset (mirrors TOPIC_RE in eventstreams.js — also keeps
// the id safe to embed in generated code/prompts).
const GROUP_RE = /^[a-zA-Z0-9._-]+$/
const MAX_GROUP = 100

// --- registry (systems/<id>/consumers.json) -------------------------------------

function consumersFile(system) {
  return path.join(systemDir(system), 'consumers.json')
}
// Tolerate an absent/garbled file (a system with no consumers yet has an empty list).
// Records written before named groups existed lack `groupId` — normalize to the
// historical derived id on read, so every consumer of this registry (the API, the
// diagram, the delete flow, launched sessions) can rely on `c.groupId` being set.
function readConsumers(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(consumersFile(system), 'utf8'))
    const consumers = (Array.isArray(raw?.consumers) ? raw.consumers : []).map((c) =>
      c && typeof c.groupId !== 'string' ? { ...c, groupId: groupIdFor(c.service, c.name) } : c,
    )
    return { consumers }
  } catch {
    return { consumers: [] }
  }
}
function writeConsumers(system, data) {
  fs.writeFileSync(consumersFile(system), JSON.stringify(data, null, 2) + '\n')
}

function readManifest(system) {
  return JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
}
function writeManifest(system, manifest) {
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
}

// The DEFAULT consumer group id for a function: one Kafka group per (service, name) so each
// function consumes the topic independently with its own offset. Since named groups landed
// this is only the default/back-compat derivation — the stored `groupId` on the record is
// authoritative everywhere (registry, prompts, delete). Group ids allow letters/digits/.-_ .
function groupIdFor(service, name) {
  return `${service}-${name}`
}

// --- streams.json (the cluster's topic registry) reconcile ----------------------

function streamsFile(system, cluster) {
  return path.join(systemDir(system), cluster, 'streams.json')
}
function readStreams(system, cluster) {
  try {
    return JSON.parse(fs.readFileSync(streamsFile(system, cluster), 'utf8'))
  } catch {
    return null
  }
}

// Add this function's consumer group to `topic` and remove it from every other topic on the
// cluster (so an edit that changes the topic MOVES the group). Additive for other groups — a
// manually-wired or different-function group is never touched. `members` is the full list of
// CONTAINERS in the group (a scaled service contributes every replica — they all join the
// same Kafka group and split the topic's partitions). Returns true if the file changed.
export function setConsumerGroup(system, cluster, topic, groupId, members) {
  const data = readStreams(system, cluster)
  if (!data || !Array.isArray(data.topics)) return false
  const before = JSON.stringify(data)
  for (const t of data.topics) {
    if (!t || !Array.isArray(t.consumers)) {
      if (t) t.consumers = Array.isArray(t.consumers) ? t.consumers : []
      continue
    }
    // Strip our group from any topic that currently carries it.
    t.consumers = t.consumers.filter((g) => !(g && g.groupId === groupId))
  }
  const t = data.topics.find((x) => x && x.id === topic)
  if (!t) return false
  if (!Array.isArray(t.consumers)) t.consumers = []
  t.consumers.push({ groupId, members: Array.isArray(members) ? [...members] : [members] })
  const after = JSON.stringify(data)
  if (after === before) return false
  fs.writeFileSync(streamsFile(system, cluster), JSON.stringify(data, null, 2) + '\n')
  return true
}

// Remove this function's consumer group from every topic on the cluster. Returns true if changed.
function removeConsumerGroup(system, cluster, groupId) {
  const data = readStreams(system, cluster)
  if (!data || !Array.isArray(data.topics)) return false
  let changed = false
  for (const t of data.topics) {
    if (!t || !Array.isArray(t.consumers)) continue
    const before = t.consumers.length
    t.consumers = t.consumers.filter((g) => !(g && g.groupId === groupId))
    if (t.consumers.length !== before) changed = true
  }
  if (changed) fs.writeFileSync(streamsFile(system, cluster), JSON.stringify(data, null, 2) + '\n')
  return changed
}

// --- manifest edge service->cluster --------------------------------------------

// Ensure a {from:service, to:cluster, origin:"consumer-fn"} edge exists (so the diagram draws a
// persistent dep line + can trace it). The consumer subscribes to / reads from the cluster, so the
// arrow points from the consuming service to the stream. Tagged with `origin` so delete knows it's
// ours to remove.
function addConsumerEdge(system, manifest, cluster, service) {
  if (!Array.isArray(manifest.edges)) manifest.edges = []
  if (manifest.edges.some((e) => e && e.from === service && e.to === cluster)) return false
  manifest.edges.push({ from: service, to: cluster, origin: 'consumer-fn' })
  return true
}

// Remove the service->cluster edge IFF we added it (origin "consumer-fn") and no remaining
// consumer function still pairs this (cluster, service). Returns true if changed.
function removeConsumerEdge(system, manifest, cluster, service, remaining) {
  if (!Array.isArray(manifest.edges)) return false
  const stillNeeded = remaining.some((c) => c.cluster === cluster && c.service === service)
  if (stillNeeded) return false
  const before = manifest.edges.length
  manifest.edges = manifest.edges.filter(
    (e) => !(e && e.from === service && e.to === cluster && e.origin === 'consumer-fn'),
  )
  return manifest.edges.length !== before
}

// --- validation -----------------------------------------------------------------

function autoDescription({ service, cluster, topic, pollRate }) {
  return `Consume messages from topic "${topic}" on the ${cluster} Kafka cluster, polling every ${pollRate}ms, in service ${service}.`
}

// Validate a consumer-function spec. `requireServiceNode:false` lets the consumer-group
// creation flow (customTypes/consumerGroup.js) pre-validate the whole spec BEFORE the
// owning service node exists — everything else (cluster, topic, name, group, pollRate)
// is checked identically, so a bad spec fails before any file is written.
export function validateInput(body, { requireServiceNode = true } = {}) {
  if (!isValidSystem(body.system)) throw bad(`unknown system "${body.system}"`)
  const system = body.system
  const manifest = readManifest(system)

  const service = body.service
  if (requireServiceNode) {
    const svcNode = manifest.nodes.find((n) => n.id === service && n.type === 'service')
    if (!svcNode) throw bad(`"${service}" is not an internal service in this system`)
  } else if (typeof service !== 'string' || !service) {
    throw bad('service is required')
  }

  const cluster = body.cluster
  const clusterNode = manifest.nodes.find((n) => n.id === cluster && n.origin === 'create-event-stream')
  if (!clusterNode) throw bad(`"${cluster}" is not an event stream in this system`)

  const name = body.name
  if (typeof name !== 'string' || !FUNCTION_NAME_RE.test(name) || name.length > MAX_NAME) {
    throw bad('function name must start with a letter or underscore and use only letters, digits and underscores')
  }

  const topic = body.topic
  const streams = readStreams(system, cluster)
  const topics = streams && Array.isArray(streams.topics) ? streams.topics : []
  if (typeof topic !== 'string' || !topics.some((t) => t && t.id === topic)) {
    throw bad(`topic "${topic}" does not exist on cluster "${cluster}"`)
  }

  let pollRate = Number(body.pollRate)
  if (!Number.isFinite(pollRate) || pollRate <= 0) pollRate = POLL_DEFAULT
  pollRate = Math.min(POLL_MAX, Math.max(POLL_MIN, Math.round(pollRate)))

  // The Kafka consumer-group id — user-named, defaulting to the historical derived id.
  // Unique per cluster across OTHER functions: two functions sharing a group would steal
  // each other's messages (each message goes to one member of a group), which is never
  // what defining two separately-described functions means.
  let groupId = typeof body.groupId === 'string' ? body.groupId.trim() : ''
  if (!groupId) groupId = groupIdFor(service, name)
  if (!GROUP_RE.test(groupId) || groupId.length > MAX_GROUP) {
    throw bad('consumer group must use only letters, digits, dot, underscore and hyphen (max 100)')
  }
  const taken = readConsumers(system).consumers.some(
    (c) => c && c.cluster === cluster && c.groupId === groupId && !(c.service === service && c.name === name),
  )
  if (taken) throw bad(`consumer group "${groupId}" is already used by another consumer function on "${cluster}"`)

  let description = typeof body.description === 'string' ? body.description : ''
  if (description.length > MAX_DESC) throw bad('description is too long')

  const conversationId = typeof body.conversationId === 'string' ? body.conversationId : ''

  // `downstream` is Claude-managed connection metadata (the node ids this consumer's loop
  // calls/reads/writes), edited directly in consumers.json after the loop is implemented — NOT
  // part of the spec the modal POSTs. Only honor it when sent explicitly (filtered to real node
  // ids); when the body omits it, leave it OFF the returned input so the upsert PRESERVES the
  // existing value on edit instead of wiping a populated trace. Mirrors endpoints.js.
  const nodeIds = new Set(manifest.nodes.map((n) => n.id))
  const downstream = Array.isArray(body.downstream)
    ? body.downstream.filter((d) => nodeIds.has(d))
    : undefined

  return { system, manifest, service, cluster, name, topic, pollRate, groupId, downstream, description, conversationId }
}

// --- operations -----------------------------------------------------------------

function listConsumers(system) {
  return { ok: true, consumers: readConsumers(system).consumers }
}

// The group-lag metric card a consumer-group service carries next to its HTTP cards —
// kafka-exporter labels lag by consumergroup, so the query follows the group id.
function groupLagMetric(cluster, groupId) {
  return {
    label: 'lag',
    query: `sum(kafka_consumergroup_lag{job="${cluster}",consumergroup="${groupId}"}) or vector(0)`,
    unit: '',
  }
}

// Create or replace the consumer function identified by (service, name). createdAt + implemented
// are preserved on update (Claude owns `implemented`, set true after it writes the loop + rebuilds);
// topic/pollRate/description/conversationId change and updatedAt bumps. Every save appends a history
// snapshot. The streams.json group + manifest edge are reconciled here (no docker rebuild).
// Exported for customTypes/consumerGroup.js, whose onAdd registers the new service's function
// through this same path (identical history/description/registry semantics).
export function upsertConsumer(input) {
  const { system, manifest, service, cluster, name, topic, pollRate, groupId, downstream, conversationId } = input
  const data = readConsumers(system)
  const now = new Date().toISOString()
  const i = data.consumers.findIndex((c) => c && c.service === service && c.name === name)
  const prev = i >= 0 ? data.consumers[i] : null

  // Description: auto-generate the template only when creating with a blank description (never
  // regenerate on edit, or the changelog's append diff breaks). On edit the client sends the
  // already-joined description; store it verbatim.
  let description = input.description
  if (!prev && !description.trim()) description = autoDescription({ service, cluster, topic, pollRate })

  const snapshot = { at: now, description, topic, pollRate, groupId }
  let fn
  if (prev) {
    const history = Array.isArray(prev.history) ? prev.history : []
    fn = {
      ...prev,
      service,
      name,
      cluster,
      topic,
      pollRate,
      groupId,
      // Only an explicit downstream overrides; otherwise ...prev preserves the existing trace.
      ...(downstream !== undefined ? { downstream } : {}),
      description,
      conversationId: conversationId || prev.conversationId || '',
      implemented: prev.implemented === true, // Claude owns this; an edit must not reset it
      updatedAt: now,
      history: [...history, snapshot],
    }
    data.consumers[i] = fn
  } else {
    fn = {
      service,
      name,
      cluster,
      topic,
      pollRate,
      groupId,
      ...(downstream !== undefined ? { downstream } : {}),
      description,
      implemented: false,
      conversationId: conversationId || '',
      createdAt: now,
      updatedAt: now,
      history: [snapshot],
    }
    data.consumers.push(fn)
  }
  writeConsumers(system, data)

  // Mechanical scaffold (no rebuild): streams.json consumer group + manifest edge. An edit
  // that changed the GROUP ID (or, defensively, the cluster) must strip the old group first —
  // setConsumerGroup only reconciles occurrences of the NEW id. Members are every container
  // running the service's code (a scaled service contributes all its replicas).
  const svcNode = manifest.nodes.find((n) => n.id === service)
  if (prev && (prev.groupId !== groupId || prev.cluster !== cluster)) {
    removeConsumerGroup(system, prev.cluster, prev.groupId)
  }
  setConsumerGroup(system, cluster, topic, groupId, svcNode ? serviceCodeContainers(svcNode) : [service])
  let manifestDirty = addConsumerEdge(system, manifest, cluster, service)

  // A consumer-group SERVICE (customTypes/consumerGroup.js) carries its group identity on
  // the node and a live-mounted scaler config — keep both in sync with the registry so a
  // group-id edit propagates to the diagram's lag card and the running scaler (mtime-read,
  // no rebuild).
  if (svcNode?.service_type === 'consumer_group' && !svcNode.instanceOf) {
    if (svcNode.consumerGroup?.cluster !== cluster || svcNode.consumerGroup?.groupId !== groupId) {
      svcNode.consumerGroup = { cluster, groupId }
      svcNode.metrics = [...serviceMetrics(service), groupLagMetric(cluster, groupId)]
      manifestDirty = true
    }
    const scalerFile = path.join(systemDir(system), service, 'scaler.json')
    try {
      const scaler = JSON.parse(fs.readFileSync(scalerFile, 'utf8'))
      if (scaler.groupId !== groupId) {
        scaler.groupId = groupId
        // In place, never tmp+rename — single-file bind mounts pin their inode.
        fs.writeFileSync(scalerFile, JSON.stringify(scaler, null, 2) + '\n')
      }
    } catch {
      /* no scaler yet (or unreadable) — nothing to sync */
    }
  }
  if (manifestDirty) writeManifest(system, manifest)

  return { ok: true, consumer: fn, groupChanged: prev ? prev.groupId !== groupId : false }
}

// Rename the consumer function (service, oldName) -> (service, newName). The name is the
// function's permanent id, so this is its own operation (like the endpoint PUT rename), not an
// upsert (which would create a second function). Updates the consumers.json entry name, moves
// its streams.json consumer group id <service>-<old> -> <service>-<new>, and records a history
// snapshot. The cluster->service manifest edge doesn't reference the name, so it's untouched.
// No docker rebuild here: when the function is `implemented`, the caller launches a session to
// rename the loop (_consume_<name>) + its group_id in app.py and rebuild that one service.
function renameConsumer(body) {
  const { system } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const service = body.service
  if (typeof service !== 'string' || !service) throw bad('service is required')
  const oldName = body.oldName
  if (typeof oldName !== 'string' || !oldName) throw bad('oldName is required')
  const newName = body.newName
  if (typeof newName !== 'string' || !FUNCTION_NAME_RE.test(newName) || newName.length > MAX_NAME) {
    throw bad('new function name must start with a letter or underscore and use only letters, digits and underscores')
  }

  const data = readConsumers(system)
  const i = data.consumers.findIndex((c) => c && c.service === service && c.name === oldName)
  if (i < 0) throw bad(`no consumer function "${oldName}" on "${service}"`)
  const prev = data.consumers[i]
  if (newName === oldName) {
    return { ok: true, consumer: prev, wasImplemented: prev.implemented === true, renamed: false }
  }
  if (data.consumers.some((c) => c && c.service === service && c.name === newName)) {
    throw bad(`a consumer function "${newName}" already exists on ${service}`)
  }

  const now = new Date().toISOString()
  const history = Array.isArray(prev.history) ? prev.history : []
  // The group id follows the rename ONLY when it was the old derived default — a
  // user-named group is the function's stable Kafka identity and survives the rename
  // (its committed offsets stay live; nothing in code changes but the function name).
  const wasDefaultGroup = prev.groupId === groupIdFor(service, oldName)
  const groupId = wasDefaultGroup ? groupIdFor(service, newName) : prev.groupId
  const fn = {
    ...prev,
    name: newName,
    groupId,
    updatedAt: now,
    // A rename snapshot keeps the changelog honest (same spec, recorded `renamedFrom`).
    history: [...history, { at: now, description: prev.description, topic: prev.topic, pollRate: prev.pollRate, groupId, name: newName, renamedFrom: oldName }],
  }
  data.consumers[i] = fn
  writeConsumers(system, data)

  // Move the consumer group in streams.json only when the id actually changed, keeping a
  // consumer-group service's node block + live scaler config in step (mirrors upsert).
  if (wasDefaultGroup) {
    const manifest = readManifest(system)
    const svcNode = manifest.nodes.find((n) => n.id === service)
    removeConsumerGroup(system, prev.cluster, prev.groupId)
    setConsumerGroup(system, prev.cluster, prev.topic, groupId, svcNode ? serviceCodeContainers(svcNode) : [service])
    if (svcNode?.service_type === 'consumer_group' && !svcNode.instanceOf) {
      svcNode.consumerGroup = { cluster: prev.cluster, groupId }
      svcNode.metrics = [...serviceMetrics(service), groupLagMetric(prev.cluster, groupId)]
      writeManifest(system, manifest)
      const scalerFile = path.join(systemDir(system), service, 'scaler.json')
      try {
        const scaler = JSON.parse(fs.readFileSync(scalerFile, 'utf8'))
        scaler.groupId = groupId
        fs.writeFileSync(scalerFile, JSON.stringify(scaler, null, 2) + '\n')
      } catch {
        /* no scaler yet — nothing to sync */
      }
    }
  }

  return { ok: true, consumer: fn, wasImplemented: prev.implemented === true, renamed: true, groupChanged: wasDefaultGroup, oldGroupId: prev.groupId }
}

function deleteConsumer(body) {
  const { system, service, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  if (typeof service !== 'string' || !service) throw bad('service is required')
  if (typeof name !== 'string' || !name) throw bad('name is required')

  const data = readConsumers(system)
  const gone = data.consumers.find((c) => c && c.service === service && c.name === name)
  const remaining = data.consumers.filter((c) => !(c && c.service === service && c.name === name))
  const removed = remaining.length !== data.consumers.length
  writeConsumers(system, { consumers: remaining })

  if (gone) {
    removeConsumerGroup(system, gone.cluster, gone.groupId)
    const manifest = readManifest(system)
    if (removeConsumerEdge(system, manifest, gone.cluster, service, remaining)) writeManifest(system, manifest)
  }
  return { ok: true, removed, wasImplemented: gone?.implemented === true, groupId: gone?.groupId }
}

export default function consumers() {
  const json = (res, code, b) => {
    res.statusCode = code
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(b))
  }
  return {
    name: 'consumers',
    configureServer(server) {
      server.middlewares.use('/api/consumers', async (req, res, next) => {
        try {
          if (req.method === 'GET') {
            const system = new URL(req.url, 'http://localhost').searchParams.get('system')
            if (!isValidSystem(system)) return json(res, 400, { ok: false, error: 'unknown system' })
            return json(res, 200, listConsumers(system))
          }
          if (req.method === 'POST') {
            const input = validateInput(await readJsonBody(req))
            return json(res, 200, upsertConsumer(input))
          }
          if (req.method === 'PUT') {
            // Rename (service, oldName) -> (service, newName); identity-only change.
            return json(res, 200, renameConsumer(await readJsonBody(req)))
          }
          if (req.method === 'DELETE') {
            return json(res, 200, deleteConsumer(await readJsonBody(req)))
          }
          return next()
        } catch (err) {
          return json(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
