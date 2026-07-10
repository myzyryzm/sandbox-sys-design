// Prompt builders for the launched Claude session that implements (or updates / deletes)
// a service's etcd LISTENER — a watch_prefix loop keeping a live in-memory view of a
// keyspace (etcd pushes updates; no polling). The repeatable procedure lives in the
// sandbox-etcd skill. Shared by the etcd node's Keyspaces tab (EtcdKeyspacesTab, which
// manages listeners keyspace-first) and the per-service Subscribers tab
// (ServiceSubscribersTab, service-first) so both build identical prompts.

// The per-event handler name: Python snake_case, `on_`-prefixed, matching the diagram's
// SUB-row label (onName → onLlmWorker; the authored code → on_llm_worker).
const handlerName = (id) => 'on_' + String(id).toLowerCase().replace(/[^a-z0-9]+/g, '_')

// A LISTENER on a DISCOVERY keyspace: watch_prefix loop keeping a live in-memory worker map.
export function buildListenerPrompt({ systemId, etcdId, keyspaceService, listener, prefix, description, editing, priorDescription }) {
  const handler = handlerName(keyspaceService)
  const lines = [
    `Use the sandbox-etcd skill to ${editing ? 'UPDATE' : 'IMPLEMENT'} an etcd LISTENER in the "${systemId}" system:`,
    `service "${listener}" watching keyspace ${prefix} (the "${keyspaceService}" workers) on cluster "${etcdId}".`,
    '',
  ]
  if (editing) {
    lines.push(
      `This listener ALREADY EXISTS in systems/${systemId}/${listener}/app.py. FIRST read it, then`,
      `modify it in place. Keep the metrics middleware and every other route/loop untouched.`,
      '',
      `Current behavior (existing description):`,
      (priorDescription || '').trim() || '(none recorded)',
      '',
    )
  }
  lines.push(
    `What it should do — this is the body of the per-event handler ${handler}(...):`,
    (description || '').trim() || `(no per-event behavior — just keep a live worker map of ${keyspaceService}; the handler can be a no-op)`,
    '',
    `Per the skill's "Watcher loop" contract:`,
    `- Add a daemon-thread watcher to systems/${systemId}/${listener}/app.py: on (re)connect do an`,
    `  initial get_prefix("${prefix}") into a module-level worker map, then watch_prefix — etcd`,
    `  PUSHES every change (a PUT adds/updates a worker; a DELETE or lease expiry removes it).`,
    `  Never poll. Resync from scratch on any watch error. Endpoints come from ETCD_ENDPOINTS`,
    `  (already in the compose def).`,
    `- KEEP maintaining that live worker map exactly as before — it is the non-negotiable base.`,
    `- ON TOP of the map, author  def ${handler}(event_type, key, value, workers):  and invoke it`,
    `  INSIDE _apply, once per event, AFTER the map is updated — "put"/"delete" for event_type, the`,
    `  new value (None on delete) for value, the live map for workers. Run the behavior above on`,
    `  every pushed change; do NOT call it for the baseline get_prefix keys (pushed events only).`,
    `- Guard it: the handler runs on the watch's callback thread, so it must NEVER block or raise —`,
    `  wrap the call in a _fire(...) try/except that logs and swallows, or an unguarded throw kills`,
    `  the watch and the anti-entropy sweep resync-storms.`,
    `- Expose the debug route GET /discovery/${keyspaceService} returning the current map, per the skill.`,
    `- Add the pinned etcd client deps to systems/${systemId}/${listener}/requirements.txt.`,
    `- Rebuild ONLY that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${listener}`,
    `- Verify per the skill (kill a ${keyspaceService} worker → it drops from the map within the TTL`,
    `  and ${handler} fires once with "delete"), then set "implemented": true on this listener entry`,
    `  (keyspace "${keyspaceService}", service "${listener}") in systems/${systemId}/etcd.json.`,
  )
  return lines.join('\n')
}

// Delete: registry + compose scrub already done by the DELETE; the session strips the code.
export function buildListenerDeletePrompt({ systemId, etcdId, keyspaceService, listener, prefix }) {
  const handler = handlerName(keyspaceService)
  return [
    `Use the sandbox-etcd skill to DELETE an etcd listener in the "${systemId}" system: service`,
    `"${listener}" no longer watches ${prefix} (the "${keyspaceService}" workers) on cluster "${etcdId}".`,
    '',
    `Its listener entry in etcd.json is already removed. Strip the watch loop, its ${handler} handler`,
    `(and the _fire helper if nothing else uses it), and the GET /discovery/${keyspaceService} route`,
    `from systems/${systemId}/${listener}/app.py, leaving the metrics middleware and every other`,
    `route/loop intact, then rebuild only that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${listener}`,
  ].join('\n')
}

// A LISTENER on a CONFIG keyspace: the same watcher shape, but the map holds config values
// (the app writes them via etcdctl — persistent keys, no lease, no registration half).
export function buildConfigListenerPrompt({ systemId, etcdId, keyspaceName, listener, prefix, description, editing, priorDescription }) {
  const handler = handlerName(keyspaceName)
  const lines = [
    `Use the sandbox-etcd skill to ${editing ? 'UPDATE' : 'IMPLEMENT'} an etcd CONFIG LISTENER in the "${systemId}" system:`,
    `service "${listener}" watching config keyspace ${prefix} on cluster "${etcdId}".`,
    '',
  ]
  if (editing) {
    lines.push(
      `This listener ALREADY EXISTS in systems/${systemId}/${listener}/app.py. FIRST read it, then`,
      `modify it in place. Keep the metrics middleware and every other route/loop untouched.`,
      '',
      `Current behavior (existing description):`,
      (priorDescription || '').trim() || '(none recorded)',
      '',
    )
  }
  lines.push(
    `What it should do — this is the body of the per-event handler ${handler}(...):`,
    (description || '').trim() || '(no per-event behavior — just keep a live config map; the handler can be a no-op)',
    '',
    `Per the skill's "Config watcher loop" contract:`,
    `- Add a daemon-thread watcher to systems/${systemId}/${listener}/app.py: on (re)connect do an`,
    `  initial get_prefix("${prefix}") into a module-level CONFIG dict (key -> value), then`,
    `  watch_prefix — etcd PUSHES every change (a PUT adds/updates a key; a DELETE removes it).`,
    `  Never poll. These are PERSISTENT keys the web app writes (no lease — a DeleteEvent only ever`,
    `  means an explicit delete). After a cluster recreation the app replays the values, so resync`,
    `  from scratch on any watch error and keep the skill's 30s anti-entropy sweep. Endpoints come`,
    `  from ETCD_ENDPOINTS (already in the compose def).`,
    `- KEEP maintaining that live config map exactly as before — it is the non-negotiable base.`,
    `- ON TOP of the map, author  def ${handler}(event_type, key, value, config):  and invoke it`,
    `  INSIDE _apply, once per event, AFTER the map is updated — "put"/"delete" for event_type, the`,
    `  new value (None on delete) for value, the live map for config. Run the behavior above on every`,
    `  pushed change; do NOT call it for the baseline get_prefix keys (pushed events only).`,
    `- Guard it: the handler runs on the watch's callback thread, so it must NEVER block or raise —`,
    `  wrap the call in a _fire(...) try/except that logs and swallows, or an unguarded throw kills`,
    `  the watch and the anti-entropy sweep resync-storms.`,
    `- Expose the debug route GET /config/${keyspaceName} returning the current map, per the skill.`,
    `- Add the pinned etcd client deps to systems/${systemId}/${listener}/requirements.txt.`,
    `- Rebuild ONLY that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${listener}`,
    `- Verify per the skill (edit a value in the Keyspaces tab -> the map updates within watch latency`,
    `  and ${handler} fires once with "put"), then set "implemented": true on this listener entry`,
    `  (keyspace "${keyspaceName}", service "${listener}") in systems/${systemId}/etcd.json.`,
  )
  return lines.join('\n')
}

export function buildConfigListenerDeletePrompt({ systemId, etcdId, keyspaceName, listener, prefix }) {
  const handler = handlerName(keyspaceName)
  return [
    `Use the sandbox-etcd skill to DELETE an etcd config listener in the "${systemId}" system: service`,
    `"${listener}" no longer watches ${prefix} on cluster "${etcdId}".`,
    '',
    `Its listener entry in etcd.json is already removed. Strip the watch loop, its ${handler} handler`,
    `(and the _fire helper if nothing else uses it), and the GET /config/${keyspaceName} route from`,
    `systems/${systemId}/${listener}/app.py, leaving the metrics middleware and every other route/loop`,
    `intact, then rebuild only that service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${listener}`,
  ].join('\n')
}
