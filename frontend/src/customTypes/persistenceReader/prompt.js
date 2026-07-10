// Prompts seeding the launched Claude sessions that author (or update) a persistence
// reader group's poll/persist loop. The repeatable procedure — the XREADGROUP claim
// loop, the typed token-stream contract, the ack-after-persist rule, the metrics
// contract — lives in the sandbox-llm-persistence skill, so these stay short. The
// persistence.json entry, compose env and manifest node already exist (written by
// POST /api/custom-services); the session writes the code + rebuilds the service.

function targetLines({ db, table, field, freeform }) {
  if (freeform) {
    return [
      `Persist target: SPECIALIZED implementation — no structured table/field target. Follow this spec:`,
      freeform.trim(),
    ]
  }
  return [
    `Persist target: database "${db}" (env DB_NODE), table/collection "${table}", field/column "${field}".`,
    `Fill the target row's OTHER required columns with judgment (e.g. for a chat-db "message" row:`,
    `derive chat_id from the user_message row whose id == run_id, set role='assistant', mint an id).`,
  ]
}

export function buildPersistencePrompt({ systemId, service, worker, stream, group, db, table, field, freeform, description, editing, priorDescription }) {
  const lines = [
    `Use the sandbox-llm-persistence skill to ${editing ? 'UPDATE' : 'IMPLEMENT'} the persistence reader group "${service}" in the "${systemId}" system.`,
    '',
    `Reader group "${service}" — persists finished generations of LLM worker "${worker}".`,
    `Announcement stream: "runs:started" on redis "${stream}" (env REDIS_HOST / ANNOUNCE_STREAM).`,
    `Redis consumer group: "${group}" (env READER_GROUP); consumer name = os.environ["SERVICE_ID"] (unique per member).`,
    ...targetLines({ db, table, field, freeform }),
    '',
  ]
  if (!editing) {
    lines.push(
      `The service "${service}" was JUST created for this group (fresh FastAPI code from the plain`,
      `template, container already built and running). Its compose entry ALREADY sets the env above —`,
      `do NOT edit docker-compose.yml. The group scales to N member containers that all run this same`,
      `loop; the group divides runs:started announcements across them, one reader per run.`,
      '',
    )
  } else {
    lines.push(
      `This reader group ALREADY EXISTS and is implemented in systems/${systemId}/${service}/app.py.`,
      `FIRST read it, then MODIFY it in place to match the target above. Keep the metrics middleware`,
      `and every other route/loop untouched.`,
      '',
      `Current behavior (existing description):`,
      (priorDescription || '').trim() || '(none recorded)',
      '',
    )
  }
  lines.push(
    `What it should do:`,
    (description || '').trim() || '(no description — accumulate each run and persist it sensibly)',
    '',
    `Per the skill's canonical reading algorithm:`,
    `- A background loop claims announcements: XREADGROUP on "runs:started" (group "${group}",`,
    `  consumer SERVICE_ID, count=1, block~5000ms; XGROUP CREATE mkstream first, BUSYGROUP ignored).`,
    `- For each claimed run_id, accumulate its typed token stream tokens:<run_id> (entries`,
    `  {type: token|done|error, text} — key off type, never the text): done → persist the joined`,
    `  text as complete; error → persist as failed; ~30s with no new entries → persist as partial.`,
    `- XACK the announcement only AFTER persisting (crash-safe); re-EXPIRE tokens:<run_id> when done.`,
    `- Export the metrics contract (persistence_runs_total{status}, persistence_active_runs) and add`,
    `  GET /reader/state returning at least { group: "${group}" } plus live counters.`,
    `- Rebuild ONLY this service:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${service}`,
    `- Then set "implemented": true on this entry in systems/${systemId}/persistence.json (the one`,
    `  with service "${service}") — via POST /api/custom/persistence-reader/update or a direct edit.`,
  )
  return lines.join('\n')
}
