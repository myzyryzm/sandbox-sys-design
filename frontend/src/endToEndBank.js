// Builds the terse prompt seeded into the Claude session that RUNS an end-to-end process. The
// process definition (client_list / failure_list / constraint_list) already lives in
// systems/<id>/endtoend.json — the session reads it from there — so this stays short (the terminal
// slices the positional prompt to 8000 chars and it rides a WebSocket query param). The repeatable
// procedure lives in the sandbox-end-to-end-process skill; here we only name the target + duration.
export function buildEndToEndRunPrompt({ systemId, processId, processName, durationSeconds, apiBase }) {
  return [
    `Use the sandbox-end-to-end-process skill to RUN the end-to-end process "${processName}" in the "${systemId}" system.`,
    ``,
    `Process id: ${processId}`,
    `Run duration: ${durationSeconds} seconds`,
    `Control-plane base URL (poll this for early-stop): ${apiBase}`,
    ``,
    `Read the process definition from systems/${systemId}/endtoend.json (find the entry with this id):`,
    `it lists client_list (methods + how often to call each, in seconds), constraint_list (rules of`,
    `the valid world you must UPHOLD — seed any out-of-scope preconditions they imply and only use`,
    `legal inputs) and failure_list (states that mean the system is broken / poorly designed — probe`,
    `for them).`,
    ``,
    `Coordinate the whole run per the skill: FIRST seed the external/out-of-scope data the constraints`,
    `and methods require (the system often won't create it itself — e.g. accounts/orders — write it`,
    `into the datastores with docker compose exec, keeping referential integrity). THEN call each`,
    `client method at its rate for the duration (synthesizing legal arguments that reference the`,
    `seeded ids, chaining responses), probe the datastore/metrics/call log for the failure states,`,
    `and each loop tick also poll`,
    `GET ${apiBase}/api/endtoend?system=${systemId} and stop early if run.running is false or run.id`,
    `is no longer "${processId}". When done, print a PASS/FAIL report, write the run report JSON,`,
    `POST ${apiBase}/api/endtoend/stop {"system":"${systemId}","id":"${processId}"}, then print the`,
    `completion sentinel as your final line.`,
  ].join('\n')
}
