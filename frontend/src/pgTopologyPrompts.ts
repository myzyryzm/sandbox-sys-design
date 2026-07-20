// Prompt builder for the launched Claude session that RETROFITS the services attached to a
// postgres node after its topology changed (Topology tab apply). The containers, scrape
// jobs, watcher and manifest block are already reconciled by POST /api/postgres/topology —
// the session's job is ONLY the judgment work: point every service that talks to the
// database at a multi-host DSN so it survives a failover, optionally route read-only work
// to the standbys, then rebuild the touched services. The repeatable procedure lives in the
// sandbox-postgres-topology skill.

import type { PostgresHaBlock } from './types/manifest'

export function buildPgTopologyRetrofitPrompt({
  systemId, dbId, mode, ha, replicas, services, dsn,
}: {
  systemId: string
  dbId: string
  mode: string
  ha?: PostgresHaBlock | null
  replicas?: unknown
  services?: string[] | null
  dsn?: { readWrite?: string; readOnly?: string } | null
}): string {
  void replicas
  const svc = services || []
  const members = ha?.members || [dbId]
  const sync = ha?.sync || {}
  const syncStandbys = sync.standbys || []

  const lines = [
    `Use the sandbox-postgres-topology skill to RETROFIT the services that use postgres "${dbId}"`,
    `in the "${systemId}" system — its topology just changed to ${mode.toUpperCase()}.`,
    '',
    'The containers, scrape jobs, failover watcher and manifest block are ALREADY reconciled by',
    'the web app. Your job is only the service code: the connection strings, then rebuild.',
    '',
  ]

  if (mode === 'replicated') {
    lines.push(
      `Topology now: ${members.length} members — ${members.join(', ')} (each a real postgres container).`,
      `A "${ha?.watcher || `${dbId}-failover`}" watcher promotes the most caught-up standby when the primary`,
      `dies (after ${ha?.downAfterMs ?? 5000}ms), repoints the survivors, and fences a returning stale primary`,
      'read-only so it cannot take writes.',
      syncStandbys.length
        ? `Synchronous replication: ${sync.method || 'ANY'} ${sync.quorum ?? 1} (${syncStandbys.join(', ')}), synchronous_commit=${sync.commitLevel || 'on'}.`
        : 'Replication is fully asynchronous (no standby is marked synchronous).',
      '',
      'THE ONE CHANGE THAT MATTERS — every service must connect with a MULTI-HOST DSN.',
      'A hardcoded single-host DSN keeps dialing the old primary and fails after a failover.',
      'libpq tries the hosts in order and target_session_attrs decides which it settles on, so',
      'failover needs no reconnect logic, no retry loop and no code beyond the string itself:',
      '',
      `  WRITES (and anything transactional):`,
      `    ${dsn?.readWrite || ''}`,
      '',
      `  READ-ONLY work you want served by a standby (optional — accepts replica lag):`,
      `    ${dsn?.readOnly || ''}`,
      '',
      'Notes that decide correctness:',
      '- target_session_attrs=read-write makes libpq skip any host answering `SHOW transaction_read_only`',
      '  = on. That is exactly how a FENCED ex-primary is avoided — do not defeat it by pinning a host.',
      '- Keep connect_timeout small (2s) or a dead host stalls every request while libpq waits on it.',
      '- Do NOT route writes to the read-only DSN: a standby rejects them outright.',
    )
  } else {
    lines.push(
      `Topology now: a single standalone container "${dbId}" on port 5432 — no standbys, no watcher.`,
      '',
      `Strip any multi-host DSN back to the plain single-host form:`,
      `    postgresql://sandbox:sandbox@${dbId}:5432/${dbId.replace(/-/g, '_')}`,
      'and remove any read-only/standby DSN and the read/write split that used it.',
    )
  }

  lines.push('', 'Services that talk to this database (from endpoints.json / consumers.json downstream):')
  if (svc.length) {
    for (const s of svc) lines.push(`  - ${s}`)
    lines.push(
      '',
      'Grep each one for the existing DSN constant (e.g. `LEDGER_DB_DSN`, `psycopg.connect(`) — the',
      'host is usually hardcoded in a module-level `os.environ.get(..., "postgresql://…")` default.',
      '',
      'Rebuild ONLY the touched services:',
      `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${svc.join(' ')}`,
    )
  } else {
    lines.push('  (none recorded — verify with a repo grep that nothing else connects, then stop)')
  }
  lines.push(
    '',
    "Then run the skill's ## Verify: drive a write through the lb, kill the primary, and confirm the",
    'SAME endpoint still succeeds against the promoted standby.',
  )
  return lines.join('\n')
}
