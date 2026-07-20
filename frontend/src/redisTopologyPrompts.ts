// Prompt builder for the launched Claude session that RETROFITS the services
// attached to a redis node after its topology changed (Topology tab apply). The
// containers/scrape/manifest are already reconciled by POST /api/redis/topology —
// the session's job is ONLY the judgment work: point every declared writer/reader
// at the right client (Sentinel discovery / RedisCluster / plain), wire WAIT
// pseudo-sync writes where declared, and rebuild the touched services. The
// repeatable procedure lives in the sandbox-redis-topology skill.

// The services a topology change affects: every declared writer/reader across the
// node's keyspaces (deduped, keyspace-role annotated for the prompt).
import type { RedisClusterBlock, RedisKeyspace, SentinelBlock } from './types/manifest'

export function affectedServices(keyspaces?: RedisKeyspace[] | null): Map<string, string[]> {
  const byService = new Map<string, string[]>()
  for (const ks of keyspaces || []) {
    for (const svc of ks.writers || []) {
      if (!byService.has(svc)) byService.set(svc, [])
      const wm = ks.writeModes?.[svc]
      byService.get(svc)!.push(
        `WRITES ${ks.name}${ks.match === 'prefix' ? '*' : ''} (${ks.type})` +
          (wm?.mode === 'wait' ? ` [write mode: WAIT numreplicas=${wm.numreplicas} timeout=${wm.timeoutMs}ms]` : ''),
      )
    }
    for (const svc of ks.readers || []) {
      if (!byService.has(svc)) byService.set(svc, [])
      byService.get(svc)!.push(`READS ${ks.name}${ks.match === 'prefix' ? '*' : ''} (${ks.type})`)
    }
  }
  return byService
}

export function buildRedisTopologyRetrofitPrompt({
  systemId,
  redisId,
  mode,
  sentinel,
  cluster,
  replicas,
  keyspaces,
}: {
  systemId: string
  redisId: string
  mode: string
  sentinel?: SentinelBlock | null
  cluster?: RedisClusterBlock | null
  replicas?: Array<{ id: string }> | null
  keyspaces?: RedisKeyspace[] | null
}): string {
  const roles = affectedServices(keyspaces)
  const services = [...roles.keys()]
  const lines = [
    `Use the sandbox-redis-topology skill to RETROFIT the services attached to redis "${redisId}"`,
    `in the "${systemId}" system — its topology just changed to ${mode.toUpperCase()}.`,
    '',
    'The containers, scrape jobs and manifest blocks are ALREADY reconciled by the web app.',
    'Your job is only the service code: the right client wiring per the skill, then rebuild.',
    '',
  ]
  if (mode === 'replicated') {
    lines.push(
      `Topology now: primary "${redisId}" + ${replicas?.length || 0} read replica(s)` +
        `${replicas?.length ? ` (${replicas.map((r) => r.id).join(', ')})` : ''},`,
      `monitored by Redis Sentinel: ${(sentinel?.members || []).join(', ')} on port 26379,`,
      `master name "${sentinel?.masterName || redisId}", quorum ${sentinel?.quorum ?? 2}.`,
    )
  } else if (mode === 'cluster') {
    lines.push(
      `Topology now: Redis Cluster "${redisId}" — ${cluster?.shards} shards × ${1 + (cluster?.replicasPerShard || 0)} nodes,`,
      `members ${(cluster?.members || []).join(', ')} on port 6379 (16384 hash slots, no "${redisId}" container).`,
    )
  } else {
    lines.push(`Topology now: a single standalone container "${redisId}" on port 6379.`)
  }
  lines.push('', 'Affected services and their declared keyspace roles:')
  if (services.length) {
    for (const [svc, notes] of roles) lines.push(`  - ${svc}: ${notes.join(', ')}`)
  } else {
    lines.push('  (none declared — verify with a repo grep that nothing else connects, then stop)')
  }
  lines.push('', 'For each service, per the skill\'s client contracts:')
  if (mode === 'replicated') {
    lines.push(
      `- WRITES go through sentinel discovery — redis.sentinel.Sentinel([("${(sentinel?.members || [`${redisId}-sentinel-1`])[0]}", 26379), …])`,
      `  .master_for("${sentinel?.masterName || redisId}") — never a hardcoded "${redisId}" host: after a failover the`,
      '  promoted replica is the master and writes to the old hostname fail read-only.',
      `- READS may use slave_for("${sentinel?.masterName || redisId}") (read scaling) or the master handle.`,
    )
  } else if (mode === 'cluster') {
    lines.push(
      `- Replace redis.Redis(host="${redisId}") with redis.cluster.RedisCluster(startup_nodes=[ClusterNode("${(cluster?.members || [])[0] || `${redisId}-1`}", 6379), …])`,
      '  (needs redis>=4.3 in requirements.txt). The client follows MOVED redirects; multi-key',
      '  operations only work within one hash slot — co-locate related keys with {hash} tags.',
    )
  } else {
    lines.push(
      `- Strip any Sentinel/RedisCluster client code back to a plain redis.Redis(host="${redisId}", port=6379).`,
    )
  }
  lines.push(
    '- Every writer with a declared WAIT write mode: call r.wait(numreplicas, timeoutMs) immediately',
    '  after each write to that keyspace (pseudo-synchronous replication ack — log a degraded ack',
    '  when the return value is below numreplicas). Writers without one stay fire-and-forget.',
    '',
    'Rebuild ONLY the touched services:',
    `    docker compose -f systems/${systemId}/docker-compose.yml up -d --build ${services.join(' ') || '<service>'}`,
    '',
    'Verify per the skill\'s ## Verify, then re-run the keyspace scan so wait-mode writers show',
    `implemented (POST /api/redis/scan {"system":"${systemId}","id":"${redisId}"} detects the WAIT call).`,
  )
  return lines.join('\n')
}
