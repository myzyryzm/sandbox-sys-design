// Shared metadata for redis KEYSPACES (the `keyspaces` block on a type:"redis"
// manifest node, managed by /api/redis — see frontend/server/redisKeyspaces.js).
// Used by the diagram rows/trace, the Keyspaces edit tab, and the create-database
// modal. Mirrors the server-side constants in frontend/server/databases.js.

// Row badge per declared type — ≤4 chars so it sits like the HTTP-verb badges.
export const REDIS_BADGE = {
  string: 'STR',
  list: 'LIST',
  set: 'SET',
  hash: 'HSET',
  zset: 'ZSET',
  stream: 'STRM',
  geo: 'GEO',
}

export const REDIS_KS_TYPES = Object.keys(REDIS_BADGE)

// Canonical verb per type, used as the trace-edge labels (writer → redis and
// redis → reader) so the arrows read like the actual redis calls.
export const REDIS_WRITE_VERB = {
  string: 'SET', list: 'RPUSH', set: 'SADD', hash: 'HSET', zset: 'ZADD', stream: 'XADD', geo: 'GEOADD',
}
export const REDIS_READ_VERB = {
  string: 'GET', list: 'LRANGE', set: 'SMEMBERS', hash: 'HGETALL', zset: 'ZRANGE', stream: 'XREAD', geo: 'GEOSEARCH',
}

// Client-side mirrors of the server's validation (friendly pre-checks only —
// the backend re-validates).
export const REDIS_KS_RE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/
export const REDIS_SHORTHAND_RE = /^[A-Za-z][A-Za-z0-9_-]{0,31}$/

// TYPE reports geo data as zset (GEO is a zset encoding), so that pair isn't drift.
export const redisTypesCompatible = (declared, observed) =>
  declared === observed || (declared === 'geo' && observed === 'zset')

// What a keyspace row displays: the shorthand when set, else the raw key name
// (a prefix keyspace reads naturally as `tokens:`).
export const keyspaceLabel = (ks) => ks.shorthand || ks.name

// A writer's declared write mode: `{ mode:'wait', numreplicas, timeoutMs, … }` for
// pseudo-synchronous (WAIT) writers, null for the async default.
export const writeModeOf = (ks, writerId) => (writerId && ks?.writeModes?.[writerId]) || null

// The trace-edge label for one arrow, e.g. `XADD tokens:*` / `GET presence:*`.
// A wait-mode writer's arrow also shows its WAIT contract:
// `SET session:* +WAIT(1,500ms)` — the write blocks until 1 replica acks (≤500ms).
export const keyspaceEdgeLabel = (ks, direction, writerId) => {
  const verb = (direction === 'write' ? REDIS_WRITE_VERB : REDIS_READ_VERB)[ks.type] || direction
  const base = `${verb} ${ks.name}${ks.match === 'prefix' ? '*' : ''}`
  const wm = direction === 'write' ? writeModeOf(ks, writerId) : null
  return wm?.mode === 'wait' ? `${base} +WAIT(${wm.numreplicas},${wm.timeoutMs}ms)` : base
}
