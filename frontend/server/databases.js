// Vite dev-server plugin: provision a real database into the active system.
//
// POST /api/databases  { system, type, name, entities }
//
// Given a database type and a set of entities, this:
//   1. writes an init script that creates those entities,
//   2. adds the DB service (+ a Prometheus exporter, or native metrics for
//      MinIO) to the system's docker-compose.yml,
//   3. adds a scrape job to the system's prometheus.yml,
//   4. adds a node to the system's manifest.json (no edge — the new DB is NOT
//      auto-wired to any service; that's a deliberate separate step), and
//   5. rebuilds the stack with `docker compose up -d` (NOT ./start.sh, which
//      would kill the very dev server this code runs inside).
//
// The compose/prometheus edits go through the `yaml` Document API so the
// hand-written comments in those files survive. All user input is validated
// against strict whitelists and only ever lands in generated files — never in a
// shell argument.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parseDocument } from 'yaml'
import { repoRoot, systemsDir, systemDir, isValidSystem, nextNodePosition } from './systems.js'
import { readModelsFile } from './models.js'

const pexec = promisify(execFile)

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

export class HttpError extends Error {
  constructor(status, message) {
    super(message)
    this.statusCode = status
  }
}
export const bad = (msg) => new HttpError(400, msg)

const DB_NAME_RE = /^[a-z][a-z0-9-]*$/ // also a valid compose service name
const IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/ // SQL/Mongo identifier-ish
const BUCKET_RE = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/ // S3-ish bucket name

// Redis keyspaces (key namespaces). Unlike IDENT_RE this allows ':' — the redis
// namespacing convention (`tokens:<id>`) — while excluding whitespace, quotes and
// shell metacharacters so a name is safe inside the generated seed script and in
// execFile arg arrays. Shared with the /api/redis plugin (redisKeyspaces.js).
export const REDIS_KS_RE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/
export const REDIS_SHORTHAND_RE = /^[A-Za-z][A-Za-z0-9_-]{0,31}$/
export const REDIS_KS_TYPES = new Set(['string', 'list', 'set', 'hash', 'zset', 'stream', 'geo'])

// Redis persistence (RDB + AOF). A redis node with no `persistence` manifest block
// runs the image defaults below; the block is written by /api/redis/persistence
// (redisPersistence.js) and read back here so EVERY builder that (re)generates a
// redis data container — standalone, replica, cluster member — bakes the same
// flags into its compose command. Shared with redisTopology.js / replicas.js.
export const REDIS_PERSISTENCE_DEFAULTS = {
  rdb: {
    enabled: true,
    rules: [
      { seconds: 3600, changes: 1 },
      { seconds: 300, changes: 100 },
      { seconds: 60, changes: 10000 },
    ],
  },
  aof: { enabled: false, fsync: 'everysec', rewritePercent: 100, rewriteMinMb: 64 },
}
export const REDIS_PERSISTENCE_LIMITS = {
  maxRules: 4,
  secondsMin: 1, secondsMax: 86400,
  changesMin: 1, changesMax: 1000000,
  rewritePercentMin: 0, rewritePercentMax: 1000, // 0 disables auto-rewrite
  rewriteMinMbMin: 1, rewriteMinMbMax: 1024,
  fsync: ['always', 'everysec', 'no'],
}

// Compose `command:` flags for a persistence block; [] when the node has none
// (bare image defaults). The multi-pair save value is ONE argv token — redis 7
// accepts `save "3600 1 300 100"` as a single directive; "" clears all save points.
export function redisPersistenceFlags(p) {
  if (!p) return []
  const save = p.rdb.enabled ? p.rdb.rules.map((r) => `${r.seconds} ${r.changes}`).join(' ') : ''
  const flags = ['--save', save, '--appendonly', p.aof.enabled ? 'yes' : 'no']
  if (p.aof.enabled) {
    flags.push(
      '--appendfsync', p.aof.fsync,
      '--auto-aof-rewrite-percentage', String(p.aof.rewritePercent),
      '--auto-aof-rewrite-min-size', `${p.aof.rewriteMinMb}mb`,
    )
  }
  return flags
}

const SQL_FIELD_TYPES = new Set([
  'text', 'varchar', 'integer', 'bigint', 'numeric', 'boolean',
  'timestamp', 'timestamptz', 'date', 'uuid', 'jsonb', 'serial', 'bigserial',
])
const MONGO_FIELD_TYPES = new Set([
  'string', 'number', 'boolean', 'date', 'objectId', 'object', 'array',
])

// Health rule shared by every DB node: red when the target is down, green up.
export const HEALTH_RULES = [
  { color: 'red', when: 'value < 1' },
  { color: 'green', when: 'value >= 1' },
]

// Engines whose schema can be authored from the model bank by a launched session.
export const MODEL_ENGINES = new Set(['postgres', 'mongodb', 'dynamodb', 'cassandra'])

export function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk
      if (data.length > 1_000_000) reject(bad('request body too large'))
    })
    req.on('end', () => {
      if (!data) return resolve({})
      try {
        resolve(JSON.parse(data))
      } catch {
        reject(bad('invalid JSON body'))
      }
    })
    req.on('error', reject)
  })
}

// ---------------------------------------------------------------------------
// Per-type templates
//
// Each builder returns everything needed to splice one database into a system:
//   services  — map of compose service name -> service definition object
//   scrapeJob — a prometheus scrape_configs entry
//   metrics   — manifest node metrics[]
//   health    — manifest node health{}
//   nodeType  — manifest node `type` (shown on the diagram)
//   files     — init scripts to write under systems/<id>/<name>/
// `name` is the DB node id / primary service name; `job` (== name) labels the
// scrape so multiple DBs of the same engine stay distinguishable in PromQL.
// ---------------------------------------------------------------------------

function sqlColumns(entity) {
  const fields = entity.fields || []
  if (fields.length === 0) return ['  "id" bigserial primary key']
  return fields.map((f) => `  "${f.name}" ${f.type}`)
}

function buildPostgres({ name, dbName, entities }) {
  const tables = entities
    .map((e) => `CREATE TABLE IF NOT EXISTS "${e.name}" (\n${sqlColumns(e).join(',\n')}\n);`)
    .join('\n\n')
  const initSql = `-- Generated by "Add database". Runs once on first init of ${name}.\n${tables}\n`

  return {
    nodeType: 'postgres',
    services: {
      [name]: {
        image: 'postgres:16-alpine',
        environment: {
          POSTGRES_USER: 'sandbox',
          POSTGRES_PASSWORD: 'sandbox',
          POSTGRES_DB: dbName,
        },
        volumes: [`./${name}/init.sql:/docker-entrypoint-initdb.d/init.sql:ro`],
      },
      [`${name}-exporter`]: {
        image: 'quay.io/prometheuscommunity/postgres-exporter:v0.16.0',
        environment: {
          DATA_SOURCE_NAME: `postgresql://sandbox:sandbox@${name}:5432/${dbName}?sslmode=disable`,
        },
        depends_on: [name],
      },
    },
    scrapeJob: { job_name: name, static_configs: [{ targets: [`${name}-exporter:9187`] }] },
    metrics: [
      { label: 'connections', query: `sum(pg_stat_database_numbackends{job="${name}"})`, unit: '' },
      { label: 'commits/s', query: `sum(rate(pg_stat_database_xact_commit{job="${name}"}[1m]))`, unit: '/s' },
      { label: 'rows fetch/s', query: `sum(rate(pg_stat_database_tup_fetched{job="${name}"}[1m]))`, unit: '/s' },
    ],
    health: { query: `pg_up{job="${name}"}`, rules: HEALTH_RULES },
    files: [{ rel: 'init.sql', content: initSql }],
  }
}

function mongoSampleValue(type) {
  switch (type) {
    case 'number': return 0
    case 'boolean': return false
    case 'date': return 'new Date()'
    case 'objectId': return 'new ObjectId()'
    case 'object': return '{}'
    case 'array': return '[]'
    default: return '"sample"'
  }
}

function buildMongo({ name, dbName, entities }) {
  const lines = [`// Generated by "Add database". Runs once on first init of ${name}.`]
  for (const e of entities) {
    lines.push(`db.createCollection("${e.name}");`)
    const fields = e.fields || []
    if (fields.length) {
      const doc = fields.map((f) => `  "${f.name}": ${mongoSampleValue(f.type)}`).join(',\n')
      lines.push(`db.${e.name}.insertOne({\n${doc}\n});`)
    }
  }
  const initJs = lines.join('\n') + '\n'

  return {
    nodeType: 'mongodb',
    services: {
      [name]: {
        image: 'mongo:7',
        environment: { MONGO_INITDB_DATABASE: dbName },
        volumes: [`./${name}/init.js:/docker-entrypoint-initdb.d/init.js:ro`],
      },
      [`${name}-exporter`]: {
        image: 'percona/mongodb_exporter:0.40',
        command: [`--mongodb.uri=mongodb://${name}:27017`, '--collect-all', '--compatible-mode'],
        depends_on: [name],
      },
    },
    scrapeJob: { job_name: name, static_configs: [{ targets: [`${name}-exporter:9216`] }] },
    metrics: [
      { label: 'connections', query: `sum(mongodb_ss_connections{job="${name}",conn_type="current"})`, unit: '' },
      { label: 'ops/s', query: `sum(rate(mongodb_ss_opcounters{job="${name}"}[1m]))`, unit: '/s' },
    ],
    health: { query: `mongodb_up{job="${name}"}`, rules: HEALTH_RULES },
    files: [{ rel: 'init.js', content: initJs }],
  }
}

// One type-appropriate seed command per keyspace, so the seeded key's live TYPE
// matches the declaration (a prefix keyspace seeds `<name>sample`, an exact one the
// key itself). Names are validated by REDIS_KS_RE, so they are safe in the quotes.
// `cli` is the redis-cli invocation prefix — plain (`redis-cli -h cache`) or
// cluster-aware (`redis-cli -c -h cache-1`, used by the Topology tab's cluster init).
export function redisSeedCommand(cli, e) {
  const key = e.match === 'prefix' ? `${e.name}sample` : e.name
  switch (e.type) {
    case 'list': return `${cli} RPUSH "${key}" "seed"`
    case 'set': return `${cli} SADD "${key}" "seed"`
    case 'hash': return `${cli} HSET "${key}" sample "seed"`
    case 'zset': return `${cli} ZADD "${key}" 0 "seed"`
    case 'stream': return `${cli} XADD "${key}" '*' sample "seed"`
    case 'geo': return `${cli} GEOADD "${key}" 0 0 "seed"`
    default: return `${cli} SET "${key}" "seed"`
  }
}

// Exported for the Topology tab (redisTopology.js), which rebuilds the standalone
// base (services/metrics/health/seeder) when a cluster is converted back —
// `persistence` is the node's manifest block so a rebuild keeps its RDB/AOF flags.
export function buildRedis({ name, entities, persistence }) {
  // Redis has no init-dir mechanism, so a one-shot sidecar seeds one sample key
  // per keyspace once the server answers PING. With no keyspaces declared there is
  // nothing to seed, so the sidecar is omitted entirely rather than started to do
  // nothing (a later keyspace add is a live manifest edit anyway — no rebuild).
  const seed = entities.length
    ? [
        'set -e',
        `until redis-cli -h ${name} ping | grep -q PONG; do sleep 1; done`,
        ...entities.map((e) => redisSeedCommand(`redis-cli -h ${name}`, e)),
      ].join('\n')
    : null

  // The declared keyspaces persist onto the manifest node (verified: the user just
  // typed them) so the diagram renders typed KEY rows and /api/redis can manage them.
  const ts = new Date().toISOString()
  const keyspaces = entities.map((e) => ({
    name: e.name,
    match: e.match,
    type: e.type,
    ...(e.shorthand ? { shorthand: e.shorthand } : {}),
    writers: [],
    readers: [],
    verified: true,
    origin: 'user',
    suggestedWriters: [],
    suggestedReaders: [],
    createdAt: ts,
    updatedAt: ts,
  }))

  const persistFlags = redisPersistenceFlags(persistence)
  return {
    keyspaces,
    nodeType: 'redis',
    services: {
      [name]: {
        image: 'redis:7-alpine',
        ...(persistFlags.length ? { command: ['redis-server', ...persistFlags] } : {}),
      },
      ...(seed
        ? {
            [`${name}-init`]: {
              image: 'redis:7-alpine',
              depends_on: [name],
              restart: 'no',
              entrypoint: ['sh', '-c', seed],
            },
          }
        : {}),
      [`${name}-exporter`]: {
        image: 'oliver006/redis_exporter:v1.62.0',
        environment: { REDIS_ADDR: `redis://${name}:6379` },
        depends_on: [name],
      },
    },
    scrapeJob: { job_name: name, static_configs: [{ targets: [`${name}-exporter:9121`] }] },
    metrics: [
      { label: 'clients', query: `redis_connected_clients{job="${name}"}`, unit: '' },
      { label: 'ops/s', query: `sum(rate(redis_commands_processed_total{job="${name}"}[1m]))`, unit: '/s' },
      { label: 'keys', query: `sum(redis_db_keys{job="${name}"})`, unit: '' },
    ],
    health: { query: `redis_up{job="${name}"}`, rules: HEALTH_RULES },
    files: [],
  }
}

function buildBlob({ name, entities }) {
  // MinIO exposes Prometheus metrics natively (no exporter); a one-shot `mc`
  // sidecar creates the buckets. Auth set to public so Prometheus can scrape.
  const mc = [
    'set -e',
    `until mc alias set local http://${name}:9000 sandbox sandbox123; do sleep 1; done`,
    ...entities.map((e) => `mc mb -p local/${e.name} || true`),
  ].join('\n')

  return {
    nodeType: 'object-store',
    services: {
      [name]: {
        image: 'minio/minio:latest',
        command: ['server', '/data', '--console-address', ':9001'],
        environment: {
          MINIO_ROOT_USER: 'sandbox',
          MINIO_ROOT_PASSWORD: 'sandbox123',
          MINIO_PROMETHEUS_AUTH_TYPE: 'public',
        },
      },
      [`${name}-init`]: {
        image: 'minio/mc:latest',
        depends_on: [name],
        restart: 'no',
        entrypoint: ['sh', '-c', mc],
      },
    },
    scrapeJob: {
      job_name: name,
      metrics_path: '/minio/v2/metrics/cluster',
      static_configs: [{ targets: [`${name}:9000`] }],
    },
    metrics: [
      { label: 'capacity', query: `sum(minio_cluster_capacity_usable_total_bytes{job="${name}"})`, unit: 'GB', scale: 1e-9 },
      { label: 'S3 req/s', query: `sum(rate(minio_s3_requests_total{job="${name}"}[1m]))`, unit: '/s' },
    ],
    health: { query: `up{job="${name}"}`, rules: HEALTH_RULES },
    files: [],
  }
}

// --- Custom exporters for engines with no off-the-shelf Prometheus exporter ---
// DynamoDB Local and Cassandra don't expose Prometheus metrics that fit the
// "separate exporter container" pattern (Dynamo has none; Cassandra only via
// fragile JMX), so each ships a tiny Python prometheus_client sidecar built from
// these files under <name>/exporter/. Each sets a `<engine>_up` gauge to 1/0 so
// the shared HEALTH_RULES work exactly like the image-based exporters.

const DDB_EXPORTER_DOCKERFILE = `FROM python:3.12-slim
RUN pip install --no-cache-dir boto3 prometheus_client
COPY exporter.py /exporter.py
CMD ["python", "/exporter.py"]
`

const DDB_EXPORTER_PY = `# Tiny Prometheus exporter for DynamoDB Local (no native metrics endpoint).
import os, time
import boto3
from botocore.config import Config
from prometheus_client import start_http_server, Gauge

ENDPOINT = os.environ.get("DDB_ENDPOINT", "http://localhost:8000")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

up = Gauge("dynamodb_up", "DynamoDB endpoint reachable (1) or not (0)")
tables = Gauge("dynamodb_table_count", "Number of tables")
items = Gauge("dynamodb_item_count", "Item count per table", ["table"])
latency = Gauge("dynamodb_probe_latency_seconds", "Latency of the list-tables probe")

CFG = Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 0})

def client():
    return boto3.client(
        "dynamodb", endpoint_url=ENDPOINT, region_name=REGION,
        aws_access_key_id="sandbox", aws_secret_access_key="sandbox", config=CFG)

def count_items(c, name):
    total, kwargs = 0, {"TableName": name, "Select": "COUNT"}
    while True:
        resp = c.scan(**kwargs)
        total += resp.get("Count", 0)
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            return total
        kwargs["ExclusiveStartKey"] = lek

def collect():
    try:
        c = client()
        t0 = time.monotonic()
        names = c.list_tables().get("TableNames", [])
        latency.set(time.monotonic() - t0)
        tables.set(len(names))
        for name in names:
            try:
                items.labels(table=name).set(count_items(c, name))
            except Exception:
                pass
        up.set(1)
    except Exception:
        up.set(0)

if __name__ == "__main__":
    start_http_server(9100)
    while True:
        collect()
        time.sleep(5)
`

const CASS_EXPORTER_DOCKERFILE = `FROM python:3.12-slim
ENV CASS_DRIVER_NO_EXTENSIONS=1
RUN pip install --no-cache-dir cassandra-driver prometheus_client
COPY exporter.py /exporter.py
CMD ["python", "/exporter.py"]
`

const CASS_EXPORTER_PY = `# Tiny Prometheus exporter for Cassandra (JMX-free; reads system tables).
import os, time
from prometheus_client import start_http_server, Gauge
from cassandra.cluster import Cluster

HOST = os.environ.get("CASSANDRA_HOST", "localhost")
PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))

up = Gauge("cassandra_up", "Cassandra reachable (1) or not (0)")
nodes = Gauge("cassandra_node_count", "Nodes in the cluster (local + peers)")
tables = Gauge("cassandra_table_count", "Tables per keyspace", ["keyspace"])
latency = Gauge("cassandra_probe_latency_seconds", "Latency of the schema probe query")

SYSTEM_KS = {
    "system", "system_schema", "system_auth", "system_distributed",
    "system_traces", "system_views", "system_virtual_schema",
}

def collect(session):
    t0 = time.monotonic()
    rows = list(session.execute("SELECT keyspace_name FROM system_schema.tables"))
    latency.set(time.monotonic() - t0)
    counts = {}
    for r in rows:
        if r.keyspace_name in SYSTEM_KS:
            continue
        counts[r.keyspace_name] = counts.get(r.keyspace_name, 0) + 1
    for ks, n in counts.items():
        tables.labels(keyspace=ks).set(n)
    peers = list(session.execute("SELECT peer FROM system.peers"))
    nodes.set(len(peers) + 1)

def main():
    start_http_server(9100)
    session = None
    while True:
        try:
            if session is None:
                session = Cluster([HOST], port=PORT, connect_timeout=5).connect()
            collect(session)
            up.set(1)
        except Exception:
            up.set(0)
            session = None
        time.sleep(5)

if __name__ == "__main__":
    main()
`

function buildDynamo({ name, entities }) {
  // DynamoDB Local has no init-dir; a one-shot aws-cli sidecar creates a table per
  // entity (id partition key). Streams are enabled so a CDC worker can tail them.
  // -sharedDb makes every client (init, exporter, services) see one shared DB
  // regardless of creds/region; -inMemory keeps it lightweight (seeds give
  // rebuild-durability, matching the other engines' ephemeral-on-recreate model).
  // init.sh is MOUNTED (not inline) so the model-schema + seeding flows can rewrite
  // it and a rebuild reproduces the tables — mirroring Cassandra's init.cql.
  const create =
    '#!/bin/sh\nset -e\n' +
    `until aws dynamodb list-tables --endpoint-url http://${name}:8000 >/dev/null 2>&1; do sleep 1; done\n` +
    entities
      .map((e) =>
        `aws dynamodb create-table --endpoint-url http://${name}:8000 --table-name "${e.name}" ` +
        '--attribute-definitions AttributeName=id,AttributeType=S ' +
        '--key-schema AttributeName=id,KeyType=HASH --billing-mode PAY_PER_REQUEST ' +
        '--stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES || true')
      .join('\n') +
    // The Seed tab mounts seed.sh here (if any); replay it after tables exist.
    '\n[ -f /seed.sh ] && sh /seed.sh || true\n'

  const awsEnv = {
    AWS_ACCESS_KEY_ID: 'sandbox',
    AWS_SECRET_ACCESS_KEY: 'sandbox',
    AWS_DEFAULT_REGION: 'us-east-1',
  }

  return {
    nodeType: 'dynamodb',
    services: {
      [name]: {
        image: 'amazon/dynamodb-local:latest',
        command: ['-jar', 'DynamoDBLocal.jar', '-sharedDb', '-inMemory'],
      },
      [`${name}-init`]: {
        image: 'amazon/aws-cli:latest',
        depends_on: [name],
        restart: 'no',
        environment: awsEnv,
        volumes: [`./${name}/init.sh:/init.sh:ro`],
        entrypoint: ['sh', '/init.sh'],
      },
      [`${name}-exporter`]: {
        build: `./${name}/exporter`,
        depends_on: [name],
        environment: { DDB_ENDPOINT: `http://${name}:8000`, ...awsEnv },
      },
    },
    scrapeJob: { job_name: name, static_configs: [{ targets: [`${name}-exporter:9100`] }] },
    metrics: [
      { label: 'tables', query: `dynamodb_table_count{job="${name}"}`, unit: '' },
      { label: 'items', query: `sum(dynamodb_item_count{job="${name}"})`, unit: '' },
      { label: 'probe', query: `dynamodb_probe_latency_seconds{job="${name}"}`, unit: 'ms', scale: 1000 },
    ],
    health: { query: `dynamodb_up{job="${name}"}`, rules: HEALTH_RULES },
    files: [
      { rel: 'init.sh', content: create },
      { rel: 'exporter/Dockerfile', content: DDB_EXPORTER_DOCKERFILE },
      { rel: 'exporter/exporter.py', content: DDB_EXPORTER_PY },
    ],
  }
}

function buildCassandra({ name, dbName, entities }) {
  // Cassandra has no init-dir either; a one-shot sidecar waits for CQL then applies
  // init.cql (a keyspace + a table per entity with an `id text` partition key).
  const tables = entities
    .map((e) => `CREATE TABLE IF NOT EXISTS ${dbName}.${e.name} (id text PRIMARY KEY);`)
    .join('\n')
  const initCql =
    `-- Generated by "Add database". Applied once ${name} accepts CQL.\n` +
    `CREATE KEYSPACE IF NOT EXISTS ${dbName} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\n` +
    tables + '\n'

  const wait = [
    'set -e',
    `until cqlsh ${name} -e 'describe keyspaces' >/dev/null 2>&1; do echo "waiting for cassandra"; sleep 5; done`,
    `cqlsh ${name} -f /init.cql`,
    // The Seed tab mounts seed.cql here (if any); replay it after the schema.
    `[ -f /seed.cql ] && cqlsh ${name} -f /seed.cql || true`,
  ].join('\n')

  return {
    nodeType: 'cassandra',
    services: {
      [name]: {
        image: 'cassandra:5',
        environment: {
          CASSANDRA_CLUSTER_NAME: 'sandbox',
          // Cap the JVM so a sandbox host isn't swamped by Cassandra's defaults.
          MAX_HEAP_SIZE: '512M',
          HEAP_NEWSIZE: '128M',
        },
      },
      [`${name}-init`]: {
        image: 'cassandra:5',
        depends_on: [name],
        restart: 'no',
        volumes: [`./${name}/init.cql:/init.cql:ro`],
        entrypoint: ['sh', '-c', wait],
      },
      [`${name}-exporter`]: {
        build: `./${name}/exporter`,
        depends_on: [name],
        environment: { CASSANDRA_HOST: name, CASSANDRA_PORT: '9042' },
      },
    },
    scrapeJob: { job_name: name, static_configs: [{ targets: [`${name}-exporter:9100`] }] },
    metrics: [
      { label: 'tables', query: `sum(cassandra_table_count{job="${name}"})`, unit: '' },
      { label: 'nodes', query: `cassandra_node_count{job="${name}"}`, unit: '' },
      { label: 'probe', query: `cassandra_probe_latency_seconds{job="${name}"}`, unit: 'ms', scale: 1000 },
    ],
    health: { query: `cassandra_up{job="${name}"}`, rules: HEALTH_RULES },
    files: [
      { rel: 'init.cql', content: initCql },
      { rel: 'exporter/Dockerfile', content: CASS_EXPORTER_DOCKERFILE },
      { rel: 'exporter/exporter.py', content: CASS_EXPORTER_PY },
    ],
  }
}

const TYPES = {
  postgres: { label: 'PostgreSQL', fieldTypes: SQL_FIELD_TYPES, entityRe: IDENT_RE, build: buildPostgres },
  mongodb: { label: 'MongoDB', fieldTypes: MONGO_FIELD_TYPES, entityRe: IDENT_RE, build: buildMongo },
  redis: { label: 'Redis', fieldTypes: null, entityRe: IDENT_RE, build: buildRedis },
  blob: { label: 'Blob (S3)', fieldTypes: null, entityRe: BUCKET_RE, build: buildBlob },
  dynamodb: { label: 'DynamoDB', fieldTypes: null, entityRe: IDENT_RE, build: buildDynamo },
  cassandra: { label: 'Cassandra', fieldTypes: null, entityRe: IDENT_RE, build: buildCassandra },
}

// ---------------------------------------------------------------------------
// Validation of the request body
// ---------------------------------------------------------------------------

function validate(body) {
  const { system, type, name } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const spec = TYPES[type]
  if (!spec) throw bad(`unknown database type "${type}"`)
  if (typeof name !== 'string' || !DB_NAME_RE.test(name) || name.length > 40) {
    throw bad('name must be lowercase letters, digits and hyphens (start with a letter)')
  }

  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  if (manifest.nodes.some((n) => n.id === name)) {
    throw bad(`a node named "${name}" already exists in this system`)
  }
  if (fs.existsSync(path.join(systemDir(system), name))) {
    throw bad(`systems/${system}/${name}/ already exists`)
  }

  // Model mode (postgres/mongodb only): the schema is authored from selected model-bank
  // models by a Claude session, so we provision an EMPTY container here (no entities) and
  // record the chosen models on the node. Entities are not used in this mode.
  const models = Array.isArray(body.models) ? body.models : null
  if (models && models.length) {
    if (!MODEL_ENGINES.has(type)) {
      throw bad('selecting models is not supported for this engine')
    }
    const bank = new Set(readModelsFile(system).models.map((m) => m.name))
    for (const n of models) {
      if (typeof n !== 'string' || !bank.has(n)) throw bad(`unknown model "${n}"`)
    }
    return { system, type, spec, name, manifest, entities: [], models }
  }

  const rawEntities = Array.isArray(body.entities) ? body.entities : []
  // Redis keyspaces are optional: a cache can be provisioned bare and get its typed
  // key namespaces later from the node's Keyspaces tab (/api/redis, no rebuild), which
  // is also the state a redis lands in once its last keyspace is deleted.
  if (rawEntities.length === 0 && type !== 'redis') throw bad('add at least one entity')

  // Redis entities are keyspaces: { name, match, type, shorthand } instead of the
  // name+fields shape. Legacy name-only rows (scripted callers) normalize to the
  // old seeding semantics: a `<name>:` string prefix.
  if (type === 'redis') {
    const seen = new Set()
    const shorthands = new Set()
    const entities = rawEntities.map((raw) => {
      const legacy = raw && raw.match === undefined && raw.type === undefined
      const e = legacy
        ? { name: `${raw?.name || ''}:`, match: 'prefix', type: 'string', shorthand: '' }
        : {
            name: typeof raw?.name === 'string' ? raw.name.trim() : '',
            match: raw?.match,
            type: raw?.type,
            shorthand: typeof raw?.shorthand === 'string' ? raw.shorthand.trim() : '',
          }
      if (!REDIS_KS_RE.test(e.name)) throw bad(`invalid keyspace name "${raw && raw.name}"`)
      if (e.match !== 'prefix' && e.match !== 'exact') throw bad('match must be "prefix" or "exact"')
      if (!REDIS_KS_TYPES.has(e.type)) throw bad(`invalid keyspace type "${raw && raw.type}"`)
      if (e.shorthand && !REDIS_SHORTHAND_RE.test(e.shorthand)) {
        throw bad(`invalid shorthand "${e.shorthand}"`)
      }
      if (seen.has(e.name)) throw bad(`duplicate keyspace "${e.name}"`)
      seen.add(e.name)
      if (e.shorthand) {
        if (shorthands.has(e.shorthand)) throw bad(`duplicate shorthand "${e.shorthand}"`)
        shorthands.add(e.shorthand)
      }
      return e
    })
    return { system, type, spec, name, manifest, entities, models: null }
  }

  const entities = rawEntities.map((e) => {
    if (!e || !spec.entityRe.test(e.name || '')) {
      throw bad(`invalid entity name "${e && e.name}" for ${spec.label}`)
    }
    const fields = (spec.fieldTypes && Array.isArray(e.fields) ? e.fields : []).map((f) => {
      if (!IDENT_RE.test(f.name || '')) throw bad(`invalid field name "${f && f.name}"`)
      if (!spec.fieldTypes.has(f.type)) throw bad(`invalid field type "${f && f.type}"`)
      return { name: f.name, type: f.type }
    })
    return { name: e.name, fields }
  })

  return { system, type, spec, name, manifest, entities, models: null }
}

// ---------------------------------------------------------------------------
// File mutations
// ---------------------------------------------------------------------------

// Append a service to docker-compose.yml, preserving its comments.
export function addComposeServices(system, services, name, label, by = 'Add database') {
  const file = path.join(systemDir(system), 'docker-compose.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  let first = true
  for (const [svc, def] of Object.entries(services)) {
    const node = doc.createNode(def)
    if (first) {
      node.commentBefore = ` ${label} "${name}" — added by ${by}`
      first = false
    }
    doc.setIn(['services', svc], node)
  }
  fs.writeFileSync(file, doc.toString())
}

// Append a scrape job to prometheus.yml, preserving its comments.
export function addScrapeJob(system, scrapeJob, name, by = 'Add database', kind = 'Database') {
  const file = path.join(systemDir(system), 'prometheus', 'prometheus.yml')
  const doc = parseDocument(fs.readFileSync(file, 'utf8'))
  const node = doc.createNode(scrapeJob)
  node.commentBefore = ` ${kind} "${name}" — added by ${by}`
  doc.addIn(['scrape_configs'], node)
  fs.writeFileSync(file, doc.toString())
}

// Append the node to manifest.json (no edge). The node's label is just its name —
// the engine is already shown by the node `type` in the header's upper-right corner,
// so prefixing the label with it (e.g. "PostgreSQL · ") would duplicate it.
function addManifestNode(system, manifest, built, name, schemaModels) {
  const node = {
    id: name,
    label: name,
    type: built.nodeType,
    origin: 'create-database',
    position: nextNodePosition(manifest),
    metrics: built.metrics,
    health: built.health,
  }
  // Model mode records which bank models were turned into tables/collections, so the
  // Schema tab can show them and additive updates can extend the set.
  if (schemaModels && schemaModels.length) node.schemaModels = schemaModels
  // Redis: the declared keyspaces (typed KEY rows on the diagram, managed by /api/redis).
  if (built.keyspaces && built.keyspaces.length) node.keyspaces = built.keyspaces
  manifest.nodes.push(node)
  fs.writeFileSync(
    path.join(systemDir(system), 'manifest.json'),
    JSON.stringify(manifest, null, 2) + '\n',
  )
  return node
}

async function rebuild(system) {
  // Escape hatch for tests/CI: validate the file generation without spending a
  // minute pulling images and starting containers.
  if (process.env.CREATE_DB_SKIP_REBUILD === '1') return '(rebuild skipped)'

  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  const opts = { cwd: repoRoot, timeout: 300_000, maxBuffer: 16 * 1024 * 1024 }
  let log = ''
  try {
    // up -d (no --build) leaves existing services running and only creates the
    // new ones. Prometheus's service definition is unchanged, so its mounted
    // config won't reload on `up` — restart it to pick up the new scrape job.
    const up = await pexec('docker', ['compose', '-f', compose, 'up', '-d'], opts)
    log += up.stdout + up.stderr
    const r = await pexec('docker', ['compose', '-f', compose, 'restart', 'prometheus'], opts)
    log += r.stdout + r.stderr
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}` || err.message
    throw new HttpError(500, `docker compose failed:\n${detail}`)
  }
  return log
}

export async function handleCreate(body) {
  const { system, spec, name, manifest, entities, models } = validate(body)
  const dbName = name.replace(/-/g, '_')
  const built = spec.build({ name, dbName, entities })

  // 1. init scripts (in model mode `entities` is empty, so this is a header-only
  //    placeholder — the launched Claude session writes the real schema).
  const dir = systemDir(system)
  if (built.files.length) {
    for (const f of built.files) {
      const dest = path.join(dir, name, f.rel)
      fs.mkdirSync(path.dirname(dest), { recursive: true }) // f.rel may be nested (e.g. exporter/Dockerfile)
      fs.writeFileSync(dest, f.content)
    }
  }

  // 2-4. compose, prometheus, manifest
  addComposeServices(system, built.services, name, spec.label)
  addScrapeJob(system, built.scrapeJob, name)
  const node = addManifestNode(system, manifest, built, name, models)

  // 5. rebuild (frontend-safe)
  const log = await rebuild(system)
  return { ok: true, node, log }
}

// POST /api/db-models — record which bank models an EXISTING postgres/mongodb database's
// schema is built from. No rebuild: the launched Claude session applies the schema to the
// live container. The set is merged (additive) since updates add tables/collections.
export function handleSetModels(body) {
  const { system, id } = body
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifestPath = path.join(systemDir(system), 'manifest.json')
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.origin !== 'create-database') {
    throw bad(`"${id}" is not a database in this system`)
  }
  if (node.replicaOf) throw bad('cannot author schema on a read replica')
  if (!MODEL_ENGINES.has(node.type)) {
    throw bad('selecting models is not supported for this engine')
  }
  const models = Array.isArray(body.models) ? body.models : []
  if (!models.length) throw bad('select at least one model')
  const bank = new Set(readModelsFile(system).models.map((m) => m.name))
  for (const n of models) {
    if (typeof n !== 'string' || !bank.has(n)) throw bad(`unknown model "${n}"`)
  }
  node.schemaModels = [...new Set([...(node.schemaModels || []), ...models])]
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + '\n')
  return { ok: true, node }
}

export default function createDatabase() {
  return {
    name: 'create-database',
    configureServer(server) {
      const send = (res, code, body) => {
        res.statusCode = code
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(body))
      }

      server.middlewares.use('/api/databases', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          const result = await handleCreate(await readJsonBody(req))
          send(res, 200, result)
        } catch (err) {
          send(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })

      // Record which bank models an existing postgres/mongodb db's schema is built from
      // (the frontend then launches a Claude session to apply the schema).
      server.middlewares.use('/api/db-models', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        try {
          const result = handleSetModels(await readJsonBody(req))
          send(res, 200, result)
        } catch (err) {
          send(res, err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
