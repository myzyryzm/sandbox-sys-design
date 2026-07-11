// Vite dev-server plugin: introspect a database's CURRENT schema.
//
//   GET /api/db-schema?system=<id>&id=<dbNodeId>
//     -> { ok, type, entities: [{ name, fields: [{ name, type }] }] }
//
// Rather than echo back the init script the DB was created from, this reads the
// live container so it reflects the database's actual current state (a service or
// a Claude session may have altered it). Each engine is introspected with its own
// client via `docker compose exec`:
//   postgres     -> information_schema.columns  (tables + columns)
//   mongodb      -> getCollectionNames + a sampled doc  (collections + fields)
//   redis        -> --scan keys grouped by `namespace:`  (key namespaces)
//   object-store -> ls /data  (MinIO stores one directory per bucket)
//
// All user input is validated against the manifest (the db node id is a whitelist
// of real node ids); commands run via execFile arg arrays, never a shell string.
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { repoRoot, systemsDir, systemDir, isValidSystem } from './systems.js'

const pexec = promisify(execFile)

class HttpError extends Error {
  constructor(status, message) {
    super(message)
    this.statusCode = status
  }
}

export function composeExec(system, service, cmd) {
  const compose = path.join(systemsDir, system, 'docker-compose.yml')
  return pexec(
    'docker',
    ['compose', '-f', compose, 'exec', '-T', ...cmd.envFlags, service, ...cmd.argv],
    { cwd: repoRoot, timeout: 20_000, maxBuffer: 8 * 1024 * 1024 },
  )
}

// --- per-engine introspection -> normalized { entities } ---------------------

async function introspectPostgres(system, id) {
  const dbName = id.replace(/-/g, '_')
  const sql =
    "SELECT table_name, column_name, data_type FROM information_schema.columns " +
    "WHERE table_schema='public' ORDER BY table_name, ordinal_position;"
  const { stdout } = await composeExec(system, id, {
    envFlags: ['-e', 'PGPASSWORD=sandbox'],
    argv: ['psql', '-U', 'sandbox', '-d', dbName, '-w', '-t', '-A', '-F', '|', '-c', sql],
  })
  const byTable = new Map()
  for (const line of stdout.split('\n')) {
    const [table, column, type] = line.split('|')
    if (!table || !column) continue
    if (!byTable.has(table)) byTable.set(table, [])
    byTable.get(table).push({ name: column, type })
  }
  return [...byTable].map(([name, fields]) => ({ name, fields }))
}

async function introspectMongo(system, id) {
  const dbName = id.replace(/-/g, '_')
  // For each collection, sample one document and report its field names + JS types.
  const js =
    'db.getCollectionNames().forEach(function(c){' +
    'var d=db.getCollection(c).findOne();' +
    'var f=d?Object.keys(d).filter(function(k){return k!=="_id"}).map(function(k){' +
    'return k+":"+((d[k]&&d[k].constructor)?d[k].constructor.name:typeof d[k])}).join(","):"";' +
    'print(c+"\\t"+f)})'
  const { stdout } = await composeExec(system, id, {
    envFlags: [],
    argv: ['mongosh', dbName, '--quiet', '--eval', js],
  })
  const entities = []
  for (const line of stdout.split('\n')) {
    const t = line.indexOf('\t')
    if (t < 0) continue
    const name = line.slice(0, t).trim()
    if (!name) continue
    const fields = line
      .slice(t + 1)
      .split(',')
      .filter(Boolean)
      .map((pair) => {
        const i = pair.indexOf(':')
        return { name: pair.slice(0, i), type: pair.slice(i + 1) }
      })
    entities.push({ name, fields })
  }
  return entities
}

async function introspectRedis(system, id, node) {
  // In cluster mode (Topology tab) there is no `<id>` container — the keys are
  // sharded across the member containers, so scan each and merge (a replica holds
  // copies of its shard master's keys; the Set dedupes them).
  const targets = node?.redisCluster?.members?.length ? node.redisCluster.members : [id]
  const seen = new Set()
  for (const target of targets) {
    const { stdout } = await composeExec(system, target, {
      envFlags: [],
      argv: ['redis-cli', '--scan'],
    })
    for (const raw of stdout.split('\n')) {
      const key = raw.trim()
      if (key) seen.add(key)
    }
  }
  // Group keys by the namespace before the first ':'. Keys without one go under
  // "(no namespace)". Fields are the concrete keys in that namespace.
  const byNs = new Map()
  for (const key of seen) {
    const ns = key.includes(':') ? key.slice(0, key.indexOf(':')) : '(no namespace)'
    if (!byNs.has(ns)) byNs.set(ns, [])
    byNs.get(ns).push({ name: key, type: 'key' })
  }
  return [...byNs].map(([name, fields]) => ({ name, fields }))
}

async function introspectBlob(system, id) {
  // MinIO (filesystem backend) stores one directory per bucket under /data.
  const { stdout } = await composeExec(system, id, {
    envFlags: [],
    argv: ['ls', '-1', '/data'],
  })
  return stdout
    .split('\n')
    .map((s) => s.trim())
    .filter((s) => s && s !== '.minio.sys')
    .map((name) => ({ name, fields: [] }))
}

async function introspectDynamo(system, id) {
  // DynamoDB Local has no CLI in its container, so introspect via the boto3-equipped
  // exporter sidecar. DynamoDB is schemaless beyond its keys, so "fields" are the
  // table's key attributes (partition/sort) with their role + attribute type.
  const py =
    'import os, boto3\n' +
    'c = boto3.client("dynamodb", endpoint_url=os.environ["DDB_ENDPOINT"], region_name="us-east-1",' +
    ' aws_access_key_id="sandbox", aws_secret_access_key="sandbox")\n' +
    'for name in c.list_tables().get("TableNames", []):\n' +
    '    d = c.describe_table(TableName=name)["Table"]\n' +
    '    types = {a["AttributeName"]: a["AttributeType"] for a in d.get("AttributeDefinitions", [])}\n' +
    '    parts = [k["AttributeName"] + ":" + k["KeyType"] + "(" + types.get(k["AttributeName"], "?") + ")" for k in d.get("KeySchema", [])]\n' +
    '    print(name + "\\t" + ",".join(parts))\n'
  const { stdout } = await composeExec(system, `${id}-exporter`, { envFlags: [], argv: ['python', '-c', py] })
  return parseTabbed(stdout)
}

async function introspectCassandra(system, id) {
  // Introspect via the cassandra-driver-equipped exporter sidecar: read the columns
  // of the db's keyspace from system_schema, reporting each column's type + kind
  // (partition_key / clustering / regular).
  const dbName = id.replace(/-/g, '_')
  const py =
    'import os\n' +
    'from collections import defaultdict\n' +
    'from cassandra.cluster import Cluster\n' +
    's = Cluster([os.environ["CASSANDRA_HOST"]], port=int(os.environ.get("CASSANDRA_PORT", "9042"))).connect()\n' +
    'rows = s.execute("SELECT table_name, column_name, type, kind FROM system_schema.columns WHERE keyspace_name=%s", (os.environ["KS"],))\n' +
    'cols = defaultdict(list)\n' +
    'for r in rows:\n' +
    '    cols[r.table_name].append(r.column_name + ":" + r.type + "(" + r.kind + ")")\n' +
    'for t, fs in cols.items():\n' +
    '    print(t + "\\t" + ",".join(fs))\n'
  const { stdout } = await composeExec(system, `${id}-exporter`, { envFlags: ['-e', `KS=${dbName}`], argv: ['python', '-c', py] })
  return parseTabbed(stdout)
}

// Parse `table\tname:type,name:type` lines (shared by the dynamo/cassandra probes).
function parseTabbed(stdout) {
  const entities = []
  for (const line of stdout.split('\n')) {
    const t = line.indexOf('\t')
    if (t < 0) continue
    const name = line.slice(0, t).trim()
    if (!name) continue
    const fields = line
      .slice(t + 1)
      .split(',')
      .filter(Boolean)
      .map((pair) => {
        const i = pair.indexOf(':')
        return i < 0 ? { name: pair, type: '' } : { name: pair.slice(0, i), type: pair.slice(i + 1) }
      })
    entities.push({ name, fields })
  }
  return entities
}

const INTROSPECTORS = {
  postgres: introspectPostgres,
  mongodb: introspectMongo,
  redis: introspectRedis,
  'object-store': introspectBlob,
  dynamodb: introspectDynamo,
  cassandra: introspectCassandra,
}

export async function getSchema(system, id) {
  if (!isValidSystem(system)) throw new HttpError(400, `unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const node = manifest.nodes.find((n) => n.id === id)
  if (!node || node.origin !== 'create-database') {
    throw new HttpError(400, `"${id}" is not a database in this system`)
  }
  const introspect = INTROSPECTORS[node.type]
  if (!introspect) throw new HttpError(400, `no schema introspection for type "${node.type}"`)

  try {
    const entities = await introspect(system, id, node)
    return { ok: true, type: node.type, label: node.label, entities }
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}`.trim() || err.message
    throw new HttpError(502, `could not read schema: ${detail}`)
  }
}

export default function dbSchema() {
  return {
    name: 'db-schema',
    configureServer(server) {
      server.middlewares.use('/api/db-schema', async (req, res, next) => {
        if (req.method !== 'GET') return next()
        const json = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }
        try {
          const url = new URL(req.url, 'http://localhost')
          const system = url.searchParams.get('system')
          const id = url.searchParams.get('id')
          return json(200, await getSchema(system, id))
        } catch (err) {
          json(err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
