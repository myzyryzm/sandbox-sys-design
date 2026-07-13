// Vite dev-server plugin: manage per-column postgres indexes from the Schema tab.
//
//   POST /api/db-indexes   { system, id, action: 'add'|'drop', table, column, method?, name? }
//     -> { ok, entities }   (the refreshed live schema, same shape as GET /api/db-schema)
//
// Purely mechanical — no Claude session (dbseed.js precedent). The DDL is applied to
// the LIVE container first, so a failure (e.g. gin on a plain text column) surfaces to
// the UI and nothing is persisted; then the same idempotent statement is recorded in
// the db's init.sql under a managed marker at the end of the file, so a from-scratch
// (`down -v`) rebuild replays the index state. Dropping an index that predates this
// feature (hand/session-authored DDL above the marker — never edited in place) records
// a DROP INDEX IF EXISTS instead; statement order makes the replay converge.
//
// Table/column names are whitelisted against the live introspected schema, never the
// request; the method comes from a hard whitelist; the index name is server-generated;
// docker runs via execFile arg arrays (composeExec), never a shell string.
import fs from 'node:fs'
import path from 'node:path'
import { systemDir, isValidSystem } from './systems.js'
import { composeExec, getSchema } from './dbschema.js'
import { readJsonBody, bad, HttpError } from './databases.js'

const PG_INDEX_METHODS = new Set(['btree', 'hash', 'gin', 'brin', 'gist', 'spgist'])

const MARKER = '-- Indexes managed by the Schema tab. Idempotent — replayed on from-scratch rebuild.'
const BLOCK_START = '\n' + MARKER + '\n'

// Resolve + validate a request to a real postgres primary.
function dbNode(system, id) {
  if (!isValidSystem(system)) throw bad(`unknown system "${system}"`)
  const manifest = JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))
  const node = (manifest.nodes || []).find((n) => n.id === id)
  if (!node || node.origin !== 'create-database') throw bad(`"${id}" is not a database in this system`)
  if (node.type !== 'postgres') throw bad(`indexes are managed for postgres only (not ${node.type})`)
  if (node.replicaOf) throw bad('indexes are managed on the primary (replicas inherit them)')
  return node
}

const qid = (name) => `"${name.replace(/"/g, '""')}"`

const createStmt = (name, table, method, column) =>
  `CREATE INDEX IF NOT EXISTS ${qid(name)} ON ${qid(table)} USING ${method} (${qid(column)});`
const dropStmt = (name) => `DROP INDEX IF EXISTS ${qid(name)};`

async function applyPgSql(system, id, sql) {
  try {
    await composeExec(system, id, {
      envFlags: ['-e', 'PGPASSWORD=sandbox'],
      argv: ['psql', '-U', 'sandbox', '-d', id.replace(/-/g, '_'), '-w', '-v', 'ON_ERROR_STOP=1', '-c', sql],
    })
  } catch (err) {
    const detail = `${err.stdout || ''}${err.stderr || ''}`.trim() || err.message
    throw new HttpError(502, `index change failed: ${detail}`)
  }
}

// --- init.sql managed block ----------------------------------------------------
// The block lives at the very end of init.sql: a blank line, the marker comment,
// then one statement per line. Everything before it is preserved byte-for-byte, so
// an add/drop round trip leaves a clean `git diff`.

function parseInit(text) {
  const i = text.indexOf(BLOCK_START)
  if (i < 0) return { head: text, block: [] }
  return {
    head: text.slice(0, i),
    block: text
      .slice(i + BLOCK_START.length)
      .split('\n')
      .map((s) => s.trim())
      .filter(Boolean),
  }
}

function writeInit(file, head, block) {
  const base = head.endsWith('\n') || head === '' ? head : head + '\n'
  fs.writeFileSync(file, block.length ? base + BLOCK_START + block.join('\n') + '\n' : base)
}

function recordAdd(file, name, stmt) {
  const { head, block } = parseInit(fs.readFileSync(file, 'utf8'))
  // A stale managed DROP for this name would undo the CREATE on replay.
  const next = block.filter((l) => l !== dropStmt(name))
  if (!next.includes(stmt)) next.push(stmt)
  writeInit(file, head, next)
}

function recordDrop(file, name) {
  const { head, block } = parseInit(fs.readFileSync(file, 'utf8'))
  const createPrefix = `CREATE INDEX IF NOT EXISTS ${qid(name)} `
  const next = block.filter((l) => !l.startsWith(createPrefix))
  // Index defined outside the managed block (session-authored) — suppress it on
  // replay instead of touching the original DDL.
  if (next.length === block.length && !next.includes(dropStmt(name))) next.push(dropStmt(name))
  writeInit(file, head, next)
}

// --- request handler -------------------------------------------------------------

async function handleMutate(body) {
  const { system, id } = body
  dbNode(system, id)
  const table = String(body.table || '')
  const column = String(body.column || '')
  const schema = await getSchema(system, id) // throws 502 if the container is down
  const entity = schema.entities.find((e) => e.name === table)
  if (!entity) throw bad(`unknown table "${table}"`)
  const field = entity.fields.find((f) => f.name === column)
  if (!field) throw bad(`unknown column "${column}" on "${table}"`)

  const initSql = path.join(systemDir(system), id, 'init.sql')
  if (!fs.existsSync(initSql)) throw bad(`${id} has no init.sql to record the index in`)

  if (body.action === 'add') {
    const method = String(body.method || '')
    if (!PG_INDEX_METHODS.has(method)) throw bad(`unknown index method "${method}"`)
    const name = `${table}_${column}_${method}_idx`
    if (name.length > 63) throw bad('generated index name exceeds postgres’ 63-character limit')
    // A same-named index ANYWHERE would make CREATE INDEX IF NOT EXISTS silently no-op.
    for (const e of schema.entities) {
      for (const f of e.fields) {
        for (const ix of f.indexes || []) {
          if (ix.name !== name) continue
          throw bad(
            e.name === table && f.name === column
              ? `index "${name}" already exists`
              : `index name "${name}" is already in use`,
          )
        }
      }
    }
    const stmt = createStmt(name, table, method, column)
    await applyPgSql(system, id, stmt)
    recordAdd(initSql, name, stmt)
  } else if (body.action === 'drop') {
    const name = String(body.name || '')
    const ix = (field.indexes || []).find((x) => x.name === name)
    if (!ix) throw bad(`no index "${name}" on "${table}"."${column}"`)
    if (ix.constraint) throw bad(`"${name}" backs a constraint — drop the constraint instead`)
    await applyPgSql(system, id, dropStmt(name))
    recordDrop(initSql, name)
  } else {
    throw bad('action must be "add" or "drop"')
  }

  const fresh = await getSchema(system, id)
  return { ok: true, entities: fresh.entities }
}

export default function dbIndexes() {
  return {
    name: 'db-indexes',
    configureServer(server) {
      server.middlewares.use('/api/db-indexes', async (req, res, next) => {
        if (req.method !== 'POST') return next()
        const send = (code, body) => {
          res.statusCode = code
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify(body))
        }
        try {
          send(200, await handleMutate(await readJsonBody(req)))
        } catch (err) {
          send(err.statusCode || 500, { ok: false, error: err.message })
        }
      })
    },
  }
}
