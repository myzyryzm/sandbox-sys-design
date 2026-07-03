// Shared helpers for the per-system "model bank" (systems/<id>/models.json). A model is
// a TypeScript interface (`ts`) and "references" another model whenever that model's name
// appears as a field type (a `\b<Name>\b` word-boundary match). These helpers are used by
// the endpoint flow (inline a request/response model's TypeScript) and the database flow
// (turn selected models into tables/collections, with model-to-model references as FKs).

// Strip `//` line and `/* … */` block comments. A model's `ts` may carry comments that
// name OTHER models (e.g. `// transaction => foreign key field to Transaction.id`); a real
// reference is always a field *type*, never inside a comment, so the `\b<Name>\b` reference
// scan runs on this stripped text to avoid phantom FKs. The prompt and UI keep the full,
// commented `ts` — this is scan-only.
function stripTsComments(ts) {
  return (ts || '').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')
}

// Collect the records for `names` plus every model they reference, transitively, each once.
// Word-boundary name scan (over comment-stripped text), deduped, depth-capped (also stops
// reference cycles).
export function collectModels(names, models) {
  const byName = new Map((models || []).map((m) => [m.name, m]))
  const out = []
  const seen = new Set()
  const visit = (n, depth) => {
    if (seen.has(n) || depth > 12) return
    const m = byName.get(n)
    if (!m) return
    seen.add(n)
    out.push(m)
    const code = stripTsComments(m.ts)
    for (const other of byName.keys()) {
      if (other === n || seen.has(other)) continue
      if (new RegExp(`\\b${other}\\b`).test(code)) visit(other, depth + 1)
    }
  }
  for (const n of names || []) visit(n, 0)
  return out
}

// One model's TypeScript plus the definitions of every model it references, transitively.
// Used when an endpoint references a model so the implementing session sees the full shape.
export function resolveModelTs(name, models) {
  const out = collectModels([name], models)
  if (!out.length) return `// model "${name}" is not defined in the models bank`
  return out.map((m) => (m.ts || '').trim()).join('\n\n')
}

// The OTHER bank-model names a model directly references (FK hint in the picker UI).
// Scans the comment-stripped definition so a model named only in a comment isn't a phantom FK.
export function referencedModels(name, models) {
  const me = (models || []).find((m) => m.name === name)
  if (!me) return []
  const code = stripTsComments(me.ts)
  return (models || [])
    .map((m) => m.name)
    .filter((other) => other !== name && new RegExp(`\\b${other}\\b`).test(code))
}

// The reverse of referencedModels: given a set of model names, every model that
// transitively REFERENCES any of them — a change to a model ripples UP to whatever uses
// it. Includes the seed names. Depth-capped + seen-guarded (like collectModels) so
// reference cycles terminate.
export function dependentModels(names, models) {
  // Reverse adjacency: ref -> [models that reference it].
  const rev = new Map()
  for (const m of models || []) {
    for (const ref of referencedModels(m.name, models)) {
      if (!rev.has(ref)) rev.set(ref, [])
      rev.get(ref).push(m.name)
    }
  }
  const seen = new Set()
  const visit = (n, depth) => {
    if (seen.has(n) || depth > 12) return
    seen.add(n)
    for (const dependent of rev.get(n) || []) visit(dependent, depth + 1)
  }
  for (const n of names || []) visit(n, 0)
  return [...seen]
}

// What editing `names` will affect: the transitive set of dependent models, plus the
// union of endpoints/databases/streams that reference any model in that set. `usage` is the
// map from GET /api/model-usage. Endpoints are deduped by service|method|path|field, dbs by
// id, stream topics by cluster|topic.
export function modelImpact({ names, models, usage }) {
  const closure = dependentModels(names, models)
  const endpoints = []
  const databases = []
  const streams = []
  const epSeen = new Set()
  const dbSeen = new Set()
  const stSeen = new Set()
  for (const n of closure) {
    const u = (usage && usage[n]) || { endpoints: [], databases: [], streams: [] }
    for (const e of u.endpoints || []) {
      const key = `${e.service}|${e.method}|${e.path}|${e.field}`
      if (epSeen.has(key)) continue
      epSeen.add(key)
      endpoints.push(e)
    }
    for (const d of u.databases || []) {
      if (dbSeen.has(d.id)) continue
      dbSeen.add(d.id)
      databases.push(d)
    }
    for (const s of u.streams || []) {
      const key = `${s.cluster}|${s.topic}`
      if (stSeen.has(key)) continue
      stSeen.add(key)
      streams.push(s)
    }
  }
  return { models: closure, endpoints, databases, streams }
}

// The single consolidated Claude prompt that propagates a batch of model edits to every
// affected endpoint and database. systems/<id>/models.json is ALREADY saved with the new
// shapes before this runs; this session updates the consumers. Kept lean (the terminal
// slices the positional prompt to 8000 chars): inline only the changed models' new TS and
// point the session at models.json for any types they reference. `allModels` is the whole
// bank, available if a tighter prompt later wants resolveModelTs enrichment.
export function buildModelUpdatePrompt({ systemId, edits, impact, allModels }) {
  void allModels
  const changed = (edits || [])
    .map((e) => `## ${e.name}\n\`\`\`ts\n${(e.ts || '').trim()}\n\`\`\``)
    .join('\n\n')
  const epLines = (impact?.endpoints || []).length
    ? impact.endpoints.map((e) => `- ${e.service} ${e.method} /${e.service}${e.path}  (${e.field} body)`).join('\n')
    : '- (none)'
  const dbLines = (impact?.databases || []).length
    ? impact.databases.map((d) => `- ${d.id} (${d.engine})`).join('\n')
    : '- (none)'
  // Only ENFORCED topics need code changes; documented-only topics re-resolve their TS
  // automatically, so they're omitted from the work list.
  const enforced = (impact?.streams || []).filter((s) => s.enforce)
  const stLines = enforced.length
    ? enforced.map((s) => `- ${s.cluster} / ${s.topic}`).join('\n')
    : '- (none)'
  return [
    `Several model-bank models in the "${systemId}" system changed. systems/${systemId}/models.json is ALREADY updated with the new shapes below — your job is to propagate them to every affected endpoint, database and enforced event-stream topic in THIS one session.`,
    ``,
    `Changed models (new TypeScript — read systems/${systemId}/models.json for any types they reference):`,
    changed || '(none)',
    ``,
    `Affected HTTP endpoints — for EACH, use the sandbox-endpoint skill to update the route's request/response handling to the new shape, then rebuild that service and verify:`,
    epLines,
    ``,
    `Affected databases — for EACH, use the sandbox-database skill to migrate the schema ADDITIVELY (CREATE … IF NOT EXISTS / guarded createCollection; add new columns/fields; NEVER drop or lose existing data), update its init script so a rebuild reproduces it, then verify:`,
    dbLines,
    ``,
    `Affected event-stream topics (schema ENFORCED) — for EACH, use the sandbox-event-stream skill to update the message validation in the topic's producers and consumers to the new shape, then rebuild those services and verify:`,
    stLines,
    ``,
    `Do all of the above in this session. When done, the changed models and their endpoints, databases and enforced topics must all agree on the new shape.`,
  ].join('\n')
}

// Build the Claude prompt that authors a database's schema from selected models. The
// backend has already provisioned the container (create) or it already exists (update);
// this session writes the actual tables/collections + foreign keys via the sandbox-database
// skill. `allModels` is the whole bank (so referenced-but-unselected models still resolve).
export function buildDbSchemaPrompt({ systemId, dbId, engine, models, allModels, update }) {
  const needed = collectModels(models, allModels)
  const tsBlock = needed.length
    ? needed.map((m) => (m.ts || '').trim()).join('\n\n')
    : '// (no model definitions found in the bank)'
  const isPg = engine === 'postgres'
  const word = isPg ? 'table' : 'collection'
  const initFile = isPg ? 'init.sql' : 'init.js'

  return [
    `Use the sandbox-database skill to ${update ? 'update the' : 'set up the'} schema of the ${engine} database "${dbId}" in the "${systemId}" system from models in the model bank.`,
    ``,
    update
      ? `ADD the models below to the EXISTING database "${dbId}" as new ${word}s, idempotently — do NOT drop or alter existing ${word}s or data.`
      : `The container was just provisioned EMPTY. Author its full schema from the models below.`,
    ``,
    `Models — each becomes one ${word} (in selection order): ${models.join(', ')}`,
    ``,
    `Definitions (TypeScript). A field whose type is ANOTHER model below is a FOREIGN KEY to that ${word}:`,
    '```ts',
    tsBlock,
    '```',
    ``,
    `Rules:`,
    `- The \`//\` comments in the definitions above are AUTHORITATIVE schema directives — read them and apply exactly what they state: primary keys, foreign keys, unique constraints, indexes, column lengths/precision, check constraints, defaults, datetime/uuid/serial types, nullability. A comment may sit on the line(s) ABOVE the interface (often \`// field => …\`, or a table-level note like \`// unique constraint on (owner_id, name)\`) or TRAIL a field. These directives OVERRIDE the generic defaults below whenever they conflict.`,
    isPg
      ? `- One table per model (snake_case the name). If no field/comment designates a primary key, add a synthetic \`id uuid primary key default gen_random_uuid()\`; otherwise use the designated key with its stated type (e.g. a \`// primary key\` comment on \`id: number\` snowflake → \`id bigint primary key\`).`
      : `- One collection per model (named after the model).`,
    isPg
      ? `- A field \`f: OtherModel\` (singular) → a foreign-key column \`f_id\` referencing that table's primary key; the column's TYPE must match that referenced primary key (per its comments), not assume uuid.`
      : `- A field \`f: OtherModel\` (singular) → a field holding the referenced document's _id.`,
    isPg
      ? `- A field \`f: OtherModel[]\` (array) → a one-to-many: put a foreign-key column on the CHILD table referencing this table's primary key (matching its type).`
      : `- A field \`f: OtherModel[]\` (array) → store an array of referenced _ids (or embed the subdocuments).`,
    isPg
      ? `- Default type mapping (unless a comment narrows it, e.g. \`integer only\`→\`integer\`, \`max length of 3\`→\`varchar(3)\`): string→text, number→numeric, boolean→boolean, Date→timestamptz, Record<…>/nested object/array-of-primitive→jsonb. A \`?\` (optional) field is nullable.`
      : `- Default type mapping (unless a comment narrows it): string→string, number→number, boolean→boolean, Date→date, Record<…>/nested object→object, array-of-primitive→array.`,
    `- A reference to a model that is NOT in the selected set degrades to a plain ${isPg ? 'jsonb' : 'object'} field (no foreign key).`,
    ``,
    update
      ? `Apply with idempotent DDL against the live container (${isPg ? '`CREATE TABLE IF NOT EXISTS …`, and add FK constraints only if absent' : 'guard `createCollection` with `getCollectionNames()`'}) AND append the new ${word}s to systems/${systemId}/${dbId}/${initFile} so a fresh rebuild reproduces them. Then verify per the skill.`
      : `Write systems/${systemId}/${dbId}/${initFile} with the full schema, apply it to the live container, and verify per the skill.`,
    ``,
    `Finally, if systems/${systemId}/${dbId}/${isPg ? 'seed.sql' : 'seed.js'} exists, re-run it against the live container after the migration (it is idempotent) so any seeded data is preserved.`,
  ].join('\n')
}
