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
import type {
  ModelRecord,
  ModelUsageDatabase,
  ModelUsageEndpoint,
  ModelUsageMap,
  ModelUsageStream,
} from './types/registries'

function stripTsComments(ts?: string): string {
  return (ts || '').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')
}

// Collect the records for `names` plus every model they reference, transitively, each once.
// Word-boundary name scan (over comment-stripped text), deduped, depth-capped (also stops
// reference cycles).
export function collectModels(
  names: string[] | null | undefined,
  models: ModelRecord[] | null | undefined,
): ModelRecord[] {
  const byName = new Map((models || []).map((m) => [m.name, m]))
  const out: ModelRecord[] = []
  const seen = new Set<string>()
  const visit = (n: string, depth: number) => {
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
export function resolveModelTs(name: string, models?: ModelRecord[] | null): string {
  const out = collectModels([name], models)
  if (!out.length) return `// model "${name}" is not defined in the models bank`
  return out.map((m) => (m.ts || '').trim()).join('\n\n')
}

// The OTHER bank-model names a model directly references (FK hint in the picker UI).
// Scans the comment-stripped definition so a model named only in a comment isn't a phantom FK.
export function referencedModels(name: string, models?: ModelRecord[] | null): string[] {
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
export function dependentModels(
  names: string[] | null | undefined,
  models: ModelRecord[] | null | undefined,
): string[] {
  // Reverse adjacency: ref -> [models that reference it].
  const rev = new Map<string, string[]>()
  for (const m of models || []) {
    for (const ref of referencedModels(m.name, models)) {
      if (!rev.has(ref)) rev.set(ref, [])
      rev.get(ref)!.push(m.name)
    }
  }
  const seen = new Set<string>()
  const visit = (n: string, depth: number) => {
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
export interface ModelImpact {
  models: string[]
  endpoints: ModelUsageEndpoint[]
  databases: ModelUsageDatabase[]
  streams: ModelUsageStream[]
}

export function modelImpact({
  names,
  models,
  usage,
}: {
  names: string[]
  models?: ModelRecord[] | null
  usage?: ModelUsageMap | null
}): ModelImpact {
  const closure = dependentModels(names, models)
  const endpoints: ModelUsageEndpoint[] = []
  const databases: ModelUsageDatabase[] = []
  const streams: ModelUsageStream[] = []
  const epSeen = new Set<string>()
  const dbSeen = new Set<string>()
  const stSeen = new Set<string>()
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
export function buildModelUpdatePrompt({
  systemId,
  edits,
  impact,
  allModels,
}: {
  systemId: string
  edits?: Array<Pick<ModelRecord, 'name' | 'ts'>> | null
  impact?: ModelImpact | null
  allModels?: ModelRecord[] | null
}): string {
  void allModels
  const changed = (edits || [])
    .map((e) => `## ${e.name}\n\`\`\`ts\n${(e.ts || '').trim()}\n\`\`\``)
    .join('\n\n')
  const epLines = impact?.endpoints?.length
    ? impact.endpoints.map((e) => `- ${e.service} ${e.method} /${e.service}${e.path}  (${e.field} body)`).join('\n')
    : '- (none)'
  const dbLines = impact?.databases?.length
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

// Per-engine schema-authoring guidance for buildDbSchemaPrompt. Each entry captures
// the engine-specific prose the DB-authoring session needs. postgres/mongodb reproduce
// the original SQL-vs-NoSQL binary exactly; dynamodb/cassandra add key-model + no-join
// (denormalized) guidance, since their data models differ sharply from relational/document.
interface SchemaSpec {
  word: string
  initFile: string
  seedFile: string
  degrade: string
  refNote: string
  oneEntity: string
  refSingular: string
  refArray: string
  typeMap: string
  idempotentApply: string
}

const SCHEMA_SPECS: Record<string, SchemaSpec> = {
  postgres: {
    word: 'table',
    initFile: 'init.sql',
    seedFile: 'seed.sql',
    degrade: 'jsonb',
    refNote: `A field whose type is ANOTHER model below is a FOREIGN KEY to that table`,
    oneEntity: `- One table per model (snake_case the name). If no field/comment designates a primary key, add a synthetic \`id uuid primary key default gen_random_uuid()\`; otherwise use the designated key with its stated type (e.g. a \`// primary key\` comment on \`id: number\` snowflake → \`id bigint primary key\`).`,
    refSingular: `- A field \`f: OtherModel\` (singular) → a foreign-key column \`f_id\` referencing that table's primary key; the column's TYPE must match that referenced primary key (per its comments), not assume uuid.`,
    refArray: `- A field \`f: OtherModel[]\` (array) → a one-to-many: put a foreign-key column on the CHILD table referencing this table's primary key (matching its type).`,
    typeMap: `- Default type mapping (unless a comment narrows it, e.g. \`integer only\`→\`integer\`, \`max length of 3\`→\`varchar(3)\`): string→text, number→numeric, boolean→boolean, Date→timestamptz, Record<…>/nested object/array-of-primitive→jsonb. A \`?\` (optional) field is nullable.`,
    idempotentApply: '`CREATE TABLE IF NOT EXISTS …`, and add FK constraints only if absent',
  },
  mongodb: {
    word: 'collection',
    initFile: 'init.js',
    seedFile: 'seed.js',
    degrade: 'object',
    refNote: `A field whose type is ANOTHER model below is a FOREIGN KEY to that collection`,
    oneEntity: `- One collection per model (named after the model).`,
    refSingular: `- A field \`f: OtherModel\` (singular) → a field holding the referenced document's _id.`,
    refArray: `- A field \`f: OtherModel[]\` (array) → store an array of referenced _ids (or embed the subdocuments).`,
    typeMap: `- Default type mapping (unless a comment narrows it): string→string, number→number, boolean→boolean, Date→date, Record<…>/nested object→object, array-of-primitive→array.`,
    idempotentApply: 'guard `createCollection` with `getCollectionNames()`',
  },
  dynamodb: {
    word: 'table',
    initFile: 'init.sh',
    seedFile: 'seed.sh',
    degrade: 'map attribute',
    refNote: `DynamoDB has NO joins — a field whose type is ANOTHER model below is a DENORMALIZED reference (embed it), never a foreign key`,
    oneEntity: `- One DynamoDB table per model. Pick the partition (HASH) key from a \`// PK\`/\`// partition key\` comment, else a field named \`id\`; if a \`// SK\`/\`// sort key\` comment names a field, add it as the RANGE key. Only key attributes are declared (DynamoDB is schemaless otherwise). Create each table with \`--billing-mode PAY_PER_REQUEST\` and \`--stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES\` (so CDC can tail it).`,
    refSingular: `- A field \`f: OtherModel\` (singular) → denormalize: embed the referenced item as a nested map attribute (or store its key). Do NOT create a foreign key.`,
    refArray: `- A field \`f: OtherModel[]\` (array) → store a list of nested maps (or a list of keys). No join table.`,
    typeMap: `- Attribute types are per-item at write time, not in the schema; only KEY attributes need a declared type (S=string, N=number, B=binary). Use S for string/uuid keys, N for numeric keys.`,
    idempotentApply: '`aws dynamodb create-table`, ignoring a ResourceInUseException (table already exists)',
  },
  cassandra: {
    word: 'table',
    initFile: 'init.cql',
    seedFile: 'seed.cql',
    degrade: 'text',
    refNote: `Cassandra has NO joins — a field whose type is ANOTHER model below is a DENORMALIZED reference, never a foreign key`,
    oneEntity: `- One Cassandra table per model in keyspace \`${'${dbName}'}\` (query-driven, denormalized). Define the PRIMARY KEY from comments — partition key from \`// PK\`/\`// partition key\`, clustering columns from \`// CK\`/\`// clustering key\` in order; if none given, use \`id text\` as the partition key.`,
    refSingular: `- A field \`f: OtherModel\` (singular) → denormalize: flatten the referenced model's columns into this table (prefixed), or model it as a UDT (\`CREATE TYPE\`). Do NOT create a foreign key.`,
    refArray: `- A field \`f: OtherModel[]\` (array) → a collection column (\`list<…>\`/\`set<…>\` of a frozen UDT), or a separate query-optimized table. No join table.`,
    typeMap: `- Default CQL type mapping (unless a comment narrows it): string→text, number→bigint (int/decimal per comment), boolean→boolean, Date→timestamp, uuid→uuid, Record<…>/nested object→a UDT or \`text\` (JSON), array-of-primitive→\`list<…>\`.`,
    idempotentApply: '`CREATE TABLE IF NOT EXISTS …` / `CREATE TYPE IF NOT EXISTS …`',
  },
}

// Build the Claude prompt that authors a database's schema from selected models. The
// backend has already provisioned the container (create) or it already exists (update);
// this session writes the actual tables/collections + keys/references via the
// sandbox-database skill. `allModels` is the whole bank (so referenced-but-unselected
// models still resolve).
export function buildDbSchemaPrompt({
  systemId,
  dbId,
  engine,
  models,
  allModels,
  update,
}: {
  systemId: string
  dbId: string
  engine: string
  models: string[]
  allModels?: ModelRecord[] | null
  update?: boolean
}): string {
  const needed = collectModels(models, allModels)
  const tsBlock = needed.length
    ? needed.map((m) => (m.ts || '').trim()).join('\n\n')
    : '// (no model definitions found in the bank)'
  const spec = SCHEMA_SPECS[engine] || SCHEMA_SPECS.postgres
  const dbName = dbId.replace(/-/g, '_')
  const { word, initFile, seedFile } = spec
  const oneEntity = spec.oneEntity.replace('${dbName}', dbName)

  return [
    `Use the sandbox-database skill to ${update ? 'update the' : 'set up the'} schema of the ${engine} database "${dbId}" in the "${systemId}" system from models in the model bank.`,
    ``,
    update
      ? `ADD the models below to the EXISTING database "${dbId}" as new ${word}s, idempotently — do NOT drop or alter existing ${word}s or data.`
      : `The container was just provisioned EMPTY. Author its full schema from the models below.`,
    ``,
    `Models — each becomes one ${word} (in selection order): ${models.join(', ')}`,
    ``,
    `Definitions (TypeScript). ${spec.refNote}:`,
    '```ts',
    tsBlock,
    '```',
    ``,
    `Rules:`,
    `- The \`//\` comments in the definitions above are AUTHORITATIVE schema directives — read them and apply exactly what they state: primary keys, foreign keys, unique constraints, indexes, column lengths/precision, check constraints, defaults, datetime/uuid/serial types, nullability. A comment may sit on the line(s) ABOVE the interface (often \`// field => …\`, or a table-level note like \`// unique constraint on (owner_id, name)\`) or TRAIL a field. These directives OVERRIDE the generic defaults below whenever they conflict.`,
    oneEntity,
    spec.refSingular,
    spec.refArray,
    spec.typeMap,
    `- A reference to a model that is NOT in the selected set degrades to a plain ${spec.degrade} field (no foreign key).`,
    ``,
    update
      ? `Apply with idempotent DDL against the live container (${spec.idempotentApply}) AND append the new ${word}s to systems/${systemId}/${dbId}/${initFile} so a fresh rebuild reproduces them. Then verify per the skill.`
      : `Write systems/${systemId}/${dbId}/${initFile} with the full schema, apply it to the live container, and verify per the skill.`,
    ``,
    `Finally, if systems/${systemId}/${dbId}/${seedFile} exists, re-run it against the live container after the migration (it is idempotent) so any seeded data is preserved.`,
  ].join('\n')
}
