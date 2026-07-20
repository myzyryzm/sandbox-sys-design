// Shared helpers + Claude-session prompt builders for the gRPC bank and the
// per-service gRPC tab. The bank is pure shape (the backend already ran protoc
// and wrote proto/_pb2/registry before any of these prompts are built), so:
//  - buildGrpcUpdatePrompt   — ONE propagation session after a staged bank apply
//    touches attached contracts (sandbox-grpc-contract skill).
//  - buildGrpcAttachPrompt   — implement + wire a contract's servicer on the ONE
//    service that now owns it (sandbox-grpc-attach skill).
//  - buildGrpcDescriptionsPrompt — edit served method bodies in place from new
//    description text (sandbox-grpc-attach skill).
//  - buildGrpcDetachPrompt   — unwire a served contract (sandbox-grpc-attach).

import type { GrpcMethodRecord } from './types/registries'

function shape(obj?: Record<string, unknown>): string {
  const keys = Object.keys(obj || {})
  return keys.length ? keys.map((k) => `${k}: ${obj?.[k]}`).join(', ') : ''
}

// A method's display signature. Form-authored methods carry request/response
// field maps; uploaded methods carry only the message type names — fall back to
// those (with any stream markers) when the maps are empty.
export function methodSig(m: GrpcMethodRecord): string {
  const reqInner = shape(m.request) || `${m.requestStreaming ? 'stream ' : ''}${m.requestType || 'empty'}`
  const resInner = shape(m.response) || (m.responseType || 'empty')
  return `(${reqInner}) → ${m.responseStreaming ? 'stream ' : ''}(${resInner})`
}

// A new description chunk is APPENDED to a method's existing description rather
// than replacing it (an empty chunk leaves it unchanged), so behavior text
// accumulates over successive edits — same discipline as endpoints.json.
export function joinDescription(base?: string | null, addition?: string | null): string {
  const b = (base || '').trim()
  const a = (addition || '').trim()
  if (!b) return a
  if (!a) return b
  return `${b}\n\n${a}`
}

const sigLine = (m: GrpcMethodRecord) => `  ${m.name} ${methodSig(m)}`
const descLine = (m: GrpcMethodRecord) =>
  `  ${m.name} ${methodSig(m)}: ${
    (m.description || '').trim() || '(no description — stub it: context.abort UNIMPLEMENTED)'
  }`

// ---------------------------------------------------------------------------
// Bank apply → one propagation session over every affected service
// ---------------------------------------------------------------------------

// `entries` describe what was just applied, one per changed contract:
//   { contract, kind: 'methods'|'replace-proto'|'delete',
//     upserts?: [methodRecord], deletes?: [name], methods?: [postApplyRecords] }
// `impact` is the backend's manifest join: { owners:[{contract,service}],
//   clients:[{contract,service}] }.
export interface GrpcApplyEntry {
  contract: string
  kind: 'methods' | 'replace-proto' | 'delete'
  upserts?: GrpcMethodRecord[]
  deletes?: string[]
  methods?: GrpcMethodRecord[]
}

export interface GrpcImpact {
  owners?: Array<{ contract: string; service: string }>
  clients?: Array<{ contract: string; service: string }>
}

export function buildGrpcUpdatePrompt({
  systemId,
  entries,
  impact,
}: {
  systemId: string
  entries: GrpcApplyEntry[]
  impact: GrpcImpact
}): string {
  const lines = [
    `Use the sandbox-grpc-contract skill to propagate gRPC contract changes in the "${systemId}" system.`,
    ``,
    `The contract bank was just edited and the backend has ALREADY updated`,
    `systems/${systemId}/grpc/: the _registry.json, every changed .proto, and the regenerated`,
    `_pb2.py/_pb2_grpc.py bindings (deleted contracts are already scrubbed from the registry,`,
    `the manifest, and the generated files). Do NOT re-run protoc and do NOT edit any .proto.`,
    `Your job is to bring the affected services' CODE in line with the new shapes.`,
    ``,
    `What changed:`,
  ]
  for (const e of entries) {
    if (e.kind === 'delete') {
      lines.push(`- ${e.contract}: DELETED.`)
      continue
    }
    if (e.kind === 'replace-proto') {
      lines.push(`- ${e.contract}: replaced by an uploaded .proto. Its methods are now:`)
      lines.push(...(e.methods || []).map(sigLine))
      continue
    }
    lines.push(`- ${e.contract}:`)
    if (e.upserts?.length) {
      lines.push(`  added/changed methods:`)
      lines.push(...e.upserts.map((m) => `  ${descLine(m)}`))
    }
    if (e.deletes?.length) lines.push(`  removed methods: ${e.deletes.join(', ')}`)
  }
  lines.push(``, `Affected services (joined from the manifest before the change):`)
  for (const o of impact.owners || []) {
    lines.push(
      `- ${o.service} SERVES ${o.contract}: update systems/${systemId}/grpc/${o.contract}_servicer.py to the`,
      `  new method set — keep existing behavior for unchanged methods, implement changed/new methods from`,
      `  their descriptions above (a method with no description gets an UNIMPLEMENTED stub), remove deleted`,
      `  methods. For a DELETED contract, unwire the server registration from ${o.service}'s app.py instead.`,
    )
  }
  for (const c of impact.clients || []) {
    lines.push(
      `- ${c.service} CALLS ${c.contract}: update its call sites to the new message shapes; remove calls`,
      `  to deleted methods/contracts (and the stub wiring if the whole contract is gone).`,
    )
  }
  lines.push(
    ``,
    `Then rebuild each affected service (docker compose -f systems/${systemId}/docker-compose.yml`,
    `up -d --build <service>) and verify per the skill.`,
  )
  return lines.join('\n')
}

// ---------------------------------------------------------------------------
// Service tab: attach / describe / detach (all sandbox-grpc-attach)
// ---------------------------------------------------------------------------

// `methods` are the contract's full method records with the (possibly blank)
// descriptions the user just wrote in the attach form.
export function buildGrpcAttachPrompt({
  systemId,
  service,
  contract,
  methods,
}: {
  systemId: string
  service: string
  contract: string
  methods?: GrpcMethodRecord[] | null
}): string {
  return [
    `Use the sandbox-grpc-attach skill to attach the gRPC contract "${contract}" to service "${service}"`,
    `as its SERVER in the "${systemId}" system.`,
    ``,
    `${service} is now the contract's single owning server (the manifest grpc block and the registry`,
    `descriptions are already written). The .proto and _pb2 bindings already exist under`,
    `systems/${systemId}/grpc/ — do NOT regenerate them.`,
    ``,
    `Author the servicer systems/${systemId}/grpc/${contract}_servicer.py implementing each method`,
    `from its description:`,
    ...(methods || []).map(descLine),
    ``,
    `Then wire the service per the skill: grpc.aio server on port 50051 in the FastAPI lifespan`,
    `(+ the prometheus metrics interceptor), grpcio in requirements, the shared grpc package`,
    `mounted/importable, EXPOSE 50051 — and rebuild just this service.`,
  ].join('\n')
}

// `methods` here are only the EDITED ones: [{ name, signature-bearing record,
// priorDescription, change }].
export function buildGrpcDescriptionsPrompt({
  systemId,
  service,
  contract,
  methods,
}: {
  systemId: string
  service: string
  contract: string
  methods: Array<GrpcMethodRecord & { priorDescription?: string; change?: string }>
}): string {
  const lines = [
    `Use the sandbox-grpc-attach skill to UPDATE served gRPC method implementations on service`,
    `"${service}" for contract "${contract}" in the "${systemId}" system.`,
    ``,
    `These methods ALREADY EXIST in systems/${systemId}/grpc/${contract}_servicer.py. FIRST read it,`,
    `then modify ONLY the listed methods IN PLACE — preserve behavior the change doesn't mention,`,
    `leave other methods, the app wiring, and the metrics untouched. Do not re-run protoc; the`,
    `registry descriptions are already saved.`,
    ``,
  ]
  for (const m of methods) {
    lines.push(
      `${m.name} ${methodSig(m)}`,
      `  Current behavior (existing description): ${(m.priorDescription || '').trim() || '(none recorded)'}`,
      `  Change to apply: ${(m.change || '').trim()}`,
      ``,
    )
  }
  lines.push(`Then rebuild just this service and verify the changed methods respond per the skill.`)
  return lines.join('\n')
}

export function buildGrpcDetachPrompt({
  systemId,
  service,
  contract,
}: {
  systemId: string
  service: string
  contract: string
}): string {
  return [
    `Use the sandbox-grpc-attach skill to DETACH the gRPC contract "${contract}" from service`,
    `"${service}" in the "${systemId}" system.`,
    ``,
    `The manifest grpc block and the registry are already updated, and no other service dials`,
    `${service} for this contract. Do this:`,
    `1. Remove the server wiring from systems/${systemId}/${service}/app.py: the servicer import and`,
    `   add_${contract}Servicer_to_server(...). Keep the grpc server itself only if the service still`,
    `   serves other contracts; otherwise remove it (and the 50051 EXPOSE/wiring).`,
    `2. Delete systems/${systemId}/grpc/${contract}_servicer.py — the contract no longer has an owner`,
    `   (its .proto and _pb2 bindings stay in the bank).`,
    `3. Rebuild just this service and verify :50051 no longer serves ${contract}.`,
  ].join('\n')
}
