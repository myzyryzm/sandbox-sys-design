// Mechanical .proto authoring for the gRPC contract bank.
//
// The bank is pure SHAPE: the backend synthesizes/edits proto text from the
// registry's method records and generates the _pb2 bindings itself (real protoc
// in a throwaway container) — no Claude session touches a .proto. Behavior
// (the servicer) is authored separately, at attach time, from per-method
// descriptions (see grpc.js + the sandbox-grpc-attach skill).
//
// Two proto sources, two edit strategies:
//   - source:"form"   — the registry field maps are authoritative; the whole
//     proto is REGENERATED (synthesizeFormProto), preserving the package line
//     and every persisting field's number, and emitting `reserved` for removed
//     numbers so they are never reused.
//   - source:"upload" — the on-disk .proto is authoritative; form edits are a
//     mechanical SPLICE (spliceUploadedProto): new rpc lines into the service
//     block + synthesized messages at EOF. Upload-born methods are opaque
//     (nested types etc.) and can only be deleted or replaced by re-upload.
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { HttpError } from './scaffold.js'

const pexec = promisify(execFile)

const bad = (msg) => new HttpError(400, msg)

// ---------------------------------------------------------------------------
// Comment handling
// ---------------------------------------------------------------------------

// Drop // line and /* */ block comments so a comment can't fake or hide a
// `service`/`import`/`rpc` token during structural checks.
export function stripProtoComments(src) {
  return src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')
}

// Same, but replace comment characters with spaces so every index in the
// blanked copy maps 1:1 onto the original — lets us scan on the blanked text
// and slice/splice the original (comments preserved).
function blankComments(src) {
  return src
    .replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' '))
    .replace(/\/\/[^\n]*/g, (m) => ' '.repeat(m.length))
}

// rpc <Name> ( [stream] <Req> ) returns ( [stream] <Res> )  — body or `;` either way.
export const RPC_RE =
  /\brpc\s+([A-Za-z_]\w*)\s*\(\s*(stream\s+)?([A-Za-z_][\w.]*)\s*\)\s*returns\s*\(\s*(stream\s+)?([A-Za-z_][\w.]*)\s*\)/g

// The registry method shape for one parsed rpc (upload/replace paths).
export function parseRpcMethods(protoText) {
  const src = stripProtoComments(protoText)
  return [...src.matchAll(RPC_RE)].map((m) => ({
    name: m[1],
    request: {},
    response: {},
    requestType: m[3],
    responseType: m[5],
    requestStreaming: !!m[2],
    responseStreaming: !!m[4],
  }))
}

// ---------------------------------------------------------------------------
// Structural parser (brace-matched top-level blocks; flat-message fields)
// ---------------------------------------------------------------------------

// Find the index of the `}` closing the `{` at `open` (indexes into `blank`).
function matchBrace(blank, open) {
  let depth = 0
  for (let i = open; i < blank.length; i++) {
    if (blank[i] === '{') depth++
    else if (blank[i] === '}') {
      depth--
      if (depth === 0) return i
    }
  }
  return -1
}

// `reserved 2, 5, 9 to 11;` -> [2, 5, 9, 10, 11]
function parseReserved(body) {
  const out = []
  for (const m of body.matchAll(/\breserved\s+([^;]+);/g)) {
    for (const part of m[1].split(',')) {
      const range = part.match(/^\s*(\d+)\s+to\s+(\d+)\s*$/)
      if (range) {
        for (let n = +range[1]; n <= +range[2]; n++) out.push(n)
      } else {
        const n = part.match(/^\s*(\d+)\s*$/)
        if (n) out.push(+n[1])
      }
    }
  }
  return out
}

// Parse a .proto's top-level structure. All start/end indexes are into the
// ORIGINAL text (end exclusive), so callers can slice/splice it verbatim.
export function parseProto(text) {
  const blank = blankComments(text)

  const pkgMatch = blank.match(/\bpackage\s+([A-Za-z_][\w.]*)\s*;/)
  const pkg = pkgMatch ? pkgMatch[1] : null

  const services = []
  const messages = new Map()
  const blockRe = /\b(service|message|enum)\s+([A-Za-z_]\w*)\s*\{/g
  let m
  while ((m = blockRe.exec(blank))) {
    const open = m.index + m[0].length - 1
    const close = matchBrace(blank, open)
    if (close < 0) throw bad(`unbalanced braces in ${m[1]} ${m[2]}`)
    // Only top-level blocks: skip if inside a previously seen block.
    const inside =
      services.some((b) => m.index > b.start && m.index < b.end) ||
      [...messages.values()].some((b) => m.index > b.start && b.end && m.index < b.end)
    if (inside) continue
    const block = {
      kind: m[1],
      name: m[2],
      start: m.index,
      bodyStart: open + 1,
      bodyEnd: close,
      end: close + 1,
    }
    if (m[1] === 'service') {
      services.push(block)
    } else if (m[1] === 'message') {
      const body = blank.slice(block.bodyStart, block.bodyEnd)
      // A message containing nested blocks/oneofs is "complex": kept verbatim,
      // never field-edited (only whole-message carry-over or replacement).
      const complex = /\b(message|enum|oneof|map\s*<)\b/.test(body)
      const fields = complex
        ? []
        : [...body.matchAll(/(repeated\s+)?([A-Za-z_][\w.]*)\s+([a-z][a-z0-9_]*)\s*=\s*(\d+)\s*;/g)]
            .filter((f) => f[2] !== 'reserved')
            .map((f) => ({ repeated: !!f[1], type: (f[1] ? 'repeated ' : '') + f[2], name: f[3], number: +f[4] }))
      messages.set(block.name, {
        ...block,
        complex,
        fields,
        reserved: parseReserved(body),
        raw: text.slice(block.start, block.end),
      })
    }
    blockRe.lastIndex = close + 1
  }

  // rpc extents (into the original text): from `rpc` to its `;` or `{...}` body end.
  const rpcs = []
  const rpcRe = new RegExp(RPC_RE.source, 'g')
  while ((m = rpcRe.exec(blank))) {
    let end = m.index + m[0].length
    while (end < blank.length && /\s/.test(blank[end])) end++
    if (blank[end] === '{') {
      const close = matchBrace(blank, end)
      if (close < 0) throw bad(`unbalanced braces in rpc ${m[1]}`)
      end = close + 1
    } else if (blank[end] === ';') {
      end++
    }
    rpcs.push({
      name: m[1],
      requestType: m[3],
      responseType: m[5],
      requestStreaming: !!m[2],
      responseStreaming: !!m[4],
      start: m.index,
      end,
    })
  }

  return { pkg, services, messages, rpcs }
}

// ---------------------------------------------------------------------------
// Synthesis (source:"form" — the registry is authoritative)
// ---------------------------------------------------------------------------

// proto package identifiers can't contain `-` (system ids can): strip to a
// bare lowercase word, matching the hand-authored style (llmworker, …).
function packageFor(systemId) {
  const pkg = String(systemId).toLowerCase().replace(/[^a-z0-9]/g, '')
  return /^[a-z]/.test(pkg) ? pkg : `s${pkg || 'andbox'}`
}

export const requestTypeOf = (m) => m.requestType || `${m.name}Request`
export const responseTypeOf = (m) => m.responseType || `${m.name}Reply`

// Build one flat message from a { field: type } map, preserving numbers from
// `prevMsg` for persisting field names, appending new fields at max+1, and
// reserving the numbers of removed fields (plus anything already reserved).
function buildMessage(name, fieldMap, prevMsg) {
  const prevFields = prevMsg && !prevMsg.complex ? prevMsg.fields : []
  const prevReserved = prevMsg ? prevMsg.reserved : []
  let next = Math.max(0, ...prevFields.map((f) => f.number), ...prevReserved) + 1

  const lines = Object.entries(fieldMap).map(([fname, ftype]) => {
    const prev = prevFields.find((f) => f.name === fname)
    const number = prev ? prev.number : next++
    return `  ${ftype} ${fname} = ${number};`
  })
  const reserved = [
    ...new Set([...prevReserved, ...prevFields.filter((f) => !(f.name in fieldMap)).map((f) => f.number)]),
  ].sort((a, b) => a - b)
  if (reserved.length) lines.unshift(`  reserved ${reserved.join(', ')};`)

  return `message ${name} {\n${lines.join('\n')}${lines.length ? '\n' : ''}}`
}

// Regenerate a form contract's whole .proto from its desired method list.
// `methods` are full registry records ({ name, request, response, requestType,
// responseType, requestStreaming, responseStreaming, formAuthored }).
// Locked (non-form) methods and helper messages are carried over verbatim from
// `existingText`; messages owned only by dropped methods are dropped (protoc
// then catches any dangling reference before anything is persisted).
export function synthesizeFormProto(systemId, contract, methods, existingText) {
  const prev = existingText ? parseProto(existingText) : null
  const pkg = prev?.pkg || packageFor(systemId)

  const rpcLines = methods.map((m) => {
    const req = `${m.requestStreaming ? 'stream ' : ''}${requestTypeOf(m)}`
    const res = `${m.responseStreaming ? 'stream ' : ''}${responseTypeOf(m)}`
    return `  rpc ${m.name} (${req}) returns (${res});`
  })

  // Message names owned by the desired methods, in emission order.
  const chunks = []
  const emitted = new Set()
  for (const m of methods) {
    for (const [msgName, fieldMap] of [
      [requestTypeOf(m), m.request || {}],
      [responseTypeOf(m), m.response || {}],
    ]) {
      if (emitted.has(msgName)) continue
      emitted.add(msgName)
      const prevMsg = prev?.messages.get(msgName)
      if (m.formAuthored === false && prevMsg) {
        chunks.push(prevMsg.raw) // locked method: keep its message verbatim
      } else {
        chunks.push(buildMessage(msgName, fieldMap, prevMsg))
      }
    }
  }

  // Helper messages (referenced types like WorkerEndpoint): every previous
  // message that is not owned by ANY method, past or present, is carried over.
  if (prev) {
    const ownedBefore = new Set(prev.rpcs.flatMap((r) => [r.requestType, r.responseType]))
    for (const [name, msg] of prev.messages) {
      if (!emitted.has(name) && !ownedBefore.has(name)) {
        chunks.push(msg.raw)
        emitted.add(name)
      }
    }
  }

  return [
    'syntax = "proto3";',
    '',
    `package ${pkg};`,
    '',
    `service ${contract} {`,
    ...rpcLines,
    '}',
    '',
    ...chunks.flatMap((c) => [c, '']),
  ].join('\n')
}

// ---------------------------------------------------------------------------
// Splice (source:"upload" — the on-disk .proto is authoritative)
// ---------------------------------------------------------------------------

// Apply form-method upserts/deletes to an uploaded proto by string surgery:
// remove deleted rpc lines (their messages stay — orphans compile) and replaced
// rpc lines (their messages are removed too, unless shared, and their field
// numbers are preserved into the re-synthesized ones), insert new rpc lines
// before the service block's closing `}`, and append the synthesized messages
// at EOF. protoc re-validates the result before anything is persisted.
// `lockedNames` are upload-born methods (delete-only).
export function spliceUploadedProto(text, { upserts = [], deletes = [], lockedNames = new Set() }) {
  let out = text
  for (const u of upserts) {
    if (lockedNames.has(u.name)) {
      throw bad(`method "${u.name}" came from an uploaded .proto — delete it or re-upload the contract to reshape it`)
    }
  }

  // 1. Remove rpc extents for deletes and replaced upserts, plus the replaced
  //    upserts' own messages (kept when another surviving rpc shares the type).
  const parsed = parseProto(out)
  const byName = new Map(parsed.rpcs.map((r) => [r.name, r]))
  const gone = new Set([...deletes, ...upserts.map((u) => u.name)])
  const survivingTypes = new Set(
    parsed.rpcs.filter((r) => !gone.has(r.name)).flatMap((r) => [r.requestType, r.responseType]),
  )
  const removals = parsed.rpcs.filter((r) => gone.has(r.name)).map((r) => ({ start: r.start, end: r.end }))
  const prevMsgOf = new Map() // message name -> parsed prev message (for numbering)
  for (const u of upserts) {
    const old = byName.get(u.name)
    if (!old) continue
    for (const t of [old.requestType, old.responseType]) {
      const msg = parsed.messages.get(t)
      if (!msg || survivingTypes.has(t) || prevMsgOf.has(t)) continue
      prevMsgOf.set(t, msg)
      removals.push({ start: msg.start, end: msg.end })
    }
  }
  removals.sort((a, b) => b.start - a.start)
  for (const r of removals) out = out.slice(0, r.start) + out.slice(r.end)

  if (!upserts.length) return out

  // 2. Collision check + message synthesis against the post-removal text.
  const after = parseProto(out)
  const service = after.services[0]
  if (!service) throw bad('no service block found in the stored .proto')
  const newMessages = []
  const rpcLines = []
  for (const u of upserts) {
    const reqName = requestTypeOf(u)
    const resName = responseTypeOf(u)
    for (const [msgName, fieldMap] of [[reqName, u.request || {}], [resName, u.response || {}]]) {
      if (after.messages.has(msgName)) {
        throw bad(`message "${msgName}" already exists in ${service.name}.proto — pick a different method name or re-upload`)
      }
      if (!newMessages.some((n) => n.name === msgName)) {
        newMessages.push({ name: msgName, text: buildMessage(msgName, fieldMap, prevMsgOf.get(msgName) || null) })
      }
    }
    const req = `${u.requestStreaming ? 'stream ' : ''}${reqName}`
    const res = `${u.responseStreaming ? 'stream ' : ''}${resName}`
    rpcLines.push(`  rpc ${u.name} (${req}) returns (${res});`)
  }

  // 3. Insert rpc lines just before the service's closing brace, messages at EOF.
  out =
    out.slice(0, service.bodyEnd).replace(/\s*$/, '\n') +
    rpcLines.join('\n') +
    '\n' +
    out.slice(service.bodyEnd).replace(/\s*$/, '\n') +
    '\n' +
    newMessages.map((n) => n.text).join('\n\n') +
    '\n'
  return out.replace(/\n{3,}/g, '\n\n')
}

// ---------------------------------------------------------------------------
// protoc (validate + generate, outputs KEPT)
// ---------------------------------------------------------------------------

// Compile `protoFiles` (names like "Ping.proto", PascalCase-validated upstream)
// in `dir` with the real protoc, KEEPING the generated _pb2.py/_pb2_grpc.py in
// `dir`. One container run for the lot. Pin grpcio-tools to match the runtime
// pins in service requirements (mismatched protobuf fails at import). protoc
// diagnostics ("<C>.proto:LINE:COL: …") come back verbatim as a 400; anything
// else (pull/pip/daemon) is a 500 infra failure. The proto text only ever lands
// in mounted files, never a shell arg.
export async function runProtocKeep(dir, protoFiles) {
  if (process.env.GRPC_SKIP_PROTOC === '1') return
  const script =
    `pip install -q --root-user-action=ignore grpcio-tools==1.68.1 >/dev/null 2>&1 && ` +
    `python -m grpc_tools.protoc -I /g --python_out=/g --grpc_python_out=/g ${protoFiles.join(' ')}`
  try {
    await pexec(
      'docker',
      ['run', '--rm', '-v', `${dir}:/g`, '-w', '/g', 'python:3.12-slim', 'sh', '-c', script],
      { timeout: 180_000, maxBuffer: 16 * 1024 * 1024 },
    )
  } catch (err) {
    const detail = `${err.stderr || ''}${err.stdout || ''}`.replaceAll('/g/', '').trim()
    if (/\.proto:\d+/.test(detail) || protoFiles.some((f) => detail.includes(`${f}:`))) {
      throw new HttpError(400, `the .proto did not compile:\n${detail}`)
    }
    throw new HttpError(500, `proto generation could not run (is Docker available?):\n${detail || err.message}`)
  }
}
