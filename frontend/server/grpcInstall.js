// Direct-write installer for FIXED gRPC contracts shipped by a custom service type.
//
// A custom service type (e.g. the Download Coordinator) owns known, fixed contracts.
// Rather than the modal's "register metadata → launch a Claude session to codegen"
// path, this installs them deterministically: it writes the .proto + the single shared
// servicer into systems/<id>/grpc/, generates the _pb2/_pb2_grpc bindings with the real
// protoc, and upserts the _registry.json entry — producing an END STATE byte-identical
// to a modal/session-authored contract (same files, same registry shape), so the gRPC
// modal lists/views/edits them exactly the same. There is NO separate hidden path: the
// bank is the one and only home for these contracts.
//
// Reuses the throwaway-container protoc invocation from grpc.js' validate path, but
// keeps the generated outputs (writes them into the real grpc dir).
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { systemDir } from './systems.js'
import { HttpError } from './scaffold.js'

const pexec = promisify(execFile)

const grpcDir = (system) => path.join(systemDir(system), 'grpc')
const registryFile = (system) => path.join(grpcDir(system), '_registry.json')

function readRegistry(system) {
  try {
    const raw = JSON.parse(fs.readFileSync(registryFile(system), 'utf8'))
    return raw && typeof raw.contracts === 'object' && raw.contracts ? raw : { contracts: {} }
  } catch {
    return { contracts: {} }
  }
}
function writeRegistry(system, registry) {
  fs.mkdirSync(grpcDir(system), { recursive: true })
  fs.writeFileSync(registryFile(system), JSON.stringify(registry, null, 2) + '\n')
}

// Same rpc parser as grpc.js' upload path, so a directly-installed contract records the
// identical method shape (name + request/response message types + streaming flags).
const RPC_RE =
  /\brpc\s+([A-Za-z_]\w*)\s*\(\s*(stream\s+)?([A-Za-z_][\w.]*)\s*\)\s*returns\s*\(\s*(stream\s+)?([A-Za-z_][\w.]*)\s*\)/g
function parseMethods(proto) {
  return [...proto.matchAll(RPC_RE)].map((m) => ({
    name: m[1],
    request: {},
    response: {},
    requestType: m[3],
    responseType: m[5],
    requestStreaming: !!m[2],
    responseStreaming: !!m[4],
  }))
}

// Write a contract's source-of-truth files + registry entry. Does NOT generate bindings
// (call generateBindings once after writing all of a package's contracts — one protoc
// run for the lot). Mirrors the registry "upload-shape" used by grpc.js so the modal
// treats it identically.
export function writeContract(system, { contract, proto, servicer, instruction, source }) {
  const dir = grpcDir(system)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(path.join(dir, `${contract}.proto`), proto)
  fs.writeFileSync(path.join(dir, `${contract}_servicer.py`), servicer)

  const registry = readRegistry(system)
  registry.contracts[contract] = {
    instruction: instruction || '',
    methods: parseMethods(proto),
    // Deterministic provenance id (no Claude session). The modal can still open it.
    conversationId: registry.contracts[contract]?.conversationId || `custom-${source || 'custom'}-${contract}`,
    createdAt: registry.contracts[contract]?.createdAt || new Date().toISOString(),
    source: source || 'custom',
  }
  writeRegistry(system, registry)
  return registry.contracts[contract]
}

// Generate <Contract>_pb2.py + _pb2_grpc.py for the given contracts INTO
// systems/<id>/grpc/, using the real protoc in a throwaway container (one run for all).
// The .proto files must already be written there.
export async function generateBindings(system, contracts) {
  if (process.env.GRPC_SKIP_PROTOC === '1') return
  const dir = grpcDir(system)
  const protos = contracts.map((c) => `${c}.proto`).join(' ')
  // Pin grpcio-tools so the generated _pb2 matches the protobuf/grpcio runtime pinned in
  // the coordinator/worker requirements (mismatched protobuf versions fail at import).
  const script =
    `pip install -q --root-user-action=ignore grpcio-tools==1.68.1 >/dev/null 2>&1 && ` +
    `python -m grpc_tools.protoc -I /g --python_out=/g --grpc_python_out=/g ${protos}`
  try {
    await pexec(
      'docker',
      ['run', '--rm', '-v', `${dir}:/g`, '-w', '/g', 'python:3.12-slim', 'sh', '-c', script],
      { timeout: 180_000, maxBuffer: 16 * 1024 * 1024 },
    )
  } catch (err) {
    const detail = `${err.stderr || ''}${err.stdout || ''}`.replaceAll('/g/', '').trim()
    if (/\.proto:\d+/.test(detail)) throw new HttpError(400, `a .proto did not compile:\n${detail}`)
    throw new HttpError(500, `proto generation could not run (is Docker available?):\n${detail || err.message}`)
  }
}

// Convenience: write a batch of contracts then generate all their bindings in one run.
// `contracts` is [{ contract, proto, servicer, instruction, source }].
export async function installContracts(system, contracts) {
  for (const c of contracts) writeContract(system, c)
  await generateBindings(system, contracts.map((c) => c.contract))
  return contracts.map((c) => c.contract)
}
