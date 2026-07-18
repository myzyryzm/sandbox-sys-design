// Shapes of the per-system plain-JSON registries (systems/<id>/*.json) the
// frontend reads live, plus the Prometheus HTTP API responses. Grounded in the
// live registry files across systems/ and in CLAUDE.md's registry contract.

// ─── endpoints.json ─────────────────────────────────────────────────────────

// One entry of an endpoint's edit history (previous saved revisions).
export interface EndpointHistoryEntry {
  at?: string
  alias?: string
  path?: string
  request?: Record<string, unknown>
  response?: Record<string, unknown>
  requestModel?: string | null
  responseModel?: string | null
  description?: string
  downstream?: string[]
  downstreamDescriptions?: Record<string, string>
  conversationId?: string
}

export interface EndpointEntry {
  method: string
  path: string
  protocol?: string
  alias?: string
  request?: Record<string, unknown>
  response?: Record<string, unknown>
  requestModel?: string | null
  responseModel?: string | null
  description?: string
  // What the diagram's lifecycle trace draws.
  downstream?: string[]
  downstreamDescriptions?: Record<string, string>
  conversationId?: string
  history?: EndpointHistoryEntry[]
  internal?: boolean
  createdAt?: string
  updatedAt?: string
}

// service id → its endpoint entries.
export type EndpointsMap = Record<string, EndpointEntry[]>

// GET /api/endpoints merges the registry onto live OpenAPI discovery through the
// lb: each entry gains its owning service (its `path` is LB-prefixed as
// `/<service><local>`) and the resolved downstream method refs per node.
export interface DiscoveredEndpoint extends EndpointEntry {
  service: string
  downstreamMethods?: Record<string, string[]>
}

// ─── models.json (the per-system model bank) ────────────────────────────────

export interface ModelRecord {
  name: string
  // The TypeScript interface source; `//` comments are schema directives.
  ts: string
  description?: string
  createdAt?: string
  updatedAt?: string
}

export interface ModelsFile {
  models: ModelRecord[]
}

// ─── scenarios.json (client functions) ──────────────────────────────────────

export interface ScenarioArg {
  name: string
  type: string
}

// Statically re-inferred from the client's Python on every read.
export interface ScenarioStep {
  method: string
  path: string
  label?: string
}

export interface ScenarioFunction {
  client: string
  name: string
  args?: ScenarioArg[]
  description?: string
  steps?: ScenarioStep[]
  conversationId?: string
  history?: unknown[]
  createdAt?: string
  updatedAt?: string
}

export interface ScenariosFile {
  functions: ScenarioFunction[]
}

// ─── consumers.json (per-service Kafka consumer functions) ──────────────────

export interface ConsumerEntry {
  service: string
  name: string
  cluster: string
  topic: string
  pollRate?: number
  groupId?: string
  downstream?: string[]
  downstreamDescriptions?: Record<string, string>
  description?: string
  implemented?: boolean
  conversationId?: string
  history?: unknown[]
  createdAt?: string
  updatedAt?: string
}

export interface ConsumersFile {
  consumers: ConsumerEntry[]
}

// ─── etcd.json (cluster config + keyspace registry) ─────────────────────────

export interface EtcdClusterConfig {
  id: string
  size: number
  heartbeatMs: number
  electionMs: number
  leaseTtlSeconds: number
  createdAt?: string
  updatedAt?: string
}

export interface EtcdListener {
  service: string
  description?: string
  implemented?: boolean
  conversationId?: string
  history?: unknown[]
}

// Discovery keyspace (identity = service, prefix /services/<service>/).
// Entries without `type` are discovery.
export interface EtcdDiscoveryKeyspace {
  type?: 'discovery'
  service: string
  prefix: string
  description?: string
  implemented?: boolean
  conversationId?: string
  history?: unknown[]
  listeners?: EtcdListener[]
  createdAt?: string
  updatedAt?: string
}

// Config keyspace (identity = name, prefix /config/<name>/): persistent
// key/values edited in the Keyspaces tab, no lease.
export interface EtcdConfigKeyspace {
  type: 'config'
  name: string
  prefix: string
  description?: string
  values?: Array<{ key: string; value: string }>
  listeners?: EtcdListener[]
  createdAt?: string
  updatedAt?: string
}

export type EtcdKeyspace = EtcdDiscoveryKeyspace | EtcdConfigKeyspace

export interface EtcdFile {
  cluster: EtcdClusterConfig
  keyspaces: EtcdKeyspace[]
}

// ─── streams.json (per Kafka cluster) ───────────────────────────────────────

export interface TopicConsumer {
  groupId: string
  members?: string[]
}

export interface TopicEntry {
  id: string
  producers?: string[]
  consumers?: TopicConsumer[]
  // Model-bank name used as the topic's message contract.
  schemaModel?: string
  enforceSchema?: boolean
}

export interface StreamsFile {
  topics: TopicEntry[]
  consumersPaused?: boolean
}

// ─── grpc/_registry.json (the gRPC contract bank — pure shape) ──────────────

export interface GrpcMethodRecord {
  name: string
  request?: Record<string, unknown>
  response?: Record<string, unknown>
  requestType?: string
  responseType?: string
  requestStreaming?: boolean
  responseStreaming?: boolean
  formAuthored?: boolean
  description?: string
}

export interface GrpcContract {
  instruction?: string
  methods: GrpcMethodRecord[]
  // The one owning service that serves this contract.
  server?: string
  source?: string
  conversationId?: string
  createdAt?: string
}

export interface GrpcRegistry {
  contracts: Record<string, GrpcContract>
}

// ─── endtoend.json (end-to-end test processes) ──────────────────────────────

// A stateless client row carries requestsPerSecond; a stateful one, instances.
export interface EndToEndClientRow {
  client: string
  method: string
  requestsPerSecond?: number
  instances?: number
  // Legacy rows, normalized on read (rps = 1/interval).
  intervalSeconds?: number
}

export interface EndToEndProcess {
  id: string
  name: string
  client_list?: EndToEndClientRow[]
  websocket_list?: unknown[]
  failure_list?: string[]
  constraint_list?: string[]
  createdAt?: string
  updatedAt?: string
}

export interface EndToEndFile {
  processes: EndToEndProcess[]
}

// ─── <db>/cdc.json (a database's Change-Data-Capture rules) ─────────────────

export interface CdcRule {
  table: string
  operations?: string[]
  stream?: string
  topic?: string
}

// ─── GET /api/model-usage (what references each model-bank model) ───────────

export interface ModelUsageEndpoint {
  service: string
  method: string
  path: string
  field?: string
}

export interface ModelUsageDatabase {
  id: string
  engine?: string
}

export interface ModelUsageStream {
  cluster: string
  topic: string
  enforce?: boolean
}

export type ModelUsageMap = Record<
  string,
  {
    endpoints?: ModelUsageEndpoint[]
    databases?: ModelUsageDatabase[]
    streams?: ModelUsageStream[]
  }
>

// ─── Prometheus HTTP API (via the /api/prometheus proxy) ────────────────────

export interface PromSample {
  metric: Record<string, string>
  // [unix seconds, value-as-string]
  value: [number, string]
}

export interface PromInstantResponse {
  status: string
  error?: string
  data?: {
    resultType: string
    result: PromSample[]
  }
}

export interface VectorSample {
  labels: Record<string, string>
  value: number
}
