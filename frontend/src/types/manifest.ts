// Shared shapes of systems/<id>/manifest.json — the core abstraction the whole
// frontend renders. Grounded in the live manifests (systems/*/manifest.json) and
// the reads in SystemDiagram/App/the topology tabs; the backend scaffolding
// (frontend/server/scaffold.js and the feature plugins) is what writes these.

export type NodeType =
  | 'load_balancer'
  | 'service'
  | 'service-lb'
  | 'external_service'
  | 'client'
  | 'postgres'
  | 'mongodb'
  | 'redis'
  | 'object-store'
  | 'kafka'
  | 'etcd'
  | 'cdc'
  | 'dynamodb'
  | 'cassandra'
  | 'prometheus'
  | (string & {}) // custom service types ('download-coordinator', …) stay open

export interface Position {
  x: number
  y: number
}

export interface Boundary {
  x: number
  y: number
  w: number
  h: number
}

export interface MetricSpec {
  label: string
  query: string
  unit?: string
  scale?: number
}

export interface HealthRule {
  color: string
  // A tiny safe `value <op> number` expression, e.g. "value < 1"; first match wins.
  when: string
}

export interface HealthSpec {
  query: string
  rules: HealthRule[]
}

// A writer's declared write mode: WAIT pseudo-sync vs the async default.
export interface RedisWriteMode {
  mode?: string // 'wait'
  numreplicas?: number
  timeoutMs?: number
  // Scan-owned: whether a WAIT call was found in the writer's source.
  implemented?: boolean
  updatedAt?: string
}

// A redis node's live-edited typed key namespaces (managed by /api/redis, no rebuild).
export interface RedisKeyspace {
  name: string
  match?: string
  type?: string // 'string' | 'hash' | 'list' | 'set' | 'zset' | 'stream' | …
  shorthand?: string
  writers?: string[]
  readers?: string[]
  // Per-writer write mode: async vs WAIT pseudo-sync.
  writeModes?: Record<string, RedisWriteMode>
  verified?: boolean
  origin?: string
  suggestedWriters?: string[]
  suggestedReaders?: string[]
  observedType?: string
  lastScanAt?: string
  createdAt?: string
  updatedAt?: string
}

// Per-service load-balancer cluster entry (`type: "service-lb"` nodes).
export interface SvcLbBlock {
  algorithm: string
  // Instance node ids ('<name>-1'…) — serviceLb.js writes the id list, not a count.
  instances: string[]
}

// Replicated-with-Sentinel redis topology (primary + replicas + 3 sentinels).
export interface SentinelBlock {
  size: number
  quorum: number
  masterName?: string
  members: string[]
}

// Real Redis Cluster topology (`<name>-1..M` members behind the one node).
export interface RedisClusterBlock {
  shards: number
  replicasPerShard: number
  members: string[]
}

export interface PostgresHaSync {
  method?: string
  quorum?: number
  commitLevel?: string
  standbys?: string[]
}

// Postgres replicated-with-failover topology (streaming standbys + watcher).
export interface PostgresHaBlock {
  enabled: boolean
  autoDegrade?: boolean
  downAfterMs?: number
  primary?: string
  members?: string[]
  watcher?: string
  settings?: Record<string, unknown>
  dsn?: string
  sync?: PostgresHaSync
}

// The etcd singleton node's cluster block (a real N-member Raft cluster).
export interface EtcdBlock {
  size: number
  quorum?: number
  heartbeatMs?: number
  electionMs?: number
  leaseTtlSeconds?: number
  members: string[]
}

// One grpc.clients entry: a contract this service dials + its target node ids
// (as server/grpc.js writes it and the gRPC service tab reads it).
export interface GrpcClientEntry {
  contract: string
  targets?: string[]
}

// gRPC attachments: contracts this service serves / calls (bank-owned shapes).
export interface GrpcBlock {
  servers?: string[]
  clients?: GrpcClientEntry[]
  overrides?: unknown[]
}

// Circuit-breaker half of a connection's resilience policy, as the
// ConnectionResilienceModal writes it (edges[].resilience.circuit_breaker).
export interface CircuitBreakerConfig {
  enabled?: boolean
  failure_threshold?: number
  pause_duration_seconds?: number
  half_open_trial_calls?: number
  open_behavior?: string // 'fail_fast' | 'fallback'
  fallback_response?: string
}

// Retry half of a connection's resilience policy.
export interface RetryConfig {
  enabled?: boolean
  max_attempts?: number
  strategy?: string // 'exponential_backoff' | 'exponential_backoff_jitter'
  base_delay_seconds?: number
  max_delay_seconds?: number
}

// Per-connection resilience policy / connection pool blocks, keyed as the
// resilience & connection-pool flows write them.
export interface ResilienceBlock {
  circuit_breaker?: CircuitBreakerConfig
  retry?: RetryConfig
  instruction?: string
}

export interface ConnectionPoolBlock {
  enabled?: boolean
  max_connections?: number
  min_idle?: number
  idle_timeout_seconds?: number
  max_lifetime_seconds?: number
  instruction?: string
}

// Kafka consumer-group custom node's link back to its cluster + group.
export interface ConsumerGroupBlock {
  cluster: string
  groupId: string
}

// LLM worker node's link to its token-stream redis.
export interface LlmBlock {
  stream: string
}

// Persistence-reader node's claim/accumulate/persist wiring.
export interface PersistenceBlock {
  worker: string
  stream: string
  announce: string
  group: string
  fn: string
  db: string
  table: string
  field: string
}

export interface ManifestNode {
  id: string
  label: string
  type: NodeType
  position: Position
  metrics?: MetricSpec[]
  health?: HealthSpec
  // Provenance: which create-flow wrote this node ('create-service', …).
  origin?: string
  // Clients + external services: drawn outside the system boundary.
  external?: boolean
  // Stateful client (session-loop instances instead of req/s).
  stateful?: boolean
  // Custom service type ('llm_worker', 'download_coordinator', …).
  service_type?: string
  // Ownership / relationship links.
  instanceOf?: string
  replicaOf?: string
  cdcOf?: string
  scalerOf?: string
  streamOf?: string
  // Replica-group members (`<id>-2..N`) written by the shared replica reconciler
  // (server/replicaGroup.js) when a custom service scales.
  replicas?: { instances: string[] }
  // Download coordinator's distribution config (chunk size in bytes).
  coordinator?: { chunk_size: number }
  // Download worker → the coordinator node it registers with.
  coordinatorId?: string
  replication?: 'sync' | 'async'
  // False on a cluster member that accepts writes (e.g. a cassandra ring peer).
  readonly?: boolean
  // Model-bank names backing a database's schema.
  schemaModels?: string[]
  svcLb?: SvcLbBlock
  grpc?: GrpcBlock
  resilience?: ResilienceBlock
  connection_pool?: ConnectionPoolBlock
  keyspaces?: RedisKeyspace[]
  sentinel?: SentinelBlock
  redisCluster?: RedisClusterBlock
  postgresHa?: PostgresHaBlock
  etcd?: EtcdBlock
  consumerGroup?: ConsumerGroupBlock
  llm?: LlmBlock
  persistence?: PersistenceBlock
  // WebSocket tier roles ('lb' | 'server') + which tier a node belongs to.
  wsRole?: string
  wsTier?: string
}

export interface ManifestEdge {
  from: string
  to: string
  // e.g. 'consumer-fn' for a Kafka consumer-function edge.
  origin?: string
  // Per-connection policy blocks the resilience / connection-pool flows write
  // on the edge (App reads them to seed ConnectionResilienceModal).
  resilience?: ResilienceBlock
  connection_pool?: ConnectionPoolBlock
}

export interface Manifest {
  system_id?: string
  name?: string
  prometheus_base: string
  poll_interval_ms?: number
  nodes: ManifestNode[]
  edges: ManifestEdge[]
  boundary?: Boundary
}
