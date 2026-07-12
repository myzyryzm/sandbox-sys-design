// Shared source of truth for the user-configurable "prefix" colors — the small
// colored label badges on diagram rows (HTTP verbs, the client-function ƒ, PULL,
// KEY/SUB, WATCH, RPC) and the diagram edges/arrowheads "tinted to match" them.
// The Settings modal edits these; they persist to the repo-root settings.json via
// /api/settings and load at app start.
//
// Each ROLE maps to badge CSS variable(s) consumed by styles.css AND, for the three
// relationship edges, a color the diagram reads through SystemDiagram's `colors`
// prop (TRACE_COLOR / CONSUME_COLOR / GRPC_COLOR / ETCD_COLOR). The defaults below
// MUST match the fallbacks hardcoded in styles.css and SystemDiagram.jsx so an unset
// value renders identically to before this feature existed.
export const DEFAULT_PREFIX_COLORS = {
  http: '#38ffbd', // GET/POST/PUT/PATCH/DELETE badges
  function: '#6ea8fe', // ƒ + WATCH badges, and the endpoint/function trace edge
  consumer: '#e0a44f', // PULL badge + the Kafka consume edge
  grpc: '#b18cf2', // RPC badge + the gRPC edge
  etcdKey: '#ff9eed', // etcd KEY / SUB badges
  etcdEdge: '#5aa0c0', // etcd discovery-wiring edge (edge-only, no badge)
  redisKey: '#ff6b5e', // redis keyspace type badges (STR / LIST / SET / HSET / ZSET / STRM / GEO)
  // A CDC rule row badges the OPERATIONS that fire it — one badge per op, so a rule can show
  // several. They get a color each (rather than one shared "CDC" color like redisKey) because
  // create / modify / remove is the distinction you actually read off the node at a glance.
  cdcInsert: '#4ec9a0', // CDC INS badge
  cdcUpdate: '#d9a441', // CDC UPD badge
  cdcDelete: '#e0574f', // CDC DEL badge
}

// Role -> the CSS custom property styles.css reads for that role's badge(s). Roles
// with no badge (etcdEdge is edge-only) are absent here.
export const BADGE_VARS = {
  http: '--badge-http',
  function: '--badge-fn',
  consumer: '--badge-consume',
  grpc: '--badge-rpc',
  etcdKey: '--badge-etcd',
  redisKey: '--badge-redis',
  cdcInsert: '--badge-cdc-ins',
  cdcUpdate: '--badge-cdc-upd',
  cdcDelete: '--badge-cdc-del',
}

// Human labels for the Settings UI: which prefixes each role paints.
export const PREFIX_ROLE_LABELS = {
  http: 'HTTP (GET / POST / …)',
  function: 'Function ƒ / WATCH',
  consumer: 'PULL (Kafka consumer)',
  grpc: 'RPC (gRPC)',
  etcdKey: 'KEY / SUB (etcd)',
  etcdEdge: 'etcd discovery edge',
  redisKey: 'STR / STRM / … (redis keyspace)',
  cdcInsert: 'INS (CDC insert)',
  cdcUpdate: 'UPD (CDC update)',
  cdcDelete: 'DEL (CDC delete)',
}

// A #rrggbb string, the only shape the backend accepts (mirrors the server regex).
export const HEX_RE = /^#[0-9a-fA-F]{6}$/

// Set the --badge-* CSS variables on :root from a colors map, so every badge across
// the SVG diagram (fill) and the HTML lists (color) re-tints live. Missing/invalid
// keys fall back to the defaults. Called by App on load and by the Settings modal on save.
export function applyBadgeColors(colors) {
  const root = document.documentElement
  for (const [role, cssVar] of Object.entries(BADGE_VARS)) {
    const value = HEX_RE.test(colors?.[role]) ? colors[role] : DEFAULT_PREFIX_COLORS[role]
    root.style.setProperty(cssVar, value)
  }
}
