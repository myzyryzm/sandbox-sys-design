// Shared metadata for CDC RULES (the `rules` in systems/<id>/<db>/cdc.json, managed by
// /api/db-cdc — see frontend/server/cdc.js). Used by the diagram rows/trace and the
// database's CDC edit tab. Mirrors the server-side CANON_OPS in frontend/server/cdc.js.

// The canonical operation order — rows badge a rule's operations in this order regardless
// of the order they were checked, so two rules with the same ops always read the same way.
import type { CdcRule } from './types/registries'

export const CDC_OPS = ['INSERT', 'UPDATE', 'DELETE']

// Row badge per operation — 3 chars, so several fit on one row beside the entity name.
export const CDC_BADGE: Record<string, string> = {
  INSERT: 'INS',
  UPDATE: 'UPD',
  DELETE: 'DEL',
}

// Per-operation badge class (each op has its own configurable color — see prefixColors.js).
export const CDC_BADGE_CLASS: Record<string, string> = {
  INSERT: 'endpoint-method-cdc-ins',
  UPDATE: 'endpoint-method-cdc-upd',
  DELETE: 'endpoint-method-cdc-del',
}

// A rule's operations in canonical order, ignoring anything unrecognized.
export const cdcOpsOf = (rule?: CdcRule | null): string[] =>
  CDC_OPS.filter((op) => (rule?.operations || []).includes(op))

// A rule's identity, as the backend upserts it (frontend/server/cdc.js): the same table can
// feed several topics, so the table alone doesn't identify a rule.
export const cdcRuleKey = (r: CdcRule): string => `${r.table}|${r.stream}|${r.topic}`

// The trace-edge label for the rule's publish arrow (cdc worker → its stream), e.g.
// "publishes refund to refunds".
export const cdcEdgeLabel = (rule: CdcRule): string => `publishes ${rule.table} to ${rule.topic}`
