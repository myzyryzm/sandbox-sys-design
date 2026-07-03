/**
 * Health-rule evaluation for node coloring.
 *
 * Rules in the manifest look like:
 *   { "color": "red",   "when": "value < 1" }
 *   { "color": "green", "when": "value >= 1" }
 *
 * `when` is a deliberately tiny expression language: the literal `value`, a
 * comparison operator, and a number. We do NOT eval() arbitrary strings — we
 * parse exactly this shape so the manifest stays declarative and safe.
 */

const RULE_RE = /^value\s*(<=|>=|==|!=|<|>)\s*(-?\d+(?:\.\d+)?)$/

function matches(when, value) {
  const m = String(when).trim().match(RULE_RE)
  if (!m) {
    console.warn(`Unparseable health rule: "${when}"`)
    return false
  }
  const op = m[1]
  const n = Number(m[2])
  switch (op) {
    case '<':
      return value < n
    case '<=':
      return value <= n
    case '>':
      return value > n
    case '>=':
      return value >= n
    case '==':
      return value === n
    case '!=':
      return value !== n
    default:
      return false
  }
}

/**
 * Return the color of the first rule whose condition holds, or 'gray' when we
 * have no value yet (query failed / no data) — the "unknown" state.
 */
export function pickColor(rules, value) {
  if (value == null) return 'gray'
  for (const rule of rules || []) {
    if (matches(rule.when, value)) return rule.color
  }
  return 'gray'
}
