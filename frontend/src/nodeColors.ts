// Shared source of truth for the user-configurable NODE colors — the color of a node's
// header strip and outline on the diagram. The Settings modal edits these; they persist to
// the repo-root settings.json via /api/settings alongside the prefix colors (see
// prefixColors.js) and load at app start.
//
// Most nodes are colored by their manifest `health` rules (green / yellow / red, gray when
// there's no value yet). The nginx load balancer has no health block — it has always
// rendered in that "unknown" gray — so its color is free to be a user choice. The default
// below is exactly that gray, so an unset setting renders identically to before.
//
// Precedence in SystemDiagram: an outage still wins (orange), and a load_balancer that DOES
// carry health rules keeps being health-colored; this only paints the otherwise-static node.
export const DEFAULT_NODE_COLORS: Record<string, string> = {
  load_balancer: '#9e9e9e', // must match COLOR_HEX.gray in SystemDiagram.jsx
}

// Human labels for the Settings UI: which node each role paints.
export const NODE_ROLE_LABELS: Record<string, string> = {
  load_balancer: 'nginx load balancer',
}
