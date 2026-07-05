// Shared websocket-server hooks — ONE file for every server in the tier, bind-mounted
// read-only at /app/shared/hooks.js (host: systems/<id>/ws-shared/hooks.js).
// Edit this file + `docker compose restart` the ws servers — no image rebuild.
//
// Contract (ADDITIVE ONLY — the base relay behavior in server.js is fixed):
//   onMessage(msg, ctx)            fires after a client frame is received, parsed, and
//                                  routed — alongside (never instead of) base routing.
//   onSend(clientId, payload, ctx) fires when a payload is delivered to a locally
//                                  connected client — alongside the actual send.
// Both may be async. They run fire-and-forget: errors are caught and logged by the
// server, and they can never block, veto, or reorder routing/delivery. Never touch
// the six ws_* metric names the manifest's PromQL reads (ws_connections,
// ws_messages_received_total, ws_messages_delivered_local_total,
// ws_messages_routed_remote_total, ws_messages_dropped_total, ws_delivery_seconds).
//
// ctx: {
//   serverId,     this relay's SERVER_ID
//   clientId,     onMessage only: the SENDER's clientId (onSend's first arg is the recipient)
//   localMap,     Map of clientId -> ws connection (this server only)
//   presence,     ioredis handle on the presence cache (`presence:<clientId>` -> serverId)
//   pub,          ioredis handle on the pub/sub bus (publish-capable connection)
//   deliverLocal, (clientId, payload) — the local delivery primitive
//   route,        (targetClientId, payload) — the local-vs-remote routing entry point
// }
export default {
  async onMessage(msg, ctx) {},
  async onSend(clientId, payload, ctx) {},
}
