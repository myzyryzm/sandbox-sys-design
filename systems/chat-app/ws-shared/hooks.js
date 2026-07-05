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
import { MongoClient } from 'mongodb'

// --- notification-db sink (mongodb) ---------------------------------------
// The ws-server image now ships the `mongodb` driver (added to every
// ws-server-*/package.json). One lazily-established, pooled connection per
// server process, reused across hook fires; a failed connect nulls the promise
// so the next offline message retries. notification-db has no host port — it is
// reached over the docker network as `notification-db:27017` (db `notification_db`,
// collection `Notification`, all fields required + typed string by its validator).
const NOTIFICATION_DB_URL = process.env.NOTIFICATION_DB_URL || 'mongodb://notification-db:27017'
let mongoConnect = null
function notificationCollection() {
  if (!mongoConnect) {
    const client = new MongoClient(NOTIFICATION_DB_URL, { serverSelectionTimeoutMS: 5000 })
    mongoConnect = client.connect().catch((err) => {
      mongoConnect = null // let a later offline message retry the connection
      throw err
    })
  }
  return mongoConnect.then((c) => c.db('notification_db').collection('Notification'))
}

export default {
  // Entry (2026-07-05): if the target client (msg.to) is not online, persist the
  // undelivered message to notification-db. "Online" mirrors the base routing
  // check: connected to THIS server (localMap) or owned by some server per the
  // presence cache. Purely additive — base routing already dropped it.
  async onMessage(msg, ctx) {
    if (!msg || msg.to == null) return
    const online =
      ctx.localMap.has(msg.to) ||
      !!(await ctx.presence.get(`presence:${msg.to}`).catch(() => null))
    if (online) return
    const col = await notificationCollection()
    await col.insertOne({
      id: String(msg.msgId ?? crypto.randomUUID()),
      to: String(msg.to),
      from: String(msg.from ?? ''),
      message: typeof msg.body === 'string' ? msg.body : JSON.stringify(msg.body ?? ''),
      sentAt: String(msg.sentAt ?? Date.now()),
    })
  },
  async onSend(clientId, payload, ctx) {},
}
