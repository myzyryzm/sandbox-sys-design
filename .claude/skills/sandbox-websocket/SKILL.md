---
name: sandbox-websocket
description: >-
  Work on a WebSocket tier in a "Distributed Systems Sandbox" system (systems/<id>/) — an
  haproxy L4 (tcp) load balancer in front of N node.js `ws` relay servers, with a redis
  pub/sub bus for cross-server message routing and a redis presence cache mapping connected
  clients to their server, plus a host-run websocket client pool script. Use whenever the
  task is to implement one of the tier's SHARED server methods (the onMessage / onSend hooks
  in ws-shared/hooks.js, authored from description entries), customize a websocket server's
  behavior, change the lb algorithm or the server set, understand or drive the client pool
  (spawn N clients, send/receive, delivery stats), or wire the tier into end-to-end tests —
  it covers the tier's compose services, the websockets.json registry + generated
  haproxy.cfg, the shared-hooks contract, the ws-native Prometheus metrics the diagram
  depends on, the pool script's CLI + __WS_RESULTS__ contract, and the docker
  rebuild/verify steps.
---

# Working on a sandbox WebSocket tier

You are operating inside the "Distributed Systems Sandbox" web app. Each system lives in
`systems/<id>/`; your session's system prompt names the specific `<id>` and inlines its
current `manifest.json`. The web frontend runs under `npm run dev` and reads these files
live, so **never run `./start.sh`** — it tears down the dev server you're attached to.
Rebuild with `docker compose` directly (commands below).

A tier is provisioned in one shot by the web app's "Add WebSockets" button
(`POST /api/websockets`, `frontend/server/websockets.js`) — that part is mechanical and
already done by the time you're launched. Your work is what comes **after**: customizing
server behavior, resizing/re-balancing the tier, and driving/interpreting the client pool.
One tier per system today. Related: [[sandbox-end-to-end-process]] runs the pools inside
test processes; [[sandbox-database]] covers ordinary (non-tier) redis nodes.

## The tier's anatomy (working dir is the repo root)

For a tier named `ws` (the name prefixes every id):

1. **Compose services** in `systems/<id>/docker-compose.yml`:
   - `ws-lb` — `haproxy:3.0-alpine`, `mode tcp`, publishes host port **8090**, bind-mounts
     `./ws-lb/haproxy.cfg`. Its native prometheus exporter listens on `:8405` (scraped over
     the docker network; not published).
   - `ws-server-1..N` — built from `systems/<id>/ws-server-*/` (node:22-alpine; `ws` +
     `ioredis` + `prom-client`). All identity/wiring is compose **environment**: `SERVER_ID`,
     `WS_PORT` (8080), `METRICS_PORT` (9100), `BUS_REDIS_URL`, `PRESENCE_REDIS_URL`,
     `HEARTBEAT_MS`, `PRESENCE_TTL_S`.
   - `ws-bus`, `ws-presence` — `redis:7-alpine`, each with an `oliver006/redis_exporter`
     sidecar (`<name>-exporter`, port 9121). The bus carries `server:<SERVER_ID>` pub/sub
     channels; presence holds `presence:<clientId> -> <SERVER_ID>` keys (TTL 60s, refreshed
     on pong).
2. **`systems/<id>/ws-lb/websockets.json`** — the tier **registry**, the durable source of
   truth for the lb: `{ lb, algorithm, hostPort, wsPort, statsPort, metricsPort, servers[],
   bus, presence, client, methods }`. `haproxy.cfg` is **rendered from it** — if you change
   the algorithm or the server set, update the registry AND regenerate/edit the cfg to
   match, then `restart` the lb. `methods` holds the two shared server methods (see
   "Shared methods" below).
   **`systems/<id>/ws-shared/hooks.js`** — the tier's ONE shared hooks file, bind-mounted
   read-only into every server at `/app/shared/hooks.js` (compose volume
   `./ws-shared:/app/shared:ro`). Editing it needs only a `restart` of the servers — no
   image rebuild.
3. **Manifest nodes** (`origin: "create-websockets"`): the lb (`type: websocket-lb`,
   `wsRole: "lb"`), each server (`websocket-server`, `wsRole: "server"`), both redis
   (`wsRole: "bus"` / `"presence"`), and the client (`type: "client"`, `external: true`,
   `wsRole: "client"`). Every non-lb node carries `wsTier: "<lb-id>"`. **The tier is one
   deletion unit**: deleting the lb cascades the whole tier (servers, redis, client, pool
   script + stats file), and every non-lb member is individually delete-BLOCKED — the app's
   Delete tabs (and `POST /api/delete` / `DELETE /api/clients`) all point at the lb instead.
4. **Prometheus scrape jobs** — one per lb/server/redis (`prometheus/prometheus.yml`).
5. **The client pool script** `systems/<id>/ws-clients/<name>-client.mjs` — host-run
   (node ≥ 22, zero npm deps, built-in `WebSocket`), connects to `ws://localhost:8090`.
   On every run its `finish()` also writes `ws-clients/<name>-client.stats.json`
   (`{ ts, args, results }`, results = the `__WS_RESULTS__` shape) next to itself —
   script-relative, so any driver leaves stats the UI displays on the client node.

## The message contract

Every frame is `{ msgId, from, to, body, sentAt }`. A server routes on `to`: locally
connected → deliver; otherwise look up `presence:<to>` and publish to `server:<theirServer>`
on the bus; unknown/offline → **dropped** (barebones by design). Receivers dedupe by `msgId`.

## Shared methods (onMessage / onSend hooks)

Every server in the tier runs two SHARED methods from the one mounted
`systems/<id>/ws-shared/hooks.js` (ESM, `export default { onMessage, onSend }`):

- `onMessage(msg, ctx)` — fires after a client frame is received, parsed, and routed.
- `onSend(clientId, payload, ctx)` — fires when a payload is delivered to a locally
  connected client (locally-routed AND bus-arriving frames both funnel through here).

The **base implementation is fixed** (the routing/delivery path in `server.js` — never
modify it for a shared-method task). Hooks are **additive, fire-and-forget side effects**:
server.js catches and logs their errors, and they must never block, veto, or reorder base
routing/delivery, and never touch the six `ws_*` metric names (listed under "Customizing a
server"). `ctx` is `{ serverId, clientId (onMessage only: the SENDER), localMap
(clientId → ws, this server only), presence (ioredis, `presence:<clientId>` keys), pub
(ioredis, publish-capable bus handle), deliverLocal(clientId, payload),
route(targetClientId, payload) }`.

The registry's `methods.onMessage` / `methods.onSend` each hold `{ base, entries[],
implemented, conversationId, updatedAt }`. `base` is the immutable description of the
built-in behavior; `entries[]` (`{ at, text }`) is the append-only list of added behaviors —
your job is to make the hook body implement **all** entries, in order. The app writes the
entry and sets `implemented: false` before launching you; **you own the flip back**: after
editing hooks.js, restarting, and verifying, set `"implemented": true` on that method in
`systems/<id>/<lb>/websockets.json`.

Reload (no image rebuild — the hooks file is a mounted directory):

```bash
docker compose -f systems/<id>/docker-compose.yml restart ws-server-1 ws-server-2
```

Caveats:
- **New npm dependency?** The image only ships `ws` + `ioredis` + `prom-client`. Prefer
  dep-free code, node's built-in `fetch` (e.g. to call a service through the nginx lb at
  `http://lb/<service>/...`), or the provided `presence`/`pub` redis handles. If a package
  is truly needed, add it to **each** `systems/<id>/ws-server-*/package.json` and
  `up -d --build` all servers.
- **Tier predates shared methods?** If a server's `server.js` has no hooks loader (grep for
  `fireHook`), copy the loader + the two `fireHook(...)` call sites from
  `frontend/server/templates/websocket/server/server.js` into every clone, and use
  `up -d --build` instead of `restart` so the just-added `./ws-shared:/app/shared:ro`
  volume mounts (the app backfills the compose volumes + ws-shared/hooks.js when the first
  entry is saved).
- A broken hooks.js can't take a relay down (the import is guarded), but it silently
  disables ALL hooks — check `docker compose … logs ws-server-1 | grep hooks:` after every
  reload.

Verify a shared-method change: run the pool (below) and confirm **delivered ≈ sent still
holds** (a hook must not regress base delivery), confirm the new side effect happened (its
target table/topic/log), and check the server logs are free of `hooks.` errors.

## Customizing a server

For behavior that should stay **per-server** (deliberate divergence — e.g. one slow
replica), edit that one `systems/<id>/ws-server-<n>/server.js` and rebuild **only that
service** (for tier-wide behavior use the shared hooks above instead — one file, no
rebuild):

```bash
docker compose -f systems/<id>/docker-compose.yml up -d --build ws-server-1
```

**Keep the metric names** — the manifest node's PromQL reads exactly: `ws_connections`
(gauge), `ws_messages_received_total`, `ws_messages_delivered_local_total`,
`ws_messages_routed_remote_total`, `ws_messages_dropped_total` (counters),
`ws_delivery_seconds` (histogram). Add new metrics freely (and, if worth showing, append a
`{ label, query, unit }` row to that server's manifest `metrics[]` — live, no rebuild); never
rename the existing ones without updating the manifest queries in the same change.

## Changing the lb algorithm or server set

- **Algorithm** (`leastconn` | `roundrobin` | `source`): set it in `websockets.json`, change
  the `balance <algo>` line in `ws-lb/haproxy.cfg`, then
  `docker compose -f systems/<id>/docker-compose.yml restart ws-lb` (the cfg is a bind mount).
- **Removing a server**: the app no longer deletes individual tier members (every non-lb
  node's delete is blocked — the whole tier goes away via the lb's Delete tab). Shrinking
  the server set is a **hand-only** procedure: registry `servers[]`, the cfg's `server`
  line, the compose service + its folder, the scrape job, the manifest node/edges — then
  `up -d --remove-orphans`, `restart ws-lb`, `restart prometheus`.
- **Adding a server**: clone an existing `ws-server-*` folder, add the compose service (same
  env shape, unique `SERVER_ID`), scrape job, manifest node (+ `ws-lb → ws-server-N`,
  `→ ws-bus`, `→ ws-presence` edges), append to registry `servers[]` + a cfg `server` line;
  build it, `up -d`, `restart ws-lb`, `restart prometheus`.

## The client pool script

```bash
node systems/<id>/ws-clients/<name>-client.mjs --count 5 --duration 10 --rate 2
```

Spawns `--count` clients (`<name>-client-1..N`), waits up to 10s for connects, then each
connected client sends `--rate` msgs/s to random peers for `--duration` seconds, drains 2s,
and prints ONE machine-readable line (parse the **last** such line):

```
__WS_RESULTS__ {"spawned":5,"connected":5,"sent":100,"delivered":100,"duplicates":0,
                "errors":0,"latencyMs":{"p50":4,"p95":11,"max":32}}
```

The same run is exposed at `POST /api/websockets/run {system, client, count,
durationSeconds, rate}` (bounds 200 / 120s / 20). Each pool client is one host fd — for
pools near the 200 cap run `ulimit -n 4096` in the spawning shell first. In end-to-end
processes the pool rides `websocket_list` rows (`{ client, clientCount, messagesPerSecond }`)
— see [[sandbox-end-to-end-process]].

`GET /api/websockets?system=<id>` returns `{ ok, tier, stats, clientMethods }`: the registry,
the stats-file contents (or null before the first run), and the descriptor of the client's
two **built-in methods** — `spawnAndSend(count=5, durationSeconds=10, rate=1)` (the pool run)
and `onReceive(message)` (the dedupe-by-msgId / latency-measuring receive handler). The UI
renders them read-only on the client node and its Functions tab: they are not editable, not
deletable, and only end-to-end processes invoke them.

## Verify

```bash
# all tier containers up (lb, servers, 2 redis, 2 exporters)
docker compose -f systems/<id>/docker-compose.yml ps
# haproxy config is valid (after any cfg edit, BEFORE restarting)
docker compose -f systems/<id>/docker-compose.yml exec -T ws-lb haproxy -c -f /usr/local/etc/haproxy/haproxy.cfg
# per-server sessions visible to the lb's exporter (8405 isn't published — exec inside the net)
docker compose -f systems/<id>/docker-compose.yml exec -T prometheus wget -qO- http://ws-lb:8405/metrics | grep haproxy_server_current_sessions
# a server's own metrics
docker compose -f systems/<id>/docker-compose.yml exec -T prometheus wget -qO- http://ws-server-1:9100/metrics | grep ws_connections
# every tier scrape target is up
curl -s 'http://localhost:9090/api/v1/targets' | python3 -c "import json,sys;[print(t['labels']['job'],t['health']) for t in json.load(sys.stdin)['data']['activeTargets']]"
# a pool run round-trips: delivered ≈ sent, duplicates = 0
node systems/<id>/ws-clients/<name>-client.mjs --count 5 --duration 10 --rate 2
# …and left its report on disk for the UI (ts + args + results)
cat systems/<id>/ws-clients/<name>-client.stats.json
# cross-server routing actually happens (needs ≥2 servers + a multi-client pool)
curl -s 'http://localhost:9090/api/v1/query?query=sum(rate(ws_messages_routed_remote_total%5B1m%5D))'
```

Watch the diagram: the lb shows total sessions + servers-up, each server shows its own
`ws conns` AND the lb's per-server session count (so you can see leastconn balance), and the
two redis nodes tick `ops/s` while a pool runs.
